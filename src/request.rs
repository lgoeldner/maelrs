use crate::{address::Address, msg_id, Body, Error, Message, Payload};

use log::error;
use std::{
    collections::{HashMap, HashSet},
    sync::{Arc, OnceLock},
    time::Duration,
};
use tokio::sync::{mpsc, oneshot, RwLock};

/// gets created for every incoming message and passed to the handler function
/// abstraction for shared state and the RPC protocol
#[derive(Debug, Clone)]
pub struct Request {
    message: Message,
    shared: Arc<Shared>,
}

#[derive(Debug)]
/// holds shared state in a request
pub(super) struct Shared {
    reply_tx: mpsc::Sender<String>,
    rpc_tx: crate::RegisterCallbackSender,
    this_node: u32,
    // TODO: maybe use std::sync::RwLock depending on how often this will block
    received_broadcasts: RwLock<HashSet<u32>>,
    // only available after `topology` has been received
    adjacent_nodes: OnceLock<Vec<u32>>,
}

impl Shared {
    pub(super) fn new(
        reply_tx: mpsc::Sender<String>,
        rpc_tx: crate::RegisterCallbackSender,
        this_node: u32,
    ) -> Self {
        Self {
            reply_tx,
            rpc_tx,
            this_node,
            received_broadcasts: RwLock::new(HashSet::new()),
            adjacent_nodes: OnceLock::new(),
        }
    }
}

const RPC_TIMEOUT: Duration = Duration::from_secs(10);

impl Request {
    pub(super) fn new(shared: Arc<Shared>, message: Message) -> Self {
        Self { message, shared }
    }

    pub fn message_payload(&self) -> &Payload {
        &self.message.body.payload
    }

    pub fn adjacent_nodes(&self) -> Option<&[u32]> {
        self.shared.adjacent_nodes.get().map(Vec::as_slice)
    }

    // Error if topology has already existed
    pub fn add_topology(&self, topology: HashMap<Address, Vec<Address>>) -> Result<(), Error> {
        if self.shared.adjacent_nodes.get().is_some() {
            error!("Tried to initialise topology twice");
            return Err(Error::TopologyExistedAlready);
        }

        let adj = &topology[&Address::Node {
            id: self.shared.this_node,
        }];

        let it: Vec<_> = adj
            .iter()
            .map(|it| match it {
                Address::Node { id } | Address::Client { id } => *id,
            })
            .collect();

        let Ok(()) = self.shared.adjacent_nodes.set(it) else {
            error!("Tried to initialise topology twice");
            return Err(Error::TopologyExistedAlready);
        };

        Ok(())
    }

    // Adds a received message to
    pub async fn add_broadcast_message(&self, message: u32) -> bool {
        let mut l = self.shared.received_broadcasts.write().await;
        // TODO: use hashmap?
        if !l.contains(&message) {
            l.insert(message);
            false
        } else {
            true
        }
    }

    /// copy the already received messages into a Vec
    pub async fn get_broadcast_messages(&self) -> Vec<u32> {
        let lock = self.shared.received_broadcasts.read().await;
        lock.iter().copied().collect()
    }

    /// send a message awaiting a response
    /// returns the response when it arrives
    /// timeout after 60 seconds
    pub async fn rpc(&self, payload: Payload) -> Result<Message, Error> {
        // first, send the reply
        self.reply(payload).await?;
        self.wait_for_rpc_response(self.message.body.msg_id).await
    }

    async fn wait_for_rpc_response(&self, msg_id: u32) -> Result<Message, Error> {
        // then wait for the response by registering a oneshot callback channel
        let (tx, rx) = oneshot::channel();
        self.shared.rpc_tx.send((msg_id, tx)).await.map_err(|e| {
            error!("RPC from msg {:?} failed due to {e:?}", self.message);
            Error::SendFailed
        })?;

        // wait for the response on `rx` or timeout after 60 seconds
        let res = tokio::time::timeout(RPC_TIMEOUT, rx).await;

        // handle errors
        match res {
            Err(e) => {
                error!("RPC timeout after {e:?}");
                Err(Error::RPCTimeout)
            }
            Ok(Err(e)) => {
                error!("awaiting RPC response failed due to {e:?}");
                Err(Error::RPCFailed)
            }
            Ok(Ok(o)) => Ok(o),
        }
    }

    /// reply to the message (`self.message()`)
    /// with custom payload
    /// handles everything else
    pub async fn reply(&self, payload: Payload) -> Result<(), Error> {
        // construct the message with correct src, destination and reply_to
        // also get a new message id
        let reply_to = self.message();
        let msg = Message {
            src: reply_to.dest.clone(),
            dest: reply_to.src.clone(),
            body: Body {
                in_reply_to: Some(reply_to.body.msg_id),
                msg_id: msg_id::create_unique(),
                payload,
            },
        };

        self.send(msg).await
    }

    /// send a finished message. used by `Self::reply` and `Self::rpc`
    async fn send(&self, msg: Message) -> Result<(), Error> {
        let json = match serde_json::to_string(&msg) {
            Ok(o) => o,
            Err(e) => {
                error!("failed to serialize {msg:?} due to {e}!");
                return Err(Error::SerializeFailed);
            }
        };

        self.shared.reply_tx.send(json).await.map_err(|e| {
            error!("failed to send {msg:?} to send task due to {e:?}!");
            Error::SendFailed
        })
    }

    pub fn message(&self) -> &Message {
        &self.message
    }

    pub async fn send_rpc_to(&self, dest: Address, payload: Payload) -> Result<Message, Error> {
        let msg_id = msg_id::create_unique();
        let msg = Message {
            dest,
            src: self.message.dest.clone(),
            body: Body {
                msg_id,
                in_reply_to: None,
                payload,
            },
        };

        self.send(msg).await?;
        self.wait_for_rpc_response(msg_id).await
    }
}

pub struct MessageFailed {}
