use crate::{msg_id, Body, Error, Message, Payload};

use log::error;
use std::{collections::HashSet, sync::Arc, time::Duration};
use tokio::sync::{mpsc, oneshot, RwLock};

/// gets created for every incoming message and passed to the handler function
/// abstraction for shared state and the RPC protocol
#[derive(Debug)]
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
        }
    }
}

impl Request {
    pub(super) fn new(shared: Arc<Shared>, message: Message) -> Self {
        Self { message, shared }
    }

    pub fn message_payload(&self) -> &Payload {
        &self.message.body.payload
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

        // then wait for the response by registering a oneshot callback channel
        let (tx, rx) = oneshot::channel();
        self.shared
            .rpc_tx
            .send((self.message.body.msg_id, tx))
            .await
            .map_err(|e| {
                error!("RPC from msg {:?} failed due to {e:?}", self.message);
                Error::SendFailed
            })?;

        // wait for the response on `rx` or timeout after 60 seconds
        let res = tokio::time::timeout(Duration::from_secs(60), rx).await;

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
}
