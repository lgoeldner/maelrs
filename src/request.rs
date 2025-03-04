use crate::{address::Address, msg_id, Body, Error, Message, Payload};

use log::error;
use std::{sync::Arc, time::Duration};
use tokio::sync::{mpsc, oneshot};

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
    other_nodes: Box<[Address]>,
}

impl Shared {
    pub(super) fn new(
        reply_tx: mpsc::Sender<String>,
        rpc_tx: crate::RegisterCallbackSender,
        this_node: u32,
        other_nodes: Box<[Address]>,
    ) -> Self {
        Self {
            reply_tx,
            rpc_tx,
            this_node,
            other_nodes,
        }
    }
}

const RPC_TIMEOUT: Duration = Duration::from_secs(10);

impl Request {
    pub(super) fn new(shared: Arc<Shared>, message: Message) -> Self {
        Self { message, shared }
    }

    #[inline]
    pub fn message_payload(&self) -> &Payload {
        &self.message.body.payload
    }

    pub fn this_node(&self) -> Address {
        Address::Node {
            id: self.shared.this_node,
        }
    }

    #[inline]
    pub fn other_nodes(&self) -> &[Address] {
        self.shared.other_nodes.as_ref()
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
            Err(_) => {
                error!("RPC timeout");
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

    pub async fn send_msg_to(&self, dest: Address, payload: Payload) -> Result<(), Error> {
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

        self.send(msg).await
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
