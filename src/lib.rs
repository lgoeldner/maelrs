use std::sync::Arc;

use log::error;
use serde::{Deserialize, Serialize};
use tokio::sync::{mpsc, oneshot};

#[derive(Debug)]
pub enum Error {
    CantHandleMsg,
    SendFailed,
    SerializeFailed,
    InitFailed,
    TimeError,
    RPCFailed,
    NoRPCHandler,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Message {
    pub src: Arc<str>,
    pub dest: Arc<str>,
    pub body: Body,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Body {
    pub msg_id: u32,
    pub in_reply_to: Option<u32>,
    #[serde(flatten)]
    pub payload: Payload,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum Payload {
    Echo {
        echo: String,
    },
    EchoOk {
        echo: String,
    },
    InitOk,
    Init {
        node_id: String,
        node_ids: Vec<String>,
    },
    Generate,
    GenerateOk {
        id: u128,
    },
}

pub type RegisterCallbackSender = mpsc::Sender<(u32, oneshot::Sender<Message>)>;

/// gets created for every incoming message and passed to the handler function
/// abstraction for shared state and the RPC protocol
#[derive(Debug)]
pub struct Request {
    // when a message gets sent through this, the corresponding `oneshot::Receiver<_>`
    // can expect to be calles when a reply is available
    register_rpc_callback: RegisterCallbackSender,
    send: mpsc::Sender<String>,
    message: Message,
    this_node_id: u32,
}

impl Request {
    pub fn new(
        send: mpsc::Sender<String>,
        message: Message,
        this_node_id: u32,
        register_rpc_callback: RegisterCallbackSender,
    ) -> Self {
        Self {
            send,
            message,
            this_node_id,
            register_rpc_callback,
        }
    }

    /// send a message awaiting a response
    /// returns the response when it arrives
    pub async fn rpc(&self, payload: Payload) -> Result<Message, Error> {
        // first, send the reply
        self.reply(payload).await?;

        // then wait for the response by registering a oneshot callback channel
        let (tx, rx) = oneshot::channel();
        self.register_rpc_callback
            .send((self.message.body.msg_id, tx))
            .await
            .map_err(|e| {
                error!(
                "register rpc callback sender was dropped! RPC from msg {:?} failed due to {e:?}",
                self.message
            );
                Error::SendFailed
            })?;

        rx.await.map_err(|e| {
            error!("awaiting RPC response failed due to {e:?}");
            Error::RPCFailed
        })
    }

    /// reply to the message (`self.message()`)
    /// with custom payload
    /// handles everything else
    pub async fn reply(&self, payload: Payload) -> Result<(), Error> {
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

    async fn send(&self, msg: Message) -> Result<(), Error> {
        let json = match serde_json::to_string(&msg) {
            Ok(o) => o,
            Err(e) => {
                error!("failed to serialize {msg:?} due to {e}!");
                return Err(Error::SerializeFailed);
            }
        };

        self.send.send(json).await.map_err(|e| {
            error!("failed to send {msg:?} to send task due to {e:?}!");
            Error::SendFailed
        })
    }

    pub fn message(&self) -> &Message {
        &self.message
    }

    pub fn message_payload(&self) -> &Payload {
        &self.message.body.payload
    }

    pub fn this_node_id(&self) -> u32 {
        self.this_node_id
    }
}

pub mod msg_id {
    use std::sync::atomic::{AtomicU32, Ordering};

    static LAST_ID: AtomicU32 = AtomicU32::new(0);

    pub fn create_unique() -> u32 {
        LAST_ID.fetch_add(1, Ordering::AcqRel)
    }
}
