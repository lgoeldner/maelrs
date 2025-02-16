use std::sync::Arc;

use log::error;
use serde::{Deserialize, Serialize};
use tokio::sync::mpsc;

#[derive(Debug)]
pub enum Error {
    CantHandleMsg,
    SendFailed,
    SerializeFailed,
    InitFailed,
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
#[serde(tag = "type", rename = "snake_case")]
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
}

/// gets created for every incoming message and passed to the handler function
/// abstraction for shared state and the RPC protocol
#[derive(Debug)]
pub struct Request {
    send: mpsc::Sender<String>,
    message: Message,
}

impl Request {
    pub fn new(send: mpsc::Sender<String>, message: Message) -> Self {
        Self { send, message }
    }

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
}

pub mod msg_id {
    use std::sync::atomic::{AtomicU32, Ordering};

    static LAST_ID: AtomicU32 = AtomicU32::new(0);

    pub fn create_unique() -> u32 {
        LAST_ID.fetch_add(1, Ordering::AcqRel)
    }
}
