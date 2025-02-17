use request::Request;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use tokio::sync::{mpsc, oneshot};

pub use server::Server;

mod handling;
mod msg_id;
mod request;
mod server;

type RegisterCallbackSender = mpsc::Sender<(u32, oneshot::Sender<Message>)>;

#[derive(Debug)]
pub enum Error {
    CantHandleMsg,
    SendFailed,
    SerializeFailed,
    InitFailed,
    TimeError,
    RPCFailed,
    RPCTimeout,
    NoRPCHandler,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
struct Message {
    src: Arc<str>,
    dest: Arc<str>,
    body: Body,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
struct Body {
    msg_id: u32,
    in_reply_to: Option<u32>,
    #[serde(flatten)]
    payload: Payload,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(tag = "type", rename_all = "snake_case")]
enum Payload {
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
    Broadcast {
        message: u32,
    },
    BroadcastOk,
}
