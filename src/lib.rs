use address::Address;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use tokio::sync::{mpsc, oneshot};

pub mod address;
mod handling;
mod msg_id;
pub mod request;
pub mod server;

type RegisterCallbackSender = mpsc::Sender<(u32, oneshot::Sender<Message>)>;

#[derive(Debug)]
pub enum Error {
    CannotHandle,
    SendFailed,
    SerializeFailed,
    InitFailed,
    TimeError,
    RPCFailed,
    RPCTimeout,
    NoRPCHandler,
    TopologyExists,
    ParseError,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Message {
    src: address::Address,
    dest: address::Address,
    body: Body,
}

impl Message {
    pub fn payload(&self) -> &Payload {
        &self.body.payload
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Body {
    msg_id: u32,
    in_reply_to: Option<u32>,
    #[serde(flatten)]
    payload: Payload,
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

    Init {
        node_id: Address,
        node_ids: Vec<Address>,
    },
    InitOk,

    Generate,
    GenerateOk {
        id: u128,
    },

    Broadcast {
        message: u32,
    },
    BroadcastOk,

    Read,
    ReadOk {
        messages: Vec<u32>,
    },

    Topology {
        topology: HashMap<Address, Vec<Address>>,
    },
    TopologyOk,

    Add {
        delta: u32,
    },
    AddOk,
}
