use address::Address;
use core::fmt;
use request::Request;
use serde::{Deserialize, Serialize};
use std::{
    collections::HashMap,
    future::Future,
    pin::{pin, Pin},
    sync::Arc,
    task::Poll,
};
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
    TopologyExistedAlready,
    ParseError,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
struct Message {
    src: address::Address,
    dest: address::Address,
    body: Body,
}

mod address;

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
}
