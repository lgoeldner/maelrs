use env_logger::Target;
use log::{error, info, warn, LevelFilter};
use maelrs::{Body, Error, Message, Payload, RegisterCallbackSender, Request};
use rand::RngCore;
use std::collections::HashMap;
use std::io;
use std::time::SystemTime;
use tokio::sync::mpsc::Sender;
use tokio::sync::oneshot;
use tokio::{
    io::{AsyncBufReadExt, AsyncWriteExt, BufReader},
    sync::mpsc::{self, Receiver},
};

#[tokio::main]
async fn main() -> Result<(), Error> {
    eprintln!("startup");
    // setup logging to stderr
    env_logger::builder()
        .filter_level(LevelFilter::max())
        .init();

    warn!("start");

    // create task that receives JSON message and writes it to stdout
    // also channel that gets passed to every
    let (reply_tx, reply_rx) = mpsc::channel(10);
    tokio::spawn(sender_task(reply_rx));

    // each line in stdin is a new JSON message
    let mut lines = BufReader::new(tokio::io::stdin()).lines();

    // the dictionary of messages that expect replies to a
    // `oneshot::Sender<_>` that will get called when a response comes in
    let mut waiting_for_response: HashMap<u32, oneshot::Sender<Message>> =
        HashMap::with_capacity(1024);
    // channel to register a task waiting for a response
    let (rpc_tx, mut rpc_rx): (RegisterCallbackSender, _) = mpsc::channel(10);

    // the first message is the `init` message
    // => https://github.com/jepsen-io/maelstrom/blob/main/doc/protocol.md
    // we handle that specially so the data we receive in it can be made available in Requests
    let init_msg = lines.next_line().await;
    let (this_node, _other_nodes) = handle_init_message(&reply_tx, init_msg, &rpc_tx).await?;

    info!("init complete");

    loop {
        // wait for each message to come in
        // also add any incoming rpc callbacks
        let next_line = tokio::select! {
            Some((id, callback)) = rpc_rx.recv() => {
                info!("received callback waiting on {id}");
                waiting_for_response.insert(id, callback);
                None
            },

            next_line = lines.next_line() => {
                next_line.unwrap_or_else(|e| {
                        error!("failed to read next message due to {e}");
                        None
                    })
            }
        };

        if let Some(s) = next_line {
            match dispatch_message(
                reply_tx.clone(),
                rpc_tx.clone(),
                this_node,
                &s,
                &mut waiting_for_response,
            ) {
                Ok(()) => {}
                Err(e) => {
                    error!("Error while dispatching message: {e:?}");
                }
            }
        }
    }
}

fn dispatch_message(
    tx: Sender<String>,
    rpc_tx: RegisterCallbackSender,
    this_node: u32,
    next_line: &String,
    handlers: &mut HashMap<u32, oneshot::Sender<Message>>,
) -> Result<(), Error> {
    // parse the message
    let msg: Message = match serde_json::from_str(&next_line) {
        Ok(o) => o,
        Err(e) => {
            error!("Could not parse line! err: {e}");
            return Err(Error::SerializeFailed);
        }
    };

    if let Some(reply_to) = msg.body.in_reply_to {
        // this is a RPC message
        // search for the corresponding handler
        match handlers.remove(&reply_to) {
            Some(reply) => match reply.send(msg) {
                Ok(_) => {}
                Err(msg) => {
                    error!("RPC failed, msg: {msg:?}");
                    return Err(Error::RPCFailed);
                }
            },
            None => {
                error!("no handler for reply to {reply_to}");
                return Err(Error::NoRPCHandler);
            }
        }
    } else {
        // and create the request object
        let req = Request::new(tx.clone(), msg, this_node, rpc_tx.clone());

        // finally, spawn a task to handle the message
        // TODO: proper error reporting
        tokio::spawn(handle_msg(req));
    }

    Ok(())
}

async fn handle_init_message(
    reply_channel: &Sender<String>,
    init_msg: io::Result<Option<String>>,
    rpc_channel: &RegisterCallbackSender,
) -> Result<(u32, Vec<String>), Error> {
    match init_msg {
        Ok(Some(json)) => {
            let msg = serde_json::from_str::<Message>(&json);
            match msg {
                Err(e) => {
                    error!("failed to parse init JSON {json} due to {e}");
                    Err(Error::InitFailed)
                }
                Ok(msg) => {
                    match &msg.body.payload {
                        Payload::Init { node_id, node_ids } => {
                            // extract the number part and the "n" descriptor from the node id
                            let Some(("n", to_parse)) = node_id.split_at_checked(1) else {
                                error!("failed to parse self node id from {node_id}");
                                return Err(Error::InitFailed);
                            };

                            // then parse into an int
                            let this_id: u32 = to_parse.parse().map_err(|e| {
                                error!("failed to parse node id: {e:?}");
                                Error::InitFailed
                            })?;

                            // copy the needed data
                            let ret = (this_id, node_ids.clone());
                            // make a request so its easier to reply
                            let req = Request::new(
                                reply_channel.clone(),
                                msg,
                                this_id,
                                rpc_channel.clone(),
                            );
                            req.reply(Payload::InitOk).await?;

                            info!("init is ok!");
                            Ok(ret)
                        }
                        _ => {
                            error!("first message was not the `init` message");
                            Err(Error::InitFailed)
                        }
                    }
                }
            }
        }
        Ok(None) => {
            error!("failed to receive init message");
            Err(Error::InitFailed)
        }
        Err(e) => {
            error!("failed to receive init message due to {e}");
            Err(Error::InitFailed)
        }
    }
}

async fn handle_msg(req: Request) -> Result<(), Error> {
    match req.message_payload() {
        // just echo the same message back
        Payload::Echo { echo } => req.reply(Payload::EchoOk { echo: echo.clone() }).await,
        Payload::Generate => {
            // get the millis since the epoch
            let since_epoch = match SystemTime::now().duration_since(SystemTime::UNIX_EPOCH) {
                Ok(o) => o,
                Err(_) => {
                    error!("system time is set before the Unix Epoch");
                    return Err(Error::TimeError);
                }
            };

            let millis = since_epoch.as_millis();

            // combine the millis with a random number
            let random_num = rand::rng().next_u32();
            let guid = (millis << 16) ^ random_num as u128;

            req.reply(Payload::GenerateOk { id: guid }).await
        }

        Payload::Init { .. } => {
            // is handled at setup
            panic!("init message in `handle_msg`");
        }
        _ => {
            error!("cant handle payload {req:?}!");
            Err(Error::CantHandleMsg)
        }
    }
}

async fn sender_task(mut rx: Receiver<String>) -> Result<(), Error> {
    info!("started sender task!");
    let mut stdout = tokio::io::stdout();
    while let Some(to_send) = rx.recv().await {
        info!("to send: {to_send}");
        // first, write all the json data
        let res = stdout.write_all(to_send.as_bytes()).await;

        // then write a newline to finish the message and flush the buffer
        let newline_res = stdout.write_u8(b'\n').await;
        info!("sent!");
        if let Err(e) = res {
            error!("failed to write message {to_send} to stdout due to {e}")
        }

        if let Err(e) = newline_res {
            error!("failed to finish message {to_send} to stdout due to {e}")
        }
    }
    Ok(())
}
