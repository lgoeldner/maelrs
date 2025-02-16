use log::{error, info};
use maelrs::{Body, Error, Message, Payload, Request};
use std::io;
use tokio::sync::mpsc::Sender;
use tokio::{
    io::{AsyncBufReadExt, AsyncWriteExt, BufReader},
    sync::mpsc::{self, Receiver},
};

#[tokio::main]
async fn main() -> Result<(), Error> {
    // setup logging to stderr
    simple_logger::SimpleLogger::new().init().unwrap();
    // create task that receives JSON message and writes it to stdout
    // also channel that gets passed to every
    let (tx, rx) = mpsc::channel(10);
    tokio::spawn(sender_task(rx));

    // each line in stdin is a new JSON message
    let mut lines = BufReader::new(tokio::io::stdin()).lines();

    // the first message is the `init` message
    // => https://github.com/jepsen-io/maelstrom/blob/main/doc/protocol.md
    // we handle that specially so the data we receive in it can be made available in Requests
    let init_msg = lines.next_line().await;
    let (_this_node, _other_nodes) = handle_init_message(&tx, init_msg).await?;

    // wait for each message to come in
    while let Ok(Some(next_line)) = lines.next_line().await {
        // parse the message
        let msg: Message = match serde_json::from_str(&next_line) {
            Ok(o) => o,
            Err(e) => {
                error!("Could not parse line! err: {e}");
                continue;
            }
        };

        // and create the request object
        let req = Request::new(tx.clone(), msg);

        // finally, spawn a task to handle the message
        // TODO: proper error reporting
        let _ = tokio::spawn(handle_msg(req));
    }

    Ok(())
}

async fn handle_init_message(
    reply_channel: &Sender<String>,
    init_msg: io::Result<Option<String>>,
) -> Result<(String, Vec<String>), Error> {
    let res = match init_msg {
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
                            // copy the needed data
                            let ret = (node_id.clone(), node_ids.clone());
                            // make a request so its easier to reply
                            let req = Request::new(reply_channel.clone(), msg);
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
    };

    res
}

async fn handle_msg(req: Request) -> Result<(), Error> {
    match req.message_payload() {
        Payload::Echo { echo } => req.reply(Payload::EchoOk { echo: echo.clone() }).await,

        Payload::Init { .. } => {
            // is handled at setup
            panic!("init message in `handle_msg`");
        }
        _ => {
            error!("cant handle {req:?}!");
            Err(Error::CantHandleMsg)
        }
    }
}

async fn sender_task(mut rx: Receiver<String>) -> Result<(), Error> {
    let mut stdout = tokio::io::stdout();
    while let Some(to_send) = rx.recv().await {
        let res = stdout.write_all(to_send.as_bytes()).await;
        if let Err(e) = res {
            error!("failed to write message to stdout!")
        }
    }
    Ok(())
}
