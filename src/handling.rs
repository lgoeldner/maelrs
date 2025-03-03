use crate::{address::Address, msg_id, Error, Message, Payload};
use log::{error, info};
use std::io;
use tokio::{
    io::AsyncWriteExt,
    sync::mpsc::{Receiver, Sender},
};

pub async fn sender_task(mut rx: Receiver<String>) -> Result<(), Error> {
    info!("started sender task!");
    let mut stdout = tokio::io::stdout();
    while let Some(to_send) = rx.recv().await {
        // first, write all the json data
        let res = stdout.write_all(to_send.as_bytes()).await;

        if let Err(e) = res {
            error!("failed to write message {to_send} to stdout due to {e}");
            continue;
        }

        // then write a newline to finish the message and flush the buffer
        let newline_res = stdout.write_u8(b'\n').await;
        if let Err(e) = newline_res {
            error!("failed to finish message {to_send} to stdout due to {e}")
        }
    }
    Ok(())
}

pub async fn handle_init_message(
    reply_channel: &Sender<String>,
    init_msg: io::Result<Option<String>>,
) -> Result<(u32, Vec<Address>), Error> {
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
                            let this_id = node_id.id();

                            let other_ids = node_ids
                                .iter()
                                .cloned()
                                .filter(|it| it != node_id)
                                .collect::<Vec<_>>();

                            // copy the needed data
                            let ret = (this_id, other_ids);
                            // and do the replying
                            // first, create the reply message
                            let reply = Message {
                                src: msg.dest,
                                dest: msg.src,
                                body: crate::Body {
                                    msg_id: msg_id::create_unique(),
                                    in_reply_to: Some(msg.body.msg_id),
                                    payload: Payload::InitOk,
                                },
                            };
                            let json_string = serde_json::to_string(&reply)
                                .map_err(|_| Error::SerializeFailed)?;
                            reply_channel.send(json_string).await.map_err(|e| {
                                error!("failed to send init reply: {e:?}");
                                Error::InitFailed
                            })?;

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
