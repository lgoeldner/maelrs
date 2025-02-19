use crate::{address::Address, msg_id, Error, Message, Payload, Request};
use futures::FutureExt;
use log::{error, info};
use rand::RngCore;
use std::io;
use std::time::SystemTime;
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

                            // copy the needed data
                            let ret = (this_id, node_ids.clone());
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

pub async fn handle_msg(req: Request) -> Result<(), Error> {
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

        Payload::Broadcast { message } => {
            req.reply(Payload::BroadcastOk).await?;

            let is_known_message = req.add_broadcast_message(*message).await;
            if !is_known_message {
                // gossip the message to other nodes
                // this is n^2 technically but the clusters are often very small
                let tasks = req.adjacent_nodes().unwrap_or(&[]).iter().map(|&id| {
                    req.send_rpc_to(
                        Address::Node { id },
                        Payload::Broadcast { message: *message },
                    )
                    .then(move |res| async move {
                        match res {
                            Ok(resp) => Ok(resp),
                            Err(e) => {
                                error!("failed to send RPC to {id} due to {e:?}");
                                Err(Address::Node { id })
                            }
                        }
                    })
                });

                let res = futures::future::join_all(tasks).await;
                for r in res {
                    match r {
                        Ok(resp) => match resp.body.payload {
                            Payload::BroadcastOk => {}
                            _ => error!("Wrong broadcast response type!"),
                        },
                        Err(e) => {
                            error!("failed to send message to {e}");
                            // spawn a task that will keep to retry sending that message
                            let req = req.clone();
                            let message = *message;
                            tokio::spawn(async move {
                                // try to resend, wait until succeeded
                                while let Err(_) =
                                    req.send_rpc_to(e, Payload::Broadcast { message }).await
                                {
                                    error!("failed to resend {message}");
                                }
                            });
                        }
                    }
                }
            }

            Ok(())
        }

        Payload::Read => {
            let messages = req.get_broadcast_messages().await;
            req.reply(Payload::ReadOk { messages }).await
        }

        Payload::Topology { topology } => {
            req.add_topology(topology.clone())?;
            req.reply(Payload::TopologyOk).await
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
