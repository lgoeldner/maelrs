use futures::FutureExt;
use log::{error, info, LevelFilter};
use maelrs::{
    address::Address,
    request::Request,
    server::{Handler, Server},
    Error, Payload,
};
use std::{
    collections::HashSet,
    env::temp_dir,
    sync::{Arc, OnceLock},
};
use tokio::sync::RwLock;

#[tokio::main]
async fn main() -> Result<(), Error> {
    // setup logging to stderr
    env_logger::builder()
        .filter_level(LevelFilter::max())
        .init();

    let mut server = Server::new(Broadcast::default()).await?;
    server.run().await
}

#[derive(Default)]
struct Broadcast {
    received_broadcasts: RwLock<HashSet<u32>>,
    // only available after `topology` has been received
    adjacent_nodes: OnceLock<Box<[Address]>>,
}

impl Broadcast {
    async fn add_broadcast_message(&self, msg: u32) -> bool {
        let mut map = self.received_broadcasts.write().await;
        map.insert(msg)
    }
}

impl Handler for Broadcast {
    async fn handle(self: Arc<Self>, req: Request) -> Result<(), Error> {
        match req.message_payload() {
            Payload::Broadcast { message } => {
                req.reply(Payload::BroadcastOk).await?;

                let is_known_message = self.add_broadcast_message(*message).await;
                if !is_known_message {
                    // gossip the message to other adjacent nodes respecting the given topology
                    // this is n^2 technically but the clusters are small enough
                    let tasks = self
                        .adjacent_nodes
                        .get()
                        .map_or(&[] as &[Address], |it| it.as_ref())
                        .iter()
                        .map(|&addr| {
                            req.send_rpc_to(addr, Payload::Broadcast { message: *message })
                                .then(async move |res| match res {
                                    Ok(resp) => Ok(resp),
                                    Err(e) => {
                                        error!("failed to send RPC to {addr} due to {e:?}");
                                        Err(addr)
                                    }
                                })
                        });

                    let res = futures::future::join_all(tasks).await;
                    for r in res {
                        match r {
                            Ok(resp) => match resp.payload() {
                                Payload::BroadcastOk => {}
                                _ => error!("Wrong broadcast response type!"),
                            },
                            Err(e) => {
                                error!("failed to send message to {e}, spawning retry!");
                                // spawn a task that will keep to retry sending that message
                                let req = req.clone();
                                let message = *message;
                                tokio::spawn(async move {
                                    let mut n = 0;
                                    // try to resend, wait until succeeded
                                    while let Err(_) =
                                        req.send_rpc_to(e, Payload::Broadcast { message }).await
                                    {
                                        n += 1;
                                        error!("failed to resend {message}, n={n}");
                                    }

                                    info!("resend to {e} success after {} tries", n + 1);
                                });
                            }
                        }
                    }
                }

                Ok(())
            }
            Payload::Topology { topology } => {
                let adj_nodes = topology[&req.this_node()].clone().into_boxed_slice();

                let old = self.adjacent_nodes.set(adj_nodes);
                match old {
                    Ok(()) => {}
                    Err(_) => {
                        error!("Tried to initialise topology twice");
                        return Err(Error::TopologyExists);
                    }
                }

                Ok(())
            }
            _ => Err(Error::CannotHandle),
        }
    }
}
