use std::{collections::HashMap, future::Future, sync::Arc};

use log::{error, info};
use tokio::{
    io::{AsyncBufReadExt, BufReader, Lines, Stdin},
    sync::{mpsc, oneshot},
};

use crate::{
    handling,
    request::{self, Request},
    Error, Message, RegisterCallbackSender,
};

pub struct Server<T> {
    handler: Arc<T>,
    request_shared_state: Arc<request::Shared>,
    /// the hashmap of message ids that expect replies to a
    /// `oneshot::Sender<_>` that will get called when a response comes in
    handlers: HashMap<u32, oneshot::Sender<Message>>,
    /// each line in stdin is a new JSON message
    input_stream: Lines<BufReader<Stdin>>,

    /// channel that receives message ids,
    /// for which to listen to replies to and
    /// send the messages back to a waiting task
    register_callback_rx: mpsc::Receiver<(u32, oneshot::Sender<Message>)>,
}

impl<T> Server<T>
where
    T: Handler,
{
    pub async fn new(handler: T) -> Result<Self, Error> {
        // create task that receives outgoing JSON message and writes it to stdout
        // also create the channel to send tasks to it
        let (reply_tx, reply_rx) = mpsc::channel(10);
        tokio::spawn(handling::sender_task(reply_rx));

        let mut lines = BufReader::new(tokio::io::stdin()).lines();

        // channel to register a task waiting for a response
        let (rpc_tx, rpc_rx): (RegisterCallbackSender, _) = mpsc::channel(10);

        // the first message is the `init` message
        // => https://github.com/jepsen-io/maelstrom/blob/main/doc/protocol.md
        // we handle that specially so the data we receive in it can be made available in Requests
        let init_msg = lines.next_line().await;
        let (this_node, other_nodes) = handling::handle_init_message(&reply_tx, init_msg).await?;
        info!("other_nodes={other_nodes:?}");
        Ok(Self {
            request_shared_state: Arc::new(request::Shared::new(
                reply_tx,
                rpc_tx,
                this_node,
                other_nodes.into_boxed_slice(),
            )),
            handler: Arc::new(handler),
            handlers: HashMap::new(),
            input_stream: lines,
            register_callback_rx: rpc_rx,
        })
    }

    fn new_request(&self, msg: Message) -> Request {
        Request::new(self.request_shared_state.clone(), msg)
    }

    pub async fn run(&mut self) -> Result<(), Error> {
        loop {
            // wait for each message to come in
            // also add any incoming rpc callbacks
            let next_line: Option<String> = tokio::select! {
                Some((id, callback)) = self.register_callback_rx.recv() => {
                    self.handlers.insert(id, callback);
                    None
                },

                next_line = self.input_stream.next_line() => {
                    next_line.unwrap_or_else(|e| {
                            error!("failed to read next message due to {e}");
                            None
                        })
                }
            };

            if let Some(s) = next_line {
                match self.handle_message(&s) {
                    Ok(()) => {}
                    Err(e) => {
                        error!("Error while dispatching message: {e:?}");
                    }
                }
            }
        }
    }

    fn handle_message(&mut self, json_line: &str) -> Result<(), Error> {
        // parse the message
        let msg: Message = match serde_json::from_str(json_line) {
            Ok(o) => o,
            Err(e) => {
                error!("Could not parse line! err: {e}");
                return Err(Error::SerializeFailed);
            }
        };

        if let Some(reply_to) = msg.body.in_reply_to {
            // this is a RPC message
            // search for the corresponding handler
            match self.handlers.remove(&reply_to) {
                Some(reply) => match reply.send(msg) {
                    // the message is sent and the task can continue
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
            let req = self.new_request(msg);

            // finally, spawn a task to handle the message
            // TODO: proper error reporting
            let handler = Arc::clone(&self.handler);
            tokio::spawn(handler.handle(req));
        }

        Ok(())
    }
}

pub trait Handler {
    fn handle(
        self: Arc<Self>,
        req: Request,
    ) -> impl Future<Output = Result<(), Error>> + Send + 'static;
}
