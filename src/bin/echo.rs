use std::sync::Arc;

use log::LevelFilter;
use maelrs::{
    request::Request,
    server::{Handler, Server},
    Error, Payload,
};

#[tokio::main]
async fn main() -> Result<(), Error> {
    // setup logging to stderr
    env_logger::builder()
        .filter_level(LevelFilter::max())
        .init();

    let mut server = Server::new(Echo).await?;
    server.run().await
}

struct Echo;

impl Handler for Echo {
    async fn handle(self: Arc<Self>, req: Request) -> Result<(), Error> {
        match req.message_payload() {
            Payload::Echo { echo } => req.reply(Payload::EchoOk { echo: echo.clone() }).await,
            _ => Err(Error::CannotHandle),
        }
    }
}
