use log::{error, LevelFilter};
use maelrs::{
    request::Request,
    server::{Handler, Server},
    Error, Payload,
};
use rand::RngCore;
use std::{sync::Arc, time::SystemTime};

#[tokio::main]
async fn main() -> Result<(), Error> {
    // setup logging to stderr
    env_logger::builder()
        .filter_level(LevelFilter::max())
        .init();

    let mut server = Server::new(Guid).await?;
    server.run().await
}

struct Guid;

impl Handler for Guid {
    async fn handle(self: Arc<Self>, req: Request) -> Result<(), Error> {
        match req.message_payload() {
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
            _ => Err(Error::CannotHandle),
        }
    }
}
