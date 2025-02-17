use log::LevelFilter;
use maelrs::{Error, Server};

#[tokio::main]
async fn main() -> Result<(), Error> {
    // setup logging to stderr
    env_logger::builder()
        .filter_level(LevelFilter::max())
        .init();

    let mut server = Server::new().await?;

    server.run().await
}
