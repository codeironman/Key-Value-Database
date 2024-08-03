use anyhow::Result;
use key_value_database::{client::client::Client, ClientConfig};
#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt::init();

    let client_conf = ClientConfig::load("config/client.conf")?;
    let connect_addr = client_conf.client.client_address;

    Client::run(connect_addr).await?;

    Ok(())
}
