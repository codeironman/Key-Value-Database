use anyhow::Result;
use serde_json;
use std::error::Error;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
};
use Key_Value_Database::ClientConfig;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let client_conf = ClientConfig::load("conf/client.conf")?;
    let addr = client_conf.client.client_address;
    let mut socket = TcpStream::connect(addr).await?;
    let cmd = "Set".to_string();
    let msg = Message { command: cmd };
    let request_json = serde_json::to_string(&msg).unwrap();
    let size = socket.write(request_json.as_bytes()).await?;
    println!("send info successed !,size = {}", size);

    let mut buf = vec![0u8; 1024];
    let size = socket
        .read(&mut buf)
        .await
        .expect("from socket read data fail!");
    println!(
        "Receive info :{}, size = {} ",
        String::from_utf8(buf)?,
        size
    );
    Ok(())
}
