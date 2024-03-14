use anyhow::{Ok, Result};
use bytes::Bytes;
use std::{error::Error, sync::Arc};
use tokio::{
    self,
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpListener,
};
use Key_Value_Database::ServerConfig;
use Key_Value_Database::{CommandRequest, Map};

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let server_conf = ServerConfig::load("conf/server.conf")?;
    let addr = server_conf.server.server_address;
    let socket = TcpListener::bind(addr).await?;
    let db = Arc::new(Map::<String, Bytes>::new());
    println!("Listening... ");
    loop {
        let (mut socket, _) = socket.accept().await?;
        tokio::spawn(async move {
            let mut buf = vec![0u8; 1024];
            loop {
                let size = socket
                    .read(&mut buf)
                    .await
                    .expect("socket reading data fail");
                if size == 0 {
                    break;
                }
                println!(
                    "received message : {}",
                    String::from_utf8(buf.clone()).unwrap()
                );
                let cmd = "Get".to_string();
                let msg = Message { command: cmd };
                let responce_json = serde_json::to_string(&msg).unwrap();
                socket
                    .write_all(responce_json.as_bytes())
                    .await
                    .expect("wirte to socket fail");
            }
        });
    }
}

async fn process(
    req: CommandRequest,
    storage: &Map<String, Bytes>,
) -> Result<CommandResponse, Box<dyn Error>> {
    match req {
        CommandRequest::Get => {}
        CommandRequest::Set => {}
        CommandRequest::Publish => {}
        CommandRequest::Subscribe => {}
    }
    Ok(())
}
