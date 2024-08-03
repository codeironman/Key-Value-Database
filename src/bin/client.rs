use std::error::Error;

use anyhow::Result;
use bytes::BytesMut;
use clap::Parser;
use futures::{SinkExt, StreamExt};
use key_value_database::{
    cmd::{CmdRequest, CmdResponse},
    ClientArgs, ClientConfig,
};
use prost::Message;
use tokio::net::TcpStream;
use tokio_util::codec::{Framed, LengthDelimitedCodec};
use tracing::info;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let client_conf = ClientConfig::load("conf/client.conf")?;
    let addr = client_conf.client.client_address;
    let socket = TcpStream::connect(addr).await?;
    let mut stream = Framed::new(socket, LengthDelimitedCodec::new());
    let mut buf = BytesMut::new();
    let client_args = ClientArgs::parse();
    let cmd = process_args(client_args).await?;
    cmd.encode(&mut buf).unwrap();
    stream.send(buf.freeze()).await.unwrap();

    loop {
        tokio::select! {
            Some(Ok(buf)) = stream.next() => {
                let cmd_res = CmdResponse::decode(&buf[..]).unwrap();
                info!("Receive a response: {:?}", cmd_res);
            }
        }
    }
}

async fn process_args(client_args: ClientArgs) -> Result<CmdRequest, Box<dyn Error>> {
    match client_args {
        // 生成 GET 命令
        ClientArgs::Get { key } => Ok(CmdRequest::get(key)),
        // 生成 SET 命令
        ClientArgs::Set { key, value } => Ok(CmdRequest::set(key, value)),
    }
}
