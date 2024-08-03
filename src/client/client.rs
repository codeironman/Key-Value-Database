use crate::{
    cmd::{CmdRequest, CmdResponse},
    ClientArgs,
};
use anyhow::Result;
use bytes::BytesMut;
use clap::Parser;
use futures::SinkExt;
use prost::Message;
use tokio::net::TcpStream;
use tokio_stream::StreamExt;
use tokio_util::codec::{Framed, LengthDelimitedCodec};
use tracing::info;

pub struct Client;

impl Client {
    pub async fn run(addr: String) -> Result<()> {
        let stream = TcpStream::connect(addr).await?;
        let mut stream = Framed::new(stream, LengthDelimitedCodec::new());
        let mut buf = BytesMut::new();

        let client_args = ClientArgs::parse();
        let cmd = process_args(client_args).await?;
        cmd.encode(&mut buf).unwrap();
        stream.send(buf.freeze()).await.unwrap();
        info!("Send command successed！");

        while let Some(Ok(buf)) = stream.next().await {
            let cmd_res = CmdResponse::decode(&buf[..]).unwrap();
            info!("Receive a response: {:?}", cmd_res);
        }

        Ok(())
    }
}

async fn process_args(client_args: ClientArgs) -> Result<CmdRequest> {
    match client_args {
        ClientArgs::Get { key } => Ok(CmdRequest::get(key)),
        ClientArgs::Set { key, value } => Ok(CmdRequest::set(key, value)),
    }
}
