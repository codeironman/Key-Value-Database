use anyhow::{Ok, Result};
use bytes::Bytes;
use tracing::{error, info};
use std::{error::Error, future::Future, sync::Arc};
use tokio::{
    self,
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpListener, sync::{broadcast, mpsc},
};
use Key_Value_Database::{ServerConfig, StoreService};
#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let server_conf = ServerConfig::load("conf/server.conf")?;
    let db_path = server_conf.rocksdb_path.path;
    let addr = server_conf.server.server_address;
    let mex_conns = server_conf.connects.max_connect;
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

pub struct Server {
    listen_address : String,
    service : StoreService,
}

impl Server {
    pub fn new(addr : String, server : StoreService) -> Self {
        Self{
            listen_address : addr,
            service : server,
        }
    }
    pub async fn run(&self,shutdown : impl Future) -> Result<(),Box<dyn Error>> {
        let (notify_shutdown,_) = broadcast::channel(1);
        let (shutdown_complete_tx , mut shutdown_complete_rx) = mpsc::channel::<()>(1);
        tokio::select! {
            res = self.excute(&notify_shutdown,&shutdown_complete_tx) => {
                if let Err(e) = res {
                    error!(cause = %e,"failed to accept");
                }
            }
        }
        Ok(())
    }
    pub async fn excute(&self,notify_shutdown: &broadcast::Sender<()>,shutdown_complete_tx: &mpsc::Sender<()>) -> Result<(),Box<dyn Error>> {
        let socket = TcpListener::bind(&self.listen_address).await?;
        info!("Listening on {} ......",self.listen_address);
        loop {
            
        }
        Ok(())
    }
}
