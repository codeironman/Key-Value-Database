use std::{error::Error, fs};

use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize)]
pub struct ServerConfig {
    pub server: ListenAddress,
    pub rocksdb_path : RocksdbPath,
    pub connects : Connect,

}

#[derive(Debug, Serialize, Deserialize)]
pub struct ListenAddress {
    pub server_address: String,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ClientConfig {
    pub client: ConnectAddress,
}
#[derive(Debug, Serialize, Deserialize)]
pub struct ConnectAddress {
    pub client_address: String,
}

impl ServerConfig {
    pub fn load(path: &str) -> Result<Self, Box<dyn Error>> {
        let config = fs::read_to_string(path)?;
        let server_config: Self = toml::from_str(&config)?;
        Ok(server_config)
    }
}
impl ClientConfig {
    pub fn load(path: &str) -> Result<Self, Box<dyn Error>> {
        let config = fs::read_to_string(path)?;
        let client_config: Self = toml::from_str(&config)?;
        Ok(client_config)
    }
}



//持久化存储路径
#[derive(Debug,Deserialize,Serialize)]
pub struct RocksdbPath{
    pub path : String,
}
#[derive(Debug,Deserialize,Serialize)]
pub struct Connect {
    pub max_connect : usize
}
