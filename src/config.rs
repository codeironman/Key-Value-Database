use std::{error::Error, fs};

use anyhow::Result;
use serde::{Deserialize, Serialize};

use crate::compaction::tiered::CompactOptions;

#[derive(Debug, Serialize, Deserialize)]
pub struct ServerConfig {
    pub server: ListenAddress,
    pub database_path: String,
    pub connects: Connect,
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
    pub fn load(path: &str) -> Result<Self> {
        let config = fs::read_to_string(path)?;
        let server_config: Self = toml::from_str(&config)?;
        Ok(server_config)
    }
}
impl ClientConfig {
    pub fn load(path: &str) -> Result<Self> {
        let config = fs::read_to_string(path)?;
        let client_config: Self = toml::from_str(&config)?;
        Ok(client_config)
    }
}

//持久化存储路径
#[derive(Debug, Deserialize, Serialize)]
pub struct DataBasePath {
    pub path: String,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct Connect {
    pub max_connect: usize,
}

pub struct DBConfig {
    pub block_size: usize,
    pub sstable_flush_size: usize,
    pub num_memtable_limit: usize,
    pub compaction: CompactOptions,
    pub serializable: bool,
}

impl Default for DBConfig {
    fn default() -> Self {
        Self {
            block_size: 1024,
            sstable_flush_size: 2 << 10,
            compaction: CompactOptions::default(),
            num_memtable_limit: 2,
            serializable: false,
        }
    }
}
