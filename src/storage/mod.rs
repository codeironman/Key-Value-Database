use anyhow::Result;
use bytes::Bytes;

pub mod block;
pub mod cache;
pub mod compaction;
pub mod iterators;
pub mod lsm_tree;
pub mod memory;
pub mod memtable;
pub mod presistence;
pub mod table;

pub trait Storage {
    fn get(&self, key: &Bytes) -> Result<Option<Bytes>>;
    fn set(&self, key: &Bytes, value: &Bytes) -> Result<Option<Bytes>>;
}
