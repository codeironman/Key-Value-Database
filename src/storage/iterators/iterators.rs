use anyhow::Result;
use bytes::Bytes;

pub trait StorageIterator {
    fn value(&self) -> Bytes;

    fn key(&self) -> Bytes;

    fn is_valid(&self) -> bool;

    fn next(&mut self) -> Result<()>;
}
