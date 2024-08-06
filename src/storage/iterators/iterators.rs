use anyhow::Result;
use bytes::Bytes;
pub trait StorageIterator {
    type KeyType: PartialEq + Eq + PartialOrd + Ord;
    fn value(&self) -> Bytes;

    fn key(&self) -> Self::KeyType;

    fn is_valid(&self) -> bool;

    fn next(&mut self) -> Result<()>;
}
