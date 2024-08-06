use std::sync::Arc;

use anyhow::Result;
use bytes::Bytes;

use crate::{
    block::iterator::BlockIterator, iterators::iterators::StorageIterator, mvcc::key::Key,
};

use super::table::SsTable;

pub struct SsTableIterator {
    table: Arc<SsTable>,
    block_iter: BlockIterator,
    block_index: usize,
}
impl SsTableIterator {
    pub fn new(table: Arc<SsTable>, key: Option<Key<Bytes>>) -> Result<Self> {
        let (block_index, block_iter) = match key {
            Some(k) => Self::seek_to_key_inner(&table, k)?,
            None => Self::seek_to_first_inner(&table)?,
        };
        Ok(Self {
            table,
            block_iter,
            block_index,
        })
    }
    fn seek_to_first_inner(table: &Arc<SsTable>) -> Result<(usize, BlockIterator)> {
        let block = table.read_block(0)?;
        let mut block_iter = BlockIterator::new(block);
        block_iter.seek_to(0);
        Ok((0, block_iter))
    }

    fn seek_to_key_inner(table: &Arc<SsTable>, key: Key<Bytes>) -> Result<(usize, BlockIterator)> {
        let block_index = table.find_block_index(key.clone());
        let block = table.read_block(block_index)?;
        let mut block_iter = BlockIterator::new(block);
        block_iter.seek_to_key(key);

        Ok((block_index, block_iter))
    }
}

impl StorageIterator for SsTableIterator {
    type KeyType = Key<Bytes>;

    fn is_valid(&self) -> bool {
        self.block_iter.is_vaild()
    }

    fn key(&self) -> Key<Bytes> {
        self.block_iter.key()
    }
    fn next(&mut self) -> Result<()> {
        self.block_iter.next();
        if !self.block_iter.is_vaild() {
            self.block_index += 1;
            if self.block_index < self.table.num_of_blocks() {
                self.block_iter = BlockIterator::new(self.table.read_block(self.block_index)?);
                self.block_iter.seek_to(0);
            }
        }
        Ok(())
    }
    fn value(&self) -> Bytes {
        self.block_iter.value()
    }
}
