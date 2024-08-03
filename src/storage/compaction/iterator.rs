use std::sync::Arc;

use anyhow::Result;
use bytes::Bytes;

use crate::{
    iterators::iterators::StorageIterator,
    table::{iterator::SsTableIterator, table::SsTable},
};

/// Concat multiple iterators ordered in key order and their key ranges do not overlap. We do not want to create the
/// iterators when initializing this iterator to reduce the overhead of seeking.
pub struct SstConcatIterator {
    current: Option<SsTableIterator>,
    next_sst_idx: usize,
    sstables: Vec<Arc<SsTable>>,
}

impl SstConcatIterator {
    pub fn new(sstables: Vec<Arc<SsTable>>, key: Option<Bytes>) -> Result<Self> {
        if sstables.is_empty() {
            return Ok(Self {
                current: None,
                next_sst_idx: 0,
                sstables,
            });
        }

        let start_index = match &key {
            Some(key) => sstables
                .partition_point(|table| table.first_key <= key)
                .saturating_sub(1)
                .min(sstables.len() - 1),
            None => 0,
        };

        let mut iter = Self {
            current: match &key {
                Some(key) => Some(SsTableIterator::new(
                    sstables[start_index].clone(),
                    Some(key.clone()),
                )?),
                None => Some(SsTableIterator::new(sstables[start_index].clone(), None)?),
            },
            next_sst_idx: start_index + 1,
            sstables,
        };

        iter.move_until_valid()?;
        Ok(iter)
    }

    fn move_until_valid(&mut self) -> Result<()> {
        while let Some(iter) = self.current.as_mut() {
            if iter.is_valid() {
                break;
            }
            if self.next_sst_idx >= self.sstables.len() {
                self.current = None;
            } else {
                self.current = Some(SsTableIterator::new(
                    self.sstables[self.next_sst_idx].clone(),
                    None,
                )?);
                self.next_sst_idx += 1;
            }
        }
        Ok(())
    }
}

impl StorageIterator for SstConcatIterator {
    fn is_valid(&self) -> bool {
        self.current.is_some()
    }
    fn key(&self) -> Bytes {
        self.current.as_ref().unwrap().key()
    }

    fn value(&self) -> Bytes {
        self.current.as_ref().unwrap().value()
    }

    fn next(&mut self) -> Result<()> {
        self.current.as_mut().unwrap().next()?;
        self.move_until_valid()?;
        Ok(())
    }
}
