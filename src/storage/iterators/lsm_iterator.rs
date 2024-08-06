use std::ops::Bound;

use anyhow::{bail, Result};
use bytes::Bytes;

use crate::{
    compaction::iterator::SstConcatIterator, memtable::MemTableIterator, mvcc::key::Key,
    table::iterator::SsTableIterator,
};

use super::{
    iterators::StorageIterator, merge_iterator::MergeIterator, two_merge::TwoMergeIterator,
};

/// Represents the internal type for an LSM iterator. This type will be changed across the tutorial for multiple times.
type IteratorInner = TwoMergeIterator<
    TwoMergeIterator<MergeIterator<MemTableIterator>, MergeIterator<SsTableIterator>>,
    MergeIterator<SstConcatIterator>,
>;

pub struct LsmIterator {
    inner: IteratorInner,
    end_bound: Bound<Bytes>,
    is_valid: bool,
    read_ts: u64,
    prev_key: Vec<u8>,
}

impl LsmIterator {
    pub(crate) fn new(iter: IteratorInner, end_bound: Bound<Bytes>, read_ts: u64) -> Result<Self> {
        let mut iter = Self {
            is_valid: iter.is_valid(),
            inner: iter,
            end_bound,
            read_ts,
            prev_key: Vec::new(),
        };
        iter.move_to_key()?;
        Ok(iter)
    }

    fn next_inner(&mut self) -> Result<()> {
        self.inner.next()?;
        if !self.inner.is_valid() {
            self.is_valid = false;
            return Ok(());
        }
        match self.end_bound.as_ref() {
            Bound::Unbounded => {}
            Bound::Included(key) => self.is_valid = self.inner.key().data() <= key,
            Bound::Excluded(key) => self.is_valid = self.inner.key().data() < key,
        }
        Ok(())
    }

    fn move_to_key(&mut self) -> Result<()> {
        loop {
            while self.inner.is_valid() && self.inner.key().data() == self.prev_key {
                self.next_inner()?;
            }
            if !self.inner.is_valid() {
                break;
            }
            self.prev_key.clear();
            self.prev_key.extend(self.inner.key().data());
            while self.inner.is_valid()
                && self.inner.key().data() == self.prev_key
                && self.inner.key().ts() > self.read_ts
            {
                self.next_inner()?;
            }
            if !self.inner.is_valid() {
                break;
            }
            if self.inner.key().data() != self.prev_key {
                continue;
            }
            if !self.inner.value().is_empty() {
                break;
            }
        }
        Ok(())
    }
}

impl StorageIterator for LsmIterator {
    type KeyType = Bytes;
    fn is_valid(&self) -> bool {
        self.is_valid
    }

    fn key(&self) -> Bytes {
        self.inner.key().data()
    }

    fn value(&self) -> Bytes {
        self.inner.value()
    }

    fn next(&mut self) -> Result<()> {
        self.next_inner()?;
        self.move_to_key()?;
        Ok(())
    }
}

/// A wrapper around existing iterator, will prevent users from calling `next` when the iterator is
/// invalid. If an iterator is already invalid, `next` does not do anything. If `next` returns an error,
/// `is_valid` should return false, and `next` should always return an error.
pub struct FusedIterator<I: StorageIterator> {
    iter: I,
    has_errored: bool,
}

impl<I: StorageIterator> FusedIterator<I> {
    pub fn new(iter: I) -> Self {
        Self {
            iter,
            has_errored: false,
        }
    }
}

impl<I: StorageIterator<KeyType = Key<Bytes>>> StorageIterator for FusedIterator<I> {
    type KeyType = Key<Bytes>;
    fn is_valid(&self) -> bool {
        !self.has_errored && self.iter.is_valid()
    }

    fn key(&self) -> Key<Bytes> {
        if !self.is_valid() {
            panic!("invalid access to the underlying iterator");
        }
        self.iter.key()
    }

    fn value(&self) -> Bytes {
        if !self.is_valid() {
            panic!("invalid access to the underlying iterator");
        }
        self.iter.value()
    }

    fn next(&mut self) -> Result<()> {
        if self.has_errored {
            bail!("the iterator is tainted");
        }
        if self.iter.is_valid() {
            if let Err(e) = self.iter.next() {
                self.has_errored = true;
                return Err(e);
            }
        }
        Ok(())
    }
}
