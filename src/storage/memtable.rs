use std::{
    ops::Bound,
    path::Path,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
};

use anyhow::Result;
use bytes::Bytes;
use ouroboros::self_referencing;

use crate::{memory::Map, mvcc::key::Key, presistence::wal::Wal};

use super::{
    iterators::iterators::StorageIterator, memory::MapRangeIter, table::builder::SsTableBuilder,
};

pub struct MemTable {
    map: Arc<Map>,
    wal: Wal,
    id: usize,
    size: Arc<AtomicUsize>,
}

impl MemTable {
    pub fn new(id: usize, path: impl AsRef<Path>) -> Result<Self> {
        Ok(Self {
            id,
            map: Arc::new(Map::new()),
            wal: Wal::new(path)?,
            size: Arc::new(AtomicUsize::new(0)),
        })
    }

    pub fn get(&self, key: &Key<Bytes>) -> Option<Bytes> {
        self.map.get(key)
    }

    pub fn put(&self, key: Key<Bytes>, value: &Bytes) -> Result<()> {
        let size = key.len() + value.len();
        let _ = self.map.put(key.clone(), value.clone());
        self.size.fetch_add(size, Ordering::Relaxed);
        self.wal.put(key, value.clone())?;
        Ok(())
    }
    pub fn scan(&self, lower: Bound<Key<Bytes>>, upper: Bound<Key<Bytes>>) -> MemTableIterator {
        let mut iter = MemTableIteratorBuilder {
            map: self.map.clone(),
            iter_builder: |map| map.range_iter(lower, upper),
            item: (Key::<Bytes>::new(&Bytes::new(), 0), Bytes::new()),
        }
        .build();
        iter.next().unwrap();
        iter
    }

    pub fn sync_wal(&self) -> Result<()> {
        self.wal.sync()?;
        Ok(())
    }

    pub fn flush(&self, builder: &mut SsTableBuilder) -> Result<()> {
        for entry in self.map.iter() {
            builder.add(entry.0, entry.1);
        }
        Ok(())
    }

    pub fn size(&self) -> usize {
        self.size.load(Ordering::Acquire)
    }

    pub fn id(&self) -> usize {
        self.id
    }
}
#[self_referencing]
pub struct MemTableIterator {
    map: Arc<Map>,
    #[borrows(map)]
    #[not_covariant]
    iter: MapRangeIter<'this>,
    item: (Key<Bytes>, Bytes),
}

impl StorageIterator for MemTableIterator {
    type KeyType = Key<Bytes>;
    fn value(&self) -> Bytes {
        self.borrow_item().1.clone()
    }

    fn key(&self) -> Key<Bytes> {
        self.borrow_item().0.clone()
    }

    fn is_valid(&self) -> bool {
        !self.borrow_item().0.data().is_empty()
    }

    fn next(&mut self) -> Result<()> {
        let entry = self.with_iter_mut(|iter| (iter.next()).unwrap());
        self.with_mut(|x| *x.item = entry);
        Ok(())
    }
}
