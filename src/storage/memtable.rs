use std::{
    path::Path,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
};

use anyhow::Result;
use bytes::Bytes;

use crate::{memory::Map, presistence::wal::Wal};

use super::table::builder::SsTableBuilder;

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

    pub fn get(&self, key: &Bytes) -> Option<Bytes> {
        self.map.get(key)
    }

    pub fn put(&self, key: &Bytes, value: &Bytes) -> Result<()> {
        let size = key.len() + value.len();
        let _ = self.map.put(key.clone(), value.clone());
        self.size.fetch_add(size, Ordering::Relaxed);
        self.wal.put(key, value)?;
        Ok(())
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
