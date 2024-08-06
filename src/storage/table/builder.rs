use std::{fs::File, io::Write, path::Path, sync::Arc};

use anyhow::Result;
use bytes::{BufMut, Bytes};
use farmhash::fingerprint32;

use crate::{block::builder::BlockBuilder, cache::BlockCache, mvcc::key::Key};

use super::{
    bloom::Bloom,
    table::{BlockMeta, SsTable},
};

pub struct SsTableBuilder {
    builder: BlockBuilder,
    data: Vec<u8>,
    meta: Vec<BlockMeta>,
    block_size: usize,
    key_hashes: Vec<u32>,
    first_key: Key<Bytes>,
    last_key: Key<Bytes>,
    max_ts: u64,
}

impl SsTableBuilder {
    pub fn new(block_size: usize) -> Self {
        Self {
            data: Vec::new(),
            meta: Vec::new(),
            first_key: Key::<Bytes>::new(&Bytes::new(), 0),
            last_key: Key::<Bytes>::new(&Bytes::new(), 0),
            block_size,
            builder: BlockBuilder::new(block_size),
            key_hashes: Vec::new(),
            max_ts: 0,
        }
    }

    pub fn cur_size(&self) -> usize {
        self.data.len()
    }

    pub fn push_block(&mut self) {
        let builder = std::mem::replace(&mut self.builder, BlockBuilder::new(self.block_size));
        let raw_block = builder.build().encode();
        self.meta.push(BlockMeta {
            offset: raw_block.len(),
            first_key: std::mem::take(&mut self.first_key),
            last_key: std::mem::take(&mut self.last_key),
        });
        let checksum = crc32fast::hash(&raw_block);
        self.data.extend(raw_block);
        self.data.put_u32(checksum);
    }

    pub fn add(&mut self, key: Key<Bytes>, value: Bytes) {
        self.key_hashes.push(fingerprint32(&key.data()));
        if key.ts() > self.max_ts {
            self.max_ts = key.ts();
        }
        if self.builder.add_pair(key.clone(), value.clone()) {
            return;
        }
        self.push_block();
        self.builder.add_pair(key.clone(), value);
        self.first_key = key.clone();
        self.last_key = key.clone();
    }

    pub fn build(
        mut self,
        id: usize,
        block_cache: Arc<BlockCache>,
        path: impl AsRef<Path>,
    ) -> Result<SsTable> {
        self.push_block();
        let mut buf = self.data;
        let meta_offset = buf.len();
        let meta_buf = BlockMeta::encode_block_meta(&self.meta, self.max_ts);
        buf.extend(meta_buf);
        buf.put_u32(meta_offset as u32);
        let bloom = Bloom::build_from_key_hashes(
            &self.key_hashes,
            Bloom::bloom_bits_per_key(self.key_hashes.len(), 0.01),
        );
        let bloom_offset = buf.len();
        bloom.encode(&mut buf);
        buf.put_u32(bloom_offset as u32);
        let mut file = File::create(path)?;
        file.write_all(&buf)?;
        file.sync_all()?;
        Ok(SsTable {
            id,
            file,
            first_key: self.meta.first().unwrap().first_key.clone(),
            last_key: self.meta.last().unwrap().last_key.clone(),
            block_meta: self.meta,
            block_offset: meta_offset,
            block_cache,
            bloom,
            max_ts: 0,
        })
    }
}
