use std::{fs::File, os::unix::fs::FileExt, sync::Arc};

use anyhow::{bail, Result};
use bytes::{Buf, BufMut, Bytes};

use crate::cache::BlockCache;

use super::bloom::Bloom;
use crate::block::block::Block;

pub struct SsTable {
    pub id: usize,
    pub file: File,
    pub block_cache: Arc<BlockCache>,
    pub block_meta: Vec<BlockMeta>,
    pub block_offset: usize,
    pub first_key: Bytes,
    pub last_key: Bytes,
    pub bloom: Bloom,
    pub max_ts: u64, // Timestamp
}

impl SsTable {
    pub fn read_block(&self, block_idx: usize) -> Result<Arc<Block>> {
        let offset_begin = self.block_meta[block_idx].offset;
        let offset_end = self
            .block_meta
            .get(block_idx + 1)
            .map_or(self.block_offset, |x| x.offset);
        let block_len = offset_end - offset_begin - 4; // -4 because checksum
        let mut all_data: Vec<u8> = vec![0; offset_end - offset_begin];
        self.file
            .read_exact_at(&mut all_data, offset_begin as u64)?;
        let block_data = &all_data[..block_len];
        let checksum = (&all_data[block_len..]).get_u32();
        if checksum != crc32fast::hash(block_data) {
            bail!("block checksum mismatched");
        }
        Ok(Arc::new(Block::decode(block_data)))
    }

    pub fn find_block_index(&self, key: Bytes) -> usize {
        self.block_meta
            .partition_point(|meta| meta.first_key <= key)
            .saturating_sub(1)
    }

    pub fn num_of_blocks(&self) -> usize {
        self.block_meta.len()
    }
}

pub struct BlockMeta {
    pub offset: usize,
    pub first_key: Bytes,
    pub last_key: Bytes,
}

impl BlockMeta {
    pub fn encode_block_meta(block_meta: &[BlockMeta]) -> Vec<u8> {
        let mut buf: Vec<u8> = Vec::new();
        let mut size = std::mem::size_of::<u32>();
        for meta in block_meta {
            // The size of offset
            size += std::mem::size_of::<u32>();
            // The size of key length
            size += std::mem::size_of::<u16>();
            // The size of actual key
            size += meta.first_key.len();
            // The size of key length
            size += std::mem::size_of::<u16>();
            // The size of actual key
            size += meta.last_key.len();
        }
        size += std::mem::size_of::<u32>();
        // Reserve the space to improve performance, especially when the size of incoming data is
        // large
        buf.reserve(size);
        let original_len = buf.len();
        buf.put_u32(block_meta.len() as u32);
        for meta in block_meta {
            buf.put_u32(meta.offset as u32);
            buf.put_u16(meta.first_key.len() as u16);
            buf.put_slice(&meta.first_key);
            buf.put_u16(meta.last_key.len() as u16);
            buf.put_slice(&meta.last_key);
        }
        buf.put_u32(crc32fast::hash(&buf[original_len + 4..]));
        assert_eq!(size, buf.len() - original_len);
        buf
    }

    /// Decode block meta from a buffer.
    pub fn decode_block_meta(mut buf: &[u8]) -> Result<Vec<BlockMeta>> {
        let mut block_meta = Vec::new();
        let num = buf.get_u32() as usize;
        let checksum = crc32fast::hash(&buf[..buf.remaining() - 4]);
        for _ in 0..num {
            let offset = buf.get_u32() as usize;
            let first_key_len = buf.get_u16() as usize;
            let first_key = buf.copy_to_bytes(first_key_len);
            let last_key_len: usize = buf.get_u16() as usize;
            let last_key = buf.copy_to_bytes(last_key_len);
            block_meta.push(BlockMeta {
                offset,
                first_key,
                last_key,
            });
        }
        if buf.get_u32() != checksum {
            bail!("meta checksum mismatched");
        }
        Ok(block_meta)
    }
}
