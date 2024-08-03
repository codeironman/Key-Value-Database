use bytes::{Buf, Bytes};
use std::{cmp::Ordering, sync::Arc};

use super::block::{Block, U16SIZE};
pub struct BlockIterator {
    block: Arc<Block>,
    key: Bytes,
    index: usize,
    value_range: (usize, usize),
}

impl BlockIterator {
    pub fn new(block: Arc<Block>) -> Self {
        Self {
            block,
            index: 0,
            key: Bytes::new(),
            value_range: (0, 0),
        }
    }

    pub fn key(&self) -> Bytes {
        self.key.clone()
    }

    pub fn value(&self) -> Bytes {
        Bytes::copy_from_slice(&self.block.data[self.value_range.0..self.value_range.1])
    }

    pub fn is_vaild(&self) -> bool {
        !self.key.is_empty()
    }

    fn seek_to_offset(&mut self, offset: usize) {
        let mut entry = &self.block.data[offset..];
        let key_len = entry.get_u16() as usize;
        let key = &entry[..key_len];
        self.key.clear();
        self.key = Bytes::copy_from_slice(key);
        entry.advance(key_len);
        let value_len = entry.get_u16() as usize;
        let value_offset_begin = offset + U16SIZE + U16SIZE + key_len + U16SIZE;
        let value_offset_end = value_offset_begin + value_len;
        self.value_range = (value_offset_begin, value_offset_end);
        entry.advance(value_len);
    }

    pub fn seek_to(&mut self, index: usize) {
        if index > self.block.offsets.len() {
            self.key.clear();
            self.value_range = (0, 0);
            return;
        }
        let offset = self.block.offsets[index] as usize;
        self.seek_to_offset(offset);
        self.index = index
    }

    pub fn seek_to_key(&mut self, key: Bytes) {
        let mut low = 0;
        let mut high = self.block.offsets.len();

        while low < high {
            let mid = (high + low) >> 1;
            self.seek_to(mid);
            match self.key.cmp(&key) {
                Ordering::Equal => return,
                Ordering::Greater => high = mid - 1,
                Ordering::Less => low = mid + 1,
            }
        }
        self.seek_to(low);
    }
}

impl Iterator for BlockIterator {
    type Item = Bytes;
    fn next(&mut self) -> Option<Self::Item> {
        if self.index > self.block.offsets.len() {
            return None;
        }
        self.index += 1;
        self.seek_to(self.index);
        Some(self.key.clone())
    }
}
