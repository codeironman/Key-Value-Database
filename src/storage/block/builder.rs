use bytes::{BufMut, Bytes};

use super::block::{Block, U16SIZE};

pub struct BlockBuilder {
    data: Vec<u8>,
    offsets: Vec<u16>, //recore last data positon
    block_size: usize,
}

impl BlockBuilder {
    pub fn new(block_size: usize) -> Self {
        Self {
            data: Vec::new(),
            offsets: Vec::new(),
            block_size,
        }
    }

    fn cur_size(&self) -> usize {
        U16SIZE + self.offsets.len() * U16SIZE + self.data.len()
    }
    pub fn add_pair(&mut self, key: Bytes, value: Bytes) -> bool {
        if self.cur_size() + key.len() + value.len() + U16SIZE * 3 > self.block_size {
            return false;
        }
        self.offsets.push(self.data.len() as u16);
        self.data.put_u16(key.len() as u16);
        self.data.put(key);
        self.data.put_u16(value.len() as u16);
        self.data.put(value);
        true
    }

    pub fn build(self) -> Block {
        Block {
            data: self.data,
            offsets: self.offsets,
        }
    }
}
