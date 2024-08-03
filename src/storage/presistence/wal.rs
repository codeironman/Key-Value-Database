use std::{
    fs::{File, OpenOptions},
    hash::Hasher,
    io::{BufWriter, Write},
    mem::size_of,
    path::Path,
    sync::Arc,
};

use anyhow::{Context, Result};
use bytes::{BufMut, Bytes};
use parking_lot::Mutex;

pub struct Wal {
    file: Arc<Mutex<BufWriter<File>>>,
}

///           Wal struct
/// | key_len | key | value_len | value |

impl Wal {
    pub fn new(path: impl AsRef<Path>) -> Result<Self> {
        Ok(Self {
            file: Arc::new(Mutex::new(BufWriter::new(
                OpenOptions::new()
                    .read(true)
                    .create_new(true)
                    .write(true)
                    .open(path)
                    .context("fail to open wal")?,
            ))),
        })
    }
    pub fn put(&self, key: &Bytes, value: &Bytes) -> Result<()> {
        let mut file = self.file.lock();
        let mut buf: Vec<u8> =
            Vec::with_capacity(key.len() + value.len() + 2 * size_of::<u16>() + size_of::<u32>());

        let mut hasher = crc32fast::Hasher::new();
        hasher.write_u16(key.len() as u16);
        buf.put_u16(key.len() as u16);
        hasher.write(key);
        buf.put_slice(key);
        hasher.write_u16(value.len() as u16);
        buf.put_u16(value.len() as u16);
        hasher.write(value);
        buf.put_slice(value);
        buf.put_u32(hasher.finalize());
        file.write_all(&buf)?;
        Ok(())
    }

    pub fn sync(&self) -> Result<()> {
        let mut file = self.file.lock();
        file.flush()?;
        file.get_mut().sync_all()?;
        Ok(())
    }
}
