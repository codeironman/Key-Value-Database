use std::{
    fs::{File, OpenOptions},
    hash::Hasher,
    io::{BufWriter, Read, Write},
    mem::size_of,
    path::Path,
    sync::Arc,
};

use anyhow::{bail, Context, Result};
use bytes::{Buf, BufMut, Bytes};
use parking_lot::Mutex;

use crate::{memory::Map, mvcc::key::Key};

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
    pub fn put(&self, key: Key<Bytes>, value: Bytes) -> Result<()> {
        let mut file = self.file.lock();
        let mut buf: Vec<u8> =
            Vec::with_capacity(key.len() + value.len() + 2 * size_of::<u16>() + size_of::<u32>());

        let mut hasher = crc32fast::Hasher::new();
        hasher.write_u16(key.len() as u16);
        buf.put_u16(key.len() as u16);
        hasher.write(&key.data());
        buf.put_slice(&key.data());
        hasher.write_u64(key.ts());
        buf.put_u64(key.ts());
        hasher.write_u16(value.len() as u16);
        buf.put_u16(value.len() as u16);
        hasher.write(&value);
        buf.put_slice(&value);
        buf.put_u32(hasher.finalize());
        file.write_all(&buf)?;
        Ok(())
    }

    pub fn recover(path: impl AsRef<Path>, map: &Map) -> Result<Self> {
        let path = path.as_ref();
        let mut file = OpenOptions::new()
            .read(true)
            .append(true)
            .open(path)
            .context("failed to recover from WAL")?;
        let mut buf = Vec::new();
        file.read_to_end(&mut buf)?;
        let mut rbuf: &[u8] = buf.as_slice();
        while rbuf.has_remaining() {
            let mut hasher = crc32fast::Hasher::new();
            let key_len = rbuf.get_u16() as usize;
            hasher.write_u16(key_len as u16);
            let key = Bytes::copy_from_slice(&rbuf[..key_len]);
            hasher.write(&key);
            rbuf.advance(key_len);
            let ts = rbuf.get_u64();
            hasher.write_u64(ts);
            let value_len = rbuf.get_u16() as usize;
            hasher.write_u16(value_len as u16);
            let value = Bytes::copy_from_slice(&rbuf[..value_len]);
            hasher.write(&value);
            rbuf.advance(value_len);
            let checksum = rbuf.get_u32();
            if hasher.finalize() != checksum {
                bail!("checksum mismatch");
            }
            map.put(Key::<Bytes>::new(&key, ts), value);
        }
        Ok(Self {
            file: Arc::new(Mutex::new(BufWriter::new(file))),
        })
    }
    pub fn sync(&self) -> Result<()> {
        let mut file = self.file.lock();
        file.flush()?;
        file.get_mut().sync_all()?;
        Ok(())
    }
}
