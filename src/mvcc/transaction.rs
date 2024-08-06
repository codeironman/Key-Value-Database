use std::{
    collections::HashSet,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
};

use anyhow::{bail, Result};
use bytes::Bytes;
use dashmap::DashMap;

use crate::{
    lsm_tree::{DBInner, WriteBatchRecord},
    mvcc::mvcc::CommittedTxnData,
};
use tokio::sync::Mutex;

pub struct Transaction {
    pub read_ts: u64,
    pub inner: Arc<DBInner>,
    pub local_storage: Arc<DashMap<Bytes, Bytes>>,
    pub committed: Arc<AtomicBool>,
    /// Write set and read set
    pub key_hashes: Option<Mutex<(HashSet<u32>, HashSet<u32>)>>,
}

impl Transaction {
    pub async fn get(&self, key: &Bytes) -> Result<Option<Bytes>> {
        if self.committed.load(Ordering::SeqCst) {
            panic!("cannot operate on committed txn!");
        }
        if let Some(guard) = &self.key_hashes {
            let guard = guard.lock();
            let (_, read_set) = &mut *guard.await;
            read_set.insert(farmhash::hash32(key));
        }
        if let Some(entry) = self.local_storage.get(key) {
            if entry.value().is_empty() {
                return Ok(None);
            } else {
                return Ok(Some(entry.value().clone()));
            }
        }
        self.inner.get_with_ts(key, self.read_ts).await
    }

    pub async fn put(&self, key: &Bytes, value: &Bytes) {
        if self.committed.load(Ordering::SeqCst) {
            panic!("cannot operate on committed txn!");
        }
        self.local_storage
            .insert(Bytes::copy_from_slice(key), Bytes::copy_from_slice(value));
        if let Some(key_hashes) = &self.key_hashes {
            let key_hashes = key_hashes.lock();
            let (write_hashes, _) = &mut *key_hashes.await;
            write_hashes.insert(farmhash::hash32(key));
        }
    }
    pub async fn delete(&self, key: &Bytes) {
        if self.committed.load(Ordering::SeqCst) {
            panic!("cannot operate on committed txn!");
        }
        self.local_storage
            .insert(Bytes::copy_from_slice(key), Bytes::new());
        if let Some(key_hashes) = &self.key_hashes {
            let mut key_hashes_guard = key_hashes.lock().await;
            let (write_hashes, _) = &mut *key_hashes_guard;
            write_hashes.insert(farmhash::hash32(key));
        }
    }
    pub async fn commit(&self) -> Result<()> {
        self.committed
            .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
            .expect("cannot operate on committed txn!");
        let _commit_lock = self.inner.mvcc.commit_lock.lock().await;
        let serializability_check;
        if let Some(guard) = &self.key_hashes {
            let guard = guard.lock();
            let (write_set, read_set) = &*guard.await;
            println!(
                "commit txn: write_set: {:?}, read_set: {:?}",
                write_set, read_set
            );
            if !write_set.is_empty() {
                let committed_txns = self.inner.mvcc.committed_txns.lock();
                for (_, txn_data) in committed_txns.await.range((self.read_ts + 1)..) {
                    for key_hash in read_set {
                        if txn_data.key_hashes.contains(key_hash) {
                            bail!("serializable check failed");
                        }
                    }
                }
            }
            serializability_check = true;
        } else {
            serializability_check = false;
        }
        let batch = self
            .local_storage
            .iter()
            .map(|entry| {
                if entry.value().is_empty() {
                    WriteBatchRecord::Del(entry.key().clone())
                } else {
                    WriteBatchRecord::Put(entry.key().clone(), entry.value().clone())
                }
            })
            .collect::<Vec<_>>();
        let ts = self.inner.write_batch(&batch).await?;
        if serializability_check {
            let mut committed_txns = self.inner.mvcc.committed_txns.lock().await;
            let mut key_hashes = self.key_hashes.as_ref().unwrap().lock().await;
            let (write_set, _) = &mut *key_hashes;

            let old_data = committed_txns.insert(
                ts,
                CommittedTxnData {
                    key_hashes: std::mem::take(write_set),
                    read_ts: self.read_ts,
                    commit_ts: ts,
                },
            );
            assert!(old_data.is_none());

            // remove unneeded txn data
            let watermark = self.inner.mvcc.watermark().await;
            while let Some(entry) = committed_txns.first_entry() {
                if *entry.key() < watermark {
                    entry.remove();
                } else {
                    break;
                }
            }
        }
        Ok(())
    }
}
