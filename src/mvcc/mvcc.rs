use std::{
    collections::{BTreeMap, HashSet},
    sync::{atomic::AtomicBool, Arc},
};

use dashmap::DashMap;
use tokio::sync::Mutex;

use crate::lsm_tree::DBInner;

use super::{transaction::Transaction, watermark::Watermark};
/// Transaction
pub struct MvccInner {
    pub(crate) write_lock: Mutex<()>,
    pub(crate) commit_lock: Mutex<()>,
    pub(crate) ts: Arc<Mutex<(u64, Watermark)>>,
    pub(crate) committed_txns: Arc<Mutex<BTreeMap<u64, CommittedTxnData>>>,
}

pub(crate) struct CommittedTxnData {
    pub(crate) key_hashes: HashSet<u32>,
    pub(crate) read_ts: u64,
    pub(crate) commit_ts: u64,
}

impl MvccInner {
    pub fn new(initial_ts: u64) -> Self {
        Self {
            write_lock: Mutex::new(()),
            commit_lock: Mutex::new(()),
            ts: Arc::new(Mutex::new((initial_ts, Watermark::new()))),
            committed_txns: Arc::new(Mutex::new(BTreeMap::new())),
        }
    }
    pub async fn latest_commit_ts(&self) -> u64 {
        self.ts.lock().await.0
    }
    pub async fn update_commit_ts(&self, ts: u64) {
        self.ts.lock().await.0 = ts;
    }

    // for GC
    pub async fn watermark(&self) -> u64 {
        let ts = self.ts.lock().await;
        ts.1.watermark().unwrap_or(ts.0)
    }

    pub async fn new_txn(&self, inner: Arc<DBInner>, serializable: bool) -> Arc<Transaction> {
        let mut ts = self.ts.lock().await;
        let read_ts = ts.0;
        ts.1.add_reader(read_ts);
        Arc::new(Transaction {
            inner,
            read_ts,
            local_storage: Arc::new(DashMap::new()),
            committed: Arc::new(AtomicBool::new(false)),
            key_hashes: if serializable {
                Some(Mutex::new((HashSet::new(), HashSet::new())))
            } else {
                None
            },
        })
    }
}
