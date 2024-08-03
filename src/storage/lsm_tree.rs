use std::{
    collections::HashMap,
    fs::{create_dir_all, File},
    path::{Path, PathBuf},
    sync::{atomic::AtomicUsize, Arc},
};

use anyhow::Result;
use bytes::Bytes;
use farmhash::fingerprint32;
use tokio::{
    fs::remove_file,
    sync::{
        mpsc::{self, Sender, UnboundedReceiver, UnboundedSender},
        Mutex, MutexGuard, RwLock,
    },
    task::JoinHandle,
    time::{self, Duration},
};
use tracing::info;

use crate::{
    cache::BlockCache,
    compaction::iterator::SstConcatIterator,
    iterators::{
        iterators::StorageIterator, merge_iterator::MergeIterator, two_merge::TwoMergeIterator,
    },
    memtable::MemTable,
    table::{iterator::SsTableIterator, table::SsTable},
    DBConfig,
};

use super::{
    compaction::tiered::{CompactOptions, CompactionController},
    presistence::manifest::{Manifest, ManifestRecord},
    table::builder::SsTableBuilder,
};

pub struct ArcDB {
    pub inner: Arc<DBInner>,
    flush_notifier: UnboundedSender<()>,
    flush_task: Mutex<Option<JoinHandle<()>>>,
    compaction_notifier: UnboundedSender<()>,
    compaction_task: Mutex<Option<JoinHandle<()>>>,
}

pub struct DBInner {
    pub state: Arc<RwLock<Arc<DBState>>>,
    pub lock: Mutex<()>, //state_lock
    path: PathBuf,
    next_sst_id: AtomicUsize,
    pub block_cache: Arc<BlockCache>,
    pub config: Arc<DBConfig>,
    pub(crate) compaction_controller: CompactionController,
    pub(crate) manifest: Manifest,
}

#[derive(Clone)]
pub struct DBState {
    pub memtable: Arc<MemTable>,
    pub imm_memtables: Vec<Arc<MemTable>>,
    pub l0_sstables_index: Vec<usize>,
    pub levels: Vec<(usize, Vec<usize>)>,
    pub sstables: HashMap<usize, Arc<SsTable>>,
}

impl ArcDB {
    pub async fn new(path: impl AsRef<Path>) -> Result<Self> {
        let inner = Arc::new(DBInner::new(path)?);
        let (tx1, rx) = mpsc::unbounded_channel();
        let compaction_task = inner.spawn_compaction_task(rx)?;
        let (tx2, rx) = mpsc::unbounded_channel();
        let flush_task = inner.spawn_flush_task(rx).await?;
        Ok(Self {
            inner,
            flush_notifier: tx2,
            flush_task: Mutex::new(flush_task),
            compaction_notifier: tx1,
            compaction_task: Mutex::new(compaction_task),
        })
    }

    pub async fn put(&self, key: &Bytes, value: &Bytes) -> Result<()> {
        self.inner.put(key, value).await
    }
    pub async fn get(&self, key: &Bytes) -> Result<Option<Bytes>> {
        self.inner.get(key).await
    }
}

impl DBState {
    pub fn new(path: impl AsRef<Path>) -> Result<Self> {
        Ok(Self {
            memtable: Arc::new(MemTable::new(0, path)?),
            imm_memtables: Vec::new(),
            l0_sstables_index: Vec::new(),
            levels: Vec::new(),
            sstables: HashMap::new(),
        })
    }
}

impl DBInner {
    pub fn new(path: impl AsRef<Path>) -> Result<Self> {
        let path = path.as_ref();
        let state = DBState::new(path)?;
        let next_sst_id = 1;
        let block_cache = Arc::new(BlockCache::new(1 << 10)); //1MB
        let compaction_controller = CompactionController::new(CompactOptions::default());
        if path.exists() {
            create_dir_all(path)?
        }
        let manifest_path = path.join("MANIFEST");
        let manifest = Manifest::create(manifest_path)?;
        manifest.add_record(ManifestRecord::NewMemtable(state.memtable.id()))?;

        let storage = Self {
            state: Arc::new(RwLock::new(Arc::new(state))),
            lock: Mutex::new(()),
            path: path.to_path_buf(),
            block_cache,
            next_sst_id: AtomicUsize::new(next_sst_id),
            compaction_controller,
            manifest,
            config: Arc::new(DBConfig::default()),
        };

        Ok(storage)
    }

    pub async fn get(&self, key: &Bytes) -> Result<Option<Bytes>> {
        let snapshot = {
            let guard = self.state.read().await;
            Arc::clone(&guard)
        };
        // memtable
        if let Some(value) = snapshot.memtable.get(&key) {
            if value.is_empty() {
                return Ok(None);
            }
            return Ok(Some(value));
        }
        // immtables
        for memtable in snapshot.imm_memtables.iter() {
            if let Some(value) = memtable.get(&key) {
                if value.is_empty() {
                    return Ok(None);
                }
                return Ok(Some(value));
            }
        }

        let mut l0_iters = Vec::with_capacity(snapshot.l0_sstables_index.len());

        fn key_within(user_key: Bytes, table_begin: Bytes, table_end: Bytes) -> bool {
            table_begin <= user_key && user_key <= table_end
        }

        let may_in_table = |key: Bytes, table: &SsTable| {
            if key_within(key.clone(), table.first_key.clone(), table.last_key.clone()) {
                if table.bloom.may_contain(fingerprint32(key.as_ref())) {
                    return true;
                }
                return false;
            }
            false
        };

        for table in snapshot.l0_sstables_index.iter() {
            let table = snapshot.sstables[table].clone();
            if may_in_table(key.clone(), &table) {
                l0_iters.push(Box::new(SsTableIterator::new(table, Some(key.clone()))?))
            }
        }
        let l0_iter = MergeIterator::new(l0_iters);
        let mut level_iters = Vec::with_capacity(snapshot.levels.len());
        for (_, sst_ids) in &snapshot.levels {
            let mut level_ssts = Vec::with_capacity(sst_ids.len());
            for table_id in sst_ids {
                let table = snapshot.sstables[table_id].clone();
                if may_in_table(key.clone(), &table) {
                    level_ssts.push(table);
                }
            }
            let level_iter = SstConcatIterator::new(level_ssts, Some(key.clone()))?;
            level_iters.push(Box::new(level_iter));
        }
        let level_iter = MergeIterator::new(level_iters);
        let iter = TwoMergeIterator::create(level_iter, l0_iter)?;
        if iter.is_valid() && iter.key() == key && !iter.value().is_empty() {
            return Ok(Some(iter.value()));
        }
        Ok(None)
    }

    pub async fn put(&self, key: &Bytes, value: &Bytes) -> Result<()> {
        let mut size = 0;
        {
            let guard = self.state.read().await;
            guard.memtable.put(key, value)?;
            size = guard.memtable.size();
        }
        self.try_freeze(size).await?;
        Ok(())
    }

    pub async fn delete(&self, key: &Bytes) -> Result<()> {
        let mut size = 0;
        {
            let guard = self.state.read().await;
            guard.memtable.put(key, &Bytes::from_static(&[0u8]))?;
            size = guard.memtable.size();
        }
        self.try_freeze(size).await?;
        Ok(())
    }
    pub async fn try_freeze(&self, cur_size: usize) -> Result<()> {
        if cur_size >= self.config.sstable_flush_size {
            let state_lock = self.lock.lock().await;
            let guard = self.state.read().await;
            if guard.memtable.size() >= self.config.sstable_flush_size {
                drop(guard);
                self.freeze_memtable(&state_lock).await?
            }
        }
        Ok(())
    }

    pub async fn freeze_memtable(&self, _state_lock: &MutexGuard<'_, ()>) -> Result<()> {
        let mem_id = self.next_sst_id();
        let memtable = Arc::new(MemTable::new(mem_id, self.path_of_wal(mem_id))?);

        {
            let mut guard = self.state.write().await;
            let mut snapshot = guard.as_ref().clone();
            let old_memtable = std::mem::replace(&mut snapshot.memtable, memtable);
            snapshot.imm_memtables.insert(0, old_memtable.clone());
            *guard = Arc::new(snapshot);
            drop(guard);
            old_memtable.sync_wal()?;
        }

        self.manifest
            .add_record(ManifestRecord::NewMemtable(mem_id))?;

        Ok(())
    }

    pub async fn flush_next_imm_memtable(&self) -> Result<()> {
        let _lock = self.lock.lock();
        let flush_memtable = {
            let guard = self.state.read().await;
            guard.imm_memtables.last().expect("no imm").clone()
        };

        let mut builder = SsTableBuilder::new(self.config.block_size);
        flush_memtable.flush(&mut builder)?;
        let sst_id = flush_memtable.id();
        let sst =
            Arc::new(builder.build(sst_id, self.block_cache.clone(), self.path_of_sst(sst_id))?);

        {
            let mut guard = self.state.write().await;
            let mut snapshot = guard.as_ref().clone();
            snapshot.imm_memtables.pop().unwrap();
            snapshot.levels.insert(0, (sst_id, vec![sst_id]));
            snapshot.sstables.insert(sst_id, sst);
            *guard = Arc::new(snapshot);
        }

        remove_file(self.path_of_wal(sst_id)).await?;
        self.manifest.add_record(ManifestRecord::Flush(sst_id))?;
        self.sync_dir()?;
        Ok(())
    }

    pub async fn spawn_flush_task(
        self: &Arc<Self>,
        mut rx: UnboundedReceiver<()>,
    ) -> Result<Option<JoinHandle<()>>> {
        let this = self.clone();
        let handle = tokio::spawn(async move {
            let mut ticker = time::interval(Duration::from_millis(50));
            loop {
                tokio::select! {
                    _ = ticker.tick() => {
                        if let Err(e) = this.trigger_flush().await {
                            eprintln!("flush failed: {}", e);
                        }else {
                            info!("Flush operation succeeded");
                        }
                    },
                    _ = rx.recv() => break,
                }
            }
        });

        Ok(Some(handle))
    }
    pub fn next_sst_id(&self) -> usize {
        self.next_sst_id
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst)
    }

    pub fn path_of_sst(&self, id: usize) -> PathBuf {
        self.path.join(format!("{:05}.sst", id))
    }

    pub fn path_of_wal(&self, id: usize) -> PathBuf {
        self.path.join(format!("{:05}.wal", id))
    }

    pub fn sync_dir(&self) -> Result<()> {
        File::open(&self.path)?.sync_all()?;
        Ok(())
    }
}
