use std::{sync::Arc, time::Duration};

use anyhow::Result;
use tokio::{sync::mpsc::UnboundedReceiver, task::JoinHandle, time};

use crate::{
    iterators::{iterators::StorageIterator, merge_iterator::MergeIterator},
    lsm_tree::DBInner,
    presistence::manifest::ManifestRecord,
    table::{builder::SsTableBuilder, table::SsTable},
};

use super::{iterator::SstConcatIterator, tiered::CompactionTask};

impl DBInner {
    fn compact_generate_sst_from_iter(
        &self,
        mut iter: impl StorageIterator,
    ) -> Result<Vec<Arc<SsTable>>> {
        let mut builder = None;
        let mut new_sst = Vec::new();

        while iter.is_valid() {
            if builder.is_none() {
                builder = Some(SsTableBuilder::new(self.config.block_size));
            }
            let builder_inner = builder.as_mut().unwrap();
            builder_inner.add(iter.key(), iter.value());
            iter.next()?;
            if builder_inner.cur_size() >= self.config.sstable_flush_size {
                let sst_id = self.next_sst_id();
                let builder = builder.take().unwrap();
                let sst = Arc::new(builder.build(
                    sst_id,
                    self.block_cache.clone(),
                    self.path_of_sst(sst_id),
                )?);
                new_sst.push(sst);
            }
        }
        Ok(new_sst)
    }

    pub async fn compact(&self, task: &CompactionTask) -> Result<Vec<Arc<SsTable>>> {
        let snapshot = {
            let state = self.state.read().await;
            state.clone()
        };
        let CompactionTask { tiers, .. } = task;
        let mut iters = Vec::with_capacity(tiers.len());
        for (_levels, tier_sst_ids) in tiers {
            let mut ssts = Vec::with_capacity(tier_sst_ids.len());
            for id in tier_sst_ids.iter() {
                ssts.push(snapshot.sstables.get(id).unwrap().clone());
            }
            iters.push(Box::new(SstConcatIterator::new(ssts, None)?));
        }
        self.compact_generate_sst_from_iter(MergeIterator::new(iters))
    }
    pub async fn trigger_flush(&self) -> Result<()> {
        let res = {
            let state = self.state.read().await;
            state.imm_memtables.len() >= self.config.num_memtable_limit
        };
        if res {
            self.flush_next_imm_memtable().await?;
        }

        Ok(())
    }

    pub async fn trigger_compaction(&self) -> Result<()> {
        let snapshot = {
            let state = self.state.read().await;
            state.clone()
        };

        let task = self
            .compaction_controller
            .generate_compaction_task(&snapshot);

        let Some(task) = task else {
            return Ok(());
        };
        let sstables = self.compact(&task).await?;
        let output = sstables.iter().map(|x| x.id).collect::<Vec<_>>();
        let ssts_to_remove = {
            let _state_lock = self.lock.lock().await;
            let mut snapshot = self.state.read().await.as_ref().clone();
            let mut new_sst_ids = Vec::new();
            for sstable in sstables {
                let sst_id = sstable.id;
                new_sst_ids.push(sst_id);
                let _ = snapshot.sstables.insert(sst_id, sstable);
            }

            let (mut snapshot, files_to_remove) = self
                .compaction_controller
                .apply_compacttion(&snapshot, &task, &output);
            let mut ssts_to_remove = Vec::with_capacity(files_to_remove.len());
            for file_to_remove in &files_to_remove {
                let result = snapshot.sstables.remove(file_to_remove);
                assert!(result.is_some(), "cannot remove {}.sst", file_to_remove);
                ssts_to_remove.push(result.unwrap());
            }
            let mut state = self.state.write().await;
            *state = Arc::new(snapshot);
            drop(state);
            self.sync_dir()?;
            self.manifest
                .add_record(ManifestRecord::Compaction(task, new_sst_ids))?;
            ssts_to_remove
        };
        for sst in ssts_to_remove {
            std::fs::remove_file(self.path_of_sst(sst.id))?;
        }
        self.sync_dir()?;
        Ok(())
    }

    pub fn spawn_compaction_task(
        self: &Arc<Self>,
        mut rx: UnboundedReceiver<()>,
    ) -> Result<Option<JoinHandle<()>>> {
        let this = self.clone();
        let handle = tokio::spawn(async move {
            let mut ticker = time::interval(Duration::from_millis(50));
            loop {
                tokio::select! {
                            _ = ticker.tick() => {
                                if let Err(e) = this.trigger_compaction().await {
                                    eprintln!("compaction failed: {}", e);
                                }
                            },
                            _ = rx.recv() => {
                                return;
                            }
                }
            }
        });
        Ok(Some(handle))
    }
}
