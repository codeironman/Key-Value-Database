use std::collections::HashMap;

use serde::{Deserialize, Serialize};

use crate::lsm_tree::DBState;

#[derive(Debug, Serialize, Deserialize)]
pub struct CompactionTask {
    pub tiers: Vec<(usize, Vec<usize>)>,
    bottom_tier_included: bool,
}

pub struct CompactOptions {
    pub num_tiers: usize,
    pub max_size_amplification_percent: usize,
    pub size_ratio: usize,
    pub min_merge_width: usize,
}

impl Default for CompactOptions {
    fn default() -> Self {
        Self {
            num_tiers: 3,
            max_size_amplification_percent: 200,
            size_ratio: 1,
            min_merge_width: 2,
        }
    }
}
pub struct CompactionController {
    options: CompactOptions,
}

impl CompactionController {
    pub fn new(options: CompactOptions) -> Self {
        Self { options }
    }

    pub fn generate_compaction_task(&self, snapshot: &DBState) -> Option<CompactionTask> {
        if snapshot.levels.len() < self.options.num_tiers {
            return None;
        }

        let mut size = 0;
        for id in 0..(snapshot.levels.len() - 1) {
            size += snapshot.levels[id].1.len();
        }
        // 压缩触发一
        let space_amp_ratio =
            (size as f64) / (snapshot.levels.last().unwrap().1.len() as f64) * 100.0;
        if space_amp_ratio >= self.options.max_size_amplification_percent as f64 {
            return Some(CompactionTask {
                tiers: snapshot.levels.clone(),
                bottom_tier_included: true,
            });
        }

        // 压缩触发二
        let size_ratio_trigger = (100.0 + self.options.size_ratio as f64) / 100.0;
        let mut size = 0;

        for id in 0..(snapshot.levels.len() - 1) {
            size += snapshot.levels[id].1.len();
            let next_level_size = snapshot.levels[id + 1].1.len();
            let current_size_ratio = size as f64 / next_level_size as f64;
            if current_size_ratio >= size_ratio_trigger && id + 2 >= self.options.min_merge_width {
                return Some(CompactionTask {
                    tiers: snapshot
                        .levels
                        .iter()
                        .take(id + 2)
                        .cloned()
                        .collect::<Vec<_>>(),
                    bottom_tier_included: id + 2 >= snapshot.levels.len(),
                });
            }
        }
        let num_tiers_to_take = snapshot.levels.len() - self.options.num_tiers + 2;
        return Some(CompactionTask {
            tiers: snapshot
                .levels
                .iter()
                .take(num_tiers_to_take)
                .cloned()
                .collect::<Vec<_>>(),
            bottom_tier_included: snapshot.levels.len() >= num_tiers_to_take,
        });
    }

    pub fn apply_compacttion(
        &self,
        snapshot: &DBState,
        task: &CompactionTask,
        output: &[usize],
    ) -> (DBState, Vec<usize>) {
        let mut snapshot = snapshot.clone();
        let mut tier_to_remove = task
            .tiers
            .iter()
            .map(|(x, y)| (*x, y))
            .collect::<HashMap<_, _>>();
        let mut levels = Vec::new();
        let mut new_tier_added = false;
        let mut files_to_remove = Vec::new();
        for (tier_id, files) in &snapshot.levels {
            if let Some(files) = tier_to_remove.remove(tier_id) {
                files_to_remove.extend(files.iter().copied());
            } else {
                levels.push((*tier_id, files.clone()))
            }
            if tier_to_remove.is_empty() && !new_tier_added {
                new_tier_added = true;
                levels.push((output[0], output.to_vec()))
            }
        }
        snapshot.levels = levels;
        (snapshot, files_to_remove)
    }
}
