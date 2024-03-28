use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::collections::HashSet;
use std::process::Output;

use crate::lsm_storage::LsmStorageState;

#[derive(Debug, Serialize, Deserialize)]
pub struct TieredCompactionTask {
    pub tiers: Vec<(usize, Vec<usize>)>,
    pub bottom_tier_included: bool,
}

#[derive(Debug, Clone)]
pub struct TieredCompactionOptions {
    pub num_tiers: usize,
    pub max_size_amplification_percent: usize,
    pub size_ratio: usize,
    pub min_merge_width: usize,
}

pub struct TieredCompactionController {
    options: TieredCompactionOptions,
}

impl TieredCompactionController {
    pub fn new(options: TieredCompactionOptions) -> Self {
        Self { options }
    }

    pub fn generate_compaction_task(
        &self,
        _snapshot: &LsmStorageState,
    ) -> Option<TieredCompactionTask> {
        if _snapshot.levels.len() < self.options.num_tiers {
            return None;
        }
        // space amp ratio
        let spc_amp_ratio = _snapshot.levels.iter().map(|x| x.1.len()).sum::<usize>() as f64
            / _snapshot.levels.last().unwrap().1.len() as f64
            - 1_f64;
        if spc_amp_ratio * 100.0 >= self.options.max_size_amplification_percent as f64 {
            return Some(TieredCompactionTask {
                tiers: _snapshot.levels.clone(),
                bottom_tier_included: true,
            });
        }

        // size ratio trigger
        let mut pre_sz: usize = 0;
        let sz_ratio_trigger = (100_f64 + self.options.size_ratio as f64) / 100_f64;
        for i in 0.._snapshot.levels.len() - 1 {
            pre_sz += _snapshot.levels[i].1.len();
            if pre_sz as f64 / _snapshot.levels[i + 1].1.len() as f64 >= sz_ratio_trigger {
                return Some(TieredCompactionTask {
                    tiers: Vec::from(&_snapshot.levels[0..=(i + 1)]),
                    bottom_tier_included: (i == _snapshot.levels.len() - 2),
                });
            }
        }

        // tiered compaction
        let num_tiers_to_take = _snapshot.levels.len() - self.options.num_tiers + 2;
        return Some(TieredCompactionTask {
            tiers: _snapshot
                .levels
                .iter()
                .take(num_tiers_to_take)
                .cloned()
                .collect::<Vec<_>>(),
            bottom_tier_included: _snapshot.levels.len() >= num_tiers_to_take,
        });

        // None
    }

    pub fn apply_compaction_result(
        &self,
        _snapshot: &LsmStorageState,
        _task: &TieredCompactionTask,
        _output: &[usize],
    ) -> (LsmStorageState, Vec<usize>) {
        // in state lock
        let mut new_state = _snapshot.clone();
        let mut old_hash_ssts = _task
            .tiers
            .iter()
            .map(|(x, y)| (*x, y))
            .collect::<HashMap<_, _>>();
        let mut del_ssts = vec![];
        let mut new_levels = vec![];
        let mut new_ssts_added = false;
        for (tier_idx, ids) in &_snapshot.levels {
            if let Some(old_ssts) = old_hash_ssts.remove(tier_idx) {
                assert_eq!(old_ssts, ids, "ssts in a tier stays the same{:?}", old_ssts);
                del_ssts.extend(old_ssts);
            } else {
                new_levels.push((*tier_idx, ids.clone()));
            }
            if old_hash_ssts.is_empty() && !new_ssts_added {
                new_ssts_added = true;
                new_levels.push((_output[0], Vec::from(_output)));
            }
        }
        new_state.levels = new_levels;

        (new_state, del_ssts)
    }
}
