use std::{collections::HashSet, fmt::LowerExp, hash::Hash};

use serde::{Deserialize, Serialize};

use crate::lsm_storage::LsmStorageState;

#[derive(Debug, Serialize, Deserialize)]
pub struct LeveledCompactionTask {
    // if upper_level is `None`, then it is L0 compaction
    pub upper_level: Option<usize>,
    pub upper_level_sst_ids: Vec<usize>,
    pub lower_level: usize,
    pub lower_level_sst_ids: Vec<usize>,
    pub is_lower_level_bottom_level: bool,
}

#[derive(Debug, Clone)]
pub struct LeveledCompactionOptions {
    pub level_size_multiplier: usize,
    pub level0_file_num_compaction_trigger: usize,
    pub max_levels: usize,
    pub base_level_size_mb: usize,
}

pub struct LeveledCompactionController {
    options: LeveledCompactionOptions,
}

impl LeveledCompactionController {
    pub fn new(options: LeveledCompactionOptions) -> Self {
        Self { options }
    }

    fn find_overlapping_ssts(
        &self,
        _snapshot: &LsmStorageState,
        _sst_ids: &[usize],
        _in_level: usize,
    ) -> Vec<usize> {
        let upper_min_key = _sst_ids
            .iter()
            .map(|id| _snapshot.sstables.get(id).unwrap().first_key())
            .min()
            .unwrap();
        let upper_max_key = _sst_ids
            .iter()
            .map(|id| _snapshot.sstables.get(id).unwrap().last_key())
            .max()
            .unwrap();

        let lower_len = _snapshot.levels[_in_level - 1].1.len();
        let first_id = _snapshot.levels[_in_level - 1]
            .1
            .partition_point(|id| _snapshot.sstables.get(id).unwrap().last_key() < upper_min_key);
        if first_id == lower_len {
            // no overlap
            return vec![];
        }
        let last_id = _snapshot.levels[_in_level - 1]
            .1
            .partition_point(|id| _snapshot.sstables.get(id).unwrap().first_key() <= upper_max_key);
        if last_id == 0 {
            return vec![];
        }

        if last_id <= first_id {
            return vec![];
        }

        Vec::from(&_snapshot.levels[_in_level - 1].1[first_id..last_id])
    }

    pub fn generate_compaction_task(
        &self,
        _snapshot: &LsmStorageState,
    ) -> Option<LeveledCompactionTask> {
        // 0..=levels-1 => 1..=levels
        let level_sz = _snapshot
            .levels
            .iter()
            .map(|(_, ids)| {
                if ids.is_empty() {
                    0_usize
                } else {
                    ids.iter()
                        .map(|id| _snapshot.sstables.get(id).unwrap().table_size())
                        .sum::<u64>() as usize
                }
            })
            .collect::<Vec<_>>();
        let valid_idx = level_sz.partition_point(|&x| x == 0); // first idx with size > 0
        if _snapshot.l0_sstables.len() >= self.options.level0_file_num_compaction_trigger {
            if valid_idx == level_sz.len() {
                return Some(LeveledCompactionTask {
                    upper_level: None,
                    upper_level_sst_ids: _snapshot.l0_sstables.clone(),
                    lower_level: valid_idx,
                    lower_level_sst_ids: vec![],
                    is_lower_level_bottom_level: true,
                });
            }
            if level_sz[valid_idx] >= self.options.base_level_size_mb * 1024 * 1024
                && valid_idx != 0
            {
                return Some(LeveledCompactionTask {
                    upper_level: None,
                    upper_level_sst_ids: _snapshot.l0_sstables.clone(),
                    lower_level: valid_idx,
                    lower_level_sst_ids: vec![],
                    is_lower_level_bottom_level: false,
                });
            }
            return Some(LeveledCompactionTask {
                upper_level: None,
                upper_level_sst_ids: _snapshot.l0_sstables.clone(),
                lower_level: valid_idx + 1,
                lower_level_sst_ids: self.find_overlapping_ssts(
                    _snapshot,
                    &_snapshot.l0_sstables,
                    valid_idx + 1,
                ),
                is_lower_level_bottom_level: valid_idx + 1 == level_sz.len(),
            });
        }
        if valid_idx >= level_sz.len() - 1 {
            return None;
        }
        let level_sz = level_sz
            .into_iter()
            .map(|x| (x / 1024 / 1024) as f64)
            .collect::<Vec<_>>(); // mb & valid_idx
        println!("level sz: {:?}", level_sz);
        let mut target_sz = if *level_sz.last().unwrap() > self.options.base_level_size_mb as f64 {
            *level_sz.last().unwrap()
        } else {
            self.options.base_level_size_mb as f64
        };

        let mut pri = vec![];
        for lv_idx in (valid_idx..=level_sz.len() - 2).rev() {
            target_sz = target_sz / self.options.level_size_multiplier as f64;
            pri.push((lv_idx + 1, level_sz[lv_idx] / target_sz));
        }
        pri.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap());
        println!("pri: {:?}", pri);
        if pri.last().unwrap().1 > 1.0 {
            // find level
            let lv = pri.last().unwrap().0;
            let upper_lv_ids = vec![*_snapshot.levels[lv - 1].1.last().unwrap()];
            return Some(LeveledCompactionTask {
                lower_level: lv + 1,
                lower_level_sst_ids: self.find_overlapping_ssts(_snapshot, &upper_lv_ids, lv + 1),
                is_lower_level_bottom_level: lv + 1 == level_sz.len(),
                upper_level: Some(lv),
                upper_level_sst_ids: upper_lv_ids,
            });
        }

        None
    }

    pub fn apply_compaction_result(
        &self,
        _snapshot: &LsmStorageState,
        _task: &LeveledCompactionTask,
        _output: &[usize],
    ) -> (LsmStorageState, Vec<usize>) {
        let mut new_state = _snapshot.clone();
        let mut old_upper_ids = _task
            .upper_level_sst_ids
            .iter()
            .copied()
            .collect::<HashSet<_>>();
        let mut old_lower_ids = _task
            .lower_level_sst_ids
            .iter()
            .copied()
            .collect::<HashSet<_>>();
        if let Some(lv) = _task.upper_level {
            new_state.levels[lv - 1]
                .1
                .retain(|id| !old_upper_ids.remove(&id));
        } else {
            new_state
                .l0_sstables
                .retain(|id| !old_upper_ids.remove(&id));
        }
        new_state.levels[_task.lower_level - 1]
            .1
            .retain(|id| !old_lower_ids.remove(id));

        // insert new sst
        new_state.levels[_task.lower_level - 1]
            .1
            .extend_from_slice(_output);
        if !new_state.sstables.is_empty() {
            new_state.levels[_task.lower_level - 1].1.sort_by(|a, b| {
                _snapshot
                    .sstables
                    .get(a)
                    .unwrap()
                    .first_key()
                    .cmp(_snapshot.sstables.get(b).unwrap().first_key())
            });
        }

        let mut del_ids = vec![];
        del_ids.extend(&_task.upper_level_sst_ids);
        del_ids.extend(&_task.lower_level_sst_ids);

        (new_state, del_ids)
    }
}
