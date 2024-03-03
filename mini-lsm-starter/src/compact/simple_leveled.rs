use serde::{Deserialize, Serialize};
use std::collections::HashSet;

use crate::lsm_storage::LsmStorageState;

#[derive(Debug, Clone)]
pub struct SimpleLeveledCompactionOptions {
    pub size_ratio_percent: usize,
    pub level0_file_num_compaction_trigger: usize,
    pub max_levels: usize,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct SimpleLeveledCompactionTask {
    // if upper_level is `None`, then it is L0 compaction
    pub upper_level: Option<usize>,
    pub upper_level_sst_ids: Vec<usize>,
    pub lower_level: usize,
    pub lower_level_sst_ids: Vec<usize>,
    pub is_lower_level_bottom_level: bool,
}

pub struct SimpleLeveledCompactionController {
    options: SimpleLeveledCompactionOptions,
}

impl SimpleLeveledCompactionController {
    pub fn new(options: SimpleLeveledCompactionOptions) -> Self {
        Self { options }
    }

    /// Generates a compaction task.
    ///
    /// Returns `None` if no compaction needs to be scheduled. The order of SSTs in the compaction task id vector matters.
    pub fn generate_compaction_task(
        &self,
        _snapshot: &LsmStorageState,
    ) -> Option<SimpleLeveledCompactionTask> {
        let mut lv_sizes = vec![];
        lv_sizes.push(_snapshot.l0_sstables.len());
        for (_, i) in &_snapshot.levels {
            lv_sizes.push(i.len());
        }
        assert!(lv_sizes.len() == self.options.max_levels + 1);
        for lv in 0..lv_sizes.len() - 1 {
            if lv == 0 && lv_sizes[0] < self.options.level0_file_num_compaction_trigger {
                continue;
            }
            let ratio = lv_sizes[lv + 1] as f64 / lv_sizes[lv] as f64;
            if ratio < self.options.size_ratio_percent as f64 / 100 as f64 {
                println!("[gen task] lower level: {:?}", lv + 1);
                return Some(SimpleLeveledCompactionTask {
                    upper_level: if lv == 0 { None } else { Some(lv) },
                    upper_level_sst_ids: if lv == 0 {
                        _snapshot.l0_sstables.clone()
                    } else {
                        _snapshot.levels[lv - 1].1.clone()
                    },
                    lower_level: lv + 1,
                    lower_level_sst_ids: _snapshot.levels[lv].1.clone(),
                    is_lower_level_bottom_level: lv + 1 == self.options.max_levels,
                });
            }
        }
        None
    }

    /// Apply the compaction result.
    ///
    /// The compactor will call this function with the compaction task and the list of SST ids generated. This function applies the
    /// result and generates a new LSM state. The functions should only change `l0_sstables` and `levels` without changing memtables
    /// and `sstables` hash map. Though there should only be one thread running compaction jobs, you should think about the case
    /// where an L0 SST gets flushed while the compactor generates new SSTs, and with that in mind, you should do some sanity checks
    /// in your implementation.
    pub fn apply_compaction_result(
        &self,
        _snapshot: &LsmStorageState,
        _task: &SimpleLeveledCompactionTask,
        _output: &[usize],
    ) -> (LsmStorageState, Vec<usize>) {
        let mut snapshot = _snapshot.clone();
        snapshot.levels[_task.lower_level - 1].1 = Vec::from(_output);

        if _task.upper_level.is_none() {
            // l0 compacted to l1
            let mut old_sst_ids = _task
                .upper_level_sst_ids
                .iter()
                .copied()
                .collect::<HashSet<_>>();
            let new_sst_ids = snapshot
                .l0_sstables
                .iter()
                .copied()
                .filter(|x| !old_sst_ids.remove(x))
                .collect::<Vec<_>>();

            snapshot.l0_sstables = new_sst_ids;
        } else {
            snapshot.levels[_task.upper_level.unwrap() - 1].1 = vec![];
        }

        let mut delete_sst_ids = _task.lower_level_sst_ids.clone();
        delete_sst_ids.extend_from_slice(&_task.upper_level_sst_ids[..]);
        (snapshot, delete_sst_ids)
    }
}
