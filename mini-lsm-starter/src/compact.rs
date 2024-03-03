#![allow(dead_code)] // REMOVE THIS LINE after fully implementing this functionality

mod leveled;
mod simple_leveled;
mod tiered;

use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
pub use leveled::{LeveledCompactionController, LeveledCompactionOptions, LeveledCompactionTask};
use serde::{Deserialize, Serialize};
pub use simple_leveled::{
    SimpleLeveledCompactionController, SimpleLeveledCompactionOptions, SimpleLeveledCompactionTask,
};
pub use tiered::{TieredCompactionController, TieredCompactionOptions, TieredCompactionTask};

use crate::iterators::concat_iterator::SstConcatIterator;
use crate::iterators::merge_iterator::MergeIterator;
use crate::iterators::two_merge_iterator::TwoMergeIterator;
use crate::iterators::StorageIterator;
use crate::key::KeySlice;
use crate::lsm_storage::{LsmStorageInner, LsmStorageState};
use crate::table::{SsTable, SsTableBuilder, SsTableIterator};
use bytes::Bytes;

#[derive(Debug, Serialize, Deserialize)]
pub enum CompactionTask {
    Leveled(LeveledCompactionTask),
    Tiered(TieredCompactionTask),
    Simple(SimpleLeveledCompactionTask),
    ForceFullCompaction {
        l0_sstables: Vec<usize>,
        l1_sstables: Vec<usize>,
    },
}

impl CompactionTask {
    fn compact_to_bottom_level(&self) -> bool {
        match self {
            CompactionTask::ForceFullCompaction { .. } => true,
            CompactionTask::Leveled(task) => task.is_lower_level_bottom_level,
            CompactionTask::Simple(task) => task.is_lower_level_bottom_level,
            CompactionTask::Tiered(task) => task.bottom_tier_included,
        }
    }
}

pub(crate) enum CompactionController {
    Leveled(LeveledCompactionController),
    Tiered(TieredCompactionController),
    Simple(SimpleLeveledCompactionController),
    NoCompaction,
}

impl CompactionController {
    pub fn generate_compaction_task(&self, snapshot: &LsmStorageState) -> Option<CompactionTask> {
        match self {
            CompactionController::Leveled(ctrl) => ctrl
                .generate_compaction_task(snapshot)
                .map(CompactionTask::Leveled),
            CompactionController::Simple(ctrl) => ctrl
                .generate_compaction_task(snapshot)
                .map(CompactionTask::Simple),
            CompactionController::Tiered(ctrl) => ctrl
                .generate_compaction_task(snapshot)
                .map(CompactionTask::Tiered),
            CompactionController::NoCompaction => unreachable!(),
        }
    }

    pub fn apply_compaction_result(
        &self,
        snapshot: &LsmStorageState,
        task: &CompactionTask,
        output: &[usize],
    ) -> (LsmStorageState, Vec<usize>) {
        match (self, task) {
            (CompactionController::Leveled(ctrl), CompactionTask::Leveled(task)) => {
                ctrl.apply_compaction_result(snapshot, task, output)
            }
            (CompactionController::Simple(ctrl), CompactionTask::Simple(task)) => {
                ctrl.apply_compaction_result(snapshot, task, output)
            }
            (CompactionController::Tiered(ctrl), CompactionTask::Tiered(task)) => {
                ctrl.apply_compaction_result(snapshot, task, output)
            }
            _ => unreachable!(),
        }
    }
}

impl CompactionController {
    pub fn flush_to_l0(&self) -> bool {
        matches!(
            self,
            Self::Leveled(_) | Self::Simple(_) | Self::NoCompaction
        )
    }
}

#[derive(Debug, Clone)]
pub enum CompactionOptions {
    /// Leveled compaction with partial compaction + dynamic level support (= RocksDB's Leveled
    /// Compaction)
    Leveled(LeveledCompactionOptions),
    /// Tiered compaction (= RocksDB's universal compaction)
    Tiered(TieredCompactionOptions),
    /// Simple leveled compaction
    Simple(SimpleLeveledCompactionOptions),
    /// In no compaction mode (week 1), always flush to L0
    NoCompaction,
}

impl LsmStorageInner {
    fn compact(&self, _task: &CompactionTask) -> Result<Vec<Arc<SsTable>>> {
        let snapshot = self.state.read().clone();
        match _task {
            CompactionTask::ForceFullCompaction {
                l0_sstables,
                l1_sstables,
            } => {
                let l0_iters = l0_sstables
                    .iter()
                    .map(|x| {
                        Box::new(
                            SsTableIterator::create_and_seek_to_first(
                                snapshot.sstables.get(x).unwrap().clone(),
                            )
                            .unwrap(),
                        )
                    })
                    .collect::<Vec<_>>();
                let l1_tables = l1_sstables
                    .iter()
                    .map(|x| snapshot.sstables.get(x).unwrap().clone())
                    .collect::<Vec<_>>();
                let l0_merge_iter = MergeIterator::create(l0_iters);
                let l1_iter = SstConcatIterator::create_and_seek_to_first(l1_tables)?;
                let mut merge_two_iter = TwoMergeIterator::create(l0_merge_iter, l1_iter)?;

                self.build_ssts_from_iter(&mut merge_two_iter)
            }
            _ => Ok(vec![]),
        }
    }

    pub fn force_full_compaction(&self) -> Result<()> {
        let snapshot = self.state.read().clone();
        let l0_ssts = snapshot.l0_sstables.clone();
        let l1_ssts = snapshot.levels[0].1.clone();
        let full_task = CompactionTask::ForceFullCompaction {
            l0_sstables: l0_ssts.clone(),
            l1_sstables: l1_ssts.clone(),
        };
        let new_l1_ssts = self.compact(&full_task)?;
        let mut ids = Vec::with_capacity(new_l1_ssts.len());
        {
            let state_lock = self.state_lock.lock();
            let mut new_state = self.state.write().as_ref().clone();
            for sst in new_l1_ssts {
                ids.push(sst.sst_id());
                let res = new_state.sstables.insert(sst.sst_id(), sst);
                assert!(res.is_none());
            }
            for old_sst_id in l0_ssts.iter().chain(l1_ssts.iter()) {
                let res = new_state.sstables.remove(old_sst_id);
                assert!(res.is_some());
            }
            let mut l0_hash_map = l0_ssts.iter().copied().collect::<HashSet<_>>();
            new_state.l0_sstables = new_state
                .l0_sstables
                .iter()
                .filter(|x| !l0_hash_map.remove(x))
                .copied()
                .collect();
            new_state.levels[0].1 = ids;
            *self.state.write() = Arc::new(new_state);
        }
        for old_sst_id in l0_ssts.iter().chain(l1_ssts.iter()) {
            std::fs::remove_file(self.path_of_sst(*old_sst_id))?;
        }
        Ok(())
    }

    fn trigger_compaction(&self) -> Result<()> {
        unimplemented!()
    }

    pub(crate) fn spawn_compaction_thread(
        self: &Arc<Self>,
        rx: crossbeam_channel::Receiver<()>,
    ) -> Result<Option<std::thread::JoinHandle<()>>> {
        if let CompactionOptions::Leveled(_)
        | CompactionOptions::Simple(_)
        | CompactionOptions::Tiered(_) = self.options.compaction_options
        {
            let this = self.clone();
            let handle = std::thread::spawn(move || {
                let ticker = crossbeam_channel::tick(Duration::from_millis(50));
                loop {
                    crossbeam_channel::select! {
                        recv(ticker) -> _ => if let Err(e) = this.trigger_compaction() {
                            eprintln!("compaction failed: {}", e);
                        },
                        recv(rx) -> _ => return
                    }
                }
            });
            return Ok(Some(handle));
        }
        Ok(None)
    }

    fn trigger_flush(&self) -> Result<()> {
        let read_gd = self.state.read();
        if read_gd.imm_memtables.len() >= self.options.num_memtable_limit {
            std::mem::drop(read_gd);
            self.force_flush_next_imm_memtable()?;
        }
        Ok(())
    }

    pub(crate) fn spawn_flush_thread(
        self: &Arc<Self>,
        rx: crossbeam_channel::Receiver<()>,
    ) -> Result<Option<std::thread::JoinHandle<()>>> {
        let this = self.clone();
        let handle = std::thread::spawn(move || {
            let ticker = crossbeam_channel::tick(Duration::from_millis(50));
            loop {
                crossbeam_channel::select! {
                    recv(ticker) -> _ => if let Err(e) = this.trigger_flush() {
                        eprintln!("flush failed: {}", e);
                    },
                    recv(rx) -> _ => return
                }
            }
        });
        Ok(Some(handle))
    }

    fn build_ssts_from_iter(
        &self,
        iter: &mut impl for<'a> StorageIterator<KeyType<'a> = KeySlice<'a>>,
    ) -> Result<Vec<Arc<SsTable>>> {
        let mut res = vec![];
        let mut sst_builder = SsTableBuilder::new(self.options.block_size);
        println!("enter build ssts");
        while iter.is_valid() {
            println!(
                "key: {:?}",
                bytes::Bytes::copy_from_slice(iter.key().raw_ref())
            );
            while iter.is_valid() && iter.value().is_empty() {
                println!(
                    "   pass key: {:?}",
                    bytes::Bytes::copy_from_slice(iter.key().raw_ref())
                );
                iter.next()?;
            }
            if !iter.is_valid() {
                break;
            }
            sst_builder.add(iter.key(), iter.value());
            if sst_builder.estimated_size() >= self.options.target_sst_size {
                let old_sst_builder = std::mem::replace(
                    &mut sst_builder,
                    SsTableBuilder::new(self.options.block_size),
                );
                let id = self.next_sst_id();
                res.push(Arc::new(old_sst_builder.build(
                    id,
                    Some(self.block_cache.clone()),
                    self.path_of_sst(id),
                )?));
            }
            iter.next()?;
        }
        if !sst_builder.is_empty() {
            let id = self.next_sst_id();
            res.push(Arc::new(sst_builder.build(
                id,
                Some(self.block_cache.clone()),
                self.path_of_sst(id),
            )?))
        }
        Ok(res)
    }
}
