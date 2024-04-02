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
use crate::key::{KeyBytes, KeySlice};
use crate::lsm_storage::{LsmStorageInner, LsmStorageState};
use crate::manifest::ManifestRecord;
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
        let snapshot = {
            let state = self.state.read();
            state.clone()
        };
        match _task {
            CompactionTask::Leveled(task) => {
                let concat_ssts_lower = task
                    .lower_level_sst_ids
                    .iter()
                    .map(|id| snapshot.sstables.get(id).unwrap().clone())
                    .collect::<Vec<_>>();
                let lower_ite = SstConcatIterator::create_and_seek_to_first(concat_ssts_lower)?;
                if task.upper_level.is_some() {
                    let concat_ssts_upper = task
                        .upper_level_sst_ids
                        .iter()
                        .map(|id| snapshot.sstables.get(id).unwrap().clone())
                        .collect::<Vec<_>>();
                    let upper_ite = SstConcatIterator::create_and_seek_to_first(concat_ssts_upper)?;
                    let mut ite = TwoMergeIterator::create(upper_ite, lower_ite)?;
                    self.build_ssts_from_iter(&mut ite, task.is_lower_level_bottom_level)
                } else {
                    let merge_ites = task
                        .upper_level_sst_ids
                        .iter()
                        .map(|id| {
                            Box::new(
                                SsTableIterator::create_and_seek_to_first(
                                    snapshot.sstables.get(id).unwrap().clone(),
                                )
                                .unwrap(),
                            )
                        })
                        .collect::<Vec<_>>();
                    let upper_ite = MergeIterator::create(merge_ites);

                    let mut ite = TwoMergeIterator::create(upper_ite, lower_ite)?;
                    self.build_ssts_from_iter(&mut ite, task.is_lower_level_bottom_level)
                }
            }
            CompactionTask::Tiered(task) => {
                // let (runs, last) = task.tiers.split_at(task.tiers.len() - 1);
                let runs = &task.tiers;
                let merge_ssts = runs
                    .iter()
                    .map(|(_, ids)| {
                        let concat_inside_ssts = ids
                            .iter()
                            .map(|x| snapshot.sstables.get(x).unwrap().clone())
                            .collect::<Vec<_>>();

                        Box::new(
                            SstConcatIterator::create_and_seek_to_first(concat_inside_ssts)
                                .unwrap(),
                        )
                    })
                    .collect::<Vec<_>>();
                // let concat_ssts = last[0].1.iter().map(|id| {snapshot.sstables.get(id).unwrap().clone()})
                //     .collect::<Vec<_>>();
                let mut merge_ite = MergeIterator::create(merge_ssts);
                // let concat_ite = SstConcatIterator::create_and_seek_to_first(concat_ssts)?;
                // let mut two_merge_ite = TwoMergeIterator::create(merge_ite, concat_ite)?;

                self.build_ssts_from_iter(&mut merge_ite, false)
            }
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

                self.build_ssts_from_iter(&mut merge_two_iter, false)
            }
            CompactionTask::Simple(task) => {
                let upper_tables = task
                    .upper_level_sst_ids
                    .iter()
                    .map(|id| snapshot.sstables.get(id).unwrap().clone())
                    .collect::<Vec<_>>();

                let lower_tables = task
                    .lower_level_sst_ids
                    .iter()
                    .map(|id| snapshot.sstables.get(id).unwrap().clone())
                    .collect::<Vec<_>>();
                if task.upper_level.is_none() {
                    let upper_iter = MergeIterator::create(
                        upper_tables
                            .iter()
                            .map(|x| {
                                Box::new(
                                    SsTableIterator::create_and_seek_to_first(x.clone()).unwrap(),
                                )
                            })
                            .collect::<Vec<_>>(),
                    );
                    let lower_iter = SstConcatIterator::create_and_seek_to_first(lower_tables)?;
                    let mut merge_two_iter = TwoMergeIterator::create(upper_iter, lower_iter)?;
                    self.build_ssts_from_iter(&mut merge_two_iter, false)
                } else {
                    let upper_iter = SstConcatIterator::create_and_seek_to_first(upper_tables)?;
                    let lower_iter = SstConcatIterator::create_and_seek_to_first(lower_tables)?;
                    println!(
                        "[compact] upper table ids: {:?} lower table ids {:?}",
                        task.upper_level_sst_ids, task.lower_level_sst_ids
                    );
                    println!(
                        "[compact] upper table first key: {:?}",
                        bytes::Bytes::copy_from_slice(upper_iter.key().key_ref())
                    );
                    if lower_iter.is_valid() {
                        println!(
                            "[compact] lower table first key: {:?}",
                            bytes::Bytes::copy_from_slice(lower_iter.key().key_ref())
                        );
                    }
                    let mut merge_two_iter = TwoMergeIterator::create(upper_iter, lower_iter)?;
                    self.build_ssts_from_iter(&mut merge_two_iter, false)
                }
            }
        }
    }

    pub fn force_full_compaction(&self) -> Result<()> {
        let snapshot = {
            let state = self.state.read();
            state.clone()
        };
        let l0_ssts = snapshot.l0_sstables.clone();
        let l1_ssts = snapshot.levels[0].1.clone();
        let full_task = CompactionTask::ForceFullCompaction {
            l0_sstables: l0_ssts.clone(),
            l1_sstables: l1_ssts.clone(),
        };
        let new_l1_ssts = self.compact(&full_task)?;
        let man_record = ManifestRecord::Compaction(
            full_task,
            new_l1_ssts
                .iter()
                .map(|sst| sst.sst_id())
                .collect::<Vec<_>>(),
        );
        let mut ids = Vec::with_capacity(new_l1_ssts.len());
        {
            let _state_lock = self.state_lock.lock();
            let mut new_state = self.state.read().as_ref().clone();
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

            /* below is better */
            new_state.l0_sstables.retain(|x| !l0_hash_map.remove(x));
            new_state.levels[0].1 = ids;
            *self.state.write() = Arc::new(new_state);

            self.sync_dir()?;
            if let Some(man) = &self.manifest {
                man.add_record(&_state_lock, man_record)?;
            }
        }
        for old_sst_id in l0_ssts.iter().chain(l1_ssts.iter()) {
            std::fs::remove_file(self.path_of_sst(*old_sst_id))?;
        }
        self.sync_dir()?;
        Ok(())
    }

    fn trigger_compaction(&self) -> Result<()> {
        let prev_snapshot = {
            let state = self.state.read();
            state.clone()
        };
        if let Some(task) = self
            .compaction_controller
            .generate_compaction_task(&prev_snapshot)
        {
            // println!("[compact] before find a compact task");
            // self.dump_structure();
            // println!("[compact] enter gen ssds");
            let gen_ssts = self.compact(&task)?;
            let gen_ssts_id = gen_ssts.iter().map(|x| x.sst_id()).collect::<Vec<_>>();
            let delete_ssts =
                {
                    let _state_lock = self.state_lock.lock();
                    let mut prev_snapshot2 = self.state.read().as_ref().clone();
                    for sst in gen_ssts {
                        prev_snapshot2.sstables.insert(sst.sst_id(), sst);
                    }
                    let (snapshot, delete_ssts) = self
                        .compaction_controller
                        .apply_compaction_result(&prev_snapshot2, &task, &gen_ssts_id);
                    let mut write_gd = self.state.write();
                    *write_gd = Arc::new(snapshot);
                    self.sync_dir()?;
                    if let Some(man) = &self.manifest {
                        let man_record = ManifestRecord::Compaction(task, gen_ssts_id.clone());
                        man.add_record(&_state_lock, man_record)?;
                    }
                    delete_ssts
                };
            println!(
                "[compact] gen sst id: {:?}\n[compact]  delete sst: {:?}",
                gen_ssts_id, delete_ssts
            );
            for old_sst_id in delete_ssts {
                std::fs::remove_file(self.path_of_sst(old_sst_id))?;
            }
            self.sync_dir()?;
        }
        Ok(())
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
        let res = {
            let state = self.state.read();
            state.imm_memtables.len() >= self.options.num_memtable_limit
        };
        if res {
            println!("trigger flush");
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
        _is_bottom: bool,
    ) -> Result<Vec<Arc<SsTable>>> {
        let mut res = vec![];
        let mut sst_builder = SsTableBuilder::new(self.options.block_size);
        let mut cur_key;
        while iter.is_valid() {
            cur_key = KeyBytes::from_bytes_with_ts(
                Bytes::copy_from_slice(iter.key().key_ref()),
                iter.key().ts(),
            );
            while iter.key().key_ref() == cur_key.key_ref() {
                // while is_bottom && iter.is_valid() && iter.value().is_empty() {
                //     iter.next()?;
                // }
                if !iter.is_valid() {
                    break;
                }
                sst_builder.add(iter.key(), iter.value());
                iter.next()?;
            }
            // while is_bottom && iter.is_valid() && iter.value().is_empty() {
            //     iter.next()?;
            // }
            if !iter.is_valid() {
                break;
            }
            // sst_builder.add(iter.key(), iter.value());
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
            // iter.next()?;
        }
        if !sst_builder.is_empty() {
            let id = self.next_sst_id();
            res.push(Arc::new(sst_builder.build(
                id,
                Some(self.block_cache.clone()),
                self.path_of_sst(id),
            )?))
        }
        println!("[compact]  build new ssts finish");
        for table in &res {
            println!(
                "[compact]    first key {:?}, last key {:?}",
                bytes::Bytes::copy_from_slice(table.first_key().key_ref()),
                bytes::Bytes::copy_from_slice(table.last_key().key_ref())
            );
        }
        Ok(res)
    }
}
