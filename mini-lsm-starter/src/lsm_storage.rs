#![allow(dead_code)] // REMOVE THIS LINE after fully implementing this functionality

use std::collections::HashMap;
use std::ops::{Bound, Deref};
use std::path::{Path, PathBuf};
use std::sync::atomic::AtomicUsize;
use std::sync::Arc;
use std::time::Duration;

use anyhow::Context;
use anyhow::Result;
use bytes::Bytes;
use parking_lot::{Mutex, MutexGuard, RwLock};

use crate::block::Block;
use crate::compact::{
    CompactionController, CompactionOptions, LeveledCompactionController, LeveledCompactionOptions,
    SimpleLeveledCompactionController, SimpleLeveledCompactionOptions, TieredCompactionController,
};
use crate::iterators::concat_iterator::SstConcatIterator;
use crate::iterators::merge_iterator::MergeIterator;
use crate::iterators::two_merge_iterator::TwoMergeIterator;
use crate::iterators::StorageIterator;
use crate::key::*;
use crate::lsm_iterator::{FusedIterator, LsmIterator};
use crate::manifest::Manifest;
use crate::mem_table::{MemTable, MemTableIterator};
use crate::mvcc::LsmMvccInner;
use crate::table::{SsTable, SsTableBuilder, SsTableIterator};

pub type BlockCache = moka::sync::Cache<(usize, usize), Arc<Block>>;

/// Represents the state of the storage engine.
#[derive(Clone)]
pub struct LsmStorageState {
    /// The current memtable.
    pub memtable: Arc<MemTable>,
    /// Immutable memtables, from latest to earliest.
    pub imm_memtables: Vec<Arc<MemTable>>,
    /// L0 SSTs, from latest to earliest.
    pub l0_sstables: Vec<usize>,
    /// SsTables sorted by key range; L1 - L_max for leveled compaction, or tiers for tiered
    /// compaction.
    pub levels: Vec<(usize, Vec<usize>)>,
    /// SST objects.
    pub sstables: HashMap<usize, Arc<SsTable>>,
}

pub enum WriteBatchRecord<T: AsRef<[u8]>> {
    Put(T, T),
    Del(T),
}

impl LsmStorageState {
    fn create(options: &LsmStorageOptions) -> Self {
        let levels = match &options.compaction_options {
            CompactionOptions::Leveled(LeveledCompactionOptions { max_levels, .. })
            | CompactionOptions::Simple(SimpleLeveledCompactionOptions { max_levels, .. }) => (1
                ..=*max_levels)
                .map(|level| (level, Vec::new()))
                .collect::<Vec<_>>(),
            CompactionOptions::Tiered(_) => Vec::new(),
            CompactionOptions::NoCompaction => vec![(1, Vec::new())],
        };
        Self {
            memtable: Arc::new(MemTable::create(0)),
            imm_memtables: Vec::new(),
            l0_sstables: Vec::new(),
            levels,
            sstables: Default::default(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct LsmStorageOptions {
    // Block size in bytes
    pub block_size: usize,
    // SST size in bytes, also the approximate memtable capacity limit
    pub target_sst_size: usize,
    // Maximum number of memtables in memory, flush to L0 when exceeding this limit
    pub num_memtable_limit: usize,
    pub compaction_options: CompactionOptions,
    pub enable_wal: bool,
    pub serializable: bool,
}

impl LsmStorageOptions {
    pub fn default_for_week1_test() -> Self {
        Self {
            block_size: 4096,
            target_sst_size: 2 << 20,
            compaction_options: CompactionOptions::NoCompaction,
            enable_wal: false,
            num_memtable_limit: 50,
            serializable: false,
        }
    }

    pub fn default_for_week1_day6_test() -> Self {
        Self {
            block_size: 4096,
            target_sst_size: 2 << 20,
            compaction_options: CompactionOptions::NoCompaction,
            enable_wal: false,
            num_memtable_limit: 2,
            serializable: false,
        }
    }

    pub fn default_for_week2_test(compaction_options: CompactionOptions) -> Self {
        Self {
            block_size: 4096,
            target_sst_size: 1 << 20, // 1MB
            compaction_options,
            enable_wal: false,
            num_memtable_limit: 2,
            serializable: false,
        }
    }
}

#[derive(Clone, Debug)]
pub enum CompactionFilter {
    Prefix(Bytes),
}

/// The storage interface of the LSM tree.
pub(crate) struct LsmStorageInner {
    pub(crate) state: Arc<RwLock<Arc<LsmStorageState>>>,
    pub(crate) state_lock: Mutex<()>,
    path: PathBuf,
    pub(crate) block_cache: Arc<BlockCache>,
    next_sst_id: AtomicUsize,
    pub(crate) options: Arc<LsmStorageOptions>,
    pub(crate) compaction_controller: CompactionController,
    pub(crate) manifest: Option<Manifest>,
    pub(crate) mvcc: Option<LsmMvccInner>,
    pub(crate) compaction_filters: Arc<Mutex<Vec<CompactionFilter>>>,
}

/// A thin wrapper for `LsmStorageInner` and the user interface for MiniLSM.
pub struct MiniLsm {
    pub(crate) inner: Arc<LsmStorageInner>,
    /// Notifies the L0 flush thread to stop working. (In week 1 day 6)
    flush_notifier: crossbeam_channel::Sender<()>,
    /// The handle for the compaction thread. (In week 1 day 6)
    flush_thread: Mutex<Option<std::thread::JoinHandle<()>>>,
    /// Notifies the compaction thread to stop working. (In week 2)
    compaction_notifier: crossbeam_channel::Sender<()>,
    /// The handle for the compaction thread. (In week 2)
    compaction_thread: Mutex<Option<std::thread::JoinHandle<()>>>,
}

impl Drop for MiniLsm {
    fn drop(&mut self) {
        self.compaction_notifier.send(()).ok();
        self.flush_notifier.send(()).ok();
    }
}

impl MiniLsm {
    pub fn close(&self) -> Result<()> {
        self.flush_notifier.send(())?;
        let mut flush_th = self.flush_thread.lock();
        let flush = flush_th.take().unwrap();
        flush.join().unwrap();
        self.compaction_notifier.send(())?;
        let mut compact_th = self.compaction_thread.lock();
        let compact = compact_th.take().unwrap();
        compact.join().unwrap();
        Ok(())
    }

    /// Start the storage engine by either loading an existing directory or creating a new one if the directory does
    /// not exist.
    pub fn open(path: impl AsRef<Path>, options: LsmStorageOptions) -> Result<Arc<Self>> {
        let inner = Arc::new(LsmStorageInner::open(path, options)?);
        let (tx1, rx) = crossbeam_channel::unbounded();
        let compaction_thread = inner.spawn_compaction_thread(rx)?;
        let (tx2, rx) = crossbeam_channel::unbounded();
        let flush_thread = inner.spawn_flush_thread(rx)?;
        Ok(Arc::new(Self {
            inner,
            flush_notifier: tx2,
            flush_thread: Mutex::new(flush_thread),
            compaction_notifier: tx1,
            compaction_thread: Mutex::new(compaction_thread),
        }))
    }

    pub fn new_txn(&self) -> Result<()> {
        self.inner.new_txn()
    }

    pub fn write_batch<T: AsRef<[u8]>>(&self, batch: &[WriteBatchRecord<T>]) -> Result<()> {
        self.inner.write_batch(batch)
    }

    pub fn add_compaction_filter(&self, compaction_filter: CompactionFilter) {
        self.inner.add_compaction_filter(compaction_filter)
    }

    pub fn get(&self, key: &[u8]) -> Result<Option<Bytes>> {
        self.inner.get(key)
    }

    pub fn put(&self, key: &[u8], value: &[u8]) -> Result<()> {
        self.inner.put(key, value)
    }

    pub fn delete(&self, key: &[u8]) -> Result<()> {
        self.inner.delete(key)
    }

    pub fn sync(&self) -> Result<()> {
        self.inner.sync()
    }

    pub fn scan(
        &self,
        lower: Bound<&[u8]>,
        upper: Bound<&[u8]>,
    ) -> Result<FusedIterator<LsmIterator>> {
        self.inner.scan(lower, upper)
    }

    /// Only call this in test cases due to race conditions
    pub fn force_flush(&self) -> Result<()> {
        if !self.inner.state.read().memtable.is_empty() {
            self.inner
                .force_freeze_memtable(&self.inner.state_lock.lock())?;
        }
        if !self.inner.state.read().imm_memtables.is_empty() {
            self.inner.force_flush_next_imm_memtable()?;
        }
        Ok(())
    }

    pub fn force_full_compaction(&self) -> Result<()> {
        self.inner.force_full_compaction()
    }
}

impl LsmStorageInner {
    pub(crate) fn next_sst_id(&self) -> usize {
        self.next_sst_id
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst)
    }

    /// Start the storage engine by either loading an existing directory or creating a new one if the directory does
    /// not exist.
    pub(crate) fn open(path: impl AsRef<Path>, options: LsmStorageOptions) -> Result<Self> {
        let path = path.as_ref();
        if !path.exists() {
            std::fs::create_dir_all(path).context("failed to create DB dir")?;
        }
        let state = LsmStorageState::create(&options);

        let compaction_controller = match &options.compaction_options {
            CompactionOptions::Leveled(options) => {
                CompactionController::Leveled(LeveledCompactionController::new(options.clone()))
            }
            CompactionOptions::Tiered(options) => {
                CompactionController::Tiered(TieredCompactionController::new(options.clone()))
            }
            CompactionOptions::Simple(options) => CompactionController::Simple(
                SimpleLeveledCompactionController::new(options.clone()),
            ),
            CompactionOptions::NoCompaction => CompactionController::NoCompaction,
        };

        let storage = Self {
            state: Arc::new(RwLock::new(Arc::new(state))),
            state_lock: Mutex::new(()),
            path: path.to_path_buf(),
            block_cache: Arc::new(BlockCache::new(1024)),
            next_sst_id: AtomicUsize::new(1),
            compaction_controller,
            manifest: None,
            options: options.into(),
            mvcc: None,
            compaction_filters: Arc::new(Mutex::new(Vec::new())),
        };

        Ok(storage)
    }

    pub fn sync(&self) -> Result<()> {
        unimplemented!()
    }

    pub fn add_compaction_filter(&self, compaction_filter: CompactionFilter) {
        let mut compaction_filters = self.compaction_filters.lock();
        compaction_filters.push(compaction_filter);
    }

    /// Get a key from the storage. In day 7, this can be further optimized by using a bloom filter.
    /// from mut_mem to imm_mems to sst
    /// sstable: get
    ///     None: not exist
    ///     bytes.len == 0: deleted
    pub fn get(&self, _key: &[u8]) -> Result<Option<Bytes>> {
        let snapshot = {
            let guard = self.state.read();
            match guard.memtable.get(_key) {
                Some(x) if x.is_empty() => {
                    return Ok(None);
                }
                Some(x) => {
                    return Ok(Some(x));
                }
                None => {
                    for x in &guard.imm_memtables {
                        match x.get(_key) {
                            Some(y) if y.is_empty() => {
                                return Ok(None);
                            }
                            Some(y) => {
                                return Ok(Some(y));
                            }
                            None => {}
                        }
                    }
                }
            }
            guard.clone()
        };
        for i in &snapshot.l0_sstables {
            if let Some(table) = snapshot.sstables.get(i) {
                if !Self::key_within(_key, table.first_key(), table.last_key()) {
                    continue;
                }
                match table.get(_key)? {
                    Some(x) if !x.is_empty() => return Ok(Some(x)),
                    Some(x) if x.is_empty() => return Ok(None),
                    _ => {
                        continue;
                    }
                }
            }
        }
        let l1_tables = snapshot.levels[0]
            .1
            .iter()
            .map(|x| snapshot.sstables.get(x).unwrap().clone())
            .collect::<Vec<_>>();
        if l1_tables.is_empty() {
            return Ok(None);
        }
        let value = match l1_tables.partition_point(|x| x.first_key().raw_ref() < _key) {
            0 => l1_tables[0].get(_key)?,
            x if x == l1_tables.len() => l1_tables[x - 1].get(_key)?,
            x => l1_tables[x - 1].get(_key).map_or_else(
                |_| {
                    if l1_tables[x].first_key().raw_ref() == _key {
                        l1_tables[x].get(_key).unwrap()
                    } else {
                        None
                    }
                },
                |v| v,
            ),
        };

        Ok(value)
    }

    /// Write a batch of data into the storage. Implement in week 2 day 7.
    pub fn write_batch<T: AsRef<[u8]>>(&self, _batch: &[WriteBatchRecord<T>]) -> Result<()> {
        unimplemented!()
    }

    /// Put a key-value pair into the storage by writing into the current memtable.
    pub fn put(&self, _key: &[u8], _value: &[u8]) -> Result<()> {
        let guard = self.state.read();
        guard.memtable.put(_key, _value)?;
        if guard.memtable.approximate_size() >= self.options.target_sst_size {
            let state_lock = self.state_lock.lock();
            if guard.memtable.approximate_size() >= self.options.target_sst_size {
                std::mem::drop(guard);
                self.force_freeze_memtable(&state_lock)?;
            }
        }

        Ok(())
    }

    /// Remove a key from the storage by writing an empty value.
    pub fn delete(&self, _key: &[u8]) -> Result<()> {
        self.put(_key, b"")
    }

    pub(crate) fn path_of_sst_static(path: impl AsRef<Path>, id: usize) -> PathBuf {
        path.as_ref().join(format!("{:05}.sst", id))
    }

    pub(crate) fn path_of_sst(&self, id: usize) -> PathBuf {
        Self::path_of_sst_static(&self.path, id)
    }

    pub(crate) fn path_of_wal_static(path: impl AsRef<Path>, id: usize) -> PathBuf {
        path.as_ref().join(format!("{:05}.wal", id))
    }

    pub(crate) fn path_of_wal(&self, id: usize) -> PathBuf {
        Self::path_of_wal_static(&self.path, id)
    }

    pub(super) fn sync_dir(&self) -> Result<()> {
        unimplemented!()
    }

    /// Force freeze the current memtable to an immutable memtable
    pub fn force_freeze_memtable(&self, _state_lock_observer: &MutexGuard<'_, ()>) -> Result<()> {
        let next_sst_id = self.next_sst_id();
        let memtable = Arc::new(MemTable::create(next_sst_id));
        {
            // write lock state
            let mut wr_state = self.state.write();
            // clone state into a mutable one
            let mut snapshot = wr_state.as_ref().clone();
            // use new memtable to replace old one
            let old_mem = std::mem::replace(&mut snapshot.memtable, memtable);
            // insert old memtable to imm_memtable_vec
            snapshot.imm_memtables.insert(0, old_mem);
            // update state
            *wr_state = Arc::new(snapshot);
        }

        Ok(())
    }

    /// Force flush the earliest-created immutable memtable to disk
    pub fn force_flush_next_imm_memtable(&self) -> Result<()> {
        let _guard = self.state_lock.lock();
        let flush_memtable = {
            let read_gd = self.state.read();
            read_gd.imm_memtables.last().unwrap().clone()
        };
        let mut builder = SsTableBuilder::new(self.options.block_size);
        flush_memtable.flush(&mut builder)?;
        let sst_id = flush_memtable.id();
        let sst = builder.build(
            sst_id,
            Some(self.block_cache.clone()),
            self.path_of_sst(sst_id),
        )?;
        {
            let mut write_gd = self.state.write();
            let mut snapshot = write_gd.as_ref().clone();
            snapshot.imm_memtables.pop();
            println!("flushed {}.sst with size={}", sst_id, sst.table_size());

            snapshot.l0_sstables.insert(0, sst.sst_id());
            snapshot.sstables.insert(sst.sst_id(), Arc::new(sst));
            *write_gd = Arc::new(snapshot);
        }
        Ok(())
    }

    pub fn new_txn(&self) -> Result<()> {
        // no-op
        Ok(())
    }

    /// Create an iterator over a range of keys.
    pub fn scan(
        &self,
        _lower: Bound<&[u8]>,
        _upper: Bound<&[u8]>,
    ) -> Result<FusedIterator<LsmIterator>> {
        let (snapshot, mem_iter) = {
            let guard = self.state.read();

            let mut iters = vec![];
            let first_ite = Box::new(guard.memtable.scan(_lower, _upper));
            for table in guard.imm_memtables.iter().rev() {
                iters.insert(0, Box::new(table.scan(_lower, _upper)));
            }
            iters.insert(0, first_ite);

            (Arc::clone(&guard), MergeIterator::create(iters))
        };

        let mut ss_iters = vec![];
        for idx in &snapshot.l0_sstables {
            if let Some(table) = snapshot.sstables.get(idx) {
                if !Self::range_overlap(table.first_key(), table.last_key(), _lower, _upper) {
                    continue;
                }
                let iter = match _lower {
                    Bound::Included(x) => SsTableIterator::create_and_seek_to_key(
                        Arc::clone(table),
                        Key::from_slice(x),
                    )?,
                    Bound::Excluded(x) => {
                        let mut ite = SsTableIterator::create_and_seek_to_key(
                            Arc::clone(table),
                            Key::from_slice(x),
                        )?;
                        if ite.key().raw_ref() == x {
                            ite.next()?;
                        }
                        ite
                    }
                    _ => SsTableIterator::create_and_seek_to_first(Arc::clone(table))?,
                };
                ss_iters.push(Box::new(iter));
            }
        }
        let ss_iter = MergeIterator::create(ss_iters);
        let two_merge_iter = TwoMergeIterator::create(mem_iter, ss_iter)?;
        let concat_iter = SstConcatIterator::create_and_seek_to_first(
            snapshot.levels[0]
                .1
                .iter()
                .map(|x| snapshot.sstables.get(x).unwrap().clone())
                .collect::<Vec<_>>(),
        )?;
        let two_merge_iter = TwoMergeIterator::create(two_merge_iter, concat_iter)?;
        Ok(FusedIterator::new(LsmIterator::new(
            two_merge_iter,
            _upper,
        )?))
    }

    fn range_overlap(
        first_key: &KeyBytes,
        last_key: &KeyBytes,
        lower: Bound<&[u8]>,
        upper: Bound<&[u8]>,
    ) -> bool {
        // last key < lower: bye
        // first key > upper: bye
        match lower {
            Bound::Included(x) => {
                if last_key.raw_ref() < x {
                    return false;
                }
            }
            Bound::Excluded(x) => {
                if last_key.raw_ref() <= x {
                    return false;
                }
            }
            _ => {}
        }
        match upper {
            Bound::Included(x) => {
                if first_key.raw_ref() > x {
                    return false;
                }
            }
            Bound::Excluded(x) => {
                if first_key.raw_ref() >= x {
                    return false;
                }
            }
            _ => {}
        }
        true
    }

    fn key_within(key: &[u8], first_key: &KeyBytes, last_key: &KeyBytes) -> bool {
        key >= first_key.raw_ref() && key <= last_key.raw_ref()
    }
}
