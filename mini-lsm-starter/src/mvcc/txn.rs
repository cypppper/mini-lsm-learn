#![allow(unused_variables)] // TODO(you): remove this lint after implementing this mod
#![allow(dead_code)] // TODO(you): remove this lint after implementing this mod

use std::{
    collections::HashSet,
    ops::Bound,
    sync::{atomic::AtomicBool, Arc},
};

use anyhow::Result;
use bytes::Bytes;
use crossbeam_skiplist::SkipMap;
use ouroboros::self_referencing;
use parking_lot::Mutex;
use serde::de::value;

use crate::{
    iterators::{two_merge_iterator::TwoMergeIterator, StorageIterator},
    lsm_iterator::{FusedIterator, LsmIterator},
    lsm_storage::{LsmStorageInner, WriteBatchRecord},
};

pub struct Transaction {
    pub(crate) read_ts: u64,
    pub(crate) inner: Arc<LsmStorageInner>,
    pub(crate) local_storage: Arc<SkipMap<Bytes, Bytes>>,
    pub(crate) committed: Arc<AtomicBool>,
    /// Write set and read set
    pub(crate) key_hashes: Option<Mutex<(HashSet<u32>, HashSet<u32>)>>,
}

impl Transaction {
    fn map_ref_to_bytes_bound(ori: Bound<&[u8]>) -> Bound<Bytes> {
        match ori {
            Bound::Included(x) => Bound::Included(Bytes::copy_from_slice(x)),
            Bound::Excluded(x) => Bound::Excluded(Bytes::copy_from_slice(x)),
            Bound::Unbounded => Bound::Unbounded,
        }
    }

    pub fn get(&self, key: &[u8]) -> Result<Option<Bytes>> {
        if self.committed.load(std::sync::atomic::Ordering::SeqCst) {
            panic!("can NOT get in a committed txn");
        }
        let value_bytes = if let Some(local_get_res) = self.local_storage.get(key) {
            let local_res = local_get_res.value().clone();
            if local_res.is_empty() {
                None
            } else {
                Some(local_res)
            }
        } else {
            self.inner.get_with_ts(key, self.read_ts)?
        };

        Ok(value_bytes)
    }

    pub fn scan(self: &Arc<Self>, lower: Bound<&[u8]>, upper: Bound<&[u8]>) -> Result<TxnIterator> {
        if self.committed.load(std::sync::atomic::Ordering::SeqCst) {
            panic!("can NOT scan in a committed txn");
        }
        let scan_ite = self.inner.scan_with_ts(lower, upper, self.read_ts)?;
        let lower_bd = Self::map_ref_to_bytes_bound(lower);
        let upper_bd = Self::map_ref_to_bytes_bound(upper);
        let mut local_ite: TxnLocalIterator = TxnLocalIteratorBuilder {
            map: self.local_storage.clone(),
            iter_builder: |map| map.range((lower_bd, upper_bd)),
            item: (Bytes::new(), Bytes::new()),
        }
        .build();
        local_ite.next()?; /* init */
        let two_merge_ite = TwoMergeIterator::create(local_ite, scan_ite)?;
        TxnIterator::create(self.clone(), two_merge_ite)
    }

    pub fn put(&self, key: &[u8], value: &[u8]) {
        if self.committed.load(std::sync::atomic::Ordering::SeqCst) {
            panic!("can NOT put in a committed txn");
        }
        self.local_storage
            .insert(Bytes::copy_from_slice(key), Bytes::copy_from_slice(value));
    }

    pub fn delete(&self, key: &[u8]) {
        if self.committed.load(std::sync::atomic::Ordering::SeqCst) {
            panic!("can NOT load in a committed txn");
        }
        self.local_storage
            .insert(Bytes::copy_from_slice(key), Bytes::new());
    }

    pub fn commit(&self) -> Result<()> {
        self.committed
            .compare_exchange(
                false,
                true,
                std::sync::atomic::Ordering::SeqCst,
                std::sync::atomic::Ordering::SeqCst,
            )
            .expect("cannot do commit in a committed txn!");
        let _commit_lk = self.inner.mvcc().commit_lock.lock();
        let batch = self
            .local_storage
            .iter()
            .map(|entry| {
                if !entry.value().is_empty() {
                    WriteBatchRecord::Put(entry.key().clone(), entry.value().clone())
                } else {
                    WriteBatchRecord::Del(entry.key().clone())
                }
            })
            .collect::<Vec<_>>();
        self.inner.write_batch(&batch)?;

        Ok(())
    }
}

impl Drop for Transaction {
    fn drop(&mut self) {
        if let Some(mvcc) = &self.inner.mvcc {
            mvcc.ts.lock().1.remove_reader(self.read_ts);
        }
    }
}

type SkipMapRangeIter<'a> =
    crossbeam_skiplist::map::Range<'a, Bytes, (Bound<Bytes>, Bound<Bytes>), Bytes, Bytes>;

#[self_referencing]
pub struct TxnLocalIterator {
    /// Stores a reference to the skipmap.
    map: Arc<SkipMap<Bytes, Bytes>>,
    /// Stores a skipmap iterator that refers to the lifetime of `MemTableIterator` itself.
    #[borrows(map)]
    #[not_covariant]
    iter: SkipMapRangeIter<'this>,
    /// Stores the current key-value pair.
    item: (Bytes, Bytes),
}

impl StorageIterator for TxnLocalIterator {
    type KeyType<'a> = &'a [u8];

    fn value(&self) -> &[u8] {
        self.with_item(|(k, v)| &v[..])
    }

    fn key(&self) -> &[u8] {
        self.with_item(|(k, v)| &k[..])
    }

    fn is_valid(&self) -> bool {
        !self.key().is_empty()
    }

    fn next(&mut self) -> Result<()> {
        let new_item = self.with_iter_mut(|ite| {
            let entry = ite.next();
            if let Some(ent) = entry {
                (ent.key().clone(), ent.value().clone())
            } else {
                (Bytes::new(), Bytes::new())
            }
        });
        self.with_item_mut(|x| *x = new_item);
        Ok(())
    }
}

pub struct TxnIterator {
    txn: Arc<Transaction>,
    iter: TwoMergeIterator<TxnLocalIterator, FusedIterator<LsmIterator>>,
}

impl TxnIterator {
    pub fn create(
        txn: Arc<Transaction>,
        iter: TwoMergeIterator<TxnLocalIterator, FusedIterator<LsmIterator>>,
    ) -> Result<Self> {
        Ok(Self { txn, iter })
    }

    fn skip_deleted(&mut self) -> Result<()> {
        while self.is_valid() && self.value().is_empty() {
            self.next()?;
        }
        Ok(())
    }
}

impl StorageIterator for TxnIterator {
    type KeyType<'a> = &'a [u8] where Self: 'a;

    fn value(&self) -> &[u8] {
        self.iter.value()
    }

    fn key(&self) -> Self::KeyType<'_> {
        self.iter.key()
    }

    fn is_valid(&self) -> bool {
        self.iter.is_valid()
    }

    fn next(&mut self) -> Result<()> {
        self.iter.next()?;
        self.skip_deleted()
    }

    fn num_active_iterators(&self) -> usize {
        self.iter.num_active_iterators()
    }
}
