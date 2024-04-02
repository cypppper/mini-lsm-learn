use std::borrow::BorrowMut;
use std::default;
use std::ops::Bound;
use std::path::Path;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use anyhow::Result;
use bytes::Bytes;
use crossbeam_skiplist::SkipMap;
use nom::AsBytes;
use ouroboros::self_referencing;

use crate::iterators::StorageIterator;
use crate::key::{Key, KeyBytes, KeySlice, TS_DEFAULT, TS_RANGE_BEGIN, TS_RANGE_END};
use crate::table::SsTableBuilder;
use crate::wal::Wal;

/// A basic mem-table based on crossbeam-skiplist.
///
/// An initial implementation of memtable is part of week 1, day 1. It will be incrementally implemented in other
/// chapters of week 1 and week 2.

#[derive(Default)]
pub struct MemTable {
    // map: Arc<SkipMap<Bytes, Bytes>>,
    map: Arc<SkipMap<KeyBytes, Bytes>>,
    wal: Option<Wal>,
    id: usize,
    approximate_size: Arc<AtomicUsize>,
}

/// Create a bound of `Bytes` from a bound of `&[u8]`.
pub(crate) fn map_bound(bound: Bound<&[u8]>, is_lower: bool) -> Bound<KeyBytes> {
    match bound {
        Bound::Included(x) => {
            let key = KeyBytes::from_bytes_with_ts(
                Bytes::from_static(unsafe { std::mem::transmute(x) }),
                if is_lower {
                    TS_RANGE_BEGIN
                } else {
                    TS_RANGE_END
                },
            );
            Bound::Included(key)
        }
        Bound::Excluded(x) => {
            let key = KeyBytes::from_bytes_with_ts(
                Bytes::from_static(unsafe { std::mem::transmute(x) }),
                if is_lower {
                    TS_RANGE_END
                } else {
                    TS_RANGE_BEGIN
                },
            );
            Bound::Excluded(key)
        }
        Bound::Unbounded => Bound::Unbounded,
    }
}

impl MemTable {
    /// Create a new mem-table.
    pub fn create(_id: usize) -> Self {
        Self {
            id: _id,
            ..Default::default()
        }
    }

    /// Create a new mem-table with WAL
    pub fn create_with_wal(_id: usize, _path: impl AsRef<Path>) -> Result<Self> {
        let wal = Wal::create(_path)?;
        Ok(Self {
            wal: Some(wal),
            id: _id,
            ..Default::default()
        })
    }

    /// Create a memtable from WAL
    pub fn recover_from_wal(_id: usize, _path: impl AsRef<Path>, max_ts: &mut u64) -> Result<Self> {
        let mut mt = Self::create(_id);
        mt.wal = Some(Wal::recover(_path, &mt.map, max_ts)?);
        Ok(mt)
    }

    pub fn for_testing_put_slice(&self, key: &[u8], value: &[u8]) -> Result<()> {
        self.put(KeySlice::for_testing_from_slice_no_ts(key), value)
    }

    pub fn for_testing_get_slice(&self, key: &[u8]) -> Option<Bytes> {
        self.get(key)
    }

    pub fn for_testing_scan_slice(
        &self,
        lower: Bound<&[u8]>,
        upper: Bound<&[u8]>,
    ) -> MemTableIterator {
        self.scan(lower, upper)
    }

    /// Get a value by key.
    pub fn get(&self, _key: &[u8]) -> Option<Bytes> {
        let key = KeyBytes::from_bytes_with_ts(
            Bytes::from_static(unsafe { std::mem::transmute(_key) }),
            TS_DEFAULT,
        );
        self.map.get(&key).map(|x| x.value().clone())
    }

    /// Put a key-value pair into the mem-table.
    ///
    /// In week 1, day 1, simply put the key-value pair into the skipmap.
    /// In week 2, day 6, also flush the data to WAL.

    // pub fn put(&self, _key: &[u8], _value: &[u8]) -> Result<()> {
    pub fn put(&self, _key: KeySlice, _value: &[u8]) -> Result<()> {
        if let Some(wal) = &self.wal {
            // wal not consider TS
            wal.put(_key.key_ref(), _key.ts(), _value)?;
        }
        let key_insert =
            KeyBytes::from_bytes_with_ts(Bytes::copy_from_slice(_key.key_ref()), _key.ts());

        self.approximate_size
            .fetch_add(key_insert.raw_len() + _value.len(), Ordering::Relaxed);

        self.map.insert(key_insert, Bytes::copy_from_slice(_value));

        Ok(())
    }

    pub fn sync_wal(&self) -> Result<()> {
        if let Some(ref wal) = self.wal {
            wal.sync()?;
        }
        Ok(())
    }

    /// Get an iterator over a range of keys.
    pub fn scan(&self, _lower: Bound<&[u8]>, _upper: Bound<&[u8]>) -> MemTableIterator {
        let mut iter = MemTableIteratorBuilder {
            map: Arc::clone(&self.map),
            iter_builder: |map: &Arc<SkipMap<KeyBytes, Bytes>>| {
                map.range((map_bound(_lower, true), map_bound(_upper, false)))
            },
            item: (KeyBytes::new(), Bytes::new()),
        }
        .build();
        iter.next().unwrap();
        if let Bound::Excluded(lower) = _lower {
            if iter.key() == KeySlice::from_slice(lower, TS_RANGE_END) {
                iter.next().unwrap();
            }
        }
        iter
    }

    /// Flush the mem-table to SSTable. Implement in week 1 day 6.
    pub fn flush(&self, _builder: &mut SsTableBuilder) -> Result<()> {
        for entry in self.map.iter() {
            _builder.add(entry.key().as_key_slice(), entry.value());
        }
        Ok(())
    }

    pub fn id(&self) -> usize {
        self.id
    }

    pub fn approximate_size(&self) -> usize {
        self.approximate_size
            .load(std::sync::atomic::Ordering::Relaxed)
    }

    pub fn len(&self) -> usize {
        self.map.len()
    }

    /// Only use this function when closing the database
    pub fn is_empty(&self) -> bool {
        self.map.is_empty()
    }
}

type SkipMapRangeIter<'a> = crossbeam_skiplist::map::Range<
    'a,
    KeyBytes,
    (Bound<KeyBytes>, Bound<KeyBytes>),
    KeyBytes,
    Bytes,
>;

/// An iterator over a range of `SkipMap`. This is a self-referential structure and please refer to week 1, day 2
/// chapter for more information.
///
/// This is part of week 1, day 2.
#[self_referencing]
pub struct MemTableIterator {
    /// Stores a reference to the skipmap.
    map: Arc<SkipMap<KeyBytes, Bytes>>,
    /// Stores a skipmap iterator that refers to the lifetime of `MemTableIterator` itself.
    #[borrows(map)]
    #[not_covariant]
    iter: SkipMapRangeIter<'this>,
    /// Stores the current key-value pair.
    item: (KeyBytes, Bytes),
}

impl StorageIterator for MemTableIterator {
    type KeyType<'a> = KeySlice<'a>;

    fn value(&self) -> &[u8] {
        &self.borrow_item().1
    }

    fn key(&self) -> KeySlice {
        Key::from_slice(self.borrow_item().0.key_ref(), self.borrow_item().0.ts())
    }

    fn is_valid(&self) -> bool {
        !self.borrow_item().0.is_empty()
    }

    fn next(&mut self) -> Result<()> {
        self.with_mut(|ite| {
            let entry = ite.iter.next();
            *ite.item = match entry {
                Some(x) => {
                    let k = x.key().clone();
                    let v = x.value().clone();
                    (k, v)
                }
                None => (KeyBytes::new(), Bytes::new()),
            };
        });
        Ok(())
    }
}
