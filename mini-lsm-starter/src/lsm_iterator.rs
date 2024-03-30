use anyhow::Result;
use bytes::Bytes;
use nom::Err;
use std::ops::Bound;

use crate::iterators::concat_iterator::SstConcatIterator;
use crate::iterators::two_merge_iterator::TwoMergeIterator;
use crate::key::{Key, KeyBytes, TS_MAX, TS_MIN};
use crate::table::SsTableIterator;

use crate::{
    iterators::{merge_iterator::MergeIterator, StorageIterator},
    mem_table::MemTableIterator,
};

/// Represents the internal type for an LSM iterator. This type will be changed across the tutorial for multiple times.
// type LsmIteratorInner = MergeIterator<MemTableIterator>;
type LsmIteratorInner = TwoMergeIterator<
    TwoMergeIterator<MergeIterator<MemTableIterator>, MergeIterator<SsTableIterator>>,
    MergeIterator<SstConcatIterator>,
>;

pub struct LsmIterator {
    inner: LsmIteratorInner,
    // upper:
    // None: unbound
    // has upper & is inclu: include upper
    // has upper & not inclu: exclude upper
    upper: Option<KeyBytes>,
    is_upper_included: u8,
}

impl LsmIterator {
    // excluded: < TS_MAX
    // included: <= TS_MIN
    pub(crate) fn new(iter: LsmIteratorInner, upper: Bound<&[u8]>) -> Result<Self> {
        let (upper, is_upper_included) = match upper {
            Bound::Included(x) => (
                Some(KeyBytes::from_bytes_with_ts(
                    Bytes::copy_from_slice(x),
                    TS_MIN,
                )),
                1_u8,
            ),
            Bound::Excluded(x) => (
                Some(KeyBytes::from_bytes_with_ts(
                    Bytes::copy_from_slice(x),
                    TS_MAX,
                )),
                0,
            ),
            _ => (None, 0),
        };
        Ok(Self {
            inner: iter,
            upper,
            is_upper_included,
        })
    }

    fn exceed_upper(&self) -> bool {
        match (&self.upper, &self.is_upper_included) {
            (Some(x), i) if *i > 0 => self.inner.key() > x.as_key_slice(), // include upper
            (Some(x), 0) => self.inner.key() >= x.as_key_slice(),          // exclude upper
            _ => false,
        }
    }
}

impl StorageIterator for LsmIterator {
    type KeyType<'a> = &'a [u8];

    fn is_valid(&self) -> bool {
        self.inner.is_valid() && !self.exceed_upper()
    }

    fn key(&self) -> &[u8] {
        self.inner.key().key_ref()
    }

    fn value(&self) -> &[u8] {
        self.inner.value()
    }

    fn next(&mut self) -> Result<()> {
        self.inner.next()
    }

    fn num_active_iterators(&self) -> usize {
        self.inner.num_active_iterators()
    }
}

/// A wrapper around existing iterator, will prevent users from calling `next` when the iterator is
/// invalid. If an iterator is already invalid, `next` does not do anything. If `next` returns an error,
/// `is_valid` should return false, and `next` should always return an error.
pub struct FusedIterator<I: StorageIterator> {
    iter: I,
    has_errored: bool,
}

impl<I: StorageIterator> FusedIterator<I> {
    pub fn new(iter: I) -> Self {
        let mut i = Self {
            iter,
            has_errored: false,
        };
        i.format_init().unwrap();
        i
    }

    fn format_init(&mut self) -> Result<()> {
        while self.iter.is_valid() && self.iter.value().is_empty() {
            match self.iter.next() {
                Ok(_) => {
                    continue;
                }
                Err(x) => {
                    self.has_errored = true;
                    return Err(x);
                }
            }
        }
        Ok(())
    }
}

impl<I: StorageIterator> StorageIterator for FusedIterator<I> {
    type KeyType<'a> = I::KeyType<'a> where Self: 'a;

    fn is_valid(&self) -> bool {
        if self.has_errored {
            false
        } else {
            self.iter.is_valid()
        }
    }

    fn key(&self) -> Self::KeyType<'_> {
        self.iter.key()
    }

    fn value(&self) -> &[u8] {
        self.iter.value()
    }

    fn next(&mut self) -> Result<()> {
        if self.has_errored {
            return Err(anyhow::Error::msg("has errored!"));
        }
        if !self.iter.is_valid() {
            return Ok(());
        }
        match self.iter.next() {
            Ok(_) => self.format_init(),
            Err(x) => {
                self.has_errored = true;
                Err(x)
            }
        }
    }

    fn num_active_iterators(&self) -> usize {
        self.iter.num_active_iterators()
    }
}
