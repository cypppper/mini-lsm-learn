use anyhow::Result;
use bytes::Bytes;
use nom::Err;
use std::ops::Bound;

use crate::iterators::concat_iterator::SstConcatIterator;
use crate::iterators::two_merge_iterator::TwoMergeIterator;
use crate::key::{Key, KeyBytes, KeySlice, TS_MAX, TS_MIN};
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
    pub(crate) fn new(iter: LsmIteratorInner, upper: Bound<KeySlice>) -> Result<Self> {
        let (upper, is_upper_included) = match upper {
            Bound::Included(x) => (
                Some(KeyBytes::from_bytes_with_ts(
                    Bytes::copy_from_slice(x.key_ref()),
                    x.ts(),
                )),
                1_u8,
            ),
            Bound::Excluded(x) => (
                Some(KeyBytes::from_bytes_with_ts(
                    Bytes::copy_from_slice(x.key_ref()),
                    x.ts(),
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
    type KeyType<'a> = KeySlice<'a>;

    fn is_valid(&self) -> bool {
        self.inner.is_valid() && !self.exceed_upper()
    }

    fn key(&self) -> KeySlice {
        self.inner.key()
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
pub struct FusedIterator<I>
where
    I: StorageIterator,
{
    iter: I,
    has_errored: bool,
}

impl<I: 'static + for<'a> StorageIterator<KeyType<'a> = KeySlice<'a>>> FusedIterator<I> {
    pub fn new(iter: I) -> Self {
        let mut i = Self {
            iter,
            has_errored: false,
        };
        i.format_iter().unwrap();
        i
    }

    fn check_delete(&self) -> Option<Bytes> {
        if self.is_valid() && self.value().is_empty() {
            return Some(Bytes::copy_from_slice(self.key()));
        }

        None
    }

    fn format_iter(&mut self) -> Result<()> {
        while let Some(bt) = self.check_delete() {
            self.skip_dup(&bt)?;
        }
        Ok(())
    }

    fn skip_dup(&mut self, dup: &[u8]) -> Result<()> {
        while self.iter.is_valid() && self.iter.key().key_ref() == dup {
            match self.iter.next() {
                Ok(_) => {
                    // println!("skip dup {:?}", self.key());
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

impl<I: 'static + for<'a> StorageIterator<KeyType<'a> = KeySlice<'a>>> StorageIterator
    for FusedIterator<I>
{
    type KeyType<'a> = &'a [u8]
    where Self: 'a;

    fn is_valid(&self) -> bool {
        if self.has_errored {
            false
        } else {
            self.iter.is_valid()
        }
    }

    fn key(&self) -> &[u8] {
        self.iter.key().key_ref()
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
        let old_key = self.key().to_vec();

        match self.skip_dup(&old_key) {
            Err(x) => {
                self.has_errored = true;
                return Err(x);
            }
            _ => {}
        }
        self.format_iter()
    }

    fn num_active_iterators(&self) -> usize {
        self.iter.num_active_iterators()
    }
}
