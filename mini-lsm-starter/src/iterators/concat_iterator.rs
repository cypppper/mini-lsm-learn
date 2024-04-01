#![allow(unused_variables)] // TODO(you): remove this lint after implementing this mod
#![allow(dead_code)] // TODO(you): remove this lint after implementing this mod

use std::ops::Bound;
use std::sync::Arc;

use anyhow::Result;

use super::StorageIterator;
use crate::{
    key::KeySlice,
    table::{SsTable, SsTableIterator},
};

/// Concat multiple iterators ordered in key order and their key ranges do not overlap. We do not want to create the
/// iterators when initializing this iterator to reduce the overhead of seeking.
pub struct SstConcatIterator {
    current: Option<SsTableIterator>,
    next_sst_idx: usize,
    sstables: Vec<Arc<SsTable>>,
}

impl SstConcatIterator {
    pub fn create_and_seek_to_first(sstables: Vec<Arc<SsTable>>) -> Result<Self> {
        if sstables.is_empty() {
            return Ok(Self {
                current: None,
                next_sst_idx: 1,
                sstables,
            });
        }
        Ok(Self {
            current: Some(SsTableIterator::create_and_seek_to_first(
                sstables[0].clone(),
            )?),
            next_sst_idx: 1,
            sstables,
        })
    }

    pub fn create_and_seek_to_key(sstables: Vec<Arc<SsTable>>, key: KeySlice) -> Result<Self> {
        if sstables.is_empty() {
            return Ok(Self {
                current: None,
                next_sst_idx: 1,
                sstables,
            });
        }
        let idx = sstables.partition_point(|x| x.first_key().as_key_slice() < key);
        let mut ite = if idx == 0 {
            Self {
                current: Some(SsTableIterator::create_and_seek_to_first(
                    sstables[0].clone(),
                )?),
                next_sst_idx: 1,
                sstables,
            }
        } else {
            Self {
                current: Some(SsTableIterator::create_and_seek_to_key(
                    sstables[idx - 1].clone(),
                    key,
                )?),
                next_sst_idx: idx,
                sstables,
            }
        };

        ite.next_until_valid();

        Ok(ite)
    }

    pub fn create_and_seek_to_key_lower_bound(
        sstables: Vec<Arc<SsTable>>,
        _key: Bound<KeySlice>,
    ) -> Result<Self> {
        if sstables.is_empty() {
            return Ok(Self {
                current: None,
                next_sst_idx: 1,
                sstables,
            });
        }

        if let Bound::Excluded(key) = _key {
            let mut ite = Self::create_and_seek_to_key(sstables, key)?;
            if ite.key() == key {
                ite.next()?;
            }
            Ok(ite)
        } else if let Bound::Included(key) = _key {
            let ite = Self::create_and_seek_to_key(sstables, key)?;
            Ok(ite)
        } else {
            let ite = Self::create_and_seek_to_first(sstables)?;
            Ok(ite)
        }
    }

    fn next_until_valid(&mut self) {
        if self.current.is_none() || !self.current.as_ref().unwrap().is_valid() {
            if self.next_sst_idx >= self.sstables.len() {
                return;
            }
            self.current = Some(
                SsTableIterator::create_and_seek_to_first(self.sstables[self.next_sst_idx].clone())
                    .unwrap(),
            );
            self.next_sst_idx += 1;
        }
    }
}

impl StorageIterator for SstConcatIterator {
    type KeyType<'a> = KeySlice<'a>;

    fn key(&self) -> KeySlice {
        self.current.as_ref().unwrap().key()
    }

    fn value(&self) -> &[u8] {
        self.current.as_ref().unwrap().value()
    }

    fn is_valid(&self) -> bool {
        self.current.is_some() && self.current.as_ref().unwrap().is_valid()
    }

    fn next(&mut self) -> Result<()> {
        self.current.as_mut().unwrap().next()?;
        self.next_until_valid();
        Ok(())
    }

    fn num_active_iterators(&self) -> usize {
        1
    }
}
