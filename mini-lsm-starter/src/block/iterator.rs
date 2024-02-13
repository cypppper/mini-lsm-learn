#![allow(unused_variables)] // TODO(you): remove this lint after implementing this mod
#![allow(dead_code)] // TODO(you): remove this lint after implementing this mod

use std::{cmp::Ordering, sync::Arc};

use crate::key::{KeySlice, KeyVec};
use bytes::Buf;

use super::Block;

/// Iterates on a block.
pub struct BlockIterator {
    /// The internal `Block`, wrapped by an `Arc`
    block: Arc<Block>,
    /// The current key, empty represents the iterator is invalid
    key: KeyVec,
    /// the value range from the block
    value_range: (usize, usize),
    /// Current index of the key-value pair, should be in range of [0, num_of_elements)
    idx: usize,
    /// The first key in the block
    first_key: KeyVec,
}

impl BlockIterator {
    fn new(block: Arc<Block>) -> Self {
        Self {
            block,
            key: KeyVec::new(),
            value_range: (0, 0),
            idx: 0,
            first_key: KeyVec::new(),
        }
    }

    /// Creates a block iterator and seek to the first entry.
    pub fn create_and_seek_to_first(block: Arc<Block>) -> Self {
        let mut key = KeyVec::new();
        let (_k, _v_off1, _v_off2) = Self::seeki(&block, 0);
        key.append(_k);
        Self {
            block,
            key: key.clone(),
            value_range: (_v_off1, _v_off1 + _v_off2),
            idx: 0,
            first_key: key,
        }
    }

    /// Creates a block iterator and seek to the first key that >= `key`.
    pub fn create_and_seek_to_key(block: Arc<Block>, key: KeySlice) -> Self {
        let (mut low, mut high) = (0 as usize, block.offsets.len());
        let mut mid;
        let mut mid_key;

        while low < high {
            mid = (low + high) / 2;
            mid_key = Self::seekik(&block, mid);
            match mid_key.cmp(key.raw_ref()) {
                Ordering::Equal => {
                    low = mid;
                    break;
                }
                Ordering::Greater => {
                    high = mid;
                }
                Ordering::Less => {
                    low = mid + 1;
                }
            }
        }
        let _f_key = Self::seekik(&block, 0);
        if low == block.offsets.len() {
            return Self {
                key: KeyVec::new(),
                first_key: KeyVec::from_vec(Vec::from(_f_key)),
                block,
                value_range: (0, 0),
                idx: 0,
            };
        }
        assert!(low < block.offsets.len());
        let (_key, _v_off, _v_len) = Self::seeki(&block, low);

        Self {
            key: KeyVec::from_vec(Vec::from(_key)),
            first_key: KeyVec::from_vec(Vec::from(_f_key)),
            block,
            value_range: (_v_off, _v_off + _v_len),
            idx: low,
        }
    }

    /// Returns the key of the current entry.
    pub fn key(&self) -> KeySlice {
        KeySlice::from_slice(self.key.raw_ref())
    }

    /// Returns the value of the current entry.
    pub fn value(&self) -> &[u8] {
        &self.block.data[self.value_range.0..self.value_range.1]
    }

    /// Returns true if the iterator is valid.
    /// Note: You may want to make use of `key`
    pub fn is_valid(&self) -> bool {
        self.key.is_empty()
    }

    /// Seeks to the first key in the block.
    pub fn seek_to_first(&mut self) {
        let (_k, _v1, _vlen) = Self::seeki(&self.block, 0);
        self.key = KeyVec::from_vec(Vec::from(_k));
        self.value_range = (_v1, _v1 + _vlen);
        self.idx = 0;
    }

    /// Move to the next key in the block.
    pub fn next(&mut self) {
        self.idx += 1;
        if self.idx == self.block.offsets.len() {
            self.key = KeyVec::new();
            self.value_range = (0, 0);
            self.idx = 0;
            return;
        }
        let (_k, _v1, _vlen) = Self::seeki(&self.block, self.idx);
        self.key = KeyVec::from_vec(Vec::from(_k));
        self.value_range = (_v1, _v1 + _vlen);
    }

    /// Seek to the first key that >= `key`.
    /// Note: You should assume the key-value pairs in the block are sorted when being added by
    /// callers.
    pub fn seek_to_key(&mut self, key: KeySlice) {
        let (mut low, mut high) = (0 as usize, self.block.offsets.len());
        let mut mid;
        let mut mid_key;

        while low < high {
            mid = (low + high) / 2;
            mid_key = Self::seekik(&self.block, mid);
            match mid_key.cmp(key.raw_ref()) {
                Ordering::Equal => {
                    low = mid;
                    break;
                }
                Ordering::Greater => {
                    high = mid;
                }
                Ordering::Less => {
                    low = mid + 1;
                }
            }
        }

        if low == self.block.offsets.len() {
            self.key = KeyVec::new();
            self.value_range = (0, 0);
            self.idx = 0;
            return;
        }
        assert!(low < self.block.offsets.len());
        let (_key, _v_off, _v_len) = Self::seeki(&self.block, low);

        self.key = KeyVec::from_vec(Vec::from(_key));
        self.value_range = (_v_off, _v_off + _v_len);
        self.idx = low;
    }

    fn seeki_k(&self, idx: usize) -> &[u8] {
        let off = self.block.offsets[idx] as usize;
        let (lf, mut rt) = self.block.data.split_at(off);
        let key_len = rt.get_u16() as usize;
        &rt[..key_len]
    }

    fn seekik<'a>(blk: &'a Arc<Block>, idx: usize) -> &'a [u8] {
        let off = blk.offsets[idx] as usize;
        let off = blk.offsets[idx] as usize;
        let (lf, mut rt) = blk.data.split_at(off);
        let key_len = rt.get_u16() as usize;
        &rt[..key_len]
    }

    fn seeki<'a>(blk: &'a Arc<Block>, idx: usize) -> (&'a [u8], usize, usize) {
        let off = blk.offsets[idx] as usize;
        Self::seekikv(&blk.data[..], off)
    }

    fn seekikv(data: &[u8], idx: usize) -> (&[u8], usize, usize) {
        let mut off = &data[idx..];
        let k_len = off.get_u16() as usize;
        let (key, mut off) = off.split_at(k_len);
        let v_len = off.get_u16() as usize;
        (key, idx + 2 + k_len + 2, v_len)
    }
}
