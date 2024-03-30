use std::{cmp::Ordering, sync::Arc};

use crate::key::{KeySlice, KeyVec};
use bytes::{Buf, Bytes, BytesMut};
use nom::{combinator::rest, ExtendInto};
use std::io::BufRead;

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
        let (_v_off1, _v_off2) = Self::get_first_value_off(&block);
        let _first_key = Self::get_first_key(&block);
        println!("[create and seek first] first key {:?}", key.raw_ref());
        key.append(_first_key);
        Self {
            block,
            key: key.clone(),
            value_range: (_v_off1, _v_off2),
            idx: 0,
            first_key: key,
        }
    }

    /// Creates a block iterator and seek to the first key that >= `key`.
    pub fn create_and_seek_to_key(block: Arc<Block>, key: KeySlice) -> Self {
        let mut ite = Self::create_and_seek_to_first(block);

        let (mut low, mut high) = (0_usize, ite.block.offsets.len());
        let mut mid;
        let mut mid_key;

        while low < high {
            mid = (low + high) / 2;
            mid_key = {
                ite.seek_to_offset(mid);
                ite.key().raw_ref()
            };
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
        if low == ite.block.offsets.len() {
            ite.key.clear();
            return ite;
        }
        ite.seek_to_offset(low);

        ite
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
        !self.key.is_empty()
    }

    /// Seeks to the first key in the block.
    pub fn seek_to_first(&mut self) {
        self.seek_to_offset(0);
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
        self.seek_to_offset(self.idx);
    }

    /// Seek to the first key that >= `key`.
    /// Note: You should assume the key-value pairs in the block are sorted when being added by
    /// callers.
    pub fn seek_to_key(&mut self, key: KeySlice) {
        let (mut low, mut high) = (0_usize, self.block.offsets.len());
        let mut mid;
        let mut mid_key;

        while low < high {
            mid = (low + high) / 2;
            mid_key = {
                self.seek_to_offset(mid);
                self.key().raw_ref()
            };
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
        self.seek_to_offset(low);
    }

    fn get_first_key(blk: &Arc<Block>) -> &[u8] {
        let mut data = &blk.data[..];
        let ovlp_len = data.get_u16() as usize;
        let rest_len = data.get_u16() as usize;
        assert!(ovlp_len == 0);
        &data[..rest_len]
    }

    fn get_first_value_off(blk: &Arc<Block>) -> (usize, usize) {
        let mut data = &blk.data[..];
        let ovlp_len = data.get_u16() as usize;
        let rest_len = data.get_u16() as usize;
        assert!(ovlp_len == 0);
        data.consume(rest_len);
        let v_len = data.get_u16() as usize;
        (4 + rest_len + 2, 4 + rest_len + 2 + v_len)
    }

    fn seek_to_offset(&mut self, offset: usize) {
        assert!(offset < self.block.offsets.len());
        let mut data = &self.block.data[..];
        let kv_off = self.block.offsets[offset] as usize;
        data = &data[kv_off..];
        let ovlp_len = data.get_u16() as usize;
        let rest_len = data.get_u16() as usize;
        let rst_key;
        (rst_key, data) = data.split_at(rest_len);
        let v_len = data.get_u16() as usize;
        self.key.clear();
        self.key.append(&self.first_key.raw_ref()[..ovlp_len]);
        self.key.append(&rst_key[..]);
        self.value_range = (kv_off + 4 + rest_len + 2, kv_off + 4 + rest_len + 2 + v_len);
        self.idx = offset;
    }
}
