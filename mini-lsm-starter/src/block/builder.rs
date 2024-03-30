use std::default;

use bytes::{Buf, BufMut, Bytes};

use crate::key::{KeySlice, KeyVec};

use super::Block;

/// Builds a block.
#[derive(Default)]
pub struct BlockBuilder {
    /// Offsets of each key-value entries.
    offsets: Vec<u16>,
    /// All serialized key-value pairs in the block.
    data: Vec<u8>,
    /// The expected block size.
    block_size: usize,
    /// The first key in the block
    first_key: KeyVec,
    current_size: usize,
}

impl BlockBuilder {
    /// Creates a new block builder.
    pub fn new(block_size: usize) -> Self {
        Self {
            block_size,
            first_key: KeyVec::new(),
            ..Default::default()
        }
    }

    fn compute_overlap(first_key: KeySlice, key: KeySlice) -> usize {
        let mut i = 0;
        loop {
            if i >= first_key.len() || i >= key.len() {
                break;
            }
            if first_key.raw_ref()[i] != key.raw_ref()[i] {
                break;
            }
            i += 1;
        }
        i
    }

    /// Adds a key-value pair to the block. Returns false when the block is full.
    // first key & full
    // first_key: key_overlap_len: (0), rest_key_len:(key_len), key(key_len)
    // else: key_overlap_len: (?), rest_key_len:(key_len-?), key(key_len-?)
    #[must_use]
    pub fn add(&mut self, key: KeySlice, value: &[u8]) -> bool {
        let raw_k = key.raw_ref();
        if self.offsets.is_empty() {
            // first_key
            let external_sz: usize = 4 * 2 + raw_k.len() + value.len(); // key_overlap_len, key_rest_len, value_len, offset len 4 * 2
            self.offsets.push(self.data.len() as u16);
            let mut b: Vec<u8> = vec![];
            b.put_u16(0);
            b.put_u16(raw_k.len() as u16);
            b.put_slice(raw_k);
            b.put_u16(value.len() as u16);
            b.put_slice(value);
            self.data.extend(b);
            self.current_size += external_sz;
            self.first_key.clear();
            self.first_key.append(raw_k);
            self.current_size += 2; // block key len
            return true;
        }

        assert!(!self.first_key.is_empty());
        let overlap_len = Self::compute_overlap(self.first_key.as_key_slice(), key);
        let external_sz = 4 * 2 + key.len() - overlap_len + value.len();
        if self.current_size + external_sz > self.block_size {
            return false;
        }
        self.offsets.push(self.data.len() as u16);
        let mut b: Vec<u8> = vec![];
        b.put_u16(overlap_len as u16);
        b.put_u16((key.len() - overlap_len) as u16);
        b.put_slice(&raw_k[overlap_len..key.len()]);
        b.put_u16(value.len() as u16);
        b.put_slice(value);
        self.data.extend(b);
        self.current_size += external_sz;

        true
    }

    /// Check if there is no key-value pair in the block.
    pub fn is_empty(&self) -> bool {
        self.offsets.is_empty()
    }

    pub fn get_last_key(&self) -> Option<KeyVec> {
        if self.offsets.is_empty() {
            return None;
        }
        let off = self.offsets[self.offsets.len() - 1] as usize;
        let (_, mut rt) = self.data[..].split_at(off);
        let key_ovlp_len = rt.get_u16() as usize;
        let key_rest_len = rt.get_u16() as usize;
        let mut v: KeyVec = KeyVec::new();
        v.append(&self.first_key.raw_ref()[..key_ovlp_len]);
        v.append(&rt[..key_rest_len]);
        Some(v)
    }

    pub fn get_first_key(&self) -> Option<KeyVec> {
        if self.offsets.is_empty() {
            return None;
        }
        let mut v: KeyVec = KeyVec::new();
        v.append(self.first_key.raw_ref());
        Some(v)
    }

    /// Finalize the block.
    pub fn build(self) -> Block {
        Block {
            data: self.data,
            offsets: self.offsets,
        }
    }
}
