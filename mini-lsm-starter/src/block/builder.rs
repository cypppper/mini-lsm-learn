#![allow(unused_variables)] // TODO(you): remove this lint after implementing this mod
#![allow(dead_code)] // TODO(you): remove this lint after implementing this mod

use std::default;

use bytes::{BufMut, Bytes};

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

    /// Adds a key-value pair to the block. Returns false when the block is full.
    // first key & full
    #[must_use]
    pub fn add(&mut self, key: KeySlice, value: &[u8]) -> bool {
        let raw_k = key.raw_ref();
        let external_sz: usize = 3 * 2 + raw_k.len() + value.len();
        if !self.offsets.is_empty() && self.current_size + external_sz > self.block_size {
            return false;
        }

        self.offsets.push(self.data.len() as u16);
        let mut b: Vec<u8> = vec![];
        b.put_u16(raw_k.len() as u16);
        b.put_slice(raw_k);
        b.put_u16(value.len() as u16);
        b.put_slice(value);
        self.data.extend(b);
        self.current_size += external_sz;

        if self.offsets.len() == 1 {
            // first key
            self.first_key.append(raw_k);
            self.current_size += 2;
        }
        true
    }

    /// Check if there is no key-value pair in the block.
    pub fn is_empty(&self) -> bool {
        self.offsets.is_empty()
    }

    /// Finalize the block.
    pub fn build(self) -> Block {
        Block {
            data: self.data,
            offsets: self.offsets,
        }
    }
}
