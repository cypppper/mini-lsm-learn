#![allow(unused_variables)] // TODO(you): remove this lint after implementing this mod
#![allow(dead_code)] // TODO(you): remove this lint after implementing this mod

use std::sync::Arc;

use anyhow::Result;

use super::SsTable;
use crate::{block::BlockIterator, iterators::StorageIterator, key::KeySlice};

/// An iterator over the contents of an SSTable.
pub struct SsTableIterator {
    table: Arc<SsTable>,
    blk_iter: BlockIterator,
    blk_idx: usize,
}

impl SsTableIterator {
    /// Create a new iterator and seek to the first key-value pair in the first data block.
    pub fn create_and_seek_to_first(table: Arc<SsTable>) -> Result<Self> {
        let blk = table.read_block_cached(0)?;
        let ite = BlockIterator::create_and_seek_to_first(blk);
        Ok(Self {
            table,
            blk_iter: ite,
            blk_idx: 0,
        })
    }

    /// Seek to the first key-value pair in the first data block.
    pub fn seek_to_first(&mut self) -> Result<()> {
        if self.blk_idx == 0 {
            self.blk_iter.seek_to_first();
            return Ok(());
        }
        let blk = self.table.read_block_cached(0)?;
        self.blk_iter = BlockIterator::create_and_seek_to_first(blk);
        self.blk_idx = 0;
        Ok(())
    }

    /// Create a new iterator and seek to the first key-value pair which >= `key`.
    pub fn create_and_seek_to_key(table: Arc<SsTable>, key: KeySlice) -> Result<Self> {
        let idx = table.find_block_idx(key);
        let blk = table.read_block_cached(idx)?;
        let ite = BlockIterator::create_and_seek_to_key(blk, key);
        if !ite.is_valid() {
            // key > all keys in block
            if idx == table.num_of_blocks() - 1 {
                // key > all keys in all blocks => invalid
                return Ok(Self {
                    table,
                    blk_iter: ite,
                    blk_idx: idx,
                });
            }
            let idx = idx + 1;
            let blk = table.read_block_cached(idx)?;
            let ite = BlockIterator::create_and_seek_to_first(blk);
            assert!(ite.is_valid());
            return Ok(Self {
                table,
                blk_iter: ite,
                blk_idx: idx,
            });
        }
        Ok(Self {
            table,
            blk_iter: ite,
            blk_idx: idx,
        })
    }

    /// Seek to the first key-value pair which >= `key`.
    /// Note: You probably want to review the handout for detailed explanation when implementing
    /// this function.
    pub fn seek_to_key(&mut self, key: KeySlice) -> Result<()> {
        let idx = self.table.find_block_idx(key);
        if self.blk_idx == idx {
            self.blk_iter.seek_to_key(key);
        } else {
            self.blk_idx = idx;
            let blk = self.table.read_block_cached(self.blk_idx)?;
            self.blk_iter = BlockIterator::create_and_seek_to_key(blk, key);
        }

        if !self.blk_iter.is_valid() {
            // key > all keys in block
            if idx == self.table.num_of_blocks() - 1 {
                // key > all keys in all blocks => invalid
                return Ok(());
            }
            self.blk_idx += 1;
            let blk = self.table.read_block_cached(self.blk_idx)?;
            self.blk_iter = BlockIterator::create_and_seek_to_first(blk);
            return Ok(());
        }
        Ok(())
    }
}

impl StorageIterator for SsTableIterator {
    type KeyType<'a> = KeySlice<'a>;

    /// Return the `key` that's held by the underlying block iterator.
    fn key(&self) -> KeySlice {
        self.blk_iter.key()
    }

    /// Return the `value` that's held by the underlying block iterator.
    fn value(&self) -> &[u8] {
        self.blk_iter.value()
    }

    /// Return whether the current block iterator is valid or not.
    fn is_valid(&self) -> bool {
        self.blk_iter.is_valid()
    }

    /// Move to the next `key` in the block.
    /// Note: You may want to check if the current block iterator is valid after the move.
    fn next(&mut self) -> Result<()> {
        self.blk_iter.next();
        if !self.blk_iter.is_valid() {
            if self.blk_idx == self.table.num_of_blocks() - 1 {
                return Ok(());
            }
            self.blk_idx += 1;
            let blk = self.table.read_block_cached(self.blk_idx)?;
            self.blk_iter = BlockIterator::create_and_seek_to_first(blk);
        }
        Ok(())
    }
}
