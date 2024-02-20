use std::path::Path;
use std::sync::Arc;

use anyhow::Result;
use bytes::{BufMut, Bytes};

use super::{bloom::Bloom, BlockMeta, FileObject, SsTable};
use crate::{
    block::BlockBuilder,
    key::{KeyBytes, KeySlice},
    lsm_storage::BlockCache,
};

/// Builds an SSTable from key-value pairs.
#[derive(Default)]
pub struct SsTableBuilder {
    builder: BlockBuilder,
    data: Vec<u8>,
    pub(crate) meta: Vec<BlockMeta>,
    block_size: usize,
    key_hashes: Vec<u32>,
}

impl SsTableBuilder {
    /// Create a builder based on target block size.
    pub fn new(block_size: usize) -> Self {
        Self {
            builder: BlockBuilder::new(block_size),
            block_size,
            ..Default::default()
        }
    }

    /// Adds a key-value pair to SSTable.
    ///
    /// Note: You should split a new block when the current block is full.(`std::mem::replace` may
    /// be helpful here)
    pub fn add(&mut self, key: KeySlice, value: &[u8]) {
        if !self.builder.add(key, value) {
            // full
            self.force_build_block();
            assert!(self.builder.add(key, value));
        }
        self.key_hashes.push(farmhash::fingerprint32(key.raw_ref()));
    }

    fn force_build_block(&mut self) {
        if self.builder.is_empty() {
            return;
        }
        let old_builder = std::mem::replace(&mut self.builder, BlockBuilder::new(self.block_size));

        let fk = old_builder.get_first_key().unwrap();
        let lk = old_builder.get_last_key().unwrap();
        self.meta.push(BlockMeta {
            offset: self.data.len(),
            first_key: KeyBytes::from_bytes(Bytes::from(fk.into_inner())),
            last_key: KeyBytes::from_bytes(Bytes::from(lk.into_inner())),
        });
        self.data.put(old_builder.build().encode());
    }

    /// Get the estimated size of the SSTable.
    ///
    /// Since the data blocks contain much more data than meta blocks, just return the size of data
    /// blocks here.
    pub fn estimated_size(&self) -> usize {
        self.data.len()
    }

    /// Builds the SSTable and writes it to the given path. Use the `FileObject` structure to manipulate the disk objects.
    pub fn build(
        mut self,
        id: usize,
        block_cache: Option<Arc<BlockCache>>,
        path: impl AsRef<Path>,
    ) -> Result<SsTable> {
        self.force_build_block();
        if self.meta.is_empty() {
            return Ok(SsTable {
                file: FileObject::create(path.as_ref(), self.data)?,
                block_meta: self.meta,
                block_meta_offset: 0,
                id,
                block_cache,
                first_key: KeyBytes::default(),
                last_key: KeyBytes::default(),
                bloom: None,
                max_ts: 0,
            });
        }

        let meta_off: usize = self.data.len();
        BlockMeta::encode_block_meta(&self.meta, &mut self.data);
        self.data.put_u32(meta_off as u32);
        // bloom filter
        let bloom = Bloom::build_from_key_hashes(
            &self.key_hashes,
            Bloom::bloom_bits_per_key(self.key_hashes.len(), 0.01),
        );
        let bloom_offset = self.data.len();
        bloom.encode(&mut self.data);
        self.data.put_u32(bloom_offset as u32);

        let file = FileObject::create(path.as_ref(), self.data)?;
        let lk = self.meta[self.meta.len() - 1].last_key.clone();
        let fk = self.meta[0].first_key.clone();

        Ok(SsTable {
            file,
            block_meta: self.meta,
            block_meta_offset: meta_off,
            id,
            block_cache,
            first_key: fk,
            last_key: lk,
            bloom: Some(bloom),
            max_ts: 0,
        })
    }

    #[cfg(test)]
    pub(crate) fn build_for_test(self, path: impl AsRef<Path>) -> Result<SsTable> {
        self.build(0, None, path)
    }
}
