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
    max_ts: u64,
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
        // println!(
        //     "add key{:?} ts{:?} , value {:?}",
        //     key.key_ref(),
        //     key.ts(),
        //     value
        // );
        self.max_ts = self.max_ts.max(key.ts());
        if !self.builder.add(key, value) {
            // full
            self.force_build_block();
            assert!(self.builder.add(key, value));
        }
        self.key_hashes.push(farmhash::fingerprint32(key.key_ref()));
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
            first_key: KeyBytes::from_bytes_with_ts(Bytes::copy_from_slice(fk.key_ref()), fk.ts()),
            last_key: KeyBytes::from_bytes_with_ts(Bytes::copy_from_slice(lk.key_ref()), lk.ts()),
        });
        let block_encode = old_builder.build().encode();
        let crc32 = crc32fast::hash(&block_encode);
        self.data.put(block_encode);
        self.data.put_u32(crc32);
    }

    pub fn is_empty(&self) -> bool {
        self.builder.is_empty() && self.meta.is_empty()
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

        let meta_off: usize = self.data.len();

        let mut meta_encode_bytes_with_max_ts = vec![];
        BlockMeta::encode_block_meta(&self.meta, &mut meta_encode_bytes_with_max_ts);
        meta_encode_bytes_with_max_ts.put_u64(self.max_ts);

        let crc32 = crc32fast::hash(&meta_encode_bytes_with_max_ts);
        meta_encode_bytes_with_max_ts.put_u32(crc32);

        self.data.extend(meta_encode_bytes_with_max_ts);
        self.data.put_u32(meta_off as u32);

        /* meta_len | metas | max_ts | crc32 | meta_off | bloom | bloom_crc | blomm_off */

        // bloom filter
        let bloom = Bloom::build_from_key_hashes(
            &self.key_hashes,
            Bloom::bloom_bits_per_key(self.key_hashes.len(), 0.01),
        );

        let bloom_offset = self.data.len();
        bloom.encode(&mut self.data); // bloom + crc32
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
            max_ts: self.max_ts,
        })
    }

    #[cfg(test)]
    pub(crate) fn build_for_test(self, path: impl AsRef<Path>) -> Result<SsTable> {
        self.build(0, None, path)
    }
}
