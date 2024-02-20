pub(crate) mod bloom;
mod builder;
mod iterator;

use std::cmp::Ordering;
use std::fs::File;
use std::mem::size_of;
use std::path::Path;
use std::sync::Arc;

use anyhow::anyhow;
use anyhow::Result;
pub use builder::SsTableBuilder;
use bytes::BufMut;
use bytes::{Buf, Bytes};
pub use iterator::SsTableIterator;

use crate::block::{Block, BlockIterator};
use crate::key;
use crate::key::Key;
use crate::key::{KeyBytes, KeySlice};
use crate::lsm_storage::BlockCache;
use crate::mem_table;

use self::bloom::Bloom;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BlockMeta {
    /// Offset of this data block.
    pub offset: usize,
    /// The first key of the data block.
    pub first_key: KeyBytes,
    /// The last key of the data block.
    pub last_key: KeyBytes,
}

impl BlockMeta {
    /// Encode block meta to a buffer.
    /// You may add extra fields to the buffer,
    /// in order to help keep track of `first_key` when decoding from the same buffer in the future.
    pub fn encode_block_meta(block_meta: &[BlockMeta], buf: &mut Vec<u8>) {
        let meta_off = buf.len() as u32;
        let mut estimate_sz: usize = size_of::<u32>(); // meta len
        estimate_sz += size_of::<u32>(); // meta offset
        for meta in block_meta {
            estimate_sz += size_of::<u32>(); // offset
            estimate_sz += size_of::<u16>(); // key_len
            estimate_sz += meta.first_key.len();
            estimate_sz += size_of::<u16>(); // key2_len
            estimate_sz += meta.last_key.len();
        }
        buf.reserve(estimate_sz);
        buf.put_u32(block_meta.len() as u32);
        for meta in block_meta {
            buf.put_u32(meta.offset as u32);
            buf.put_u16(meta.first_key.len() as u16);
            buf.put(meta.first_key.raw_ref());
            buf.put_u16(meta.last_key.len() as u16);
            buf.put(meta.last_key.raw_ref());
        }
        buf.put_u32(meta_off);
    }

    /// Decode block meta from a buffer.
    pub fn decode_block_meta(buf: impl Buf) -> Vec<BlockMeta> {
        let mut rt = buf.chunk();

        let meta_len = rt.get_u32();
        let mut _off: u32;
        let mut _fk: &[u8];
        let mut _lk: &[u8];
        let mut keyf_len: u16;
        let mut keyl_len: u16;
        let mut vec: Vec<BlockMeta> = Vec::new();
        for _ in 0..meta_len {
            _off = rt.get_u32();
            keyf_len = rt.get_u16();
            _fk = rt.get(0..keyf_len as usize).unwrap();
            rt.advance(keyf_len as usize);
            keyl_len = rt.get_u16();
            _lk = rt.get(0..keyl_len as usize).unwrap();
            rt.advance(keyl_len as usize);
            vec.push(BlockMeta {
                offset: _off as usize,
                first_key: KeyBytes::from_bytes(Bytes::copy_from_slice(_fk)),
                last_key: KeyBytes::from_bytes(Bytes::copy_from_slice(_lk)),
            });
        }
        vec
    }
}

/// A file object.
pub struct FileObject(Option<File>, u64);

impl FileObject {
    pub fn read(&self, offset: u64, len: u64) -> Result<Vec<u8>> {
        use std::os::unix::fs::FileExt;
        let mut data = vec![0; len as usize];
        self.0
            .as_ref()
            .unwrap()
            .read_exact_at(&mut data[..], offset)?;
        Ok(data)
    }

    pub fn size(&self) -> u64 {
        self.1
    }

    /// Create a new file object (day 2) and write the file to the disk (day 4).
    pub fn create(path: &Path, data: Vec<u8>) -> Result<Self> {
        std::fs::write(path, &data)?;
        File::open(path)?.sync_all()?;
        Ok(FileObject(
            Some(File::options().read(true).write(false).open(path)?),
            data.len() as u64,
        ))
    }

    pub fn open(path: &Path) -> Result<Self> {
        let file = File::options().read(true).write(false).open(path)?;
        let size = file.metadata()?.len();
        Ok(FileObject(Some(file), size))
    }
}

/// An SSTable.
pub struct SsTable {
    /// The actual storage unit of SsTable, the format is as above.
    pub(crate) file: FileObject,
    /// The meta blocks that hold info for data blocks.
    pub(crate) block_meta: Vec<BlockMeta>,
    /// The offset that indicates the start point of meta blocks in `file`.
    pub(crate) block_meta_offset: usize,
    id: usize,
    block_cache: Option<Arc<BlockCache>>,
    first_key: KeyBytes,
    last_key: KeyBytes,
    pub(crate) bloom: Option<Bloom>,
    /// The maximum timestamp stored in this SST, implemented in week 3.
    max_ts: u64,
}

impl SsTable {
    #[cfg(test)]
    pub(crate) fn open_for_test(file: FileObject) -> Result<Self> {
        Self::open(0, None, file)
    }

    /// Open SSTable from a file.
    pub fn open(id: usize, block_cache: Option<Arc<BlockCache>>, file: FileObject) -> Result<Self> {
        let len = file.size();
        let raw_bloom_offset = file.read(len - 4, 4)?;
        let bloom_offset = (&raw_bloom_offset[..]).get_u32() as u64;
        let raw_bloom = file.read(bloom_offset, len - 4 - bloom_offset)?;
        let bloom_filter = Bloom::decode(&raw_bloom)?;

        let res = file.read(bloom_offset - 4, 4)?;
        let block_meta_off = (&res[..]).get_u32() as usize;
        let block_meta_code = file.read(
            block_meta_off as u64,
            bloom_offset - 4 - block_meta_off as u64,
        )?;

        let meta = BlockMeta::decode_block_meta(&block_meta_code[..]);

        Ok(Self {
            file,
            block_meta_offset: block_meta_off,
            id,
            block_cache,
            first_key: meta[0].first_key.clone(),
            last_key: meta[meta.len() - 1].last_key.clone(),
            block_meta: meta,
            bloom: Some(bloom_filter),
            max_ts: 0,
        })
    }

    /// Create a mock SST with only first key + last key metadata
    pub fn create_meta_only(
        id: usize,
        file_size: u64,
        first_key: KeyBytes,
        last_key: KeyBytes,
    ) -> Self {
        Self {
            file: FileObject(None, file_size),
            block_meta: vec![],
            block_meta_offset: 0,
            id,
            block_cache: None,
            first_key,
            last_key,
            bloom: None,
            max_ts: 0,
        }
    }

    pub fn get(&self, _key: &[u8]) -> Result<Option<Bytes>, anyhow::Error> {
        // None: not exist | bytes.len == 0: deleted
        let keep_table = |_key: &[u8], filter: &Option<Bloom>| {
            if let Some(bloom) = filter {
                if bloom.may_contain(farmhash::fingerprint32(_key)) {
                    return true;
                }
            } else {
                return true;
            }
            false
        };
        if !keep_table(_key, &self.bloom) {
            return Ok(None);
        }

        let key = Key::from_slice(_key);
        let blk = self.read_block(self.find_block_idx(key))?;
        let ite = BlockIterator::create_and_seek_to_key(blk, key);
        if ite.key().into_inner() == _key {
            Ok(Some(Bytes::copy_from_slice(ite.value())))
        } else {
            Ok(None)
        }
    }

    /// Read a block from the disk.
    pub fn read_block(&self, block_idx: usize) -> Result<Arc<Block>> {
        let meta_off_st = self.block_meta[block_idx].offset as u64;
        let meta_off_ed = match block_idx.cmp(&(self.block_meta.len() - 1)) {
            std::cmp::Ordering::Equal => self.block_meta_offset,
            _ => self.block_meta[block_idx + 1].offset,
        } as u64;
        let block_code = self
            .file
            .read(meta_off_st, meta_off_ed - meta_off_st)
            .unwrap();
        let block = Block::decode(&block_code[..]);
        Ok(Arc::new(block))
    }

    /// Read a block from disk, with block cache. (Day 4)
    pub fn read_block_cached(&self, block_idx: usize) -> Result<Arc<Block>> {
        if let Some(ref block_cache) = self.block_cache {
            let a = block_cache
                .try_get_with((self.sst_id(), block_idx), || self.read_block(block_idx))
                .map_err(|e| anyhow!("{}", e))?;
            Ok(a)
        } else {
            self.read_block(block_idx)
        }
    }

    /// Find the block that may contain `key`.
    /// Note: You may want to make use of the `first_key` stored in `BlockMeta`.
    /// You may also assume the key-value pairs stored in each consecutive block are sorted.
    pub fn find_block_idx(&self, key: KeySlice) -> usize {
        let key = KeyBytes::from_bytes(Bytes::copy_from_slice(key.into_inner()));
        let mut low = 0_usize;
        let mut high = self.block_meta.len();
        let mut mid;
        let mut mid_key;
        while low < high {
            mid = (low + high) / 2;
            mid_key = self.block_meta[mid].first_key.clone();
            match mid_key.cmp(&key) {
                Ordering::Equal => {
                    return mid;
                }
                Ordering::Less => {
                    low = mid + 1;
                }
                Ordering::Greater => {
                    high = mid;
                }
            }
        }
        if low == 0 {
            // all sst key >= key
            return 0;
        }
        low - 1
    }

    /// Get number of data blocks.
    pub fn num_of_blocks(&self) -> usize {
        self.block_meta.len()
    }

    pub fn first_key(&self) -> &KeyBytes {
        &self.first_key
    }

    pub fn last_key(&self) -> &KeyBytes {
        &self.last_key
    }

    pub fn table_size(&self) -> u64 {
        self.file.1
    }

    pub fn sst_id(&self) -> usize {
        self.id
    }

    pub fn max_ts(&self) -> u64 {
        self.max_ts
    }
}
