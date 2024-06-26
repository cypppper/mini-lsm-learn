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
use bytes::{Buf, Bytes, BytesMut};
pub use iterator::SsTableIterator;

use crate::block::{Block, BlockIterator};
use crate::key::TS_DEFAULT;
use crate::key::{KeyBytes, KeySlice};
use crate::lsm_storage::BlockCache;

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
        let mut estimate_sz: usize = size_of::<u32>() * 2 + 8; // meta len +  max_ts(outside) + crc_32(outside)

        for meta in block_meta {
            estimate_sz += size_of::<u32>(); // offset
            estimate_sz += size_of::<u16>(); // key_len
            estimate_sz += meta.first_key.raw_len();
            estimate_sz += size_of::<u16>(); // key2_len
            estimate_sz += meta.last_key.raw_len();
        }
        buf.reserve(estimate_sz);
        let mut encode_bytes = BytesMut::new();
        encode_bytes.put_u32(block_meta.len() as u32);

        for meta in block_meta {
            encode_bytes.put_u32(meta.offset as u32);
            encode_bytes.put_u16(meta.first_key.key_len() as u16);
            encode_bytes.put(meta.first_key.key_ref());
            encode_bytes.put_u64(meta.first_key.ts());
            encode_bytes.put_u16(meta.last_key.key_len() as u16);
            encode_bytes.put(meta.last_key.key_ref());
            encode_bytes.put_u64(meta.last_key.ts());
        }

        assert!(encode_bytes.len() == estimate_sz - 8 - 4);
        buf.extend(encode_bytes);
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
            _fk = &rt[..(keyf_len + 8) as usize];
            rt.advance((keyf_len + 8) as usize);
            keyl_len = rt.get_u16();
            _lk = &rt[..(keyl_len + 8) as usize];
            rt.advance((keyl_len + 8) as usize);
            vec.push(BlockMeta {
                offset: _off as usize,
                first_key: KeyBytes::from_bytes_with_ts(
                    Bytes::copy_from_slice(&_fk[..keyf_len as usize]),
                    (&_fk[keyf_len as usize..]).get_u64(),
                ),
                last_key: KeyBytes::from_bytes_with_ts(
                    Bytes::copy_from_slice(&_lk[..keyl_len as usize]),
                    (&_lk[keyl_len as usize..]).get_u64(),
                ),
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
    /// /* ( meta_len | metas | max_ts ) | crc32 | meta_off | bloom | bloom_crc | blomm_off */
    pub fn open(id: usize, block_cache: Option<Arc<BlockCache>>, file: FileObject) -> Result<Self> {
        let len = file.size();
        let bloom_offset = (&(file.read(len - 4, 4)?)[..]).get_u32() as u64;
        let bloom_with_crc_len = (len - 4) - bloom_offset;
        let raw_bloom = file.read(bloom_offset, bloom_with_crc_len)?;
        let bloom_filter = Bloom::decode(&raw_bloom)?; // bloom decode with crc check

        let block_meta_off = (&file.read(bloom_offset - 4, 4)?[..]).get_u32() as usize;
        let block_metas_with_maxts_and_crc_size = (bloom_offset - 4) - block_meta_off as u64;
        let block_metas_with_maxts_and_crc_bytes =
            file.read(block_meta_off as u64, block_metas_with_maxts_and_crc_size)?;
        let cal_crc = crc32fast::hash(
            &block_metas_with_maxts_and_crc_bytes
                [..(block_metas_with_maxts_and_crc_size - 4) as usize],
        );
        let sto_crc = (&block_metas_with_maxts_and_crc_bytes
            [(block_metas_with_maxts_and_crc_size - 4) as usize..])
            .get_u32();
        assert!(cal_crc == sto_crc);
        let meta = BlockMeta::decode_block_meta(
            &block_metas_with_maxts_and_crc_bytes
                [..(block_metas_with_maxts_and_crc_size as usize - 4 - 8/* max_ts */)],
        );

        let ts_st = block_metas_with_maxts_and_crc_size as usize - 4 - 8/* max_ts */;
        let ts_ed = block_metas_with_maxts_and_crc_size as usize - 4;
        let max_ts = (&block_metas_with_maxts_and_crc_bytes[ts_st..ts_ed]).get_u64();

        Ok(Self {
            file,
            block_meta_offset: block_meta_off,
            id,
            block_cache,
            first_key: meta[0].first_key.clone(),
            last_key: meta[meta.len() - 1].last_key.clone(),
            block_meta: meta,
            bloom: Some(bloom_filter),
            max_ts,
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

    pub fn bloom_filter(&self, _key: &[u8]) -> bool {
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
        keep_table(_key, &self.bloom)
    }

    // key without ts
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

        let key = KeySlice::from_slice(_key, TS_DEFAULT);
        let blk = self.read_block(self.find_block_idx(key))?;
        let ite = BlockIterator::create_and_seek_to_key(blk, key);
        if ite.key() == key {
            Ok(Some(Bytes::copy_from_slice(ite.value())))
        } else {
            Ok(None)
        }
    }

    /// Read a block from the disk.
    pub fn read_block(&self, block_idx: usize) -> Result<Arc<Block>> {
        let block_off_st = self.block_meta[block_idx].offset as u64;
        let block_off_ed = match block_idx.cmp(&(self.block_meta.len() - 1)) {
            std::cmp::Ordering::Equal => self.block_meta_offset,
            _ => self.block_meta[block_idx + 1].offset,
        } as u64;
        let read_len = block_off_ed - block_off_st;
        let block_code = self.file.read(block_off_st, read_len).unwrap();
        let cal_crc32 = crc32fast::hash(&block_code[..(read_len as usize - 4)]);
        let sto_crc32 = (&block_code[(read_len as usize - 4)..]).get_u32();
        assert!(cal_crc32 == sto_crc32);
        let block = Block::decode(&block_code[..(read_len as usize - 4)]);
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
        let mut low = 0_usize;
        let mut high = self.block_meta.len();
        let mut mid;
        let mut mid_key;
        while low < high {
            mid = (low + high) / 2;
            mid_key = self.block_meta[mid].first_key.as_key_slice();
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
