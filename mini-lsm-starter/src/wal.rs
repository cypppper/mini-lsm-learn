#![allow(dead_code)] // REMOVE THIS LINE after fully implementing this functionality

use std::fs::File;
use std::io::{BufWriter, Write};
use std::ops::Deref;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use anyhow::Result;
use bytes::{Buf, BufMut, Bytes};
use crossbeam_skiplist::SkipMap;
use parking_lot::Mutex;

use crate::key::KeyBytes;

pub struct Wal {
    file: Arc<Mutex<BufWriter<File>>>,
}

impl Wal {
    /*
        id is included in path
        stands for a file
    */
    pub fn create(_path: impl AsRef<Path>) -> Result<Self> {
        if _path.as_ref().exists() {
            panic!("wal file {:?} has existed", _path.as_ref().to_path_buf());
        }
        let file = std::fs::File::create(_path.as_ref())?;
        Ok(Wal {
            file: Arc::new(Mutex::new(BufWriter::new(file))),
        })
    }

    pub fn path_of_wal(id: usize, _path: impl AsRef<Path>) -> PathBuf {
        _path.as_ref().join(format!("{:05}.wal", id))
    }

    pub fn recover(
        _path: impl AsRef<Path>,
        _skiplist: &SkipMap<KeyBytes, Bytes>,
        max_ts: &mut u64,
    ) -> Result<Self> {
        if !_path.as_ref().exists() {
            panic!("wal file {:?} NOT existed", _path.as_ref().to_path_buf());
        }
        let recover_data = std::fs::read(_path.as_ref())?;
        let mut buf = &recover_data[..];
        while let Some((key, ts, value)) = Self::parse_kv(&mut buf) {
            *max_ts = (*max_ts.deref()).max(ts);
            let kby = KeyBytes::from_bytes_with_ts(Bytes::copy_from_slice(key), ts);
            _skiplist.insert(kby, Bytes::copy_from_slice(value));
        }
        let file = std::fs::File::options().append(true).open(_path)?;
        let file = Arc::new(Mutex::new(BufWriter::new(file)));

        Ok(Self { file })
    }

    pub fn put(&self, _key: &[u8], _ts: u64, _value: &[u8]) -> Result<()> {
        let mut buf: Vec<u8> = Vec::with_capacity(2 + _key.len() + 8 + 2 + _value.len() + 4); // ts , crc
        buf.put_u16(_key.len() as u16);
        buf.put_slice(_key);
        buf.put_u64(_ts);
        buf.put_u16(_value.len() as u16);
        buf.put_slice(_value);
        let crc = crc32fast::hash(&buf);
        buf.put_u32(crc);

        let mut file = self.file.lock();
        file.write_all(&buf)?;
        // file.flush()?;
        // file.get_mut().sync_all()?;
        Ok(())
    }

    pub fn sync(&self) -> Result<()> {
        let mut file = self.file.lock();
        file.flush()?;
        file.get_mut().sync_all()?;
        Ok(())
    }

    fn parse_kv<'a>(buf: &'a mut &[u8]) -> Option<(&'a [u8], u64, &'a [u8])> {
        if buf.is_empty() {
            return None;
        }
        let buf_copy = &buf[..];

        let key_len = buf.get_u16();
        let (key, _) = buf.split_at(key_len as usize);
        buf.advance(key_len as usize);
        let ts = buf.get_u64();
        let value_len = buf.get_u16();
        let (value, _) = buf.split_at(value_len as usize);
        buf.advance(value_len as usize);
        let sto_crc32 = buf.get_u32();
        let wal_len = 2 + key_len + 8 + 2 + value_len;
        let cal_crc32 = crc32fast::hash(&buf_copy[..wal_len as usize]);
        assert!(cal_crc32 == sto_crc32);
        Some((key, ts, value))
    }
}
