#![allow(dead_code)] // REMOVE THIS LINE after fully implementing this functionality

use std::fs::File;
use std::io::{BufRead, BufWriter, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use anyhow::Result;
use bytes::{Buf, BufMut, Bytes};
use crossbeam_skiplist::SkipMap;
use parking_lot::Mutex;

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
        _path.as_ref().join(format!("{id}.wal"))
    }

    pub fn recover(_path: impl AsRef<Path>, _skiplist: &SkipMap<Bytes, Bytes>) -> Result<Self> {
        if !_path.as_ref().exists() {
            panic!("wal file {:?} NOT existed", _path.as_ref().to_path_buf());
        }
        let recover_data = std::fs::read(_path.as_ref())?;
        let mut buf = &recover_data[..];
        while let Some((key, value)) = Self::parse_kv(&mut buf) {
            _skiplist.insert(Bytes::copy_from_slice(key), Bytes::copy_from_slice(value));
        }
        let file = std::fs::File::options().append(true).open(_path)?;
        let file = Arc::new(Mutex::new(BufWriter::new(file)));

        Ok(Self { file })
    }

    pub fn put(&self, _key: &[u8], _value: &[u8]) -> Result<()> {
        let mut buf: Vec<u8> = vec![];
        buf.reserve(2 + _key.len() + 2 + _value.len());
        buf.put_u16(_key.len() as u16);
        buf.put_slice(_key);
        buf.put_u16(_value.len() as u16);
        buf.put_slice(_value);
        let mut file = self.file.lock();
        file.write_all(&buf)?;
        file.flush()?;
        file.get_mut().sync_all()?;
        Ok(())
    }

    pub fn sync(&self) -> Result<()> {
        let mut file = self.file.lock();
        file.flush()?;
        file.get_mut().sync_all()?;
        Ok(())
    }

    fn parse_kv<'a>(buf: &'a mut &[u8]) -> Option<(&'a [u8], &'a [u8])> {
        if buf.is_empty() {
            return None;
        }
        let key_len = buf.get_u16();
        let (key, _) = buf.split_at(key_len as usize);
        buf.consume(key_len as usize);
        let value_len = buf.get_u16();
        let (value, _) = buf.split_at(value_len as usize);
        buf.consume(value_len as usize);
        Some((key, value))
    }
}
