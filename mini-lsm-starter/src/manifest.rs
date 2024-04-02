#![allow(dead_code)] // REMOVE THIS LINE after fully implementing this functionality

use std::path::Path;
use std::path::PathBuf;
use std::sync::Arc;
use std::{fs::File, io::Write};

use anyhow::Result;
use bytes::{Buf, BufMut, BytesMut};
use parking_lot::{Mutex, MutexGuard};
use serde::{Deserialize, Serialize};

use crate::compact::CompactionTask;
use crate::table::FileObject;

pub struct Manifest {
    file: Arc<Mutex<File>>,
}

#[derive(Serialize, Deserialize, Debug)]
pub enum ManifestRecord {
    Flush(usize),
    NewMemtable(usize),
    Compaction(CompactionTask, Vec<usize>),
}

impl Manifest {
    pub fn create(_path: impl AsRef<Path>) -> Result<Self> {
        let path = _path.as_ref().join("MANIFEST");

        let file = File::create(path.clone())?;
        Ok(Self {
            file: Arc::new(Mutex::new(file)),
        })
    }

    pub fn get_path(_path: impl AsRef<Path>) -> PathBuf {
        _path.as_ref().join("MANIFEST")
    }

    pub fn recover(_path: impl AsRef<Path>) -> Result<(Self, Vec<ManifestRecord>)> {
        let path = Self::get_path(_path);
        let fob = FileObject::open(&path)?;
        let bytes = fob.read(0, fob.size())?;
        // let mut deser = serde_json::Deserializer::from_slice(&bytes);
        let mut bytes_view = &bytes[..];

        let mut rcds = vec![];
        while !bytes_view.is_empty() {
            let mani_len = bytes_view.get_u16() as usize;

            let mani_json_bytes = &bytes_view[..mani_len];
            let mut deser = serde_json::Deserializer::from_slice(mani_json_bytes);
            let rcd = ManifestRecord::deserialize(&mut deser)?;

            let cal_crc = crc32fast::hash(mani_json_bytes);
            bytes_view.advance(mani_len);
            let sto_crc = bytes_view.get_u32();
            assert!(sto_crc == cal_crc);

            rcds.push(rcd);
        }

        let file = Arc::new(Mutex::new(File::options().append(true).open(path)?));
        file.lock().sync_all()?;
        Ok((Self { file }, rcds))
    }

    pub fn add_record(
        &self,
        _state_lock_observer: &MutexGuard<()>,
        record: ManifestRecord,
    ) -> Result<()> {
        self.add_record_when_init(record)
    }

    pub fn add_record_when_init(&self, _record: ManifestRecord) -> Result<()> {
        let mut mani_bytes = BytesMut::new();

        let json_bytes = serde_json::to_vec(&_record)?;
        mani_bytes.reserve(2 + json_bytes.len() + 4);

        mani_bytes.put_u16(json_bytes.len() as u16);
        let crc = crc32fast::hash(&json_bytes);
        mani_bytes.extend(json_bytes);
        mani_bytes.put_u32(crc);

        self.file.lock().write_all(&mani_bytes)?;
        self.file.lock().sync_all()?;
        Ok(())
    }
}
