#![allow(dead_code)] // REMOVE THIS LINE after fully implementing this functionality

use std::path::Path;
use std::path::PathBuf;
use std::sync::Arc;
use std::{fs::File, io::Write};

use anyhow::Result;
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
        println!(" file sz {:?}", file.metadata()?.len());
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
        println!("file sz {:?}", fob.size());
        let mut deser = serde_json::Deserializer::from_slice(&bytes);

        let mut rcds = vec![];
        while !deser.end().is_ok() {
            let a = ManifestRecord::deserialize(&mut deser)?;
            // println!("[recover] {:?}", a);
            rcds.push(a);
        }

        let file = Arc::new(Mutex::new(File::create(path)?));
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
        let stream = serde_json::to_vec(&_record)?;
        self.file.lock().write_all(&stream)?;
        self.file.lock().sync_all()?;
        Ok(())
    }
}
