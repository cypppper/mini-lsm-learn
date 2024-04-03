#![allow(unused_variables)] // TODO(you): remove this lint after implementing this mod
#![allow(dead_code)] // TODO(you): remove this lint after implementing this mod

use std::collections::BTreeMap;

pub struct Watermark {
    readers: BTreeMap<u64, usize>,
}

impl Watermark {
    pub fn new() -> Self {
        Self {
            readers: BTreeMap::new(),
        }
    }

    pub fn add_reader(&mut self, ts: u64) {
        let entry = self.readers.entry(ts).or_default();
        *entry += 1;
    }

    pub fn remove_reader(&mut self, ts: u64) {
        assert!(self.readers.contains_key(&ts));
        let entry = self.readers.entry(ts).and_modify(|x| *x -= 1).or_default();
        if *entry == 0 {
            self.readers.remove(&ts);
        }
    }

    pub fn watermark(&self) -> Option<u64> {
        if self.readers.is_empty() {
            None
        } else {
            Some(self.readers.iter().map(|entry| *entry.0).min().unwrap())
        }
    }

    pub fn num_retained_snapshots(&self) -> usize {
        self.readers.len()
    }
}
