
#![allow(dead_code)] // REMOVE THIS LINE after fully implementing this functionality

use crossbeam_skiplist::SkipMap;
use crossbeam_utils::thread::scope;
use std::ops::{Bound, RangeBounds};
use std::path::Path;
use std::sync::atomic::AtomicUsize;
use std::sync::Arc;

use anyhow::Result;
use bytes::Bytes;

fn fetch(k: Bytes, table: &SkipMap<Bytes, Bytes>) -> Option<Bytes> {
  match table.get(&k) {
    // Some() => {return Ok(None);}
    Some(x) => {
      Some(x.value().clone())
    },
    None => {
      None
    },
  }
}

fn main() {    
  let memtable: SkipMap<Bytes, Bytes> = SkipMap::new();
  memtable.insert(Bytes::copy_from_slice(b"av"), Bytes::copy_from_slice(b""));
  match fetch(b"av"[..].into(), &memtable) {
    Some(x) if x.len() == 0 => {
      println!("aasssaa");
    }
    Some(x) => {
      println!("aaaa");
    }
    None => {}
  }
 


  
  let person_ages = SkipMap::new();
  
  scope(|s| {
      // Insert entries into the map from multiple threads.
      s.spawn(|_| {
          person_ages.insert("Spike Garrett", 22);
          person_ages.insert("Stan Hancock", 47);
          person_ages.insert("Rea Bryan", 234);
  
          assert_eq!(person_ages.get("Spike Garrett").unwrap().value(), &22);
      });
      s.spawn(|_| {
          person_ages.insert("Bryon Conroy", 65);
          person_ages.insert("Lauren Reilly", 2);
      });
  }).unwrap();
  
  assert!(person_ages.contains_key("Spike Garrett"));
  person_ages.remove("Rea Bryan");
  assert!(!person_ages.contains_key("Rea Bryan"));
}

pub(crate) fn map_bound(bound: Bound<&[u8]>) -> Bound<Bytes> {
  match bound {
      Bound::Included(x) => Bound::Included(Bytes::copy_from_slice(x)),
      Bound::Excluded(x) => Bound::Excluded(Bytes::copy_from_slice(x)),
      Bound::Unbounded => Bound::Unbounded,
  }
}

#[derive(Default)]
pub struct MemTable {
    map: Arc<SkipMap<Bytes, Bytes>>,

    id: usize,
    approximate_size: Arc<AtomicUsize>,
}

impl MemTable {
  fn create() ->Self {
    Self {
      ..Default::default()
    }
  }
}

fn fns() {
  let p = MemTable::create();


}

