#![allow(unused_variables)] // TODO(you): remove this lint after implementing this mod
#![allow(dead_code)] // TODO(you): remove this lint after implementing this mod

mod builder;
mod iterator;

pub use builder::BlockBuilder;
use bytes::Buf;
use bytes::BufMut;
use bytes::Bytes;
pub use iterator::BlockIterator;

/// A block is the smallest unit of read and caching in LSM tree. It is a collection of sorted key-value pairs.
pub struct Block {
    pub(crate) data: Vec<u8>,
    pub(crate) offsets: Vec<u16>,
}

impl Block {
    /// Encode the internal data to the data layout illustrated in the tutorial
    /// Note: You may want to recheck if any of the expected field is missing from your output
    pub fn encode(&self) -> Bytes {
        let mut b: Vec<u8> = vec![];
        b.extend_from_slice(&self.data);
        for off in &self.offsets {
            b.put_u16(*off);
        }
        b.put_u16(self.offsets.len() as u16);
        Bytes::from(b)
    }

    /// Decode from the data layout, transform the input `data` to a single `Block`
    pub fn decode(data: &[u8]) -> Self {
        let (lf, mut rt) = data.split_at(data.len() - 2);
        let len: usize = rt.get_u16() as usize;
        let of_len = len * 2;
        let (_data, mut _off) = lf.split_at(lf.len() - of_len);
        let mut o: Vec<u16> = vec![];
        for i in 0..len {
            o.push(_off.get_u16());
        }
        Self {
            data: Vec::<u8>::from(_data),
            offsets: o,
        }
    }
}
