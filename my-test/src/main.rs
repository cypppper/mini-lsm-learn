use std::collections::BinaryHeap;
use std::ops::DerefMut;
use std::io::BufRead;
use bytes::{Bytes, Buf, BufMut};
fn main() {
  let a  = String::from("aaacccbbb");
  let b  = Bytes::from("aaaa bbb");
  let c = Bytes::from(a);

}