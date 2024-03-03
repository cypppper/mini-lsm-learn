use std::{collections::BinaryHeap, thread::sleep};
use std::ops::DerefMut;
use std::io::BufRead;
use bytes::{Bytes, Buf, BufMut};
fn main() {
    let vec = vec![1,2,3,4,5];
    let a = vec.partition_point(|&x| x>=3);
    println!("{:?}", a);

}