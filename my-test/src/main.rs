use std::collections::BinaryHeap;
use std::ops::DerefMut;
fn main() {
    let mut a = vec![1];
    a.clear();
    let mut i = a.iter();
    println!("{:?}", i.is_valid());
}