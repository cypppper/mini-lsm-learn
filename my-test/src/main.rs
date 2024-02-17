use std::{collections::BinaryHeap, thread::sleep};
use std::ops::DerefMut;
use std::io::BufRead;
use bytes::{Bytes, Buf, BufMut};
fn main() {
    use std::time::{Duration, Instant};
    use crossbeam_channel::tick;
    
    let start = Instant::now();
    let ticker = tick(Duration::from_millis(100));
    let dura = Duration::from_secs(1);
        sleep(dura);
    for _ in 0..5 {
        ticker.recv().unwrap();
        let dura = Duration::from_secs(1);
        // sleep(dura);
        println!("elapsed: {:?}", start.elapsed());
    }

}