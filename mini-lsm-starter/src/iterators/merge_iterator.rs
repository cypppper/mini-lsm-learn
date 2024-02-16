#![allow(unused_variables)] // TODO(you): remove this lint after implementing this mod
#![allow(dead_code)] // TODO(you): remove this lint after implementing this mod

use std::cmp::{self, Ordering};
use std::collections::binary_heap::PeekMut;
use std::collections::BinaryHeap;
use std::iter::Enumerate;
use std::mem::swap;
use std::ops::{Deref, DerefMut};

use anyhow::Result;

use crate::key::{Key, KeySlice};

use super::StorageIterator;

struct HeapWrapper<I: StorageIterator>(pub usize, pub Box<I>);

impl<I: StorageIterator> PartialEq for HeapWrapper<I> {
    fn eq(&self, other: &Self) -> bool {
        self.partial_cmp(other).unwrap() == cmp::Ordering::Equal
    }
}

impl<I: StorageIterator> Eq for HeapWrapper<I> {}

impl<I: StorageIterator> PartialOrd for HeapWrapper<I> {
    #[allow(clippy::non_canonical_partial_ord_impl)]
    fn partial_cmp(&self, other: &Self) -> Option<cmp::Ordering> {
        match self.1.key().cmp(&other.1.key()) {
            cmp::Ordering::Greater => Some(cmp::Ordering::Greater),
            cmp::Ordering::Less => Some(cmp::Ordering::Less),
            cmp::Ordering::Equal => self.0.partial_cmp(&other.0),
        }
        .map(|x| x.reverse())
    }
}

impl<I: StorageIterator> Ord for HeapWrapper<I> {
    fn cmp(&self, other: &Self) -> cmp::Ordering {
        self.partial_cmp(other).unwrap()
    }
}

/// Merge multiple iterators of the same type. If the same key occurs multiple times in some
/// iterators, prefer the one with smaller index.
pub struct MergeIterator<I: StorageIterator> {
    iters: BinaryHeap<HeapWrapper<I>>,
    current: Option<HeapWrapper<I>>,
}

impl<I: StorageIterator> MergeIterator<I> {
    pub fn create(iters: Vec<Box<I>>) -> Self {
        if iters.is_empty() {
            return Self {
                iters: BinaryHeap::new(),
                current: None,
            };
        }
        let mut heap_iter: BinaryHeap<HeapWrapper<I>> = BinaryHeap::new();
        for (i, single_st_iter) in iters.into_iter().enumerate() {
            if !single_st_iter.is_valid() {
                continue;
            }
            heap_iter.push(HeapWrapper(i, single_st_iter));
        }
        println!("create end!");
        Self {
            current: heap_iter.pop(),
            iters: heap_iter,
        }
    }

    fn next_inner(&mut self) -> Result<()> {
        println!("enter MergeIter.next");
        let cur_key_before_next = self.current.as_ref().unwrap();
        while let Some(mut inner_iter) = self.iters.peek_mut() {
            // println!(
            //     "heap iter key: {}, cur_key: {}",
            //     inner_iter.1.key(),
            //     cur_key_before_next.1.key().into()
            // );

            if inner_iter.1.key().cmp(&cur_key_before_next.1.key()) == Ordering::Equal {
                // println!("jump key: {:?}", inner_iter.1.key());
                inner_iter.1.next()?;

                if !inner_iter.1.is_valid() {
                    PeekMut::pop(inner_iter);
                    continue;
                }
            } else {
                break;
            }
        }

        self.current.as_mut().unwrap().1.next()?;
        if self.iters.is_empty() {
            return Ok(());
        }
        if !self.current.as_mut().unwrap().1.is_valid() {
            self.current = self.iters.pop();
            return Ok(());
        }
        let cur_key = self.current.as_ref().unwrap();
        let heap_key = self.iters.peek().unwrap();
        match cur_key.cmp(&heap_key) {
            Ordering::Greater => {
                return Ok(());
            }
            Ordering::Less => {
                swap(
                    self.iters.peek_mut().unwrap().deref_mut(),
                    self.current.as_mut().unwrap(),
                );
                return Ok(());
            }
            _ => {
                panic!("should not enter this!");
            }
        }
    }
}

impl<I: 'static + for<'a> StorageIterator<KeyType<'a> = KeySlice<'a>>> StorageIterator
    for MergeIterator<I>
{
    type KeyType<'a> = KeySlice<'a>;

    fn key(&self) -> KeySlice {
        self.current.as_ref().unwrap().1.key()
    }

    fn value(&self) -> &[u8] {
        self.current.as_ref().unwrap().1.value()
    }

    fn is_valid(&self) -> bool {
        if let Some(x) = self.current.as_ref() {
            x.1.is_valid()
        } else {
            false
        }
    }

    fn next(&mut self) -> Result<()> {
        self.next_inner()?;
        Ok(())
    }
}
