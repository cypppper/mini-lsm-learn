#![allow(unused_variables)] // TODO(you): remove this lint after implementing this mod
#![allow(dead_code)] // TODO(you): remove this lint after implementing this mod

use anyhow::Result;

use super::StorageIterator;

/// Merges two iterators of different types into one. If the two iterators have the same key, only
/// produce the key once and prefer the entry from A.
pub struct TwoMergeIterator<A: StorageIterator, B: StorageIterator> {
    a: A,
    b: B,
    // Add fields as need
    flag: u8, // 0 : A, 1 : B
}

impl<
        A: 'static + StorageIterator,
        B: 'static + for<'a> StorageIterator<KeyType<'a> = A::KeyType<'a>>,
    > TwoMergeIterator<A, B>
{
    pub fn create(a: A, mut b: B) -> Result<Self> {
        let mut flag = 0;
        if !a.is_valid() {
            flag = 1;
        }
        if !a.is_valid() || !b.is_valid() {
            return Ok(Self { a, b, flag });
        }
        let mut b_need_next_flag = 0;
        match a.key().cmp(&b.key()) {
            std::cmp::Ordering::Equal => {
                b_need_next_flag = 1;
            }
            std::cmp::Ordering::Less => {
                flag = 0;
            }
            _ => {
                flag = 1;
            }
        }
        if b_need_next_flag == 1 {
            b.next()?;
        }
        Ok(Self { a, b, flag })
    }
    fn checkb(&mut self) -> Result<()> {
        if self.a.is_valid() && self.b.is_valid() && self.a.key() == self.b.key() {
            self.b.next()?;
        }
        Ok(())
    }
}

impl<
        A: 'static + StorageIterator,
        B: 'static + for<'a> StorageIterator<KeyType<'a> = A::KeyType<'a>>,
    > StorageIterator for TwoMergeIterator<A, B>
{
    type KeyType<'a> = A::KeyType<'a>;

    fn key(&self) -> Self::KeyType<'_> {
        match self.flag {
            0 => self.a.key(),
            _ => self.b.key(),
        }
    }

    fn value(&self) -> &[u8] {
        match self.flag {
            0 => self.a.value(),
            _ => self.b.value(),
        }
    }

    fn is_valid(&self) -> bool {
        match self.flag {
            0 => self.a.is_valid(),
            _ => self.b.is_valid(),
        }
    }

    fn next(&mut self) -> Result<()> {
        match self.flag {
            0 => {
                self.a.next()?;
            }
            _ => {
                self.b.next()?;
            }
        }
        self.checkb()?;
        if !self.b.is_valid() {
            self.flag = 0;
            return Ok(());
        }
        if !self.a.is_valid() {
            self.flag = 1;
            return Ok(());
        }
        match self.a.key().cmp(&self.b.key()) {
            std::cmp::Ordering::Greater => {
                self.flag = 1;
            }
            _ => {
                self.flag = 0;
            }
        }
        Ok(())
    }
}
