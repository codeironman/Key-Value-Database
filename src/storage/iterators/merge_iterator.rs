use std::{
    cmp,
    collections::{binary_heap::PeekMut, BinaryHeap},
};

use bytes::Bytes;

use crate::mvcc::key::Key;

use super::iterators::StorageIterator;

struct HeapWrapper<I: StorageIterator>(pub usize, pub Box<I>);

pub struct MergeIterator<I>
where
    I: StorageIterator,
{
    iters: BinaryHeap<HeapWrapper<I>>,
    current: Option<HeapWrapper<I>>,
}

impl<I: StorageIterator> PartialEq for HeapWrapper<I> {
    fn eq(&self, other: &Self) -> bool {
        self.partial_cmp(other).unwrap() == cmp::Ordering::Equal
    }
}

impl<I: StorageIterator> PartialOrd for HeapWrapper<I> {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        match self.1.key().cmp(&other.1.key()) {
            cmp::Ordering::Equal => self.0.partial_cmp(&other.0),
            cmp::Ordering::Greater => Some(cmp::Ordering::Greater),
            cmp::Ordering::Less => Some(cmp::Ordering::Less),
        }
        .map(|x| x.reverse())
    }
}

impl<I: StorageIterator> Eq for HeapWrapper<I> {}

impl<I: StorageIterator> Ord for HeapWrapper<I> {
    fn cmp(&self, other: &Self) -> cmp::Ordering {
        self.partial_cmp(other).unwrap()
    }
}

impl<I: StorageIterator> MergeIterator<I> {
    pub fn new(iters: Vec<Box<I>>) -> Self {
        if iters.is_empty() {
            return Self {
                iters: BinaryHeap::new(),
                current: None,
            };
        }

        let mut heap = BinaryHeap::new();

        if iters.iter().all(|x| !x.is_valid()) {
            let mut iter = iters;
            return Self {
                iters: heap,
                current: Some(HeapWrapper(0, iter.pop().unwrap())),
            };
        }

        for (index, iter) in iters.into_iter().enumerate() {
            if iter.is_valid() {
                heap.push(HeapWrapper(index, iter));
            }
        }

        let current = heap.pop().unwrap();
        Self {
            iters: heap,
            current: Some(current),
        }
    }
}

impl<I: 'static + StorageIterator<KeyType = Key<Bytes>>> StorageIterator for MergeIterator<I> {
    type KeyType = Key<Bytes>;
    fn is_valid(&self) -> bool {
        self.current
            .as_ref()
            .map(|x| x.1.is_valid())
            .unwrap_or(false)
    }

    fn key(&self) -> Key<Bytes> {
        self.current.as_ref().unwrap().1.key()
    }

    fn value(&self) -> bytes::Bytes {
        self.current.as_ref().unwrap().1.value()
    }

    fn next(&mut self) -> anyhow::Result<()> {
        let current = self.current.as_mut().unwrap();
        while let Some(inner_iter) = self.iters.peek_mut() {
            if inner_iter.1.key() == current.1.key() {
                if !inner_iter.1.is_valid() {
                    PeekMut::pop(inner_iter);
                }
            } else {
                break;
            }
        }

        current.1.next()?;

        if !current.1.is_valid() {
            if let Some(iter) = self.iters.pop() {
                *current = iter;
            }
            return Ok(());
        }

        if let Some(mut iter) = self.iters.peek_mut() {
            if *current < *iter {
                std::mem::swap(&mut *iter, current);
            }
        }

        Ok(())
    }
}
