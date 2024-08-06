use anyhow::Result;
use bytes::Bytes;
use core::f64;
use rand::Rng;
use std::{
    fmt::{Debug, Display},
    marker::PhantomData,
    ops::Bound,
    ptr::null_mut,
    sync::atomic::{AtomicPtr, AtomicUsize, Ordering},
};

use crate::mvcc::key::Key;

const P: f64 = 0.5; //控制升入上一层的概率
const MAX_LEVEL: usize = 16;
#[derive(Debug)]
struct Node {
    key: Option<Key<Bytes>>,
    value: Option<Bytes>,
    forward: Vec<AtomicPtr<Node>>, //代表每一层的下一个值
}

impl Node {
    pub fn new(level: usize) -> Self {
        Node {
            key: None,
            value: None,
            forward: (0..level).map(|_| AtomicPtr::new(null_mut())).collect(),
        }
    }
    pub fn node_with_value(level: usize, k: Key<Bytes>, v: Bytes) -> Self {
        Node {
            key: Some(k),
            value: Some(v),
            forward: (0..level).map(|_| AtomicPtr::new(null_mut())).collect(),
        }
    }
}

pub struct Map {
    head: AtomicPtr<Node>, //做为哨兵结点，方便插入和删除
    max_height: AtomicUsize,
}

unsafe impl Send for Map {}
unsafe impl Sync for Map {}

impl Default for Map {
    fn default() -> Self {
        Self::new()
    }
}

impl Map {
    pub fn new() -> Self {
        let head = AtomicPtr::new(Box::into_raw(Box::new(Node::new(MAX_LEVEL))));
        let tail = AtomicPtr::new(Box::into_raw(Box::new(Node::new(MAX_LEVEL))));
        unsafe {
            (*head.load(Ordering::Acquire)).forward[0]
                .store(tail.load(Ordering::Acquire), Ordering::Release);
        }
        Self {
            head,
            max_height: AtomicUsize::new(1),
        }
    }

    pub fn delete(&mut self, target_key: Key<Bytes>) -> Result<()> {
        let mut node = self.head.load(Ordering::Acquire);
        let level = self.max_height.load(Ordering::Acquire);
        let mut remove_pos = [node; MAX_LEVEL];
        for i in (0..level).rev() {
            unsafe {
                while let Some(next) = (*node).forward[i].load(Ordering::Acquire).as_ref() {
                    if let Some(ref key) = next.key {
                        if key < &target_key {
                            node = (*node).forward[i].load(Ordering::Acquire);
                        } else {
                            break;
                        }
                    } else {
                        break;
                    }
                }
                remove_pos[i] = node;
            }
        }
        unsafe {
            node = (*remove_pos[0]).forward[0].load(Ordering::Acquire);
            for (i, _) in remove_pos
                .iter()
                .enumerate()
                .take(self.max_height.load(Ordering::Acquire))
            {
                if node.is_null() || (*node).key.as_ref().unwrap() != &target_key {
                    break;
                }
                (*remove_pos[i]).forward[i].store(
                    (*node).forward[i].load(Ordering::Acquire),
                    Ordering::Release,
                );
            }
            while self.max_height.load(Ordering::Acquire) > 1
                && (*self.head.load(Ordering::Acquire)).forward
                    [self.max_height.load(Ordering::Acquire) - 1]
                    .load(Ordering::Acquire)
                    .is_null()
            {
                self.max_height.fetch_sub(1, Ordering::AcqRel);
            }
            let delete_node = Box::from_raw(node);
            drop(delete_node);
        }
        Ok(())
    }
    pub fn put(&self, target_key: Key<Bytes>, target_value: Bytes) -> Result<()> {
        let mut index = self.head.load(Ordering::Acquire);
        let mut insert_pos = [self.head.load(Ordering::Acquire); MAX_LEVEL];
        for i in (0..self.max_height.load(Ordering::Acquire)).rev() {
            unsafe {
                while let Some(next) = (*index).forward[i].load(Ordering::Relaxed).as_ref() {
                    if let Some(key) = &next.key {
                        match target_key.cmp(key) {
                            std::cmp::Ordering::Equal => {
                                let node = &mut *(*index).forward[i].load(Ordering::Acquire);
                                node.value = Some(target_value);
                                return Ok(());
                            }
                            std::cmp::Ordering::Greater => {
                                index = (*index).forward[i].load(Ordering::Acquire);
                            }
                            std::cmp::Ordering::Less => break,
                        }
                    } else {
                        break;
                    }
                }
                insert_pos[i] = index;
            }
        }
        let level = random_level();
        let new_node = Box::into_raw(Box::new(Node::node_with_value(
            level,
            target_key,
            target_value,
        )));
        //插入每层的结点
        for (i, _) in insert_pos.iter().enumerate().take(level) {
            unsafe {
                loop {
                    let next_node = (*insert_pos[i]).forward[i].load(Ordering::Acquire);
                    (*new_node).forward[i].store(next_node, Ordering::Release);
                    if (*insert_pos[i]).forward[i]
                        .compare_exchange(next_node, new_node, Ordering::AcqRel, Ordering::Acquire)
                        .is_ok()
                    {
                        break;
                    }
                }
            }
        }
        //更新最高层
        self.max_height.fetch_max(level, Ordering::Relaxed);
        Ok(())
    }

    pub fn get(&self, target_key: &Key<Bytes>) -> Option<Bytes> {
        let mut node = self.head.load(Ordering::Acquire);
        let level = self.max_height.load(Ordering::Relaxed);
        for i in (0..level).rev() {
            unsafe {
                while let Some(next) = (*node).forward[i].load(Ordering::Acquire).as_ref() {
                    if let Some(ref key) = next.key {
                        match target_key.cmp(key) {
                            std::cmp::Ordering::Equal => {
                                return next.value.clone();
                            }
                            std::cmp::Ordering::Greater => {
                                node = (*node).forward[i].load(Ordering::Acquire);
                            }
                            std::cmp::Ordering::Less => break,
                        }
                    }
                }
            }
        }
        None
    }

    pub fn iter(&self) -> MapIter {
        unsafe {
            let start = (*self.head.load(Ordering::Acquire)).forward[0].load(Ordering::Acquire);
            MapIter {
                cur: start,
                _marker: PhantomData,
            }
        }
    }

    pub fn range_iter(&self, lower: Bound<Key<Bytes>>, upper: Bound<Key<Bytes>>) -> MapRangeIter {
        let mut cur = self.head.load(Ordering::Acquire);

        unsafe {
            // Find the starting position based on the lower bound
            if let Bound::Included(ref lower_bound) | Bound::Excluded(ref lower_bound) = lower {
                for i in (0..self.max_height.load(Ordering::Relaxed)).rev() {
                    while let Some(next) = (*cur).forward[i].load(Ordering::Acquire).as_ref() {
                        if let Some(ref key) = next.key {
                            if key < lower_bound
                                || (key == lower_bound && matches!(lower, Bound::Excluded(_)))
                            {
                                cur = (*cur).forward[i].load(Ordering::Acquire);
                            } else {
                                break;
                            }
                        } else {
                            break;
                        }
                    }
                }
            }

            // Move to the first node greater than or equal to the lower bound
            cur = (*cur).forward[0].load(Ordering::Acquire);

            // Find the ending position based on the upper bound
            let end = if let Bound::Included(ref upper_bound) | Bound::Excluded(ref upper_bound) =
                upper
            {
                let mut end_node = self.head.load(Ordering::Acquire);
                for i in (0..self.max_height.load(Ordering::Relaxed)).rev() {
                    while let Some(next) = (*end_node).forward[i].load(Ordering::Acquire).as_ref() {
                        if let Some(ref key) = next.key {
                            if key < upper_bound
                                || (key == upper_bound && matches!(upper, Bound::Included(_)))
                            {
                                end_node = (*end_node).forward[i].load(Ordering::Acquire);
                            } else {
                                break;
                            }
                        } else {
                            break;
                        }
                    }
                }
                end_node
            } else {
                std::ptr::null()
            };

            MapRangeIter {
                cur,
                end,
                _marker: PhantomData,
            }
        }
    }
}
impl Display for Map {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        writeln!(f, "Max_height = {}", MAX_LEVEL)?;
        let max_digits = format!("{}", MAX_LEVEL).len();
        writeln!(f, "Current SkipList:")?;
        for i in (0..self.max_height.load(Ordering::Acquire)).rev() {
            write!(f, "Level {:>width$}: ", i, width = max_digits)?;
            unsafe {
                let mut index =
                    (*self.head.load(Ordering::Relaxed)).forward[i].load(Ordering::Acquire);
                while let Some(node) = index.as_ref() {
                    if let (Some(key), Some(value)) = (&node.key, &node.value) {
                        let value = std::str::from_utf8(value).unwrap();
                        let key = std::str::from_utf8(key.0.as_ref()).unwrap();
                        write!(f, "({key},{value}) -> ")?;
                    }
                    index = node.forward[i].load(Ordering::Acquire);
                }
                write!(f, "Node")?;
            }
            writeln!(f)?;
        }
        Ok(())
    }
}
fn random_level() -> usize {
    let mut level = 1;
    let mut r = rand::thread_rng();
    while level < MAX_LEVEL && r.gen_range(0.0..1.0) < P {
        level += 1;
    }
    level
}

pub struct MapIter<'a> {
    cur: *const Node,
    _marker: PhantomData<&'a Node>,
}

impl<'a> Iterator for MapIter<'a> {
    type Item = (Key<Bytes>, Bytes);
    fn next(&mut self) -> Option<Self::Item> {
        unsafe {
            if let Some(node) = self.cur.as_ref() {
                let key = node.key.clone();
                let value = node.value.clone();
                self.cur = node.forward[0].load(Ordering::Acquire);
                if let (Some(key), Some(value)) = (key, value) {
                    return Some((key, value));
                }
            }
            None
        }
    }
}

pub struct MapRangeIter<'a> {
    cur: *const Node,
    end: *const Node,
    _marker: PhantomData<&'a Node>,
}

impl<'a> Iterator for MapRangeIter<'a> {
    type Item = (Key<Bytes>, Bytes);

    fn next(&mut self) -> Option<Self::Item> {
        unsafe {
            if self.cur.is_null() || self.cur == self.end {
                return None;
            }

            if let Some(node) = self.cur.as_ref() {
                let key = node.key.clone();
                let value = node.value.clone();
                self.cur = node.forward[0].load(Ordering::Acquire);

                if let (Some(key), Some(value)) = (key, value) {
                    return Some((key, value));
                }
            }

            None
        }
    }
}
