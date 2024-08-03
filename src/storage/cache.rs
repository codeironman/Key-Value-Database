use std::{
    hash::Hash,
    sync::{Arc, Mutex},
};

use dashmap::DashMap;

use super::block::block::Block;

pub type BlockCache = Cache<(usize, usize), Arc<Block>>;

type Link<K, V> = Arc<Mutex<Node<K, V>>>;

struct Node<K, V> {
    key: K,
    value: V,
    prev: Option<Link<K, V>>,
    next: Option<Link<K, V>>,
}
impl<K, V> Node<K, V> {
    pub fn new(key: K, value: V) -> Self {
        Self {
            key,
            value,
            prev: None,
            next: None,
        }
    }
}

struct LinkedList<K, V> {
    head: Option<Link<K, V>>,
    tail: Option<Link<K, V>>,
}

impl<K, V> LinkedList<K, V> {
    pub fn new() -> Self {
        Self {
            head: None,
            tail: None,
        }
    }
    #[allow(dead_code)]
    pub fn push_back(&mut self, key: K, value: V) -> Link<K, V> {
        let new_node = Arc::new(Mutex::new(Node::new(key, value)));
        if let Some(ref old_tail) = self.tail {
            old_tail.lock().unwrap().next = Some(new_node.clone());
            new_node.lock().unwrap().prev = Some(old_tail.clone());
        } else {
            self.head = Some(new_node.clone());
        }
        self.tail = Some(new_node.clone());
        new_node
    }

    pub fn push_front(&mut self, key: K, value: V) -> Link<K, V> {
        let new_node = Arc::new(Mutex::new(Node::new(key, value)));
        if let Some(ref old_head) = self.head {
            old_head.lock().unwrap().prev = Some(new_node.clone());
            new_node.lock().unwrap().next = Some(old_head.clone());
        } else {
            self.tail = Some(new_node.clone());
        }
        self.head = Some(new_node.clone());
        new_node
    }
    pub fn pop_back(&mut self) -> Option<Link<K, V>> {
        if let Some(tail) = self.tail.take() {
            let mut tail_node = tail.lock().unwrap();
            if let Some(pre) = tail_node.prev.take() {
                let mut pre_node = pre.lock().unwrap();
                pre_node.next = None;
                self.tail = Some(pre.clone());
            } else {
                self.head = None;
            }
            Some(tail.clone())
        } else {
            None
        }
    }
    pub fn remove(&mut self, index: Link<K, V>) {
        let node = index.lock().unwrap();
        if let Some(ref pre_node) = node.prev {
            let mut pre_node = pre_node.lock().unwrap();
            pre_node.next.clone_from(&node.next);
        } else {
            self.head.clone_from(&node.next);
        }
        if let Some(ref next_node) = node.next {
            let mut next_node = next_node.lock().unwrap();
            next_node.prev.clone_from(&node.prev);
        } else {
            self.tail.clone_from(&node.prev);
        }
    }
}

pub struct Cache<K, V> {
    pub capacity: usize,
    hash: DashMap<K, Link<K, V>>,
    list: LinkedList<K, V>,
}

impl<K, V> Cache<K, V>
where
    K: Eq + Hash + Clone,
    V: Clone,
{
    pub fn new(size: usize) -> Self {
        Cache {
            capacity: size,
            hash: DashMap::new(),
            list: LinkedList::new(),
        }
    }
    pub fn set(&mut self, key: K, value: V) {
        if let Some(index) = self.hash.get(&key) {
            self.list.remove(index.clone());
        }
        if self.hash.len() == self.capacity {
            if let Some(node) = self.list.pop_back() {
                let key = node.lock().unwrap().key.clone();
                self.hash.remove(&key);
            }
        }
        let iter = self.list.push_front(key.clone(), value);
        self.hash.insert(key, iter);
    }

    pub fn get(&mut self, key: K) -> Option<V> {
        if let Some(index) = self.hash.get(&key) {
            let value = {
                let node = index.lock().unwrap();
                node.value.clone()
            };
            self.list.remove(index.clone());
            let iter = self.list.push_front(key.clone(), value.clone());
            self.hash.insert(key, iter);
            Some(value)
        } else {
            None
        }
    }
}
