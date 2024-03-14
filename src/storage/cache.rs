use std::{collections::{HashMap, LinkedList}, sync::Mutex};

use bytes::Bytes;

pub struct LRUcache {
    pub capacity : usize,
    hash : Mutex<HashMap<String,usize>>,
    list : Mutex<LinkedList<(String,Bytes)>>
}

impl LRUcache {
    pub fn new(size : usize) -> Self{
        LRUcache{
            capacity : size,
            hash : Mutex::new(HashMap::new()),
            list : Mutex::new(LinkedList::new()),
        }
    }
    pub fn set(&mut self, key : String, value : Bytes) {
        let mut hash = self.hash.lock().unwrap();
        let mut list = self.hash.lock().unwrap();
        if let Some(&index) = hash.get(&key) {
        }
    }
}