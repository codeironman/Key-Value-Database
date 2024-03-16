use std::{collections::HashMap, sync::{Arc, Mutex}};

use bytes::Bytes;

type Link = Arc<Mutex<Node>>;

struct  Node {
    key : String,
    value : Bytes,
    prev : Option<Link>,
    next : Option<Link>,
}
impl Node {
   pub fn new(key : String, value : Bytes) -> Self{
        Self{
            key : key,
            value : value,
            prev : None,
            next : None,
        }
   } 
}

struct Linkedlist {
    head : Option<Link>,
    tail : Option<Link>,
}

impl Linkedlist {
    pub fn new() -> Self{
        Self{
            head : None,
            tail : None
        }
    }    
    pub fn push_back(&mut self, key: String, value: Bytes) -> Link{
        let new_node = Arc::new(Mutex::new(Node::new(key, value)));
        if let Some(ref old_tail) = self.tail {
            old_tail.lock().unwrap().next = Some(new_node.clone());
            new_node.lock().unwrap().prev = Some(old_tail.clone());
        } else {
            self.head = Some(new_node.clone());
        }
        self.tail = Some(new_node.clone());
        return new_node;
    }
    
    
    pub fn push_front(&mut self,key : String , value: Bytes) -> Link{
        let new_node = Arc::new(Mutex::new(Node::new(key, value)));
        if let Some(ref old_head) = self.head {
            old_head.lock().unwrap().prev = Some(new_node.clone());
            new_node.lock().unwrap().next = Some(old_head.clone());
        }else {
            self.tail = Some(new_node.clone());
        }
        self.head = Some(new_node.clone());
        return new_node;
    }
    pub fn pop_back(&mut self) -> Option<Link>{
        if let Some(tail) = self.tail.take() {
           let mut tail_node = tail.lock().unwrap();
           if let Some(pre) = tail_node.prev.take() {
              let mut pre_node = pre.lock().unwrap();
              pre_node.next = None;
              self.tail = Some(pre.clone());
           }
           else {
              self.head = None;
           }
           Some(tail.clone())
        }
        else {
           return None;
        }
      }
    pub fn remove(&mut self,index : Link){
      let node = index.lock().unwrap();
        if let Some(ref pre_node) = node.prev {
         let mut pre_node = pre_node.lock().unwrap();
         pre_node.next = node.next.clone();
        }
        else{
            self.head = node.next.clone();
        }
        if let Some(ref next_node) = node.next {
            let mut next_node = next_node.lock().unwrap();
            next_node.prev = node.prev.clone();
        }
        else {
            self.tail = node.prev.clone();
        }
    }

}

pub struct LRUcache {
    pub capacity : usize,
    hash : Mutex<HashMap<String,Link>>,
    list : Linkedlist //内部保证的线程安全，不需要额外加锁
}

impl LRUcache {
    pub fn new(size : usize) -> Self{
        LRUcache{
            capacity : size,
            hash : Mutex::new(HashMap::new()),
            list : Linkedlist::new(),
        }
    }
    pub fn set(&mut self, key : String, value : Bytes) {
        let mut hash = self.hash.lock().unwrap();
        if let Some(index) = hash.get(&key) {
            self.list.remove(index.clone());
        }
        if hash.len() == self.capacity {
            if let Some(node) = self.list.pop_back(){
                let key = node.lock().unwrap().key.clone();
                hash.remove(&key);
            }
        }
        let iter = self.list.push_front(key.clone(), value);
        hash.insert(key, iter);
    }

    pub fn get(&mut self, key : String) -> Option<Bytes> {
        let mut hash = self.hash.lock().unwrap();
        if let Some(index) = hash.get(&key) {
            let value = {
                let node = index.lock().unwrap();
                node.value.clone()
            };
            self.list.remove(index.clone());
            let iter = self.list.push_front(key.clone(), value.clone());
            hash.insert(key, iter);
            Some(value)
        } else {
            None
        }
    }
}