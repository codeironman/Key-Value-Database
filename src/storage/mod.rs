use bytes::Bytes;

use self::{cache::LRUcache, database::Database, memory::Map};

pub mod database;
pub mod memory;
pub mod cache;


pub struct Storage{
    db : database::Database,
    mem : memory::Map,
    cache : cache::LRUcache,
}

impl Storage 
{
    pub fn new() -> Self{
        Storage{
            db : Database::new(path),
            mem : Map::new(),
            cache : LRUcache::new(size),
            
        }
    }
    pub fn set(&mut self, key : String, value : Bytes){

    }
    pub fn get(&mut self, key : String) {

    }
}