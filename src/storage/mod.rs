use bytes::Bytes;
use self::{cache::LRUcache, database::Database, memory::Map};

pub mod database;
pub mod memory;
pub mod cache;
 

pub struct Storage{
    level1_cache : cache::LRUcache,
    level2_memory : memory::Map,
    level3_database : database::Database,
}

impl Storage 
{
    pub fn new(path : String) -> Self{
        Storage{
            level1_cache : LRUcache::new(16),    
            level2_memory : Map::new(),
            level3_database : Database::new(path),
        }
    }
    pub fn set(&mut self, key : String, value : Bytes) -> Result<Option<Bytes>,Box<dyn std::error::Error>>{
        self.level1_cache.set(key.clone(), value.clone());  
        self.level2_memory.set(key.clone(), value.clone())?;
        self.level3_database.set(key.clone(), value.clone())?;
        return Ok(Some(value));

    }
    pub fn get(&mut self, key : String) -> Result<Option<Bytes>,Box<dyn std::error::Error>>{
        if let Some(value) = self.level1_cache.get(key.clone()){
            Ok(Some(value))
        }
        else if  let Some(value) = self.level2_memory.get(key.clone()){
            Ok(Some(value.clone()))
        }
        else {
            Ok(None)
        }
    }
}