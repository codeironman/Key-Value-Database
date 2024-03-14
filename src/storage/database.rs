use rocksdb::DB;
use bytes::Bytes;
use std::error::Error;
#[derive(Debug)]
pub struct Database{
    pub db : DB,
}

impl Database {
    pub fn new(path : impl AsRef<std::path::Path>) -> Self {
        Self{
            db : DB::open_default(path).unwrap(),
        }
    }
}

impl Database {
    pub fn get(&mut self, 
        key : &str)
        -> Result<Option<Bytes>,Box<dyn Error>> {
        let v = self.db.get(key)?.unwrap();
        Ok(Some(v.into()))
    }
    pub fn set(&mut self,
        key : &str,
        value : &Bytes
        ) -> Result<(),Box<dyn Error>> {
        self.db.put(key, value)?;
        Ok(())
    }
}

