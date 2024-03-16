
use std::{error::Error, ops::Mul};

use bytes::Bytes;

use crate::{storage, Storage};

use self::message::{cmdrequest::RequestData, CommandRequest, CommandResponce, Get, Set};

pub mod service;
pub mod message;

pub trait CommandService {
    fn excute(self ,store : &mut storage::Storage) -> CommandResponce;
}


impl CommandRequest {
    pub fn get(key : impl Into<String>) -> Self {
        Self{
            request : Some(RequestData::Get(Get{key : key.into()})),
        }
    }
    pub fn set(key : impl Into<String>,value : Bytes) -> Self {
        Self{
            request : Some(RequestData::Set(Set{
                key : key.into(),
                value : value,
            }))
        }
    }
}

impl CommandResponce {
    pub fn new (status : usize, message : String, value : Bytes) -> Self {
        Self{
            status, 
            message,
            value,
        }
    }
}

impl From<Bytes> for CommandResponce {
    fn from(value: Bytes) -> Self {
        Self{
            status : 200usize,
            message : "success".to_string(),
            value : value
        }
    }
}

impl From<&str> for CommandResponce {
    fn from(value: &str) -> Self {
        Self{
            status : 400usize,
            message : value.to_string(),
            ..Default::default()
        }
    }
}

impl From<Box<dyn Error>> for CommandResponce {
    fn from(e: Box<dyn Error>) -> Self {
        Self{
            status : 500usize,
            message : e.to_string(),
            ..Default::default()
        }
    }
}


pub struct StoreService{
    store : Storage,
    on_revc_req : Vec<fn(&CommandRequest)>,
    on_exec_req : Vec<fn(&CommandResponce)>,
    on_before_res : Vec<fn(&CommandResponce)>,
}

impl StoreService {
    pub fn new(store : Storage) -> Self{
        Self{
            store,
            on_revc_req : Vec::new(),
            on_exec_req : Vec::new(),
            on_before_res : Vec::new(),
        }
    }
    pub fn regist_recv_req(mut self,f :fn(&CommandRequest)) -> Self {
        self.on_revc_req.push(f);
        self
    }

    pub fn regist_before_res(mut self,f : fn(&CommandResponce)) -> Self {
        self.on_exec_req.push(f);
        self
    }

    pub async fn notify_recv_req(&self,com_req : &CommandRequest){
        self.on_revc_req.iter().for_each(|f|f(com_req))
    } 
    pub async fn notify_exec_req(&self,cmd_res : &CommandResponce) {
        self.on_exec_req.iter().for_each(|f|f(cmd_res))
    }

    pub async fn notify_before_res(&self,cmd_res : &mut CommandResponce) {
        self.on_before_res.iter().for_each(|f|f(cmd_res))
    }

}