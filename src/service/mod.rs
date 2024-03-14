
use std::{default, error::Error};

use bytes::Bytes;

use crate::storage;

use self::message::{cmdrequest::RequestData, CommandRequest, CommandResponce, Get, Set};

pub mod service;
pub mod message;

pub trait CommandService {
    fn excute(self ,store : &storage::Storage) -> CommandResponce;
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
