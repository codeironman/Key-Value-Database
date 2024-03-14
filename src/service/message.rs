use bytes::Bytes;
use serde::{Deserialize, Serialize};

//客户端请求
pub struct CommandRequest {
    pub request : Option<cmdrequest::RequestData>
}

pub mod cmdrequest {
    pub enum RequestData {
        Get(super::Get),
        Set(super::Set),
        Publish(super::Publish),
        Subscribe(super::Subscribe),
        Unsubscribe(super::Unsubscribe),        
    }
}

//服务器回应
#[derive(Default)]
pub struct CommandResponce {
    pub status : usize,
    pub message : String,
    pub value : Bytes
}


pub struct Get{
    pub key : String
}

pub struct Set{
    pub key : String,
    pub value  : Bytes,
}


pub struct Publish{
    pub topic : String,
    pub value : Bytes,
}

pub struct  Subscribe {
    pub topic : String
}

pub struct Unsubscribe {
    pub topic : String,
    pub id : usize,
}

