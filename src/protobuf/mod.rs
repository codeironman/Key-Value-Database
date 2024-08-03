use bytes::Bytes;
use cmd::{
    cmd_request::ReqData, CmdRequest, CmdResponse, Get, Publish, Set, Subscribe, Unsubscribe,
};

pub mod cmd;

impl CmdRequest {
    // GET命令
    pub fn get(key: Bytes) -> Self {
        Self {
            req_data: Some(ReqData::Get(Get { key })),
        }
    }

    // SET命令
    pub fn set(key: Bytes, value: Bytes) -> Self {
        Self {
            req_data: Some(ReqData::Set(Set { key, value })),
        }
    }

    // PUBLISH命令
    pub fn publish(topic: Bytes, value: Bytes) -> Self {
        Self {
            req_data: Some(ReqData::Publish(Publish { topic, value })),
        }
    }

    // 订阅命令
    pub fn subscribe(topic: Bytes) -> Self {
        Self {
            req_data: Some(ReqData::Subscribe(Subscribe { topic })),
        }
    }

    // 解除订阅命令
    pub fn unsubscribe(topic: Bytes, id: u32) -> Self {
        Self {
            req_data: Some(ReqData::Unsubscribe(Unsubscribe { topic, id })),
        }
    }
}

impl CmdResponse {
    pub fn new(status: u32, message: String, value: Bytes) -> Self {
        Self {
            status,
            message,
            value,
        }
    }
}

impl From<Bytes> for CmdResponse {
    fn from(v: Bytes) -> Self {
        Self {
            status: 200u32,
            message: "success".to_string(),
            value: v,
        }
    }
}

impl From<&str> for CmdResponse {
    fn from(s: &str) -> Self {
        Self {
            status: 400u32,
            message: s.to_string(),
            ..Default::default()
        }
    }
}

impl From<anyhow::Error> for CmdResponse {
    fn from(e: anyhow::Error) -> Self {
        Self {
            status: 500u32,
            message: e.to_string(),
            ..Default::default()
        }
    }
}
