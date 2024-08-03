use std::{future::Future, sync::Arc};

use crate::{
    cmd::{cmd_request::ReqData, CmdRequest, CmdResponse},
    lsm_tree::ArcDB,
};
mod cmd_service;

pub type Store = ArcDB;
pub trait CmdService {
    fn excute(self, store: &Store) -> impl Future<Output = CmdResponse> + Send;
}

pub struct Service {
    store_service: Arc<StoreService>,
}

pub struct StoreService {
    store: Store,
}
impl Service {
    pub fn new(store: Store) -> Self {
        Self {
            store_service: Arc::new(StoreService { store }),
        }
    }

    // 执行命令
    pub async fn execute(&self, cmd_req: CmdRequest) -> CmdResponse {
        println!("=== Execute Command Before ===");
        let cmd_res = process_cmd(cmd_req, &self.store_service.store).await;
        println!("=== Execute Command After ===");
        cmd_res
    }
}

// 实现Clone trait
impl Clone for Service {
    fn clone(&self) -> Self {
        Self {
            store_service: self.store_service.clone(),
        }
    }
}

// 处理请求命令，返回Response
async fn process_cmd(cmd_req: CmdRequest, store: &Store) -> CmdResponse {
    match cmd_req.req_data {
        // 处理 GET 命令
        Some(ReqData::Get(cmd_get)) => cmd_get.excute(store).await,
        // 处理 SET 命令
        Some(ReqData::Set(cmd_set)) => cmd_set.excute(store).await,
        _ => "Invalid command".into(),
    }
}
