use std::{future::Future, pin::Pin};

use crate::{
    cmd::{CmdResponse, Get, Set},
    CmdService,
};

use super::Store;

impl CmdService for Get {
    fn excute(self, store: &Store) -> Pin<Box<dyn Future<Output = CmdResponse> + Send + '_>> {
        Box::pin(async move {
            match store.get(&self.key).await {
                Ok(Some(value)) => value.into(),
                Ok(None) => "Not found".into(),
                Err(e) => e.into(),
            }
        })
    }
}

impl CmdService for Set {
    fn excute(self, store: &Store) -> Pin<Box<dyn Future<Output = CmdResponse> + Send + '_>> {
        Box::pin(async move {
            match store.put(&self.key, &self.value).await {
                Ok(()) => "Put success".into(),
                Err(e) => e.into(),
            }
        })
    }
}
