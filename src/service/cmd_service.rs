use crate::{
    cmd::{Get, Set},
    CmdService,
};

use super::Store;

impl CmdService for Get {
    async fn excute(self, store: &Store) -> crate::cmd::CmdResponse {
        match store.get(&self.key).await {
            Ok(Some(value)) => value.into(),
            Ok(None) => "Not found".into(),
            Err(e) => e.into(),
        }
    }
}

impl CmdService for Set {
    async fn excute(self, store: &Store) -> crate::cmd::CmdResponse {
        match store.put(&self.key, &self.value).await {
            Ok(()) => "Put succes".into(),
            Err(e) => e.into(),
        }
    }
}
