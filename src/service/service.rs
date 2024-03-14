
use crate::{message::Get, CommandService};



impl CommandService for Get {
    fn excute(self ,store : &crate::storage::Storage) -> crate::message::CommandResponce {
        match store.get(self.key) {
            Ok(Some(value)) => value.into(),
            Ok(None) => "Not found".into(),
            Err(e) => e.into(),
        }
    }
}