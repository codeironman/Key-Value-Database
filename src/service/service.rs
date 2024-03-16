use crate::{message::{Get, Set}, CommandService};



impl CommandService for Get {
    fn excute(self ,store : &mut crate::storage::Storage) -> crate::message::CommandResponce {
        match store.get(self.key) {
            Ok(Some(value)) => value.into(),
            Ok(None) => "Not found".into(),
            Err(e) => e.into(),
        }
    }
}


impl CommandService for Set {
    fn excute(self ,store : &mut crate::storage::Storage) -> crate::message::CommandResponce {  
        match store.set(self.key, self.value) {
            Ok(Some(value)) => value.into(),
            Ok(None) => "Set fail".into(),
            Err(e) => e.into(),
        }
    }
}