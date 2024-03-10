use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize)]
enum Command {
    Get,
    Set,
    Publish,
    Subscribe,
}
#[derive(Serialize, Deserialize)]
pub struct Message {
    pub command: String,
}
