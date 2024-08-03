use bytes::Bytes;
use clap::Parser;

#[derive(Parser)]
pub enum ClientArgs {
    Get {
        #[clap(long)]
        key: Bytes,
    },
    Set {
        #[clap(long)]
        key: Bytes,
        #[clap(long)]
        value: Bytes,
    },
    // Publish {
    //     #[clap(long)]
    //     key: Bytes,
    //     #[clap(long)]
    //     value: Bytes,
    // },
    // Subscribe {
    //     #[clap(long)]
    //     topic: Bytes,
    // },
    // Unsubscribe {
    //     #[clap(long)]
    //     topic: Bytes,
    //     #[clap(long)]
    //     id: u32,
    // },
}
