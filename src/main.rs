use std::sync::atomic::AtomicUsize;
use std::sync::Arc;
use topic::Topics;
use warp::Filter;

mod handler;
mod msg;
mod topic;

extern crate pretty_env_logger;
#[macro_use]
extern crate log;
#[macro_use]
extern crate custom_error;
extern crate core;

#[tokio::main]
async fn main() {
    pretty_env_logger::init();
    info!("Start...");

    let topics = Topics::default();
    let next_uid = Arc::new(AtomicUsize::new(1));

    let index = warp::any()
        .and(warp::ws())
        .and(warp::any().map(move || topics.clone()))
        .and(warp::any().map(move || next_uid.clone()))
        .map(
            |ws: warp::ws::Ws, topics: Topics, next_uid: Arc<AtomicUsize>| {
                ws.on_upgrade(|socket| handler::on_connection(socket, topics, next_uid))
            },
        );

    warp::serve(index).run(([127, 0, 0, 1], 4444)).await;
}
