use crate::handler;
use crate::topic::Topics;
use core::clone::Clone;
use core::sync::atomic::AtomicUsize;
use std::sync::Arc;
use warp::Filter;

pub fn index() -> impl Filter<Extract = impl warp::Reply, Error = warp::Rejection> + Clone {
    let topics = Topics::default();
    let next_uid = Arc::new(AtomicUsize::new(1));

    warp::any()
        .and(warp::ws())
        .and(warp::any().map(move || topics.clone()))
        .and(warp::any().map(move || next_uid.clone()))
        .map(
            |ws: warp::ws::Ws, topics: Topics, next_uid: Arc<AtomicUsize>| {
                ws.on_upgrade(|socket| handler::on_connection(socket, topics, next_uid))
            },
        )
}
