extern crate pretty_env_logger;
#[macro_use]
extern crate log;

use y_webrtc_server_rs::route;

#[tokio::main]
async fn main() {
    pretty_env_logger::init();
    info!("Start...");

    warp::serve(route::index())
        .run(([127, 0, 0, 1], 4444))
        .await;
}
