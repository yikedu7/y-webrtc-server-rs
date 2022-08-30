use std::{env};
use std::fmt::Error;
use futures_util::{StreamExt};
use tokio::net::{TcpListener, TcpStream};
use tokio_tungstenite::tungstenite::Message;

#[macro_use]
extern crate log;

#[tokio::main]
async fn main() -> Result<(), Error> {
    env_logger::init();
    let _ = env_logger::try_init();
    let port = env::var("PORT").unwrap_or_else(|_| {String::from("4444")});
    let addr = format!("127.0.0.1:{}", port);

    let listener = TcpListener::bind(&addr).await.expect("Failed to bind");
    info!("Listening on: {:?}", addr);

    while let Ok((stream, _)) = listener.accept().await {
        tokio::spawn(accept_stream(stream));
    }

    Ok(())
}

async fn accept_stream(steam: TcpStream) {
    let addr = steam.peer_addr().expect("should have a address");
    let mut ws_stream = tokio_tungstenite::accept_async(steam).await.expect("Failed to accept socket stream");
    info!("New WebSocket connection: {:?}", addr);

    while let Some(msg) = ws_stream.next().await {
        let msg = msg.expect("Failed to get msg");
        info!("Received Others Msg {:?}", msg);
    }
}
