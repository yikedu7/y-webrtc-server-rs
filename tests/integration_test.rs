extern crate core;
use futures_util::StreamExt;
use std::collections::HashMap;
use std::time::Duration;
use tokio::sync::mpsc;
use tokio::time::timeout;
use warp::ws::Message;
use y_webrtc_server_rs::msg::SignalMessage;
use y_webrtc_server_rs::route;

#[macro_use]
extern crate log;

#[tokio::test]
async fn connect() {
    let _ = pretty_env_logger::try_init();

    warp::test::ws()
        .handshake(route::index())
        .await
        .expect("connect");
}

#[tokio::test]
async fn receive_ping() {
    let _ = pretty_env_logger::try_init();

    let mut client = warp::test::ws()
        .handshake(route::index())
        .await
        .expect("connect");

    let (tx, mut rx) = mpsc::channel::<Message>(1);
    let tx_clone = tx.clone();
    tokio::spawn(async move {
        while let Some(Ok(ws_msg)) = client.next().await {
            if !ws_msg.is_ping() {
                continue;
            }
            debug!("receive_ping ws_msg={:?}", ws_msg);
            tx_clone.send(ws_msg).await.expect("send ping");
        }
    });

    if (timeout(Duration::from_secs(30), rx.recv()).await).is_err() {
        panic!("did not received ping in 30 seconds")
    }
}

#[tokio::test]
async fn signal_ping_msg() {
    let _ = pretty_env_logger::try_init();

    let mut client = warp::test::ws()
        .handshake(route::index())
        .await
        .expect("connect");

    client.send(Message::from(&SignalMessage::ping())).await;
    let ws_msg = client.recv().await.expect("received msg");
    let msg = SignalMessage::from(&ws_msg);
    assert!(msg.is_pong())
}

#[tokio::test]
async fn subscribe() {
    let _ = pretty_env_logger::try_init();

    let index = route::index();
    let mut client1 = warp::test::ws()
        .handshake(index.clone())
        .await
        .expect("connect client1");

    let mut client2 = warp::test::ws()
        .handshake(index.clone())
        .await
        .expect("connect client2");

    let topic = String::from("test-topic");
    let data = HashMap::from([(String::from("test-key"), String::from("test-value"))]);
    client1
        .send(Message::from(&SignalMessage::subscribe(Vec::from([
            topic.clone()
        ]))))
        .await;
    client2
        .send(Message::from(&SignalMessage::subscribe(Vec::from([
            topic.clone()
        ]))))
        .await;
    // sleep 1 second to avoid the concurrency issue
    tokio::time::sleep(Duration::from_secs(1)).await;
    client1
        .send(Message::from(&SignalMessage::publish(
            topic.clone(),
            data.clone(),
        )))
        .await;

    let (tx, mut rx) = mpsc::channel::<SignalMessage>(1);
    let tx_clone = tx.clone();
    tokio::spawn(async move {
        while let Some(Ok(ws_msg)) = client2.next().await {
            if ws_msg.is_ping() {
                continue;
            }
            debug!("received ws_msg={:?}", ws_msg);
            tx_clone
                .send(SignalMessage::from(&ws_msg))
                .await
                .expect("send signal msg");
        }
    });

    match timeout(Duration::from_secs(5), rx.recv()).await {
        Ok(Some(msg)) => {
            assert!(msg.is_publish());
            assert_eq!(msg.topic.unwrap(), topic);
            assert_eq!(msg.data.unwrap(), data);
        }
        Ok(None) => panic!("signal msg not exists"),
        Err(_) => panic!("did not received publish msg"),
    }
}

#[tokio::test]
async fn unsubscribe() {
    let _ = pretty_env_logger::try_init();

    let index = route::index();
    let mut client1 = warp::test::ws()
        .handshake(index.clone())
        .await
        .expect("connect client1");

    let mut client2 = warp::test::ws()
        .handshake(index.clone())
        .await
        .expect("connect client2");

    let topic = String::from("test-topic");
    let data = HashMap::from([(String::from("test-key"), String::from("test-value"))]);
    client1
        .send(Message::from(&SignalMessage::subscribe(Vec::from([
            topic.clone()
        ]))))
        .await;
    client2
        .send(Message::from(&SignalMessage::subscribe(Vec::from([
            topic.clone()
        ]))))
        .await;
    // sleep 1 second to avoid the concurrency issue
    tokio::time::sleep(Duration::from_secs(1)).await;
    client2
        .send(Message::from(&SignalMessage::unsubscribe(Vec::from([
            topic.clone(),
        ]))))
        .await;
    // sleep 1 second to avoid the concurrency issue
    client1
        .send(Message::from(&SignalMessage::publish(
            topic.clone(),
            data.clone(),
        )))
        .await;

    let (tx, mut rx) = mpsc::channel::<SignalMessage>(1);
    let tx_clone = tx.clone();
    tokio::spawn(async move {
        while let Some(Ok(ws_msg)) = client2.next().await {
            if ws_msg.is_ping() {
                continue;
            }
            debug!("received ws_msg={:?}", ws_msg);
            tx_clone
                .send(SignalMessage::from(&ws_msg))
                .await
                .expect("send signal msg");
        }
    });

    match timeout(Duration::from_secs(5), rx.recv()).await {
        Ok(Some(_)) => panic!("should not received publish msg"),
        Ok(None) => panic!("invalid signal msg"),
        Err(_) => debug!("not received publish msg"),
    }
}
