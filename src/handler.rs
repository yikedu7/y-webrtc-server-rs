use crate::msg::SignalMessage;
use crate::topic::{AsTopic, AsTopics, Topics};
use futures_util::stream::SplitSink;
use futures_util::{SinkExt, StreamExt};
use std::collections::HashSet;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc;
use tokio::sync::mpsc::{Receiver, UnboundedReceiver, UnboundedSender};
use tokio::time;
use tokio_stream::wrappers::UnboundedReceiverStream;
use warp::ws::{Message, WebSocket};

pub async fn on_connection(socket: WebSocket, topics: Topics, next_uid: Arc<AtomicUsize>) {
    let uid = next_uid.fetch_add(1, Ordering::Relaxed);
    info!("user(uid={}) on connect", uid);
    let mut subscribed_topics = HashSet::new();
    let (ws_tx, mut ws_rx) = socket.split();

    // Use an unbounded channel to handle buffering and flushing of messages to the websocket...
    let (tx, rx): (UnboundedSender<Message>, UnboundedReceiver<Message>) =
        mpsc::unbounded_channel();
    let rx = UnboundedReceiverStream::new(rx);
    tokio::task::spawn(flush_to_ws(ws_tx, rx));

    let tx_clone = tx.clone();
    let (pong_tx, pong_rx) = mpsc::channel(1);
    tokio::spawn(interval_ping(tx_clone, pong_rx));

    while let Some(result) = ws_rx.next().await {
        let ws_msg = match result {
            Ok(msg) => msg,
            Err(e) => {
                error!("websocket error: {}", e);
                break;
            }
        };
        match ws_msg {
            _ if ws_msg.is_pong() => {
                pong_tx
                    .try_send(())
                    .unwrap_or_else(|e| error!("failed to send websocket pong: {}", e));
                debug!("received pong and forwarding: {:?}", ws_msg)
            }
            _ if ws_msg.is_text() => {
                let msg = SignalMessage::from(&ws_msg);
                debug!("received signal msg: {:?}", msg);
                match msg.message_type.as_str() {
                    "ping" => {
                        tx.send(Message::from(&SignalMessage::pong()))
                            .unwrap_or_else(|e| error!("failed to send pong: {}", e));
                        debug!("user(uid={}) pinged", uid)
                    }
                    "publish" => {
                        let msg_ref = &msg;
                        if let Some(topic_name) = msg.topic.as_ref() {
                            if let Some(topic) = topics.get(topic_name).unwrap_or_else(|e| {
                                error!("failed to get topic {}", e);
                                None
                            }) {
                                topic.publish(msg_ref).unwrap();
                                debug!(
                                    "user(uid={}) published msg={:?} to topic={}",
                                    uid,
                                    msg,
                                    topic.name()
                                )
                            }
                        }
                    }
                    "subscribe" => {
                        for topic_name in msg.topics.unwrap_or_default() {
                            topics
                                .create_if_not_exists(&topic_name)
                                .map(|topic| {
                                    topic.subscribe(uid, tx.clone()).unwrap_or_else(|e| {
                                        error!("failed to subscribe topic: {}", e)
                                    });
                                    subscribed_topics.insert(topic_name);
                                    debug!("user(uid={}) subscribed topic={}", uid, topic.name())
                                })
                                .unwrap_or_else(|e| error!("failed to create topic: {}", e));
                        }
                    }
                    "unsubscribe" => {
                        for topic_name in msg.topics.unwrap_or_default() {
                            unsubscribe(topics.clone(), topic_name.as_str(), uid);
                            subscribed_topics.remove(topic_name.as_str());
                            debug!("user(uid={}) unsubscribed topic={}", uid, topic_name)
                        }
                        debug!("user(uid={}) unsubscribed topics={:?}", uid, topics)
                    }
                    _ => error!("invalid signal msg: {:?}", msg),
                }
            }
            _ => debug!("others websocket msg: {:?}", ws_msg),
        }
    }

    for topic_name in subscribed_topics {
        unsubscribe(topics.clone(), topic_name.as_str(), uid);
    }
    info!("user(uid={}) disconnected", uid);
}

async fn flush_to_ws(
    mut ws_tx: SplitSink<WebSocket, Message>,
    mut rx: UnboundedReceiverStream<Message>,
) {
    while let Some(message) = rx.next().await {
        let mut need_close = false;
        if message.is_close() {
            need_close = true;
        }
        ws_tx.send(message).await.unwrap_or_else(|e| {
            error!("websocket send error: {}", e);
        });
        if need_close {
            ws_tx.close().await.unwrap_or_else(|e| {
                error!("websocket close error: {}", e);
            });
            break;
        }
    }
}

async fn interval_ping(tx_clone: UnboundedSender<Message>, mut pong_rx: Receiver<()>) {
    let mut received_pong = true;
    let mut interval = time::interval(Duration::from_secs(30));
    interval.tick().await;
    while received_pong {
        match tx_clone.send(Message::ping(Vec::new())) {
            Ok(_) => debug!("send ping in interval"),
            Err(e) => error!("failed to send ping msg: {}", e),
        }
        interval.tick().await;
        match pong_rx.try_recv() {
            Ok(_) => debug!("received pong"),
            Err(_) => {
                match tx_clone.send(Message::close()) {
                    Ok(_) => debug!("close connection for ping-pong timeout"),
                    Err(e) => error!("failed to send close msg: {}", e),
                }
                received_pong = false;
            }
        }
    }
}

fn unsubscribe(topics: Topics, topic_name: &str, uid: usize) {
    if let Some(topic) = topics.get(topic_name).unwrap_or_else(|e| {
        error!("failed to get topic {}", e);
        None
    }) {
        topic
            .unsubscribe(uid)
            .unwrap_or_else(|e| error!("failed to unsubscribe topic: {}", e));
        debug!("unsubscribe topic: {}", topic_name);
        match topic.is_empty() {
            Ok(is_empty) => {
                if !is_empty {
                    return;
                }
                let _ = topics
                    .remove(topic_name)
                    .map_err(|e| error!("failed to remove topic: {}", e));
                debug!("remove empty topic: {}", topic_name);
            }
            Err(e) => error!("failed to read topic: {}", e),
        }
    }
}
