use std::collections::{HashMap, HashSet};
use std::sync::{Arc, PoisonError, RwLock, RwLockReadGuard, RwLockWriteGuard};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;
use futures_util::{StreamExt,SinkExt};
use warp::{Filter, ws::WebSocket};
use serde::{Serialize, Deserialize};
use tokio::sync::mpsc;
use tokio::{task, time};
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender};
use tokio::sync::mpsc::error::SendError;
use tokio_stream::wrappers::UnboundedReceiverStream;
use warp::ws::Message;

extern crate pretty_env_logger;
#[macro_use] extern crate log;
#[macro_use] extern crate custom_error;
extern crate core;

type Subscribers = Arc<RwLock<HashMap<usize, UnboundedSender<Message>>>>;
type Topic = Arc<(String, Subscribers)>;
type Topics = Arc<RwLock<HashMap<String, Topic>>>;

custom_error!{TopicError<'a>
    Read{inner:PoisonError<RwLockReadGuard<'a, HashMap<String, Topic>>>} = "unable to read",
    Write{inner:PoisonError<RwLockWriteGuard<'a, HashMap<String, Topic>>>} = "unable to write",
    Subscribe{inner:PoisonError<RwLockWriteGuard<'a, HashMap<usize, UnboundedSender<Message>>>>} = "unable to subscribe",
    GetSubscriber{inner:PoisonError<RwLockReadGuard<'a, HashMap<usize, UnboundedSender<Message>>>>} = "unable to get subscribers",
    Publish{source:SendError<Message>} = "unable to publish",
}

impl<'a> From<PoisonError<RwLockReadGuard<'a, HashMap<String, Topic>>>> for TopicError<'a> {
    fn from(e: PoisonError<RwLockReadGuard<'a, HashMap<String, Topic>>>) -> Self {
        TopicError::Read { inner: e }
    }
}

impl<'a> From<PoisonError<RwLockWriteGuard<'a, HashMap<String, Topic>>>> for TopicError<'a> {
    fn from(e: PoisonError<RwLockWriteGuard<'a, HashMap<String, Topic>>>) -> Self {
        TopicError::Write { inner: e }
    }
}

impl<'a> From<PoisonError<RwLockWriteGuard<'a, HashMap<usize, UnboundedSender<Message>>>>> for TopicError<'a> {
    fn from(e: PoisonError<RwLockWriteGuard<'a, HashMap<usize, UnboundedSender<Message>>>>) -> Self {
        TopicError::Subscribe { inner: e }
    }
}

impl<'a> From<PoisonError<RwLockReadGuard<'a, HashMap<usize, UnboundedSender<Message>>>>> for TopicError<'a> {
    fn from(e: PoisonError<RwLockReadGuard<'a, HashMap<usize, UnboundedSender<Message>>>>) -> Self {
        TopicError::GetSubscriber { inner: e }
    }
}

trait TopicsTrait {
    fn get(&self, topic_name: &str) -> Result<Option<Topic>, TopicError>;
    fn create_if_not_exists(&self, topic_name: &str) -> Result<Topic, TopicError>;
    fn remove(&self, topic_name: &str) -> Result<Option<Topic>, TopicError>;
}

impl TopicsTrait for Topics {

    fn get(&self, topic_name: &str) -> Result<Option<Topic>, TopicError> {
        Ok(self
            .read()?
            .get(topic_name)
            .map(|t| t.to_owned())
        )
    }

    fn create_if_not_exists(&self, topic_name: &str) -> Result<Topic, TopicError> {
        if let Ok(Some(t)) = self.get(topic_name) {
            return Ok(t)
        }
        let mut topics = self.write()?;
        let topic = Topic::new((topic_name.to_string(), Subscribers::default()));
        topics.insert(topic_name.to_string(), topic.to_owned());
        Ok(topic)
    }

    fn remove(&self, topic_name: &str) -> Result<Option<Topic>, TopicError> {
        Ok(self.write()?.remove(topic_name))
    }
}

trait TopicTrait {
    fn subscribe(&self, uid: usize, user: UnboundedSender<Message>) -> Result<(), TopicError>;
    fn unsubscribe(&self, uid: usize) -> Result<(), TopicError>;
    fn publish(&self, msg: &SignalMessage) -> Result<(), TopicError>;
    fn is_empty(&self) -> Result<bool, TopicError>;
}

impl TopicTrait for Topic {

    fn subscribe(&self, uid: usize, user: UnboundedSender<Message>) -> Result<(), TopicError<'_>> {
        self.1.write()?.insert(uid, user);
        Ok(())
    }

    fn unsubscribe(&self, uid: usize) -> Result<(), TopicError> {
        self.1.write()?.remove(&uid);
        Ok(())
    }

    fn publish(&self, msg: &SignalMessage) -> Result<(), TopicError> {
        for (_, value) in self.1.read()?.iter() {
            value.send(Message::from(msg))?
        }
        Ok(())
    }

    fn is_empty(&self) -> Result<bool, TopicError<'_>> {
        let result = self.1.read()?.is_empty();
        Ok(result)
    }
}

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
        .map(|ws: warp::ws::Ws, topics: Topics, next_uid: Arc<AtomicUsize> | {
            ws.on_upgrade(|socket| on_connection(socket, topics, next_uid))
        });

    warp::serve(index)
        .run(([127, 0, 0, 1], 4444))
        .await;
}

async fn on_connection(socket: WebSocket, topics: Topics, next_uid: Arc<AtomicUsize>) {
    let uid = next_uid.fetch_add(1, Ordering::Relaxed);
    info!("user(uid={}) on connect", uid);

    let mut subscribed_topics = HashSet::new();

    let (mut ws_tx, mut ws_rx) = socket.split();

    // Use an unbounded channel to handle buffering and flushing of messages to the websocket...
    let (tx, rx): (UnboundedSender<Message>, UnboundedReceiver<Message>) = mpsc::unbounded_channel();
    let mut rx = UnboundedReceiverStream::new(rx);
    tokio::task::spawn(async move {
        while let Some(message) = rx.next().await {
            let mut need_close = false;
            if message.is_close() {
                need_close = true;
            }
            ws_tx
                .send(message)
                .await
                .unwrap_or_else(|e| {
                    error!("websocket send error: {}", e);
                });
            if need_close {
                ws_tx
                    .close()
                    .await
                    .unwrap_or_else(|e| {
                        error!( "websocket close error: {}", e);
                    });
                debug!("close websocket connection");
                break;
            }
        }
    });

    let tx_clone = tx.clone();
    let (pong_tx, mut pong_rx) = mpsc::channel(1);
    task::spawn( async move {
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
                        Ok(_) => info!("close connection for ping-pong timeout"),
                        Err(e) => error!("failed to send close msg: {}", e),
                    }
                    received_pong = false;
                },
            }
        }
    });

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
                    .unwrap_or_else(|e| error!("received pong error: {}", e));
                debug!("received pong and forwarding: {:?}", ws_msg)
            },
            _ if ws_msg.is_text() => {
                let msg = SignalMessage::from(&ws_msg);
                debug!("Received text msg json: {:?}", msg);
                match msg.message_type.as_str() {
                    "ping" => {
                        tx.send(Message::from(&SignalMessage::pong())).expect("send websocket");
                    },
                    "publish" => {
                        let msg_ref = &msg;
                        if let Some(topic_name) = msg.topic.as_ref() {
                            if let Some(topic) = topics.get(topic_name).unwrap_or_else(|e| {
                                error!("failed to get topic {}", e);
                                None
                            }) {
                                topic.publish(msg_ref).unwrap()
                            }
                        }
                    },
                    "subscribe" => {
                        for topic_name in msg.topics.unwrap_or_default() {
                            topics
                                .create_if_not_exists(&topic_name)
                                .map(|topic| {
                                    topic
                                        .subscribe(uid, tx.clone())
                                        .unwrap_or_else(|e| error!("failed to subscribe topic: {}", e));
                                    subscribed_topics.insert(topic_name);
                                })
                                .unwrap_or_else(|e| error!("failed to create topic: {}", e));
                        }
                        debug!("after subscribe topic={:?}", topics)
                    },
                    "unsubscribe" => {
                        for topic_name in msg.topics.unwrap_or_default() {
                            unsubscribe(topics.clone(), topic_name.as_str(), uid);
                            subscribed_topics.remove(topic_name.as_str());
                        }
                    },
                    _ => error!("invalid msg: {:?}", msg),
                }
            },
            _ => debug!("others websocket msg: {:?}", ws_msg)
        }
    }

    for topic_name in subscribed_topics {
        unsubscribe(topics.clone(), topic_name.as_str(), uid);
    }
    info!("user(uid={}) disconnected", uid);
}

fn unsubscribe(topics: Topics, topic_name: &str, uid: usize) {
    if let Some(topic) = topics
        .get(topic_name)
        .unwrap_or_else(|e| {
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
            },
            Err(e) => error!("failed to read topic: {}", e)
        }
    }
}

#[derive(Serialize, Deserialize, Debug)]
struct SignalMessage {
    #[serde(rename = "type")]
    message_type: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    topics: Option<Vec<String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    topic: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    data: Option<HashMap<String, String>>,
}

impl SignalMessage {
    pub fn pong() -> SignalMessage {
        SignalMessage { message_type: String::from("pong"), topics: None, topic: None, data: None }
    }
}

impl From<&Message> for SignalMessage {
    fn from(msg: &Message) -> Self {
        let msg_str = msg.to_str().expect("unexpected text msg");
        serde_json::from_str(msg_str).expect("signal message unmarshal")
    }
}

impl From<&SignalMessage> for Message {
    fn from(msg: &SignalMessage) -> Self {
        Message::text(serde_json::to_string(msg).expect("signal message marshal"))
    }
}