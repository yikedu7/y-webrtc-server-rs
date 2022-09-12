use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use warp::ws::Message;

#[derive(Serialize, Deserialize, Debug, PartialEq, Eq)]
pub(crate) enum MessageType {
    #[serde(rename = "ping")]
    Ping,
    #[serde(rename = "pong")]
    Pong,
    #[serde(rename = "publish")]
    Publish,
    #[serde(rename = "subscribe")]
    Subscribe,
    #[serde(rename = "unsubscribe")]
    Unsubscribe,
}

#[derive(Serialize, Deserialize, Debug, PartialEq, Eq)]
pub struct SignalMessage {
    #[serde(rename = "type")]
    pub(crate) message_type: MessageType,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub topics: Option<Vec<String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub topic: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub data: Option<HashMap<String, String>>,
}

impl SignalMessage {
    pub fn ping() -> SignalMessage {
        SignalMessage {
            message_type: MessageType::Ping,
            topics: None,
            topic: None,
            data: None,
        }
    }

    pub fn pong() -> SignalMessage {
        SignalMessage {
            message_type: MessageType::Pong,
            topics: None,
            topic: None,
            data: None,
        }
    }

    pub fn subscribe(topics: Vec<String>) -> SignalMessage {
        SignalMessage {
            message_type: MessageType::Subscribe,
            topics: Some(topics),
            topic: None,
            data: None,
        }
    }

    pub fn unsubscribe(topics: Vec<String>) -> SignalMessage {
        SignalMessage {
            message_type: MessageType::Unsubscribe,
            topics: Some(topics),
            topic: None,
            data: None,
        }
    }

    pub fn publish(topic: String, data: HashMap<String, String>) -> SignalMessage {
        SignalMessage {
            message_type: MessageType::Publish,
            topics: None,
            topic: Some(topic),
            data: Some(data),
        }
    }

    pub fn is_ping(&self) -> bool {
        matches!(self.message_type, MessageType::Ping)
    }

    pub fn is_pong(&self) -> bool {
        matches!(self.message_type, MessageType::Pong)
    }

    pub fn is_publish(&self) -> bool {
        matches!(self.message_type, MessageType::Publish)
    }

    pub fn is_subscribe(&self) -> bool {
        matches!(self.message_type, MessageType::Subscribe)
    }

    pub fn is_unsubscribe(&self) -> bool {
        matches!(self.message_type, MessageType::Unsubscribe)
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
