use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use warp::ws::Message;

#[derive(Serialize, Deserialize, Debug)]
pub(crate) struct SignalMessage {
    #[serde(rename = "type")]
    pub(crate) message_type: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) topics: Option<Vec<String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) topic: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) data: Option<HashMap<String, String>>,
}

impl SignalMessage {
    pub(crate) fn pong() -> SignalMessage {
        SignalMessage {
            message_type: String::from("pong"),
            topics: None,
            topic: None,
            data: None,
        }
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
