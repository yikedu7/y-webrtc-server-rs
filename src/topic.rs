use crate::msg::SignalMessage;
use std::collections::HashMap;
use std::sync::{Arc, PoisonError, RwLock, RwLockReadGuard, RwLockWriteGuard};
use tokio::sync::mpsc::error::SendError;
use tokio::sync::mpsc::UnboundedSender;
use warp::ws::Message;

type Subscribers = Arc<RwLock<HashMap<usize, UnboundedSender<Message>>>>;

type Topic = Arc<(String, Subscribers)>;

pub(crate) trait AsTopic {
    fn name(&self) -> &str;
    fn subscribe(&self, uid: usize, user: UnboundedSender<Message>) -> Result<(), TopicError>;
    fn unsubscribe(&self, uid: usize) -> Result<(), TopicError>;
    fn publish(&self, msg: &SignalMessage) -> Result<(), TopicError>;
    fn is_empty(&self) -> Result<bool, TopicError>;
}

impl AsTopic for Topic {
    fn name(&self) -> &str {
        self.0.as_str()
    }

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

pub(crate) type Topics = Arc<RwLock<HashMap<String, Topic>>>;

pub(crate) trait AsTopics {
    fn get(&self, topic_name: &str) -> Result<Option<Topic>, TopicError>;
    fn create_if_not_exists(&self, topic_name: &str) -> Result<Topic, TopicError>;
    fn remove(&self, topic_name: &str) -> Result<Option<Topic>, TopicError>;
}

impl AsTopics for Topics {
    fn get(&self, topic_name: &str) -> Result<Option<Topic>, TopicError> {
        Ok(self.read()?.get(topic_name).map(|t| t.to_owned()))
    }

    fn create_if_not_exists(&self, topic_name: &str) -> Result<Topic, TopicError> {
        if let Ok(Some(t)) = self.get(topic_name) {
            return Ok(t);
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

custom_error! {pub(crate) TopicError<'a>
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

impl<'a> From<PoisonError<RwLockWriteGuard<'a, HashMap<usize, UnboundedSender<Message>>>>>
    for TopicError<'a>
{
    fn from(
        e: PoisonError<RwLockWriteGuard<'a, HashMap<usize, UnboundedSender<Message>>>>,
    ) -> Self {
        TopicError::Subscribe { inner: e }
    }
}

impl<'a> From<PoisonError<RwLockReadGuard<'a, HashMap<usize, UnboundedSender<Message>>>>>
    for TopicError<'a>
{
    fn from(e: PoisonError<RwLockReadGuard<'a, HashMap<usize, UnboundedSender<Message>>>>) -> Self {
        TopicError::GetSubscriber { inner: e }
    }
}
