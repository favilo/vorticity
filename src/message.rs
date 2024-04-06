use std::{
    collections::HashMap,
    sync::{atomic::AtomicUsize, mpsc::Sender, Arc},
};

use anyhow::Context as _;
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use serde_json::Value;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Message<Payload> {
    /// The id of the node that sent the message.
    pub src: String,

    /// The id of the node that the message is intended for.
    #[serde(rename = "dest")]
    pub dst: String,

    /// The body of the message.
    pub body: Body<Payload>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Body<Payload> {
    /// The id of the message.
    #[serde(rename = "msg_id")]
    pub id: Option<usize>,

    /// The id of the message that this message is in reply to.
    pub in_reply_to: Option<usize>,

    /// The payload of the message.
    #[serde(flatten)]
    pub payload: Payload,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
#[serde(rename_all = "snake_case")]
pub(crate) enum InitPayload {
    /// initialization body.
    Init(Init),

    /// indicates that the initialization was successful.
    InitOk,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Init {
    /// The id of the node.
    pub node_id: String,

    /// The ids of the nodes that are connected to this node.
    pub node_ids: Vec<String>,
}

#[derive(Debug, Clone)]
pub enum Event<Payload, InjectedPayload = ()> {
    Message(Message<Payload>),
    Injected(InjectedPayload),
    Arbitrary(Message<Value>),
    Eof,
}

impl<Payload, InjectedPayload> Event<Payload, InjectedPayload>
where
    Payload: for<'de> Deserialize<'de> + Send + 'static,
    InjectedPayload: Clone + Send + 'static,
{
    pub(crate) fn is_reply(&self) -> bool {
        match self {
            Event::Message(msg) => msg.body.in_reply_to.is_some(),
            Event::Arbitrary(msg) => msg.body.in_reply_to.is_some(),
            _ => false,
        }
    }
}

#[derive(Debug, Clone)]
pub enum ToEvent<InjectedPayload = ()> {
    Message(Message<serde_json::Value>),
    Injected(InjectedPayload),
    Eof,
}

impl<IP> ToEvent<IP> {
    pub fn to_event<Payload>(&self) -> anyhow::Result<Event<Payload, IP>>
    where
        Payload: DeserializeOwned,
        IP: Clone,
    {
        let event = match self {
            ToEvent::Message(e) => {
                let body: Result<Payload, _> = serde_json::from_value(e.body.payload.clone());
                if let Ok(body) = body {
                    let message = Message {
                        src: e.src.clone(),
                        dst: e.dst.clone(),
                        body: Body {
                            id: e.body.id,
                            in_reply_to: e.body.in_reply_to,
                            payload: body,
                        },
                    };
                    Event::Message(message)
                } else {
                    Event::Arbitrary(e.clone())
                }
            }
            ToEvent::Injected(i) => Event::Injected(i.clone()),
            ToEvent::Eof => Event::Eof,
        };
        Ok(event)
    }
}

#[derive(Clone)]
pub struct Context<IP> {
    /// Allows sending messages as RPCs
    msg_out_tx: Sender<Box<dyn erased_serde::Serialize + Send + Sync + 'static>>,

    /// Allows injecting messages into the event loop
    msg_in_tx: Sender<ToEvent<IP>>,

    /// The id of the next message to be sent.
    msg_id: Arc<AtomicUsize>,
}

impl<IP> Context<IP> {
    pub fn new(
        msg_in_tx: Sender<ToEvent<IP>>,
        msg_out_tx: Sender<Box<dyn erased_serde::Serialize + Send + Sync>>,
        msg_id: Arc<AtomicUsize>,
    ) -> Self
    where
        IP: Clone + Send + 'static,
    {
        Self {
            msg_out_tx,
            msg_in_tx,
            msg_id,
        }
    }

    pub fn msg_id(&self) -> usize {
        self.msg_id.load(std::sync::atomic::Ordering::SeqCst)
    }

    pub fn send<S>(&self, s: S) -> anyhow::Result<()>
    where
        S: Serialize + Sync + Send + 'static,
    {
        self.msg_out_tx
            .send(Box::new(s))
            .context("send message to stdout")
    }

    pub fn inject(&self, s: IP) -> anyhow::Result<()>
    where
        IP: Sync + Send + 'static,
    {
        self.msg_in_tx
            .send(ToEvent::Injected(s))
            .context("inject message into event loop")
    }

    pub fn construct_reply<Payload>(
        &self,
        msg: &Message<Payload>,
        payload: Payload,
    ) -> Message<Payload>
    where
        Payload: Serialize,
    {
        let id = self
            .msg_id
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        Message {
            src: msg.dst.clone(),
            dst: msg.src.clone(),
            body: Body {
                id: Some(id),
                in_reply_to: msg.body.id,
                payload,
            },
        }
    }
}

pub struct MessageSet<Payload> {
    /// The messages that have been sent and are still waiting for a reply.
    messages: HashMap<usize, Message<Payload>>,

    /// The count of messages that were sent.
    count: usize,
}
