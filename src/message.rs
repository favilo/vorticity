use std::{
    collections::HashMap,
    sync::{atomic::AtomicUsize, mpsc::Sender, Arc},
};

use miette::{miette, Context as _, IntoDiagnostic};
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use serde_json::Value;

use crate::error::Result;

#[derive(Debug, Default)]
pub struct MessageBuilder<Payload> {
    src: Option<String>,
    dst: Option<String>,
    id: Option<usize>,
    in_reply_to: Option<usize>,
    payload: Option<Payload>,
}

impl<Payload> MessageBuilder<Payload> {
    pub fn new() -> Self {
        Self {
            src: None,
            dst: None,
            id: None,
            in_reply_to: None,
            payload: None,
        }
    }

    pub fn src(mut self, src: String) -> Self {
        self.src = Some(src);
        self
    }

    pub fn dst(mut self, dst: String) -> Self {
        self.dst = Some(dst);
        self
    }

    pub fn id(mut self, ctx: Context) -> Self {
        self.id = Some(ctx.next_msg_id());
        self
    }

    pub fn in_reply_to(mut self, in_reply_to: usize) -> Self {
        self.in_reply_to = Some(in_reply_to);
        self
    }

    pub fn payload(mut self, payload: Payload) -> Self {
        self.payload = Some(payload);
        self
    }

    pub fn build(self) -> Result<Message<Payload>> {
        Ok(Message {
            src: self
                .src
                .ok_or(miette!("src is required to build a message"))?,
            dst: self
                .dst
                .ok_or(miette!("dst is required to build a message"))?,
            body: Body {
                id: self.id,
                in_reply_to: self.in_reply_to,
                payload: self
                    .payload
                    .ok_or(miette!("payload is required to build a message"))?,
            },
        })
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Message<Payload> {
    /// The id of the node that sent the message.
    src: String,

    /// The id of the node that the message is intended for.
    #[serde(rename = "dest")]
    dst: String,

    /// The body of the message.
    body: Body<Payload>,
}

impl<Payload> Message<Payload> {
    pub fn builder() -> MessageBuilder<Payload> {
        MessageBuilder::new()
    }

    pub fn src(&self) -> &str {
        &self.src
    }

    pub fn dst(&self) -> &str {
        &self.dst
    }

    pub fn body(&self) -> &Body<Payload> {
        &self.body
    }
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
    /// A message intended for the Node.
    Message(Message<Payload>),

    /// An inected message from a Node specific event loop.
    Injected(InjectedPayload),

    /// Indicates that the event loop should stop.
    Eof,
}

impl<Payload, InjectedPayload> Event<Payload, InjectedPayload>
where
    Payload: for<'de> Deserialize<'de> + Send + 'static,
    InjectedPayload: for<'de> Deserialize<'de> + Send + 'static,
{
    pub(crate) fn is_reply(&self) -> bool {
        match self {
            Event::Message(msg) => msg.body.in_reply_to.is_some(),
            _ => false,
        }
    }
}

#[derive(Debug, Clone)]
pub enum ToEvent {
    Message(Message<serde_json::Value>),
    Injected(Value),
    Eof,
}

impl ToEvent {
    pub fn to_event<Payload, IP>(&self) -> Result<Event<Payload, IP>>
    where
        Payload: DeserializeOwned,
        IP: DeserializeOwned,
    {
        let event: Event<Payload, IP> = match self {
            ToEvent::Message(e) => {
                let body: Payload = serde_json::from_value(e.body.payload.clone())?;
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
            }
            ToEvent::Injected(i) => {
                let injected: IP = serde_json::from_value(i.clone())?;
                Event::Injected(injected)
            }
            ToEvent::Eof => Event::Eof,
        };
        Ok(event)
    }
}

#[derive(Clone)]
pub struct Context {
    /// Allows sending messages as RPCs
    msg_out_tx: Sender<Box<dyn erased_serde::Serialize + Send + Sync + 'static>>,

    /// Allows injecting messages into the event loop
    msg_in_tx: Sender<ToEvent>,

    /// The id of the next message to be sent.
    msg_id: Arc<AtomicUsize>,
}

impl Context {
    pub fn new(
        msg_in_tx: Sender<ToEvent>,
        msg_out_tx: Sender<Box<dyn erased_serde::Serialize + Send + Sync>>,
        msg_id: Arc<AtomicUsize>,
    ) -> Self {
        Self {
            msg_out_tx,
            msg_in_tx,
            msg_id,
        }
    }

    pub fn msg_id(&self) -> usize {
        self.msg_id.load(std::sync::atomic::Ordering::SeqCst)
    }

    pub fn send<S>(&self, s: S) -> Result<()>
    where
        S: Serialize + Sync + Send + 'static,
    {
        Ok(self.msg_out_tx.send(Box::new(s)).into_diagnostic()?)
        // .context("send message to stdout")
    }

    pub fn inject<IP>(&self, s: IP) -> Result<()>
    where
        IP: Serialize + Sync + Send + 'static,
    {
        let value: Value = serde_json::to_value(s)?;
        Ok(self
            .msg_in_tx
            .send(ToEvent::Injected(value))
            .into_diagnostic()
            .context("inject message into event loop")?)
    }

    pub fn next_msg_id(&self) -> usize {
        self.msg_id
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst)
    }

    pub fn construct_reply<Payload>(
        &self,
        msg: &Message<Payload>,
        payload: Payload,
    ) -> Message<Payload>
    where
        Payload: Serialize,
    {
        let id = self.next_msg_id();
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

    pub fn send_rpc<Payload>(&self, msg: Message<Payload>) -> Result<()>
    where
        Payload: Serialize + Sync + Send + 'static,
    {
        self.send(msg)
    }
}

pub struct MessageSet<Payload> {
    /// The messages that have been sent and are still waiting for a reply.
    messages: HashMap<usize, Message<Payload>>,

    /// The count of messages that were sent.
    #[allow(dead_code)]
    count: usize,
}

impl<Payload> MessageSet<Payload>
where
    Payload: Clone,
{
    pub fn new(msgs: &[Message<Payload>]) -> Self {
        let messages = msgs
            .iter()
            .map(|msg| -> (usize, Message<Payload>) { (msg.body.id.unwrap(), msg.clone()) })
            .collect();
        Self {
            messages,
            count: msgs.len(),
        }
    }

    pub fn is_matching_reply(&self, msg: &Message<Payload>) -> bool {
        msg.body
            .in_reply_to
            .map(|id| self.messages.contains_key(&id))
            .unwrap_or(false)
    }
}
