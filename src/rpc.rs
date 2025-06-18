use std::any::Any;

use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::{error::Result, message::MessageSet, Context, Message};

pub mod lin_kv;

pub enum CallbackStatus {
    MoreWork,
    Finished,
}

pub type RpcCallback<RpcPayload> = dyn Fn(
    &Message<Value>,
    &mut dyn Any,
    &mut MessageSet<RpcPayload>,
    &Message<RpcPayload>,
    Context,
) -> Result<CallbackStatus>;

pub struct CallbackInfo<RpcPayload> {
    unhandled_incoming_msg: Message<Value>,
    state: Box<dyn Any>,
    sent_msgs: MessageSet<RpcPayload>,
    callback: Box<RpcCallback<RpcPayload>>,
}

impl<RpcPayload> CallbackInfo<RpcPayload>
where
    RpcPayload: Clone + Serialize + for<'de> Deserialize<'de> + 'static,
{
    fn new<Payload>(
        orig_msg: Message<Payload>,
        state: Box<dyn Any>,
        sent_msgs: MessageSet<RpcPayload>,
        callback: Box<RpcCallback<RpcPayload>>,
    ) -> Self
    where
        Payload: Clone + Serialize + for<'de> Deserialize<'de>,
    {
        Self {
            unhandled_incoming_msg: orig_msg.to_value(),
            state,
            sent_msgs,
            callback: Box::new(callback),
        }
    }

    pub fn matches(&self, msg: &Message<RpcPayload>) -> bool {
        self.sent_msgs.is_matching_reply(msg) && self.unhandled_incoming_msg.dst() == msg.dst()
    }

    pub fn call(&mut self, msg: &Message<RpcPayload>, ctx: Context) -> Result<CallbackStatus> {
        (self.callback)(
            &self.unhandled_incoming_msg,
            &mut *self.state,
            &mut self.sent_msgs,
            msg,
            ctx,
        )
    }
}
