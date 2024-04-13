use std::{any::Any, cell::RefCell};

use serde::{Deserialize, Serialize};
use serde_json::Value;

use super::{CallbackInfo, CallbackStatus, RpcCallback};
use crate::{
    error::{Error, Result},
    message::{MessageSet, ToEvent},
    Context, Event, Handler, Message,
};

#[derive(Default)]
pub struct LinKv {
    callbacks: RefCell<Vec<CallbackInfo<LinKvPayload>>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
#[serde(rename_all = "snake_case")]
pub struct Read {
    pub key: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
#[serde(rename_all = "snake_case")]
pub struct Write {
    pub key: String,
    pub value: Value,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
#[serde(rename_all = "snake_case")]
pub struct Cas {
    pub key: String,
    pub from: Value,
    pub to: Value,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub create_if_not_exists: Option<bool>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
#[serde(rename_all = "snake_case")]
pub enum LinKvPayload {
    Read(Read),
    ReadOk { value: Value },

    Write(Write),
    WriteOk,

    Cas(Cas),
    CasOk,
}

impl Handler for LinKv {
    fn can_handle(&self, json: &ToEvent) -> bool {
        Event::<LinKvPayload, ()>::try_from(json.clone()).is_ok()
    }

    fn step(&self, json: ToEvent, ctx: Context) -> Result<()>
    where
        Self: Sized,
    {
        let event = Event::<LinKvPayload, ()>::try_from(json.clone())?;
        if !event.is_reply() {
            return Err(Error::NotReply(json));
        }

        let Event::Message(event) = event else {
            return Err(Error::WrongEvent(json));
        };

        // TODO: figure out errors from lin-kv cas, when there is contention
        match &event.body().payload {
            LinKvPayload::ReadOk { .. } | LinKvPayload::WriteOk | LinKvPayload::CasOk => {
                self.handle_reply(&event, ctx)?
            }

            LinKvPayload::Write(Write { .. })
            | LinKvPayload::Cas(Cas { .. })
            | LinKvPayload::Read(Read { .. }) => {}
        };

        Ok(())
    }
}

pub type ReadCallback<Payload> = dyn Fn(Message<Payload>, &mut dyn Any, Value, Context);
pub type WriteCallback<Payload> = dyn Fn(Message<Payload>, &mut dyn Any, Context);
pub type CasCallback<Payload> = dyn Fn(Message<Payload>, &mut dyn Any, Context);

impl LinKv {
    fn handle_reply(&self, event: &Message<LinKvPayload>, ctx: Context) -> Result<()> {
        let mut borrow = self.callbacks.borrow_mut();
        let (idx, callback_info) = borrow
            .iter_mut()
            .enumerate()
            .find(|(_, info)| info.matches(event))
            .ok_or_else(|| Error::NoCallback(event.to_value()))?;
        let status = callback_info.call(event, ctx)?;
        match status {
            CallbackStatus::Finished => {
                self.callbacks.borrow_mut().remove(idx);
            }
            CallbackStatus::MoreWork => {}
        };
        Ok(())
    }

    // TODO: Handle timeouts
    pub fn read<Payload>(
        &self,
        read: Read,
        orig_msg: &Message<Payload>,
        state: Box<dyn Any>,
        callback: Box<ReadCallback<Payload>>,
        ctx: Context,
    ) -> Result<()>
    where
        Payload: Clone + Serialize + for<'de> Deserialize<'de> + 'static,
    {
        let process_callback = move |orig_msg: &Message<Value>,
                                     state: &mut dyn Any,
                                     _set: &mut MessageSet<LinKvPayload>,
                                     msg: &Message<LinKvPayload>,
                                     ctx: Context|
              -> Result<CallbackStatus> {
            let LinKvPayload::ReadOk { value } = msg.body().payload.clone() else {
                return Err(Error::WrongEvent(ToEvent::Message(msg.to_value())));
            };
            callback(Message::to_payload(orig_msg)?, state, value, ctx);
            Ok(CallbackStatus::Finished)
        };

        self.persist_callback(
            ctx,
            LinKvPayload::Read(read),
            orig_msg,
            state,
            Box::new(process_callback),
        )?;

        Ok(())
    }

    pub fn write<Payload>(
        &self,
        write: Write,
        orig_msg: &Message<Payload>,
        state: Box<dyn Any>,
        callback: Box<WriteCallback<Payload>>,
        ctx: Context,
    ) -> Result<()>
    where
        Payload: Clone + Serialize + for<'de> Deserialize<'de> + 'static,
    {
        let process_callback = move |orig_msg: &Message<Value>,
                                     state: &mut dyn Any,
                                     _set: &mut MessageSet<LinKvPayload>,
                                     msg: &Message<LinKvPayload>,
                                     ctx: Context|
              -> Result<CallbackStatus> {
            let LinKvPayload::WriteOk = msg.body().payload.clone() else {
                return Err(Error::WrongEvent(ToEvent::Message(msg.to_value())));
            };
            callback(Message::to_payload(orig_msg)?, state, ctx);
            Ok(CallbackStatus::Finished)
        };

        self.persist_callback(
            ctx,
            LinKvPayload::Write(write),
            orig_msg,
            state,
            Box::new(process_callback),
        )?;

        Ok(())
    }

    pub fn cas<Payload>(
        &self,
        cas: Cas,
        orig_msg: &Message<Payload>,
        state: Box<dyn Any>,
        callback: Box<CasCallback<Payload>>,
        ctx: Context,
    ) -> Result<()>
    where
        Payload: Clone + Serialize + for<'de> Deserialize<'de> + 'static,
    {
        let process_callback = move |orig_msg: &Message<Value>,
                                     state: &mut dyn Any,
                                     _set: &mut MessageSet<LinKvPayload>,
                                     msg: &Message<LinKvPayload>,
                                     ctx: Context|
              -> Result<CallbackStatus> {
            let LinKvPayload::CasOk = msg.body().payload.clone() else {
                return Err(Error::WrongEvent(ToEvent::Message(msg.to_value())));
            };
            callback(Message::to_payload(orig_msg)?, state, ctx);
            Ok(CallbackStatus::Finished)
        };

        self.persist_callback(
            ctx,
            LinKvPayload::Cas(cas),
            orig_msg,
            state,
            Box::new(process_callback),
        )?;

        Ok(())
    }

    fn persist_callback<Payload>(
        &self,
        ctx: Context,
        payload: LinKvPayload,
        orig_msg: &Message<Payload>,
        state: Box<dyn Any>,
        process_callback: Box<RpcCallback<LinKvPayload>>,
    ) -> Result<()>
    where
        Payload: Clone + Serialize + for<'de> Deserialize<'de> + 'static,
    {
        let msg_set = MessageSet::new(&[Message::builder(ctx.clone())
            .dst("lin-kv")
            .id(ctx.clone())
            .payload(payload)
            .build()?]);
        ctx.send_set(&msg_set)?;
        let callback_info =
            CallbackInfo::new(orig_msg.clone(), state, msg_set, Box::new(process_callback));
        self.callbacks.borrow_mut().push(callback_info);
        Ok(())
    }
}
