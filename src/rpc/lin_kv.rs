use std::{any::Any, cell::RefCell};

use serde::{Deserialize, Serialize};
use serde_json::Value;

use super::{CallbackInfo, CallbackStatus};
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
pub enum LinKvPayload {
    Read {
        key: String,
    },
    ReadOk {
        value: Value,
    },

    Write {
        key: String,
        value: Value,
    },
    WriteOk,

    Cas {
        key: String,
        from: Value,
        to: Value,
        #[serde(skip_serializing_if = "Option::is_none")]
        create_if_not_exists: Option<bool>,
    },
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

        // handle responses here. We are going to actually be sending in methods
        // TODO: figure out errors
        match &event.body().payload {
            LinKvPayload::ReadOk { .. } => {
                self.handle_read_ok(&event, ctx)?;
            }
            LinKvPayload::WriteOk => todo!(),
            LinKvPayload::CasOk => todo!(),

            LinKvPayload::Write { .. } | LinKvPayload::Cas { .. } | LinKvPayload::Read { .. } => {}
        };

        Ok(())
    }
}

pub type ReadCallback<Payload> = dyn Fn(Message<Payload>, &mut dyn Any, Value, Context);

impl LinKv {
    fn handle_read_ok(&self, event: &Message<LinKvPayload>, ctx: Context) -> Result<()> {
        let mut borrow = self.callbacks.borrow_mut();
        let (idx, callback_info) = borrow
            .iter_mut()
            .enumerate()
            .find(|(_, info)| info.matches(&event))
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
        key: &str,
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

        let msg_set = MessageSet::new(&[Message::builder(ctx.clone())
            .dst("lin-kv")
            .id(ctx.clone())
            .payload(LinKvPayload::Read {
                key: key.to_owned(),
            })
            .build()?]);
        ctx.send_set(&msg_set)?;
        let callback_info =
            CallbackInfo::new(orig_msg.clone(), state, msg_set, Box::new(process_callback));

        self.callbacks.borrow_mut().push(callback_info);

        Ok(())
    }
}
