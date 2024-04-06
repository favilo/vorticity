use std::{collections::HashMap, marker::PhantomData};

use anyhow::Context as _;
use serde::{de::DeserializeOwned, Serialize};

use crate::{Handler, Message};

type HandlerFunc<Payload> = fn(Message<Payload>, Payload) -> Option<Message<Payload>>;

pub struct RpcHandler<Payload> {
    callbacks: HashMap<usize, CallbackData<Payload>>,
    state: PhantomData<Payload>,
}

#[derive(Debug, Clone)]
struct CallbackData<Payload> {
    reply_to: Message<Payload>,
    callback: Box<HandlerFunc<Payload>>,
}

// impl<Payload> Handler for RpcHandler<Payload>
// where
//     Payload: Serialize + DeserializeOwned + Sync + Send + 'static,
// {
//     fn can_handle(&self, json: &serde_json::Value) -> bool {
//         serde_json::from_value::<Message<Payload>>(json.clone()).is_ok()
//     }

//     fn step(
//         &mut self,
//         json: serde_json::Value,
//         ctx: crate::Context<Payload>,
//     ) -> anyhow::Result<()> {
//         let input: Message<Payload> = serde_json::from_value(json)?;
//         let msg_id = input
//             .body
//             .in_reply_to
//             .ok_or_else(|| anyhow::anyhow!("Message reply id not specified"))?;
//         let Some(callback) = self.callbacks.remove(&msg_id) else {
//             anyhow::bail!("Message wasn't registered");
//         };
//         let reply = (callback.callback)(callback.reply_to, input.body.payload);
//         if let Some(reply) = reply {
//             ctx.send(reply)
//                 .context("Sending response RPC from handler")?;
//         }

//         Ok(())
//     }
// }

// impl<In> RpcHandler<In> {}
