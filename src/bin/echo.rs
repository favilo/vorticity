use std::sync::mpsc::Sender;

use anyhow::Context as _;
use serde::{Deserialize, Serialize};
use vorticity::{Context, Event, Node, Runtime, ToEvent};

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
#[serde(rename_all = "snake_case")]
pub enum Payload {
    Echo { echo: String },
    EchoOk { echo: String },
}

pub struct EchoNode {
    pub id: usize,
}

impl Node<(), Payload> for EchoNode {
    fn step(&mut self, input: Event<Payload>, ctx: Context) -> anyhow::Result<()> {
        let Event::Message(input) = input else {
            unreachable!()
        };
        let mut reply = input.into_reply(Some(&mut self.id));
        match reply.body.payload {
            Payload::Echo { echo } => {
                reply.body.payload = Payload::EchoOk { echo };
                ctx.send(reply).context("serialize response to echo")?;
            }
            Payload::EchoOk { .. } => {}
        }

        Ok(())
    }

    fn from_init(_state: (), _init: vorticity::Init, _tx: Sender<ToEvent>) -> anyhow::Result<Self>
    where
        Self: Sized,
    {
        Ok(Self { id: 1 })
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    Runtime::<_, _, _, EchoNode>::run(()).await
}
