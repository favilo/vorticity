use std::io::{StdoutLock, Write};

use anyhow::Context;
use serde::{Deserialize, Serialize};
use vorticity::{main_loop, Message, Node};

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
    fn step(&mut self, input: Message<Payload>, output: &mut StdoutLock) -> anyhow::Result<()> {
        let mut reply = input.into_reply(Some(&mut self.id));
        match reply.body.payload {
            Payload::Echo { echo } => {
                reply.body.payload = Payload::EchoOk { echo };
                serde_json::to_writer(&mut *output, &reply)
                    .context("serialize response to echo")?;
                output.write_all(b"\n").context("write newline to output")?;
            }
            Payload::EchoOk { .. } => {}
        }

        Ok(())
    }

    fn from_init(_state: (), _init: vorticity::Init) -> anyhow::Result<Self>
    where
        Self: Sized,
    {
        Ok(Self { id: 1 })
    }
}

fn main() -> anyhow::Result<()> {
    main_loop::<_, EchoNode, Payload>(())
}
