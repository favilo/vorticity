use std::io::{StdoutLock, Write};

use anyhow::{bail, Context};
use serde::{Deserialize, Serialize};
use vorticity::{main_loop, Body, Message, Node};

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
#[serde(rename_all = "snake_case")]
pub enum Payload {
    Init(vorticity::Init),
    InitOk,

    Echo { echo: String },
    EchoOk { echo: String },
}

impl vorticity::Payload for Payload {
    fn extract_init(input: Self) -> Option<vorticity::Init> {
        let Payload::Init(init) = input else {
            return None;
        };
        Some(init)
    }

    fn gen_init_ok() -> Self {
        Self::InitOk
    }
}

pub struct EchoNode {
    pub id: usize,
}

impl Node<(), Payload> for EchoNode {
    fn step(&mut self, input: Message<Payload>, output: &mut StdoutLock) -> anyhow::Result<()> {
        match input.body.payload {
            Payload::Echo { echo } => {
                let reply = Message {
                    src: input.dst,
                    dst: input.src,
                    body: Body {
                        id: Some(self.id),
                        in_reply_to: input.body.id,
                        payload: Payload::EchoOk { echo },
                    },
                };

                serde_json::to_writer(&mut *output, &reply)
                    .context("serialize response to echo")?;
                output.write_all(b"\n").context("write newline to output")?;
            }
            Payload::EchoOk { .. } => {}
            Payload::Init { .. } => bail!("init should already be handled"),
            Payload::InitOk => bail!("Unexpected InitOk message"),
        }
        self.id += 1;

        Ok(())
    }

    fn from_init(_state: (), _init: vorticity::Init) -> anyhow::Result<Self>
    where
        Self: Sized,
    {
        Ok(Self { id: 0 })
    }
}

fn main() -> anyhow::Result<()> {
    main_loop::<_, EchoNode, Payload>(())
}
