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

    Generate,

    GenerateOk {
        #[serde(rename = "id")]
        guid: String,
    },
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

pub struct UniqueNode {
    pub msg_id: usize,
    pub node: String,
}

impl Node<(), Payload> for UniqueNode {
    fn step(&mut self, input: Message<Payload>, output: &mut StdoutLock) -> anyhow::Result<()> {
        match input.body.payload {
            Payload::Generate => {
                let guid = format!("{}-{}", self.node, self.msg_id);
                let reply = Message {
                    src: input.dst,
                    dst: input.src,
                    body: Body {
                        id: Some(self.msg_id),
                        in_reply_to: input.body.id,
                        payload: Payload::GenerateOk { guid },
                    },
                };

                serde_json::to_writer(&mut *output, &reply)
                    .context("serialize response to generate")?;
                output.write_all(b"\n").context("write newline to output")?;
            }
            Payload::Init { .. } => bail!("init should already be handled"),
            Payload::InitOk => bail!("Unexpected InitOk message"),
            Payload::GenerateOk { .. } => bail!("Unexpected GenerateOk message"),
        }
        self.msg_id += 1;

        Ok(())
    }

    fn from_init(_state: (), init: vorticity::Init) -> anyhow::Result<Self>
    where
        Self: Sized,
    {
        Ok(Self {
            msg_id: 1,
            node: init.node_id,
        })
    }
}

fn main() -> anyhow::Result<()> {
    main_loop::<(), UniqueNode, Payload>(())
}
