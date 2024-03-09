use std::io::{StdoutLock, Write};

use anyhow::Context;
use serde::{Deserialize, Serialize};
use vorticity::{main_loop, Message, Node};

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
#[serde(rename_all = "snake_case")]
pub enum Payload {
    Generate,
    GenerateOk {
        #[serde(rename = "id")]
        guid: String,
    },
}

pub struct UniqueNode {
    pub msg_id: usize,
    pub node: String,
}

impl Node<(), Payload> for UniqueNode {
    fn step(&mut self, input: Message<Payload>, output: &mut StdoutLock) -> anyhow::Result<()> {
        let mut reply = input.into_reply(Some(&mut self.msg_id));
        match reply.body.payload {
            Payload::Generate => {
                let guid = format!("{}-{}", self.node, self.msg_id);
                reply.body.payload = Payload::GenerateOk { guid };

                serde_json::to_writer(&mut *output, &reply)
                    .context("serialize response to generate")?;
                output.write_all(b"\n").context("write newline to output")?;
            }
            Payload::GenerateOk { .. } => {}
        }

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
