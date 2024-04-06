use anyhow::Context as _;
use serde::{Deserialize, Serialize};
use vorticity::{Context, Event, Node, Runtime};

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
    fn step(&mut self, input: Event<Payload>, ctx: Context<()>) -> anyhow::Result<()> {
        let Event::Message(input) = input else {
            unreachable!();
        };
        let mut reply = input.into_reply(Some(&mut self.msg_id));
        match reply.body.payload {
            Payload::Generate => {
                let guid = format!("{}-{}", self.node, self.msg_id);
                reply.body.payload = Payload::GenerateOk { guid };

                ctx.send(reply).context("serialize response to generate")?;
            }
            Payload::GenerateOk { .. } => {}
        }

        Ok(())
    }

    fn from_init(_state: (), init: vorticity::Init, _ctx: Context<()>) -> anyhow::Result<Self>
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
    Runtime::run::<_, _, _, UniqueNode>(())
}
