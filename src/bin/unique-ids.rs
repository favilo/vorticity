use miette::Context as _;
use serde::{Deserialize, Serialize};
use vorticity::{error::Result, Context, Event, Init, Node, Runtime};

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
    pub node: String,
}

impl Node<(), Payload> for UniqueNode {
    fn step(&mut self, input: Event<Payload>, ctx: Context) -> Result<()> {
        let Event::Message(input) = input else {
            unreachable!();
        };
        match input.body().payload {
            Payload::Generate => {
                let guid = format!("{}-{}", self.node, ctx.msg_id());
                let reply = ctx.construct_reply(&input, Payload::GenerateOk { guid });

                ctx.send(reply).context("serialize response to generate")?;
            }
            Payload::GenerateOk { .. } => {}
        }

        Ok(())
    }

    fn from_init(_runtime: &Runtime, _state: (), init: &Init, _ctx: Context) -> Result<Self>
    where
        Self: Sized,
    {
        Ok(Self {
            node: init.node_id.clone(),
        })
    }
}

fn main() -> Result<()> {
    Ok(Runtime::new().run::<_, _, _, UniqueNode>(())?)
}
