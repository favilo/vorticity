use miette::Context as _;
use serde::{Deserialize, Serialize};
use vorticity::{error::Result, Context, Event, Node, Runtime};

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

pub struct UniqueNode;

impl Node<(), Payload> for UniqueNode {
    fn step(&mut self, input: Event<Payload>, ctx: Context) -> Result<()> {
        let Event::Message(input) = input else {
            unreachable!();
        };
        match input.body().payload {
            Payload::Generate => {
                let guid = format!("{}-{}", ctx.node_id(), ctx.msg_id());
                let reply = ctx.construct_reply(&input, Payload::GenerateOk { guid });

                ctx.send(reply).context("serialize response to generate")?;
            }
            Payload::GenerateOk { .. } => {}
        }

        Ok(())
    }

    fn init(_runtime: &Runtime, _state: (), _ctx: Context) -> Result<Self>
    where
        Self: Sized,
    {
        Ok(Self)
    }
}

fn main() -> Result<()> {
    Ok(Runtime::new().run::<_, _, _, UniqueNode>(())?)
}
