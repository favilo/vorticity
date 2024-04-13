use miette::Context as _;
use serde::{Deserialize, Serialize};
use vorticity::{error::Result, Context, Event, Node, Runtime};

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
    fn step(&mut self, input: Event<Payload>, ctx: Context) -> Result<()> {
        let Event::Message(input) = input else {
            unreachable!()
        };
        match input.body().payload {
            Payload::Echo { ref echo } => {
                let reply = ctx.construct_reply(&input, Payload::EchoOk { echo: echo.clone() });
                ctx.send(reply).context("serialize response to echo")?;
            }
            Payload::EchoOk { .. } => {}
        }

        Ok(())
    }

    fn init(_runtime: &Runtime, _state: (), _ctx: Context) -> Result<Self>
    where
        Self: Sized,
    {
        Ok(Self { id: 1 })
    }
}

fn main() -> Result<()> {
    Ok(Runtime::new().run::<_, _, _, EchoNode>(())?)
}
