use std::io::{StdoutLock, Write};

use anyhow::{bail, Context};
use serde::{de::DeserializeOwned, Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Message<Payload> {
    pub src: String,

    #[serde(rename = "dest")]
    pub dst: String,

    pub body: Body<Payload>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Body<Payload> {
    #[serde(rename = "msg_id")]
    pub id: Option<usize>,

    pub in_reply_to: Option<usize>,

    #[serde(flatten)]
    pub payload: Payload,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
#[serde(rename_all = "snake_case")]
pub struct Init {
    pub node_id: String,
    pub node_ids: Vec<String>,
}

pub trait Payload: Sized {
    fn extract_init(input: Self) -> Option<Init>;
    fn gen_init_ok() -> Self;
}

pub trait Node<S, Payload> {
    fn from_init(state: S, init: Init) -> anyhow::Result<Self>
    where
        Self: Sized;

    fn step(&mut self, input: Message<Payload>, output: &mut StdoutLock) -> anyhow::Result<()>;
}

pub fn main_loop<S, N, P>(init_state: S) -> anyhow::Result<()>
where
    P: Payload + DeserializeOwned + Serialize,
    N: Node<S, P>,
{
    let stdin = std::io::stdin().lock();
    let mut inputs = serde_json::Deserializer::from_reader(stdin).into_iter::<Message<P>>();

    let mut stdout = std::io::stdout().lock();

    let init_msg = inputs
        .next()
        .expect("init message should always be present")
        .context("init message could not be deserialized")?;
    let init = P::extract_init(init_msg.body.payload).expect("first message should be init");
    let mut node = N::from_init(init_state, init).context("node initialization failed")?;

    let reply = Message {
        src: init_msg.dst,
        dst: init_msg.src,
        body: Body {
            id: Some(0),
            in_reply_to: init_msg.body.id,
            payload: P::gen_init_ok(),
        },
    };
    serde_json::to_writer(&mut stdout, &reply).context("serialize response to init")?;
    stdout.write_all(b"\n").context("write newline to output")?;

    for input in inputs {
        let input = input.context("Maestrom input from STDIN could not be deserialized")?;
        node.step(input, &mut stdout)
            .context("Node step function failed")?;
    }

    Ok(())
}
