use std::{
    collections::{HashMap, HashSet},
    time::Duration,
};

use base64::{
    engine::{GeneralPurpose, GeneralPurposeConfig},
    Engine,
};
use miette::{Context as _, IntoDiagnostic};
use rand::Rng;
use serde::{Deserialize, Serialize};
use vorticity::{error::Result, Context, Event, Init, Message, Node, Runtime};
use yrs::{
    updates::{decoder::Decode, encoder::Encode},
    Array, ReadTxn, Transact,
};

const ENGINE: GeneralPurpose =
    GeneralPurpose::new(&base64::alphabet::URL_SAFE, GeneralPurposeConfig::new());

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
#[serde(rename_all = "snake_case")]
pub enum Payload {
    Broadcast {
        message: usize,
    },
    BroadcastOk,
    Read,
    ReadOk {
        messages: HashSet<usize>,
    },
    Topology {
        topology: HashMap<String, Vec<String>>,
    },
    TopologyOk,

    Gossip {
        diff: String,
        state_vector: String,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
enum InjectedPayload {
    Gossip,
}

pub struct BroadcastNode {
    doc: yrs::Doc,
    messages: yrs::ArrayRef,
    known: HashMap<String, yrs::StateVector>,
    neighborhood: Vec<String>,
}

impl Node<(), Payload, InjectedPayload> for BroadcastNode {
    fn step(&mut self, input: Event<Payload, InjectedPayload>, ctx: Context) -> Result<()> {
        match input {
            Event::Message(input) => match input.body().payload {
                Payload::Broadcast { message } => {
                    let mut txn = self.doc.transact_mut();
                    self.messages.push_back(&mut txn, message as i64);

                    let reply = ctx.construct_reply(&input, Payload::BroadcastOk);
                    ctx.send(reply).context("serialize response to broadcast")?;
                }
                Payload::Read => {
                    let txn = self.doc.transact();
                    let messages = self
                        .messages
                        .iter(&txn)
                        .map(|v| {
                            v.cast::<i64>()
                                .expect("Not an integer")
                                .try_into()
                                .expect("all messages should be positive")
                        })
                        .collect();

                    let reply = ctx.construct_reply(&input, Payload::ReadOk { messages });
                    ctx.send(reply).context("serialize response to read")?;
                }
                Payload::Topology { topology: _ } => {
                    let reply = ctx.construct_reply(&input, Payload::TopologyOk);
                    ctx.send(reply).context("serialize response to topology")?;
                }
                Payload::Gossip {
                    ref state_vector,
                    ref diff,
                } => {
                    let state_vector = yrs::StateVector::decode_v1(
                        &ENGINE
                            .decode(state_vector)
                            .into_diagnostic()
                            .context("base64 decode failed")?,
                    )
                    .into_diagnostic()
                    .context("StateVector decode failed")?;
                    let update = yrs::Update::decode_v1(
                        &ENGINE
                            .decode(diff)
                            .into_diagnostic()
                            .context("base64 decode failed")?,
                    )
                    .into_diagnostic()
                    .context("Update decode failed")?;
                    self.known.insert(input.src().to_string(), state_vector);
                    let mut txn = self.doc.transact_mut();
                    txn.apply_update(update)?;
                }
                Payload::BroadcastOk | Payload::ReadOk { .. } | Payload::TopologyOk => {}
            },
            Event::Eof => {}
            Event::Injected(input) => match input {
                InjectedPayload::Gossip => {
                    for n in &self.neighborhood {
                        let remote_state_vector = &self.known[n];
                        let txn = self.doc.transact();
                        let diff = ENGINE.encode(&txn.encode_diff_v1(remote_state_vector));
                        let state_vector = &txn.state_vector();

                        // Send the update 10% of the time, even if it's the same as the remote state
                        let mut rng = rand::rng();
                        if remote_state_vector == state_vector && !rng.random_bool(0.1) {
                            continue;
                        }
                        let state_vector = ENGINE.encode(&state_vector.encode_v1());
                        eprintln!(
                            "sending state_vector to {}: {} bytes",
                            n,
                            state_vector.len()
                        );
                        eprintln!("sending diff to {}: {} bytes", n, diff.len());

                        ctx.send(
                            Message::builder(ctx.clone())
                                .dst(n)
                                .payload(Payload::Gossip { state_vector, diff })
                                .build()?,
                        )
                        .with_context(|| format!("sending Gossip to {n}"))?;
                    }
                }
            },
        }

        Ok(())
    }

    fn from_init(_runtime: &Runtime, _state: (), init: &Init, context: Context) -> Result<Self>
    where
        Self: Sized,
    {
        std::thread::spawn(move || {
            // generate gossip events
            // TODO: handle EOF signal
            loop {
                std::thread::sleep(Duration::from_millis(300));
                if context.inject(InjectedPayload::Gossip).is_err() {
                    break;
                }
            }
        });

        let doc = yrs::Doc::new();
        let messages = doc.get_or_insert_array("messages");
        let mut rng = rand::rng();
        let neighborhood = init
            .node_ids
            .iter()
            .filter(|&id| id != &init.node_id)
            .filter(|&_| rng.random_bool(0.75))
            .cloned()
            .collect();
        Ok(Self {
            doc,
            messages,
            known: init
                .node_ids
                .iter()
                .cloned()
                .map(|nid| (nid, Default::default()))
                .collect(),
            neighborhood,
        })
    }
}

fn main() -> Result<()> {
    Ok(Runtime::new().run::<_, Payload, InjectedPayload, BroadcastNode>(())?)
}
