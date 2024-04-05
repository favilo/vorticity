use std::{
    collections::{HashMap, HashSet},
    sync::mpsc::Sender,
    time::Duration,
};

use anyhow::Context as _;
use base64::{
    engine::{GeneralPurpose, GeneralPurposeConfig},
    Engine,
};
use rand::Rng;
use serde::{Deserialize, Serialize};
use vorticity::{Body, Context, Event, Message, Node, Runtime, ToEvent};
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

#[derive(Debug, Clone)]
enum InjectedPayload {
    Gossip,
}

pub struct BroadcastNode {
    msg_id: usize,
    node_id: String,
    doc: yrs::Doc,
    messages: yrs::ArrayRef,
    known: HashMap<String, yrs::StateVector>,
    neighborhood: Vec<String>,
}

impl Node<(), Payload, InjectedPayload> for BroadcastNode {
    fn step(&mut self, input: Event<Payload, InjectedPayload>, ctx: Context) -> anyhow::Result<()> {
        match input {
            Event::Message(input) => {
                let mut reply = input.into_reply(Some(&mut self.msg_id));
                match reply.body.payload {
                    Payload::Broadcast { message } => {
                        let mut txn = self.doc.transact_mut();
                        self.messages.push_back(&mut txn, message as i64);

                        reply.body.payload = Payload::BroadcastOk;
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

                        reply.body.payload = Payload::ReadOk { messages };
                        ctx.send(reply).context("serialize response to read")?;
                    }
                    Payload::Topology { topology: _ } => {
                        reply.body.payload = Payload::TopologyOk;
                        ctx.send(reply).context("serialize response to topology")?;
                    }
                    Payload::Gossip { state_vector, diff } => {
                        let state_vector = yrs::StateVector::decode_v1(
                            &ENGINE
                                .decode(&state_vector)
                                .context("base64 decode failed")?,
                        )
                        .context("StateVector decode failed")?;
                        let update = yrs::Update::decode_v1(
                            &ENGINE.decode(&diff).context("base64 decode failed")?,
                        )
                        .context("Update decode failed")?;
                        self.known.insert(reply.dst, state_vector);
                        let mut txn = self.doc.transact_mut();
                        txn.apply_update(update);
                    }
                    Payload::BroadcastOk | Payload::ReadOk { .. } | Payload::TopologyOk => {}
                }
            }
            Event::Eof => {}
            Event::Injected(input) => match input {
                InjectedPayload::Gossip => {
                    for n in &self.neighborhood {
                        let remote_state_vector = &self.known[n];
                        let txn = self.doc.transact();
                        let diff = ENGINE.encode(&txn.encode_diff_v1(&remote_state_vector));
                        let state_vector = &txn.state_vector();

                        // Send the update 10% of the time, even if it's the same as the remote state
                        let mut rng = rand::thread_rng();
                        if remote_state_vector == state_vector && !rng.gen_bool(0.1) {
                            continue;
                        }
                        let state_vector = ENGINE.encode(&state_vector.encode_v1());
                        eprintln!(
                            "sending state_vector to {}: {} bytes",
                            n,
                            state_vector.len()
                        );
                        eprintln!("sending diff to {}: {} bytes", n, diff.len());

                        ctx.send(Message {
                            src: self.node_id.clone(),
                            dst: n.clone(),
                            body: Body {
                                id: None,
                                in_reply_to: None,
                                payload: Payload::Gossip { state_vector, diff },
                            },
                        })
                        .with_context(|| format!("sending Gossip to {}", n))?;
                    }
                }
            },
        }

        Ok(())
    }

    fn from_init(
        _state: (),
        init: vorticity::Init,
        tx: Sender<ToEvent<InjectedPayload>>,
    ) -> anyhow::Result<Self>
    where
        Self: Sized,
    {
        std::thread::spawn(move || {
            // generate gossip events
            // TODO: handle EOF signal
            loop {
                std::thread::sleep(Duration::from_millis(300));
                if let Err(_) = tx.send(ToEvent::Injected(InjectedPayload::Gossip)) {
                    break;
                }
            }
        });

        let doc = yrs::Doc::new();
        let messages = doc.get_or_insert_array("messages");
        let mut rng = rand::thread_rng();
        let neighborhood = init
            .node_ids
            .iter()
            .cloned()
            .filter(|_| rng.gen_bool(0.75))
            .collect();
        Ok(Self {
            msg_id: 1,
            node_id: init.node_id,
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

fn main() -> anyhow::Result<()> {
    Runtime::run::<_, Payload, InjectedPayload, BroadcastNode>(())
}
