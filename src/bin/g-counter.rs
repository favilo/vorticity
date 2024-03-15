use std::{collections::HashMap, sync::mpsc::Sender, time::Duration};

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
    Map, ReadTxn, Transact,
};

const ENGINE: GeneralPurpose =
    GeneralPurpose::new(&base64::alphabet::URL_SAFE, GeneralPurposeConfig::new());

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
#[serde(rename_all = "snake_case")]
pub enum Payload {
    Add { delta: u64 },
    AddOk,

    Read,
    ReadOk { value: u64 },

    Gossip { diff: String, state_vector: String },
}

#[derive(Debug, Clone)]
enum InjectedPayload {
    Gossip,
}

pub struct GCounterNode {
    msg_id: usize,
    node_id: String,
    doc: yrs::Doc,
    counter: yrs::MapRef,
    known: HashMap<String, yrs::StateVector>,
    neighborhood: Vec<String>,
}

impl Node<(), Payload, InjectedPayload> for GCounterNode {
    fn step(&mut self, input: Event<Payload, InjectedPayload>, ctx: Context) -> anyhow::Result<()> {
        match input {
            Event::Message(input) => {
                let mut reply = input.into_reply(Some(&mut self.msg_id));
                match reply.body.payload {
                    Payload::Add { delta } => {
                        let mut txn = self.doc.transact_mut();
                        let old_val = self
                            .counter
                            .get(&txn, &self.doc.client_id().to_string())
                            .unwrap_or(yrs::Value::Any(0.into()))
                            .cast::<i64>()
                            .unwrap();
                        self.counter.insert(
                            &mut txn,
                            self.doc.client_id().to_string(),
                            old_val + delta as i64,
                        );

                        reply.body.payload = Payload::AddOk;
                        ctx.send(reply).context("serialize response to broadcast")?;
                    }
                    Payload::Read => {
                        let txn = self.doc.transact();
                        let value = self
                            .counter
                            .iter(&txn)
                            .map(|(_, v)| -> u64 {
                                v.cast::<i64>()
                                    .expect("Not an integer")
                                    .try_into()
                                    .expect("all messages should be positive")
                            })
                            .sum();

                        reply.body.payload = Payload::ReadOk { value };
                        ctx.send(reply).context("serialize response to read")?;
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
                    Payload::AddOk | Payload::ReadOk { .. } => {}
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
        let counter = doc.get_or_insert_map("counter");
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
            counter,
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

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    Runtime::<_, Payload, InjectedPayload, GCounterNode>::run(()).await
}
