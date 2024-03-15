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
    types::ToJson,
    updates::{decoder::Decode, encoder::Encode},
    Array, ArrayPrelim, ArrayRef, Map, ReadTxn, Transact, Value,
};

mod kafka_lib;

const ENGINE: GeneralPurpose =
    GeneralPurpose::new(&base64::alphabet::URL_SAFE, GeneralPurposeConfig::new());

type Msg = yrs::Any;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
#[serde(rename_all = "snake_case")]
enum Payload {
    Send {
        key: String,
        msg: Msg,
    },
    SendOk {
        offset: u64,
    },

    Poll {
        offsets: HashMap<String, u64>,
    },
    PollOk {
        msgs: HashMap<String, Vec<(u64, Msg)>>,
    },

    CommitOffsets {
        offsets: HashMap<String, u64>,
    },
    CommitOffsetsOk,

    ListCommittedOffsets {
        keys: Vec<String>,
    },
    ListCommittedOffsetsOk {
        offsets: HashMap<String, u64>,
    },

    Admin(AdminPayload),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
#[serde(rename_all = "snake_case")]
enum AdminPayload {
    Gossip { diff: String, state_vector: String },
}

#[derive(Clone, Debug)]
enum InjectedPayload {
    Gossip,
}

pub struct KafkaNode {
    msg_id: usize,
    node_id: String,
    doc: yrs::Doc,
    logs: yrs::MapRef,
    offsets: yrs::MapRef,
    known: HashMap<String, yrs::StateVector>,
    neighborhood: Vec<String>,
    event_sender: Sender<ToEvent<InjectedPayload>>,
}

impl Node<(), Payload, InjectedPayload> for KafkaNode {
    fn step(&mut self, input: Event<Payload, InjectedPayload>, ctx: Context) -> anyhow::Result<()> {
        match input {
            Event::Message(input) => {
                let mut reply = input.into_reply(Some(&mut self.msg_id));
                match reply.body.payload {
                    Payload::Send { key, msg } => {
                        let mut txn = self.doc.transact_mut();
                        let list = self.logs.get(&txn, &key);
                        let list = match list {
                            Some(Value::YArray(list)) => list,
                            _ => {
                                let list: ArrayRef =
                                    self.logs
                                        .insert(&mut txn, key.clone(), ArrayPrelim::default());
                                list
                            }
                        };

                        list.push_back(&mut txn, msg);
                        txn.commit();

                        reply.body.payload = Payload::SendOk {
                            offset: list.len(&txn) as u64 - 1,
                        };
                        ctx.send(reply).context("serialize response to broadcast")?;
                    }
                    Payload::Poll { offsets } => {
                        let txn = self.doc.transact();
                        let offsets = offsets
                            .iter()
                            .filter_map(|(k, v)| {
                                let list = self.logs.get(&txn, k)?.cast::<ArrayRef>().ok()?;
                                Some((
                                    k.clone(),
                                    list.iter(&txn)
                                        .enumerate()
                                        .skip(*v as usize)
                                        .map(|(i, v)| (i as u64, v.to_json(&txn)))
                                        .collect::<Vec<(u64, Msg)>>(),
                                ))
                            })
                            .collect::<HashMap<String, Vec<(u64, Msg)>>>();

                        reply.body.payload = Payload::PollOk { msgs: offsets };
                        ctx.send(reply).context("serialize response to read")?;
                    }
                    Payload::CommitOffsets { offsets } => {
                        let mut txn = self.doc.transact_mut();
                        offsets.iter().for_each(|(k, v)| {
                            self.offsets.insert(&mut txn, k.clone(), *v as i64);
                        });

                        reply.body.payload = Payload::CommitOffsetsOk;
                        ctx.send(reply).context("serialize response to commit")?;
                    }
                    Payload::ListCommittedOffsets { keys } => {
                        let txn = self.doc.transact();
                        let offsets = keys
                            .iter()
                            .map(|k| {
                                (
                                    k.clone(),
                                    self.offsets
                                        .get(&txn, k)
                                        .unwrap_or(Value::Any(0.into()))
                                        .cast::<i64>()
                                        .unwrap() as u64,
                                )
                            })
                            .collect();
                        reply.body.payload = Payload::ListCommittedOffsetsOk { offsets };
                        ctx.send(reply).context("serialize response to commit")?;
                    }

                    Payload::Admin(admin_payload) => {
                        reply.body.payload = Payload::Admin(admin_payload);
                        self.handle_admin(&mut reply, &ctx)?;
                    }
                    Payload::PollOk { .. }
                    | Payload::SendOk { .. }
                    | Payload::ListCommittedOffsetsOk { .. }
                    | Payload::CommitOffsetsOk => {}
                }
            }
            Event::Eof => {}
            Event::Injected(input) => {
                self.handle_injected(input, &ctx)?;
            }
        }

        Ok(())
    }

    fn from_init(
        _state: (),
        init: vorticity::message::Init,
        tx: Sender<ToEvent<InjectedPayload>>,
    ) -> anyhow::Result<Self>
    where
        Self: Sized,
    {
        let gossip_tx = tx.clone();
        std::thread::spawn(move || {
            // generate gossip events
            // TODO: handle EOF signal
            loop {
                std::thread::sleep(Duration::from_millis(300));
                if let Err(_) = gossip_tx.send(ToEvent::Injected(InjectedPayload::Gossip)) {
                    break;
                }
            }
        });

        let doc = yrs::Doc::new();
        let logs = doc.get_or_insert_map("counter");
        let offsets = doc.get_or_insert_map("offsets");
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
            logs,
            offsets,
            event_sender: tx,
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

impl KafkaNode {
    fn handle_injected(&mut self, input: InjectedPayload, ctx: &Context) -> anyhow::Result<()> {
        match input {
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
                            payload: Payload::Admin(AdminPayload::Gossip { state_vector, diff }),
                        },
                    })
                    .with_context(|| format!("sending Gossip to {}", n))?;
                }
            }
        };
        Ok(())
    }

    fn handle_admin(
        &mut self,
        reply: &mut Message<Payload>,
        _output: &Context,
    ) -> anyhow::Result<()> {
        let Payload::Admin(admin_payload) = &reply.body.payload else {
            anyhow::bail!("expected Admin payload");
        };
        match admin_payload {
            AdminPayload::Gossip { state_vector, diff } => {
                let state_vector = yrs::StateVector::decode_v1(
                    &ENGINE
                        .decode(&state_vector)
                        .context("base64 decode failed")?,
                )
                .context("StateVector decode failed")?;
                let update =
                    yrs::Update::decode_v1(&ENGINE.decode(&diff).context("base64 decode failed")?)
                        .context("Update decode failed")?;
                self.known.insert(reply.dst.clone(), state_vector);
                let mut txn = self.doc.transact_mut();
                txn.apply_update(update);
            }
        };

        Ok(())
    }
}

impl KafkaNode {}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    Runtime::<_, Payload, InjectedPayload, KafkaNode>::run(()).await
}
