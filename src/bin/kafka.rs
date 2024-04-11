use std::{collections::HashMap, time::Duration};

use base64::{
    engine::{GeneralPurpose, GeneralPurposeConfig},
    Engine,
};
use miette::{bail, Context as _, IntoDiagnostic};
use rand::Rng;
use serde::{Deserialize, Serialize};
use sloggers::{terminal::TerminalLoggerBuilder, Build as _};
use vorticity::{
    error::Result,
    message::{Init, MessageSet},
    Context, Event, Message, Node, Runtime,
};
use yrs::{
    types::ToJson,
    updates::{decoder::Decode, encoder::Encode},
    Array, ArrayPrelim, ArrayRef, Map, Out, ReadTxn, Transact,
};

const ENGINE: GeneralPurpose =
    GeneralPurpose::new(&base64::alphabet::URL_SAFE, GeneralPurposeConfig::new());

type Msg = yrs::Any;

#[allow(dead_code)]
enum CallbackStatus {
    MoreWork,
    Finished,
}

type RpcCallback = dyn Fn(
    &Message<Payload>,
    &mut MessageSet<Payload>,
    &Message<Payload>,
    Context,
) -> Result<CallbackStatus>;

struct CallbackInfo {
    unhandled_incoming_msg: Message<Payload>,
    sent_msgs: MessageSet<Payload>,
    callback: Box<RpcCallback>,
}

#[allow(dead_code)]
impl CallbackInfo {
    fn new(
        orig_msg: Message<Payload>,
        sent_msgs: MessageSet<Payload>,
        callback: impl Fn(
                &Message<Payload>,
                &mut MessageSet<Payload>,
                &Message<Payload>,
                Context,
            ) -> Result<CallbackStatus>
            + 'static,
    ) -> Self {
        Self {
            unhandled_incoming_msg: orig_msg,
            sent_msgs,
            callback: Box::new(callback),
        }
    }

    fn matches(&self, msg: &Message<Payload>) -> bool {
        self.sent_msgs.is_matching_reply(msg) && self.unhandled_incoming_msg.dst() == msg.dst()
    }
}

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
#[serde(rename_all = "snake_case")]
enum AdminPayload {
    Gossip { diff: String, state_vector: String },
}

#[derive(Clone, Debug, Serialize, Deserialize)]
enum InjectedPayload {
    /// Signal to send gossip to peers.
    Gossip,

    /// Signal to tick the raft node.
    Tick,
}

pub struct KafkaNode {
    node_id: String,
    doc: yrs::Doc,
    logs: yrs::MapRef,
    offsets: yrs::MapRef,
    known: HashMap<String, yrs::StateVector>,
    neighborhood: Vec<String>,

    callbacks: Vec<CallbackInfo>,
}

impl Node<(), Payload, InjectedPayload> for KafkaNode {
    fn step(&mut self, input: Event<Payload, InjectedPayload>, ctx: Context) -> Result<()> {
        match input {
            Event::Message(input) => match input.body().payload {
                Payload::Send { ref key, ref msg } => {
                    self.handle_send(key, msg, &ctx, &input)?;
                }
                Payload::Poll { ref offsets } => {
                    self.handle_poll(offsets, &ctx, &input)?;
                }
                Payload::CommitOffsets { ref offsets } => {
                    self.handle_commit_offsets(offsets, &ctx, &input)?;
                }
                Payload::ListCommittedOffsets { ref keys } => {
                    self.handle_list_committed_offsets(keys, &ctx, &input)?;
                }

                Payload::Admin(_) => {
                    self.handle_admin(&input, &ctx)?;
                }
                Payload::PollOk { .. }
                | Payload::SendOk { .. }
                | Payload::ListCommittedOffsetsOk { .. }
                | Payload::CommitOffsetsOk => {}
            },
            Event::Eof => {}
            Event::Injected(input) => {
                self.handle_injected(input, &ctx)?;
            }
        }

        Ok(())
    }

    fn from_init(_runtime: &Runtime, _state: (), init: &Init, context: Context) -> Result<Self>
    where
        Self: Sized,
    {
        let builder = &mut TerminalLoggerBuilder::new();
        builder
            .level(sloggers::types::Severity::Debug)
            .destination(sloggers::terminal::Destination::Stderr);
        let _logger = builder
            .build()
            .into_diagnostic()
            .context("failed to build logger")?;
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
        let logs = doc.get_or_insert_map("counter");
        let offsets = doc.get_or_insert_map("offsets");
        let mut rng = rand::rng();
        let neighborhood = init
            .node_ids
            .iter()
            .filter(|&_| rng.random_bool(0.75))
            .cloned()
            .collect();
        Ok(Self {
            node_id: init.node_id.clone(),
            doc,
            logs,
            offsets,
            known: init
                .node_ids
                .iter()
                .cloned()
                .map(|nid| (nid, Default::default()))
                .collect(),
            neighborhood,
            callbacks: Vec::new(),
        })
    }

    fn handle_reply(
        &mut self,
        input: Event<Payload, InjectedPayload>,
        context: Context,
    ) -> Result<()> {
        // TODO: Make this handle callbacks stored in a data structure
        let Event::Message(input) = input else {
            return Err(miette::miette!("expected Message").into());
        };

        let callback = self
            .callbacks
            .iter_mut()
            .find(|c| c.matches(&input))
            .ok_or(miette::miette!("Reply to message we don't have: {input:?}"))?;
        let f = &callback.callback;
        let status = f(
            &callback.unhandled_incoming_msg,
            &mut callback.sent_msgs,
            &input,
            context,
        )
        .context("Running callback caused an error")?;

        match status {
            CallbackStatus::MoreWork => {}
            CallbackStatus::Finished => self.callbacks.retain(|c| !c.matches(&input)),
        }

        Ok(())
    }
}

impl KafkaNode {
    fn handle_injected(&mut self, injected: InjectedPayload, ctx: &Context) -> Result<()> {
        match injected {
            InjectedPayload::Gossip => self.send_gossip(ctx)?,
            InjectedPayload::Tick => self.tick(ctx)?,
        };

        Ok(())
    }

    fn send_gossip(&mut self, ctx: &Context) -> Result<()> {
        for n in &self.neighborhood {
            if n == &self.node_id {
                continue;
            }
            let remote_state_vector = &self.known[n];
            let txn = self.doc.transact();
            let diff = ENGINE.encode(txn.encode_diff_v1(remote_state_vector));
            let state_vector = &txn.state_vector();

            // Send the update 10% of the time, even if it's the same as the remote state
            let mut rng = rand::rng();
            if remote_state_vector == state_vector && !rng.random_bool(0.1) {
                continue;
            }
            let state_vector = ENGINE.encode(state_vector.encode_v1());
            eprintln!(
                "sending state_vector to {}: {} bytes",
                n,
                state_vector.len()
            );
            eprintln!("sending diff to {}: {} bytes", n, diff.len());
            ctx.send(
                Message::builder()
                    .src(self.node_id.clone())
                    .dst(n.clone())
                    .payload(Payload::Admin(AdminPayload::Gossip { state_vector, diff }))
                    .build()?,
            )
            .with_context(|| format!("sending Gossip to {n}"))?;
        }

        Ok(())
    }

    fn handle_admin(&mut self, input: &Message<Payload>, _ctx: &Context) -> Result<()> {
        let Payload::Admin(admin_payload) = &input.body().payload else {
            return Err(miette::miette!("expected Admin payload").into());
        };
        match admin_payload {
            AdminPayload::Gossip { state_vector, diff } => {
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
        };

        Ok(())
    }

    fn handle_send(
        &mut self,
        key: &str,
        msg: &yrs::Any,
        ctx: &Context,
        input: &Message<Payload>,
    ) -> Result<()> {
        let mut txn = self.doc.transact_mut();
        let list = self.logs.get(&txn, key);
        let list = match list {
            Some(Out::YArray(list)) => list,
            _ => {
                let list: ArrayRef = self.logs.insert(&mut txn, key, ArrayPrelim::default());
                list
            }
        };
        list.push_back(&mut txn, msg.clone());
        txn.commit();

        let reply = ctx.construct_reply(
            input,
            Payload::SendOk {
                offset: list.len(&txn) as u64 - 1,
            },
        );
        ctx.send(reply).context("serialize response to broadcast")?;
        Ok(())
    }

    fn handle_poll(
        &mut self,
        offsets: &HashMap<String, u64>,
        ctx: &Context,
        input: &Message<Payload>,
    ) -> Result<()> {
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
        let reply = ctx.construct_reply(input, Payload::PollOk { msgs: offsets });
        ctx.send(reply).context("serialize response to read")?;
        Ok(())
    }

    fn handle_commit_offsets(
        &mut self,
        offsets: &HashMap<String, u64>,
        ctx: &Context,
        input: &Message<Payload>,
    ) -> Result<()> {
        let mut txn = self.doc.transact_mut();
        offsets.iter().for_each(|(k, v)| {
            self.offsets.insert(&mut txn, k.clone(), *v as i64);
        });
        let reply = ctx.construct_reply(input, Payload::CommitOffsetsOk);
        ctx.send(reply).context("serialize response to commit")?;
        Ok(())
    }

    fn handle_list_committed_offsets(
        &mut self,
        keys: &[String],
        ctx: &Context,
        input: &Message<Payload>,
    ) -> Result<()> {
        let txn = self.doc.transact();
        let offsets = keys
            .iter()
            .map(|k| {
                (
                    k.clone(),
                    self.offsets
                        .get(&txn, k)
                        .unwrap_or(Out::Any(0.into()))
                        .cast::<i64>()
                        .unwrap() as u64,
                )
            })
            .collect();
        let reply = ctx.construct_reply(input, Payload::ListCommittedOffsetsOk { offsets });
        ctx.send(reply).context("serialize response to commit")?;
        Ok(())
    }

    fn tick(&self, _ctx: &Context) -> Result<()> {
        todo!()
    }
}

impl KafkaNode {}

fn main() -> anyhow::Result<()> {
    Ok(Runtime::new().run::<_, Payload, InjectedPayload, KafkaNode>(())?)
}
