use std::{collections::HashMap, time::Duration};

use base64::{
    engine::{GeneralPurpose, GeneralPurposeConfig},
    Engine,
};
use miette::{Context as _, IntoDiagnostic as _};
use rand::Rng;
use serde::{Deserialize, Serialize};
use sloggers::{terminal::TerminalLoggerBuilder, Build};
use vorticity::{error::Result, Context, Event, Message, Node, Runtime};
use yrs::{
    types::ToJson,
    updates::{decoder::Decode, encoder::Encode},
    Array, ArrayPrelim, ArrayRef, Map, Out, ReadTxn, Transact,
};

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
    doc: yrs::Doc,
    logs: yrs::MapRef,
    offsets: yrs::MapRef,
    known: HashMap<String, yrs::StateVector>,
    neighborhood: Vec<String>,

    callbacks: HashMap<usize, Box<dyn FnOnce(Result<Payload>, Context) + Send>>,
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

    fn init(_runtime: &Runtime, _state: (), context: Context) -> Result<Self>
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
        let inner_context = context.clone();
        std::thread::spawn(move || {
            // generate gossip events
            // TODO: handle EOF signal
            loop {
                std::thread::sleep(Duration::from_millis(300));
                if inner_context.inject(InjectedPayload::Gossip).is_err() {
                    break;
                }
            }
        });

        let doc = yrs::Doc::new();
        let logs = doc.get_or_insert_map("counter");
        let offsets = doc.get_or_insert_map("offsets");
        let mut rng = rand::rng();
        let node_count = context.neighbors().count();
        let neighborhood = context
            .neighbors()
            .filter(|&_| {
                if cfg!(test) || node_count < 5 {
                    true
                } else {
                    rng.random_bool(0.75)
                }
            })
            .map(String::from)
            .collect();
        Ok(Self {
            doc,
            logs,
            offsets,
            known: context
                .neighbors()
                .map(String::from)
                .map(|nid| (nid, Default::default()))
                .collect(),
            neighborhood,

            callbacks: HashMap::new(),
        })
    }

    fn handle_reply(
        &mut self,
        input: Event<Payload, InjectedPayload>,
        _context: Context,
    ) -> Result<()> {
        // TODO: Make this handle callbacks stored in a data structure
        let Event::Message(_input) = input else {
            return Err(miette::miette!("expected Message").into());
        };

        todo!("handle replies?");
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
        eprintln!("Sending gossip to {} neighbors...", self.neighborhood.len());
        for n in &self.neighborhood {
            eprintln!("Gossiping to {n}");
            let remote_state_vector = &self.known[n];
            eprintln!("Remote state vector for {n}: {remote_state_vector:?}");
            let txn = self.doc.transact();
            let diff = ENGINE.encode(txn.encode_diff_v1(remote_state_vector));
            let state_vector = &txn.state_vector();
            eprintln!("Local state vector: {state_vector:?}");

            // Send the update 10% of the time, even if it's the same as the remote state
            let mut rng = rand::rng();
            if remote_state_vector == state_vector && !rng.random_bool(0.1) {
                eprintln!("Skipping gossip to {n}: state vector is the same");
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
                Message::builder(ctx.clone())
                    .dst(n)
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

fn main() -> Result<()> {
    let runtime = Runtime::new();
    runtime.run::<_, Payload, InjectedPayload, KafkaNode>(())
}

#[cfg(test)]
mod tests {
    use std::sync::{atomic::AtomicUsize, mpsc};

    use super::*;
    use miette::IntoDiagnostic;
    use yrs::StateVector;

    #[test]
    fn send_to_send_ok() -> Result<()> {
        let (n1_msg_out_tx, n1_msg_out_rx) = mpsc::channel();
        let n1_ctx = Context::new(
            "node1",
            &["node1".to_string(), "node2".to_string()],
            mpsc::channel().0,
            n1_msg_out_tx,
            AtomicUsize::new(0).into(),
        );
        let client_ctx = Context::new(
            "c",
            &["node1".to_string(), "node2".to_string()],
            mpsc::channel().0,
            mpsc::channel().0,
            AtomicUsize::new(0).into(),
        );
        let mut node = KafkaNode::init(&Runtime::new(), (), n1_ctx.clone()).into_diagnostic()?;
        let msg = Message::builder(client_ctx.clone())
            .dst("node1")
            .payload(Payload::Send {
                key: "test_key".to_string(),
                msg: Msg::from("test_message"),
            })
            .build()
            .into_diagnostic()?;
        node.step(Event::Message(msg), n1_ctx.clone())
            .into_diagnostic()
            .context("step failed")?;

        let reply = serde_json::to_string(&n1_msg_out_rx.recv().into_diagnostic()?)
            .into_diagnostic()
            .context("failed to serialize reply")?;
        let reply = serde_json::from_str::<Message<Payload>>(&reply)
            .into_diagnostic()
            .context("failed to deserialize reply")?
            .body()
            .payload
            .clone();
        assert!(
            matches!(reply, Payload::SendOk { offset: 0 }),
            "Expected SendOk with offset 0",
        );

        let txn = node.doc.transact();
        let list = node.logs.get(&txn, "test_key").unwrap();
        assert_eq!(
            list.cast::<ArrayRef>().unwrap().len(&txn),
            1,
            "Expected one message in the log"
        );
        Ok(())
    }

    #[test]
    fn gossip_sends() {
        let (n1_msg_out_tx, n1_msg_out_rx) = mpsc::channel();
        let n1_ctx = Context::new(
            "node1",
            &["node1".to_string(), "node2".to_string()],
            mpsc::channel().0,
            n1_msg_out_tx,
            AtomicUsize::new(0).into(),
        );
        let client_ctx = Context::new(
            "c",
            &["node1".to_string(), "node2".to_string()],
            mpsc::channel().0,
            mpsc::channel().0,
            AtomicUsize::new(0).into(),
        );

        let mut node = KafkaNode::init(&Runtime::new(), (), n1_ctx.clone())
            .expect("node initialization failed");
        let msg = Message::builder(client_ctx.clone())
            .dst("node1")
            .payload(Payload::Send {
                key: "test_key".to_string(),
                msg: Msg::from("test_message"),
            })
            .build()
            .unwrap();
        eprintln!("Sending message to node...");
        node.step(Event::Message(msg), n1_ctx.clone())
            .expect("Send step failed");

        eprintln!("Waiting for SendOk reply...");
        n1_msg_out_rx
            .recv_timeout(Duration::from_secs(0))
            .expect("Failed to receive SendOk reply");

        eprintln!("Checking log for message...");
        {
            let txn = node.doc.transact();
            let list = node.logs.get(&txn, "test_key").unwrap();
            assert_eq!(
                list.cast::<ArrayRef>().unwrap().len(&txn),
                1,
                "Expected one message in the log"
            );
        }

        eprintln!("Injecting Gossip event...");
        node.step(Event::Injected(InjectedPayload::Gossip), n1_ctx.clone())
            .expect("Gossip step failed");

        eprintln!("Waiting for Gossip reply...");
        let reply = serde_json::to_string(
            &n1_msg_out_rx
                .recv_timeout(Duration::from_secs(0))
                .expect("Failed to receive reply"),
        )
        .expect("Failed to serialize reply");
        let reply = serde_json::from_str::<Message<Payload>>(&reply)
            .expect("Failed to deserialize reply")
            .body()
            .payload
            .clone();
        assert!(
            matches!(reply, Payload::Admin(AdminPayload::Gossip { .. })),
            "Expected Admin Gossip payload",
        );
        let Payload::Admin(AdminPayload::Gossip { state_vector, diff }) = reply else {
            panic!("Expected Admin Gossip payload");
        };
        let txn = node.doc.transact();
        assert_eq!(
            state_vector,
            ENGINE.encode(txn.state_vector().encode_v1()),
            "State vector should be equal"
        );
        assert_eq!(
            diff,
            ENGINE.encode(txn.encode_diff_v1(&StateVector::default())),
            "Diff should be empty"
        );
    }
}
