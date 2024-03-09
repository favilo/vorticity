use std::{
    collections::{HashMap, HashSet},
    io::{StdoutLock, Write},
    sync::mpsc::Sender,
    time::Duration,
};

use anyhow::Context;
use serde::{Deserialize, Serialize};
use vorticity::{main_loop, Body, Event, Message, Node};

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
        seen: HashSet<usize>,
    },
}

enum InjectedPayload {
    Gossip,
}

pub struct BroadcastNode {
    msg_id: usize,
    node_id: String,
    messages: HashSet<usize>,
    known: HashMap<String, HashSet<usize>>,
    neighborhood: Vec<String>,

    msg_communicated: HashMap<usize, HashSet<usize>>,
}

impl Node<(), Payload, InjectedPayload> for BroadcastNode {
    fn step(
        &mut self,
        input: Event<Payload, InjectedPayload>,
        output: &mut StdoutLock,
    ) -> anyhow::Result<()> {
        match input {
            Event::Message(input) => {
                let mut reply = input.into_reply(Some(&mut self.msg_id));
                match reply.body.payload {
                    Payload::Broadcast { message } => {
                        self.messages.insert(message);
                        reply.body.payload = Payload::BroadcastOk;
                        reply
                            .send(&mut *output)
                            .context("serialize response to broadcast")?;
                    }
                    Payload::Read => {
                        reply.body.payload = Payload::ReadOk {
                            messages: self.messages.clone(),
                        };
                        reply
                            .send(&mut *output)
                            .context("serialize response to read")?;
                    }
                    Payload::Topology { mut topology } => {
                        self.neighborhood = topology
                            .remove(&self.node_id)
                            .unwrap_or_else(|| panic!("node {} not in topology", self.node_id));
                        reply.body.payload = Payload::TopologyOk;
                        reply
                            .send(&mut *output)
                            .context("serialize response to topology")?;
                    }
                    Payload::Gossip { seen } => self.messages.extend(seen),
                    Payload::BroadcastOk | Payload::ReadOk { .. } | Payload::TopologyOk => {}
                }
            }
            Event::Eof => {}
            Event::Injected(input) => match input {
                InjectedPayload::Gossip => {
                    for n in &self.neighborhood {
                        let known_to_n = &self.known[n];
                        Message {
                            src: self.node_id.clone(),
                            dst: n.clone(),
                            body: Body {
                                id: None,
                                in_reply_to: None,
                                payload: Payload::Gossip {
                                    seen: self
                                        .messages
                                        .iter()
                                        .copied()
                                        .filter(|n| !known_to_n.contains(n))
                                        .collect(),
                                },
                            },
                        }
                        .send(&mut *output)
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
        tx: Sender<Event<Payload, InjectedPayload>>,
    ) -> anyhow::Result<Self>
    where
        Self: Sized,
    {
        std::thread::spawn(move || {
            // generate gossip events
            // TODO: handle EOF signal
            loop {
                std::thread::sleep(Duration::from_millis(300));
                if let Err(_) = tx.send(Event::Injected(InjectedPayload::Gossip)) {
                    break;
                }
            }
        });

        Ok(Self {
            msg_id: 1,
            node_id: init.node_id,
            messages: Default::default(),
            known: init
                .node_ids
                .into_iter()
                .map(|nid| (nid, Default::default()))
                .collect(),
            neighborhood: Default::default(),

            msg_communicated: Default::default(),
        })
    }
}

fn main() -> anyhow::Result<()> {
    main_loop::<_, BroadcastNode, Payload, InjectedPayload>(())
}
