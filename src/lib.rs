use std::{
    io::{BufRead, Write},
    sync::mpsc::{Receiver, Sender},
    thread,
};

use anyhow::Context as _;

use erased_serde::Serialize;
use message::InitPayload;
pub use message::{Body, Event, Init, Message};
use serde::{de::DeserializeOwned, Deserialize};

pub mod message;
// pub mod rpc;

pub trait Handler<IP> {
    fn can_handle(&self, json: &serde_json::Value) -> bool;
    fn step(&mut self, json: serde_json::Value, ctx: Context<IP>) -> anyhow::Result<()>;
}

pub trait Node<S, Payload, InjectedPayload = ()> {
    fn from_init(state: S, init: Init, context: Context<InjectedPayload>) -> anyhow::Result<Self>
    where
        Self: Sized;

    fn step(
        &mut self,
        input: Event<Payload, InjectedPayload>,
        output: Context<InjectedPayload>,
    ) -> anyhow::Result<()>;
}

pub enum ToEvent<InjectedPayload = ()> {
    Message(Message<serde_json::Value>),
    Injected(InjectedPayload),
    Eof,
}

impl<IP> ToEvent<IP> {
    pub fn to_event<Payload>(&self) -> anyhow::Result<Event<Payload, IP>>
    where
        Payload: DeserializeOwned,
        IP: Clone,
    {
        let event = match self {
            ToEvent::Message(e) => {
                let body: Result<Payload, _> = serde_json::from_value(e.body.payload.clone());
                if let Ok(body) = body {
                    let message = Message {
                        src: e.src.clone(),
                        dst: e.dst.clone(),
                        body: Body {
                            id: e.body.id,
                            in_reply_to: e.body.in_reply_to,
                            payload: body,
                        },
                    };
                    Event::Message(message)
                } else {
                    Event::Reply(e.clone())
                }
            }
            ToEvent::Injected(i) => Event::Injected(i.clone()),
            ToEvent::Eof => Event::Eof,
        };
        Ok(event)
    }
}

#[derive(Clone)]
pub struct Context<IP> {
    /// Allows sending messages as RPCs
    msg_out_tx: Sender<Box<dyn Serialize + Send + Sync + 'static>>,

    /// Allows injecting messages into the event loop
    msg_in_tx: Sender<ToEvent<IP>>,
}

impl<IP> Context<IP> {
    pub fn send<S>(&self, s: S) -> anyhow::Result<()>
    where
        S: Serialize + Sync + Send + 'static,
    {
        self.msg_out_tx
            .send(Box::new(s))
            .context("send message to stdout")
    }

    pub fn inject(&self, s: IP) -> anyhow::Result<()>
    where
        IP: Sync + Send + 'static,
    {
        self.msg_in_tx
            .send(ToEvent::Injected(s))
            .context("inject message into event loop")
    }
}

pub struct Runtime;

impl Runtime {
    pub fn run<S, P, IP, N>(init_state: S) -> anyhow::Result<()>
    where
        P: DeserializeOwned + Send + 'static,
        N: Node<S, P, IP>,
        IP: Clone + Send + 'static,
    {
        let (msg_in_tx, msg_in_rx): (Sender<ToEvent<IP>>, Receiver<ToEvent<IP>>) =
            std::sync::mpsc::channel();

        let (msg_out_tx, msg_out_rx) = std::sync::mpsc::channel();

        let context = Context {
            msg_in_tx: msg_in_tx.clone(),
            msg_out_tx: msg_out_tx.clone(),
        };

        let node: N = Self::init_node(init_state, context.clone())?;
        let node = node;

        let stdin_tx = msg_in_tx.clone();
        let input_handle = receive_loop::<N, S, P, IP>(stdin_tx, msg_in_tx);

        let output_handle = send_loop::<N, S, P, IP>(msg_out_rx);

        event_loop(msg_in_rx, node, context)?;

        input_handle
            .join()
            .expect("failed to join input thread")
            .context("error from stdin thread")?;
        output_handle
            .join()
            .expect("failed to join output thread")
            .context("error from stdout thread")?;

        Ok(())
    }

    fn init_node<S, P, IP, N>(init_state: S, context: Context<IP>) -> Result<N, anyhow::Error>
    where
        P: DeserializeOwned + Send + 'static,
        N: Node<S, P, IP>,
        IP: Clone + Send + 'static,
    {
        let stdin = std::io::stdin().lock();
        let mut stdin = stdin.lines();
        let init_msg: Message<InitPayload> = serde_json::from_str::<Message<InitPayload>>(
            &stdin
                .next()
                .expect("no init message received")
                .context("failed to read init message from stdin")?,
        )
        .context("read init message from STDIN")?;
        let InitPayload::Init(init) = init_msg.body.payload else {
            panic!("first message should be init")
        };
        let node = N::from_init(init_state, init, context.clone())
            .context("node initialization failed")?;
        let reply = Message {
            src: init_msg.dst,
            dst: init_msg.src,
            body: Body {
                id: Some(0),
                in_reply_to: init_msg.body.id,
                payload: InitPayload::InitOk,
            },
        };
        context.send(reply).context("send init reply to stdout")?;
        Ok(node)
    }
}

fn event_loop<N, S, P, IP>(
    msg_in_rx: Receiver<ToEvent<IP>>,
    mut node: N,
    context: Context<IP>,
) -> Result<(), anyhow::Error>
where
    N: Node<S, P, IP>,
    P: for<'de> Deserialize<'de> + Send + 'static,
    IP: Clone + Send + 'static,
{
    for input in msg_in_rx {
        if let Ok(input) = input.to_event() {
            node.step(input, context.clone())
                .context("Node step function failed")?;
        } else {
            let ToEvent::Message(message) = input else {
                panic!("Impossible position");
            };
            todo!("Handle message: {:?}", message);
        }
    }

    Ok(())
}

fn receive_loop<N, S, P, IP>(
    stdin_tx: Sender<ToEvent<IP>>,
    msg_in_tx: Sender<ToEvent<IP>>,
) -> thread::JoinHandle<Result<(), anyhow::Error>>
where
    N: Node<S, P, IP>,
    IP: Clone + Send + 'static,
{
    let input_handle = thread::spawn(move || {
        let stdin = std::io::stdin().lock();
        for line in stdin.lines() {
            let line = line.context("Maestrom input from STDIN could not be deserialized")?;
            let input: Message<serde_json::Value> =
                serde_json::from_str(&line).context("read input message from STDIN")?;
            if let Err(_) = stdin_tx.send(ToEvent::Message(input)) {
                break;
            }
        }
        let _ = msg_in_tx.send(ToEvent::Eof);

        Ok::<_, anyhow::Error>(())
    });
    input_handle
}

fn send_loop<N, S, P, IP>(
    msg_out_rx: Receiver<Box<dyn Serialize + Send + Sync>>,
) -> thread::JoinHandle<Result<(), anyhow::Error>>
where
    N: Node<S, P, IP>,
    IP: Clone + Send + 'static,
{
    let output_handle = thread::spawn(move || {
        let mut stdout = std::io::stdout().lock();
        for send_msg in msg_out_rx {
            serde_json::to_writer(&mut stdout, &send_msg).context("serialize response to init")?;
            stdout.write_all(b"\n").context("write newline to output")?;
        }
        Ok::<_, anyhow::Error>(())
    });
    output_handle
}

#[derive(Debug, Clone, Default)]
pub struct NodeState {
    pub node_id: String,
    pub nodes: Vec<String>,
}
