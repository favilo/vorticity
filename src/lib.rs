use std::{
    io::{BufRead, Write},
    sync::{
        atomic::AtomicUsize,
        mpsc::{Receiver, Sender},
        Arc,
    },
    thread,
};

use anyhow::Context as _;
use erased_serde::Serialize;
use serde::{de::DeserializeOwned, Deserialize};

pub use message::{Body, Context, Event, Init, Message};
use message::{InitPayload, ToEvent};

pub mod message;
// pub mod rpc;

pub trait Handler<IP> {
    fn can_handle(&self, json: &serde_json::Value) -> bool;
    fn step(&mut self, json: serde_json::Value, ctx: Context<IP>) -> anyhow::Result<()>;
}

pub trait Node<S, Payload, InjectedPayload = ()> {
    fn from_init(state: S, init: &Init, context: Context<InjectedPayload>) -> anyhow::Result<Self>
    where
        Self: Sized;

    fn step(
        &mut self,
        input: Event<Payload, InjectedPayload>,
        output: Context<InjectedPayload>,
    ) -> anyhow::Result<()>;
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

        let context = Context::new(
            msg_in_tx.clone(),
            msg_out_tx.clone(),
            Arc::new(AtomicUsize::new(0)),
        );

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
        let InitPayload::Init(ref init) = init_msg.body.payload else {
            panic!("first message should be init")
        };
        let node = N::from_init(init_state, init, context.clone())
            .context("node initialization failed")?;
        let reply = context.construct_reply(&init_msg, InitPayload::InitOk);

        context.send(reply).context("send init reply to stdout")?;
        Ok(node)
    }
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
            if input.is_reply() {
                todo!("Handle reply");
            }
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
