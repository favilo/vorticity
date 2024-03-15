use std::{
    any::TypeId,
    cell::{OnceCell, RefCell},
    collections::HashMap,
    io::{BufRead, Lines, StdinLock, Stdout, StdoutLock, Write},
    marker::PhantomData,
    rc::Rc,
    sync::{
        atomic::AtomicU64,
        mpsc::{Receiver, Sender},
        Arc,
    },
    thread,
};

use anyhow::Context as _;
use serde::{de::DeserializeOwned, Serialize};

use message::InitPayload;
pub use message::{Body, Event, Init, Message};
use tokio::sync::{Mutex, RwLock};

pub mod message;
pub mod rpc;

pub trait Handler {
    fn can_handle(&self, json: &serde_json::Value) -> bool;
    fn step(&mut self, json: serde_json::Value, ctx: Context) -> anyhow::Result<()>;
}

pub trait Node<S, Payload, InjectedPayload = ()> {
    fn from_init(
        state: S,
        init: Init,
        inject: Sender<ToEvent<InjectedPayload>>,
    ) -> anyhow::Result<Self>
    where
        Self: Sized;

    fn step(
        &mut self,
        input: Event<Payload, InjectedPayload>,
        output: Context,
    ) -> anyhow::Result<()>;
}

pub enum ToEvent<InjectedPayload = ()> {
    Message(serde_json::Value),
    Injected(InjectedPayload),
    Eof,
}

impl<IP> ToEvent<IP> {
    pub fn to_event<Payload>(&self) -> anyhow::Result<Event<Payload, IP>>
    where
        Payload: DeserializeOwned,
        IP: Clone,
    {
        match self {
            ToEvent::Message(e) => Ok(Event::Message(serde_json::from_value(e.clone())?)),
            ToEvent::Injected(i) => Ok(Event::Injected(i.clone())),
            ToEvent::Eof => Ok(Event::Eof),
        }
    }
}

pub struct RpcRegistry {
    registry: HashMap<TypeId, Box<dyn Handler>>,
}

#[derive(Clone)]
pub struct Context<'a> {
    stdout: Rc<RefCell<StdoutLock<'a>>>,
    registry: Rc<RefCell<RpcRegistry>>,
}

impl Context<'_> {
    pub fn send<S>(&self, s: S) -> anyhow::Result<()>
    where
        S: Serialize,
    {
        let mut stdout = self.stdout.borrow_mut();
        serde_json::to_writer(&mut *stdout, &s).context("serialize message to send")?;
        (&mut stdout)
            .write_all(b"\n")
            .context("write newline to output")?;
        Ok(())
    }
}

pub struct Runtime<S, P, IP, N>
where
    N: Node<S, P, IP>,
{
    /// The current msg_id to use for the next message
    msg_id: Arc<AtomicU64>,

    /// The stdout to use for sending messages
    stdout: Arc<Mutex<Stdout>>,

    /// The node to use to handle messages
    handler: Arc<N>,

    node_state: RwLock<NodeState>,

    /// Need to keep track of types, but don't need to use them outside of N
    _phantom: PhantomData<(S, P, IP)>,
}

impl<S, P, IP, N> Runtime<S, P, IP, N>
where
    P: DeserializeOwned + Send + 'static,
    N: Node<S, P, IP>,
    IP: Clone + Send + 'static,
{
    pub async fn run(init_state: S) -> anyhow::Result<()> {
        let (tx, rx): (Sender<ToEvent<IP>>, Receiver<ToEvent<IP>>) = std::sync::mpsc::channel();

        let stdin = std::io::stdin().lock();
        let mut stdin = stdin.lines();
        let mut stdout = std::io::stdout().lock();

        let mut node: N = Self::init_node(&mut stdin, init_state, &tx, &mut stdout)?;

        let stdin_tx = tx.clone();
        drop(stdin);
        let handle = thread::spawn(move || {
            let stdin = std::io::stdin().lock();
            for line in stdin.lines() {
                let line = line.context("Maestrom input from STDIN could not be deserialized")?;
                let input = serde_json::from_str::<serde_json::Value>(&line)
                    .context("read input message from STDIN")?;
                if let Err(_) = stdin_tx.send(ToEvent::Message(input)) {
                    break;
                }
            }
            let _ = tx.send(ToEvent::Eof);

            Ok::<_, anyhow::Error>(())
        });
        let stdout = Rc::new(RefCell::new(stdout));
        let registry = Rc::new(RefCell::new(RpcRegistry {
            registry: HashMap::new(),
        }));
        let context = Context {
            registry: registry.clone(),
            stdout,
        };

        for input in rx {
            if let Ok(input) = input.to_event() {
                node.step(input, context.clone())
                    .context("Node step function failed")?;
            } else {
                let ToEvent::Message(message) = input else {
                    panic!("Impossible position");
                };

                // TODO: problem, how can I prevent registry from getting accessed in rpc handlers?
                for handler in registry.borrow_mut().registry.values_mut() {
                    if handler.can_handle(&message) {
                        handler
                            .step(message, context.clone())
                            .context("RpcHandler failed")?;
                        break;
                    }
                }
            }
        }

        handle
            .join()
            .expect("failed to join input thread")
            .context("error from stdin thread")?;

        Ok(())
    }

    fn init_node(
        stdin: &mut Lines<StdinLock>,
        init_state: S,
        tx: &Sender<ToEvent<IP>>,
        stdout: &mut StdoutLock,
    ) -> Result<N, anyhow::Error> {
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
        let node =
            N::from_init(init_state, init, tx.clone()).context("node initialization failed")?;
        let reply = Message {
            src: init_msg.dst,
            dst: init_msg.src,
            body: Body {
                id: Some(0),
                in_reply_to: init_msg.body.id,
                payload: InitPayload::InitOk,
            },
        };
        serde_json::to_writer(&mut *stdout, &reply).context("serialize response to init")?;
        stdout.write_all(b"\n").context("write newline to output")?;
        Ok(node)
    }
}

#[derive(Debug, Clone, Default)]
pub struct NodeState {
    pub node_id: String,
    pub nodes: Vec<String>,
}
