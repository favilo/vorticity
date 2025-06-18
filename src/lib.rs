use std::{
    any::TypeId,
    collections::HashMap,
    io::{BufRead, Write},
    rc::Rc,
    sync::{
        atomic::AtomicUsize,
        mpsc::{Receiver, Sender},
        Arc,
    },
    thread,
};

use erased_serde::Serialize;
use miette::Context as _;
use serde::{de::DeserializeOwned, Deserialize};

use error::{Error, Result};
pub use message::{Body, Context, Event, Init, Message};
use message::{InitPayload, ToEvent};

pub mod error;
pub mod message;
pub mod rpc;

pub trait Handler: downcast::Any {
    fn can_handle(&self, json: &ToEvent) -> bool;
    fn step(&self, json: ToEvent, ctx: Context) -> Result<()>;
}

downcast::downcast!(dyn Handler);

pub trait Node<S, Payload, InjectedPayload = ()> {
    fn init(runtime: &Runtime, state: S, context: Context) -> Result<Self>
    where
        Self: Sized;

    fn step(&mut self, input: Event<Payload, InjectedPayload>, context: Context) -> Result<()>;

    fn handle_reply(
        &mut self,
        input: Event<Payload, InjectedPayload>,
        output: Context,
    ) -> Result<()> {
        self.step(input, output)
    }
}

#[derive(Default)]
pub struct Runtime {
    handlers: HashMap<TypeId, Rc<dyn Handler>>,
}

impl Runtime {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_handler<H>(&mut self, handler: H) -> &mut Self
    where
        H: Handler + 'static,
    {
        self.handlers.insert(TypeId::of::<H>(), Rc::new(handler));
        self
    }

    pub fn get_handler<H>(&self) -> Option<Rc<dyn Handler>>
    where
        H: Handler + 'static,
    {
        let get = self.handlers.get(&TypeId::of::<H>());
        get.map(Rc::clone)
    }

    pub fn run<S, P, IP, N>(self, init_state: S) -> Result<()>
    where
        P: DeserializeOwned + Send + 'static,
        N: Node<S, P, IP>,
        IP: DeserializeOwned + Send + 'static,
    {
        let (msg_in_tx, msg_in_rx): (Sender<ToEvent>, Receiver<ToEvent>) =
            std::sync::mpsc::channel();

        let (msg_out_tx, msg_out_rx) = std::sync::mpsc::channel();

        let (node, context): (N, Context) =
            self.init_node(init_state, msg_in_tx.clone(), msg_out_tx.clone())?;
        let node = node;

        std::thread::scope(|scope| -> Result<()> {
            let stdin_tx = msg_in_tx.clone();
            receive_loop(scope, stdin_tx, msg_in_tx);

            send_loop(scope, msg_out_rx);
            event_loop(&self, msg_in_rx, node, context)
        })?;

        Ok(())
    }

    fn init_node<S, P, IP, N>(
        &self,
        init_state: S,
        msg_in_tx: Sender<ToEvent>,
        msg_out_tx: Sender<Box<dyn Serialize + Send + Sync>>,
    ) -> Result<(N, Context)>
    where
        P: DeserializeOwned + Send + 'static,
        N: Node<S, P, IP>,
        IP: DeserializeOwned + Send + 'static,
    {
        let stdin = std::io::stdin().lock();
        let mut stdin = stdin.lines();
        let init_msg: Message<InitPayload> = serde_json::from_str::<Message<InitPayload>>(
            &stdin.next().expect("no init message received")?,
        )?;
        let InitPayload::Init(ref init) = init_msg.body().payload else {
            panic!("first message should be init")
        };

        let context = Context::new(
            &init.node_id,
            &init.node_ids,
            msg_in_tx,
            msg_out_tx,
            Arc::new(AtomicUsize::new(0)),
        );
        let node =
            N::init(self, init_state, context.clone()).context("node initialization failed")?;
        let reply = context.construct_reply(&init_msg, InitPayload::InitOk);

        context.send(reply)?;
        Ok((node, context))
    }
}

#[allow(dead_code)]
fn rpc_loop<P>(
    _rpc_in_rx: Receiver<Message<P>>,
    _msg_out_tx: Sender<Box<dyn Serialize + Send + Sync>>,
) -> thread::JoinHandle<Result<()>>
where
    P: Clone + Send + 'static,
{
    thread::spawn(|| {
        todo!("Figure out how to extract this from the indvidual nodes");

        #[allow(unreachable_code)]
        Ok(())
    })
}

fn receive_loop<'s>(
    scope: &'s thread::Scope<'s, '_>,
    stdin_tx: Sender<ToEvent>,
    msg_in_tx: Sender<ToEvent>,
) -> thread::ScopedJoinHandle<'s, Result<()>> {
    scope.spawn(move || {
        let stdin = std::io::stdin().lock();
        for line in stdin.lines() {
            let line = line?;
            let input: Message<serde_json::Value> = serde_json::from_str(&line)?;
            if stdin_tx.send(ToEvent::Message(input)).is_err() {
                break;
            }
        }
        let _ = msg_in_tx.send(ToEvent::Eof);

        Ok(())
    })
}

fn send_loop<'s>(
    scope: &'s thread::Scope<'s, '_>,
    msg_out_rx: Receiver<Box<dyn Serialize + Send + Sync>>,
) -> thread::ScopedJoinHandle<'s, Result<()>> {
    scope.spawn(move || {
        let mut stdout = std::io::stdout().lock();
        for send_msg in msg_out_rx {
            serde_json::to_writer(&mut stdout, &send_msg)?;
            stdout.write_all(b"\n")?;
        }
        Ok(())
    })
}

fn event_loop<N, S, P, IP>(
    runtime: &Runtime,
    msg_in_rx: Receiver<ToEvent>,
    mut node: N,
    context: Context,
) -> Result<()>
where
    N: Node<S, P, IP>,
    P: for<'de> Deserialize<'de> + Send + 'static,
    IP: for<'de> Deserialize<'de> + Send + 'static,
{
    for input in msg_in_rx {
        if let Ok(input) = Event::<P, IP>::try_from(input.clone()) {
            if input.is_reply() {
                // TODO: Figure out how to get original Message from our RPC system
                node.handle_reply(input, context.clone())?;
                continue;
            }
            node.step(input, context.clone())?;
        } else {
            let handler = runtime.handlers.values().find(|h| h.can_handle(&input));
            let Some(handler) = handler else {
                return Err(Error::NoHandler(input));
            };
            handler.step(input, context.clone())?;
        }
    }

    Ok(())
}
