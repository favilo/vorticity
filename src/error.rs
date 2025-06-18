use miette::Diagnostic;
use serde_json::Value;
use thiserror::Error;

use crate::{message::ToEvent, Message};

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Error, Debug, Diagnostic)]
pub enum Error {
    #[error(transparent)]
    #[diagnostic(transparent)]
    Miette(#[from] Box<dyn miette::Diagnostic + Send + Sync + 'static>),

    #[error("Json error: {0}")]
    Json(#[from] serde_json::Error),

    #[error("i/o error: {0}")]
    Io(#[from] std::io::Error),

    #[error("Send error: {0}")]
    SendError(#[from] std::sync::mpsc::SendError<()>),

    #[error("Not a reply: {0:?}")]
    NotReply(ToEvent),

    #[error("Wrong Event type: {0:?}")]
    WrongEvent(ToEvent),

    #[error("No handler registered for type: {0:?}")]
    NoHandler(ToEvent),

    #[error("No callback registered for message: {0:?}")]
    NoCallback(Message<Value>),

    #[error("Not able to downcast handler")]
    Downcast,

    #[error("Not able to apply update: {0}")]
    YrsUpdate(#[from] yrs::error::UpdateError),
}

impl From<miette::Report> for Error {
    fn from(value: miette::Report) -> Self {
        Self::Miette(value.into())
    }
}
