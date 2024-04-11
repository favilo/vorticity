use miette::Diagnostic;
use thiserror::Error;

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Error, Debug, Diagnostic)]
pub enum Error {
    #[error("error: {0}")]
    #[diagnostic(transparent)]
    Miette(#[from] Box<dyn miette::Diagnostic + Send + Sync + 'static>),

    #[error("Json error: {0}")]
    Json(#[from] serde_json::Error),

    #[error("i/o error: {0}")]
    Io(#[from] std::io::Error),

    #[error("Send error: {0}")]
    SendError(#[from] std::sync::mpsc::SendError<()>),
}

#[derive(Error, Debug, Diagnostic)]
#[error("join error")]
pub struct JoinError;

impl From<miette::Report> for Error {
    fn from(value: miette::Report) -> Self {
        Self::Miette(value.into())
    }
}
