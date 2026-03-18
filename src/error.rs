use std::io;

use thiserror::Error;

#[derive(Debug, Error)]
pub enum GraniteError {
    #[error("io error: {0}")]
    Io(#[from] io::Error),

    #[error("corrupt data: {0}")]
    Corrupt(&'static str),

    #[error("invalid argument: {0}")]
    InvalidArgument(&'static str),
}

pub type Result<T> = std::result::Result<T, GraniteError>;
