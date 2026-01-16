use thiserror::Error;

#[derive(Debug, Error)]
pub enum Error {
    #[error("ERR wrong number of arguments for '{0}' command")]
    WrongArity(&'static str),

    #[error("ERR value is not an integer or out of range")]
    NotInteger,

    #[error("ERR value is not a valid float")]
    NotFloat,

    #[error("ERR invalid expire time")]
    InvalidExpireTime,

    #[error("ERR syntax error")]
    Syntax,

    #[error("WRONGTYPE Operation against a key holding the wrong kind of value")]
    WrongType,

    #[error("ERR increment or decrement would overflow")]
    Overflow,

    #[error("ERR no such key")]
    NoSuchKey,

    #[error("ERR index out of range")]
    IndexOutOfRange,

    #[error("ERR {0}")]
    Other(&'static str),

    #[error("ERR unknown command '{0}'")]
    UnknownCommand(String),

    #[error("ERR Protocol error: {0}")]
    Protocol(String),

    // JSON-specific errors
    #[error("ERR could not perform this operation on a key that doesn't exist")]
    JsonKeyNotFound,

    #[error("ERR new objects must be created at the root")]
    JsonNewAtRoot,

    #[error("ERR Path '{0}' does not exist")]
    JsonPathNotFound(String),

    #[error("ERR expected {0} but found {1}")]
    JsonTypeMismatch(&'static str, &'static str),

    #[error("ERR JSON parse error")]
    JsonParse,

    #[error("ERR invalid JSONPath")]
    JsonPathInvalid,

    #[error("{0}")]
    Custom(String),

    // Stream-specific errors
    #[error("{0}")]
    Stream(String),

    #[error(transparent)]
    Io(#[from] std::io::Error),
}

pub type Result<T> = std::result::Result<T, Error>;
