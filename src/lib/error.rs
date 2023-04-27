use mysql_async::Error as MySQLError;
use mysql_common::value::Value;
use std::borrow::Cow;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum Error {
    #[error(transparent)]
    MySQLError(#[from] MySQLError),
    #[error(transparent)]
    Io(#[from] std::io::Error),
    #[error("Cannot find value for {0} column")]
    ColumnNotFound(String),
    #[error("Cannot parse {0:?} value as {1} in {2} column")]
    ColumnParseError(Value, &'static str, String),
    #[error("Unsupported value in column {1} for {0} type")]
    ColumnNotSupported(&'static str, String),
}
