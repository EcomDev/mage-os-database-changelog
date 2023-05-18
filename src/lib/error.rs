use mysql_async::Error as MySQLError;
use mysql_common::value::Value;

use thiserror::Error;

#[derive(Error, Debug)]
pub enum Error {
    #[error(transparent)]
    MySQLError(#[from] MySQLError),
    #[error(transparent)]
    Io(#[from] std::io::Error),
    #[error(transparent)]
    Json(#[from] serde_json::error::Error),
    #[error("Cannot find value for {0} column")]
    ColumnNotFound(String),
    #[error("Cannot parse {0:?} value as {1} in {2} column")]
    ColumnParseError(Value, &'static str, String),
    #[error("Unsupported value in column {1} for {0} type")]
    ColumnNotSupported(&'static str, String),
    #[error("Output cannot be generated from aggregate")]
    OutputError,
    #[error("Failed to synchronize data between threads")]
    Synchronization,
    #[error("Binlog position cannot be found in database")]
    BinlogPositionMissing,
}
