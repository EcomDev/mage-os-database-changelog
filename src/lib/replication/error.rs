use thiserror::Error;
use mysql_async::Error as MySQLError;

#[derive(Error, Debug)]
pub enum ReplicationError {
    #[error(transparent)]
    MySQLError(#[from]MySQLError),
    #[error(transparent)]
    Io(#[from]std::io::Error),
}