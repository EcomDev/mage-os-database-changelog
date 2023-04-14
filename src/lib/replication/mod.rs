use error::ReplicationError;
use mysql_async::binlog::events::{
    BinlogEventHeader, RotateEvent, RowsEvent, RowsEventRows, TableMapEvent,
};
use mysql_async::binlog::EventType;
mod binary_table;
mod error;
mod reader;
mod rows;

#[cfg(test)]
pub(crate) mod test_fixture;

pub const BUFFER_STACK_SIZE: usize = 64;

pub use reader::ReplicationReader;

pub trait ReplicationObserver {}
