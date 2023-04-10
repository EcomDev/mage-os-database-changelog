use mysql_async::binlog::events::{BinlogEventHeader, RotateEvent, RowsEvent, RowsEventRows, TableMapEvent};
use mysql_async::binlog::EventType;
use error::ReplicationError;
mod error;
mod reader;
mod rows;

pub use reader::ReplicationReader;

pub trait ReplicationObserver {

}

