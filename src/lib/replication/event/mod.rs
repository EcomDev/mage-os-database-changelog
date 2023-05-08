use crate::replication::BinaryRow;

mod meta;
mod update_event;

pub use meta::{BinlogPosition, EventMetadata};
pub use update_event::UpdateRowEvent;

#[derive(Clone, PartialEq, Debug)]
pub enum Event {
    InsertRow(BinaryRow),
    UpdateRow(UpdateRowEvent),
    DeleteRow(BinaryRow),
}
