use crate::replication::BinaryRow;

mod update_event;

pub use update_event::UpdateEvent;

pub enum Event {
    Insert(BinaryRow),
    Update(UpdateEvent),
    Delete(BinaryRow),
}
