use crate::replication::BinaryRow;
mod observer;
mod update_event;

pub use observer::EventObserver;
pub use update_event::UpdateEvent;

pub enum Event {
    Insert(BinaryRow),
    Update(UpdateEvent),
    Delete(BinaryRow),
}
