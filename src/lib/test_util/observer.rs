use crate::error::Error;
use crate::replication::Event;
use crate::replication::EventObserver;
use crate::schema::TableSchema;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

#[derive(Default, Clone)]
pub struct ObserverSpy {
    times_executed: Arc<AtomicUsize>,
}

impl ObserverSpy {
    pub fn times_executed(&self) -> usize {
        self.times_executed.load(Ordering::Relaxed)
    }
}

impl EventObserver for ObserverSpy {
    async fn process_event(&self, _event: &Event, _table: &impl TableSchema) -> Result<(), Error> {
        self.times_executed.fetch_add(1, Ordering::Relaxed);
        Ok(())
    }
}
