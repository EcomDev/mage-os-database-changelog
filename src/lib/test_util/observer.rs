use crate::error::Error;
use crate::replication::Event;
use crate::replication::{EventMetadata, EventObserver};
use crate::schema::TableSchema;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::sync::Mutex;

#[derive(Default, Clone)]
pub struct ObserverSpy {
    processed_event_count: Arc<AtomicUsize>,
    metadata: Arc<Mutex<Vec<EventMetadata>>>,
    events: Arc<Mutex<Vec<Event>>>,
}

impl ObserverSpy {
    pub fn processed_event_count(&self) -> usize {
        self.processed_event_count.load(Ordering::Relaxed)
    }

    pub fn metadata(&self) -> Vec<EventMetadata> {
        self.metadata.lock().unwrap().clone()
    }

    pub fn events(&self) -> Vec<Event> {
        self.events.lock().unwrap().clone()
    }
}

impl EventObserver for ObserverSpy {
    async fn process_event(&self, event: &Event, _table: &impl TableSchema) -> Result<(), Error> {
        self.processed_event_count.fetch_add(1, Ordering::Relaxed);
        self.events.lock().unwrap().push((*event).clone());
        Ok(())
    }

    async fn process_metadata(&self, metadata: &EventMetadata) -> Result<(), Error> {
        self.metadata.lock().unwrap().push(metadata.clone());
        Ok(())
    }
}
