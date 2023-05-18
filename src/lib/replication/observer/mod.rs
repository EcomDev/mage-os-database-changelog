#[macro_use]
mod macros;
mod chain_observer;
mod filter_observer;

use crate::error::Error;

use crate::replication::{Event, EventMetadata};
use crate::schema::TableSchema;
use chain_observer::ChainObserver;
use filter_observer::{FilterObserver, FilterObserverPredicate};

pub trait ChangeLogEventObserver: Sized {
    async fn process_event(&self, event: &Event, table: &impl TableSchema) -> Result<(), Error>;
}

pub trait EventObserver: Sized {
    async fn process_event(&self, event: &Event, table: &impl TableSchema) -> Result<(), Error>;

    async fn process_metadata(&self, metadata: &EventMetadata) -> Result<(), Error>;
}

pub trait EventObserverExt: Sized {
    fn with<R>(self, observer: R) -> ChainObserver<Self, R>
    where
        R: EventObserver,
        Self: EventObserver,
    {
        ChainObserver::new(self, observer)
    }

    fn filter<P>(self, predicate: P) -> FilterObserver<P, Self>
    where
        P: FilterObserverPredicate,
        Self: EventObserver,
    {
        FilterObserver::new(predicate, self)
    }
}

impl<T> EventObserverExt for T where T: EventObserver {}

impl<T> EventObserver for T
where
    T: ChangeLogEventObserver,
{
    async fn process_event(&self, event: &Event, table: &impl TableSchema) -> Result<(), Error> {
        ChangeLogEventObserver::process_event(self, event, table).await
    }

    async fn process_metadata(&self, _metadata: &EventMetadata) -> Result<(), Error> {
        Ok(())
    }
}
