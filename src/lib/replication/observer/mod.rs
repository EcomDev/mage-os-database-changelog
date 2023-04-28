#[macro_use]
mod macros;
mod chain_observer;
mod filter_observer;

use crate::error::Error;
use crate::replication::Event;
use crate::schema::TableSchema;
use chain_observer::ChainObserver;
use filter_observer::{FilterObserver, FilterObserverPredicate};

pub trait EventObserver: Sized {
    async fn process_event(&self, event: &Event, table: &impl TableSchema) -> Result<(), Error>;

    fn with<R>(self, observer: R) -> ChainObserver<Self, R>
    where
        R: EventObserver,
    {
        ChainObserver::new(self, observer)
    }

    fn filter<P>(self, predicate: P) -> FilterObserver<P, Self>
    where
        P: FilterObserverPredicate,
    {
        FilterObserver::new(predicate, self)
    }
}
