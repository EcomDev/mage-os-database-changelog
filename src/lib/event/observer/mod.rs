#[macro_use]
mod macros;
mod chain_observer;
mod filter_observer;
mod product_entity;

use crate::error::Error;
use crate::event::observer::chain_observer::ChainObserver;
use crate::event::observer::filter_observer::{FilterObserver, FilterObserverPredicate};
use crate::event::Event;
use crate::TableSchema;

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
