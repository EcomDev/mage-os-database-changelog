#[macro_use]
mod macros;
mod chain_observer;
mod entity;

use crate::error::Error;
use crate::event::observer::chain_observer::ChainObserver;
use crate::event::Event;
use crate::TableSchema;
use mysql_common::frunk::labelled::chars::s;
use std::borrow::Cow;

pub trait EventObserver: Sized {
    async fn process_event(&self, event: &Event, table: &impl TableSchema) -> Result<(), Error>;

    fn with<R>(self, observer: R) -> ChainObserver<Self, R>
    where
        R: EventObserver,
    {
        ChainObserver::new(self, observer)
    }
}
