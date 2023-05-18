use super::EventObserver;
use crate::error::Error;
use crate::replication::{Event, EventMetadata};
use crate::schema::TableSchema;

pub trait FilterObserverPredicate: Sized {
    fn is_applicable(&self, table: &impl TableSchema) -> bool;
}

impl<T> FilterObserverPredicate for T
where
    T: AsRef<str>,
{
    fn is_applicable(&self, schema: &impl TableSchema) -> bool {
        schema.table_name().eq(self.as_ref())
    }
}

pub struct FilterObserver<P, O> {
    predicate: P,
    observer: O,
}

impl<P, O> FilterObserver<P, O>
where
    P: FilterObserverPredicate,
    O: EventObserver,
{
    pub fn new(predicate: P, observer: O) -> Self {
        Self {
            predicate,
            observer,
        }
    }
}

impl<P, O> EventObserver for FilterObserver<P, O>
where
    P: FilterObserverPredicate,
    O: EventObserver,
{
    async fn process_event(&self, event: &Event, table: &impl TableSchema) -> Result<(), Error> {
        if !self.predicate.is_applicable(table) {
            return Ok(());
        }

        self.observer.process_event(event, table).await
    }

    async fn process_metadata(&self, metadata: &EventMetadata) -> Result<(), Error> {
        self.observer.process_metadata(metadata).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::replication::BinlogPosition;
    use crate::replication::Event;
    use crate::replication::EventObserverExt;
    use crate::test_util::*;

    #[tokio::test]
    async fn executes_observer_when_table_name_matches() -> Result<(), Error> {
        let first = ObserverSpy::default();
        let observer = first.clone().filter("entity");

        process_event!(
            observer,
            test_table!("entity"),
            [
                Event::InsertRow(binlog_row!(1)),
                Event::InsertRow(binlog_row!(2))
            ]
        );

        assert_eq!(first.processed_event_count(), 2);
        Ok(())
    }

    #[tokio::test]
    async fn skips_observer_when_table_name_is_different() -> Result<(), Error> {
        let first = ObserverSpy::default();
        let observer = first.clone().filter("table");

        process_event!(
            observer,
            test_table!("entity"),
            [
                Event::InsertRow(binlog_row!(1)),
                Event::InsertRow(binlog_row!(2))
            ]
        );

        assert_eq!(first.processed_event_count(), 0);
        Ok(())
    }

    #[tokio::test]
    async fn passes_metadata_event_to_underlying_observer() {
        let observer = ObserverSpy::default();

        observer
            .clone()
            .filter("something")
            .process_metadata(&EventMetadata::new(10, BinlogPosition::new("", 0)))
            .await
            .unwrap();

        assert_eq!(observer.metadata().len(), 1)
    }
}
