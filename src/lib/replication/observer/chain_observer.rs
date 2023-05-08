use super::EventObserver;
use crate::error::Error;
use crate::replication::{Event, EventMetadata};
use crate::schema::TableSchema;

pub struct ChainObserver<L, R> {
    left: L,
    right: R,
}

impl<L, R> ChainObserver<L, R>
where
    L: EventObserver,
    R: EventObserver,
{
    pub fn new(left: L, right: R) -> Self {
        Self { left, right }
    }
}

impl<L, R> EventObserver for ChainObserver<L, R>
where
    L: EventObserver,
    R: EventObserver,
{
    async fn process_event(&self, event: &Event, table: &impl TableSchema) -> Result<(), Error> {
        self.left.process_event(event, table).await?;
        self.right.process_event(event, table).await?;
        Ok(())
    }

    async fn process_metadata(&self, metadata: &EventMetadata) -> Result<(), Error> {
        self.left.process_metadata(metadata).await?;
        self.right.process_metadata(metadata).await?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::replication::{
        BinlogPosition, ChangeLogEventObserver, EventMetadata, EventObserverExt,
    };
    use crate::test_util::{IntoBinlogValue, ObserverSpy};
    use std::io::ErrorKind;
    use tracing::metadata;

    struct FailureObserver;

    impl ChangeLogEventObserver for FailureObserver {
        async fn process_event(
            &self,
            _event: &Event,
            _table: &impl TableSchema,
        ) -> Result<(), Error> {
            Err(Error::Io(std::io::Error::new(
                ErrorKind::Interrupted,
                "failure of observer",
            )))
        }
    }

    #[tokio::test]
    async fn executes_both_observers_one_after_another() {
        let first = ObserverSpy::default();
        let second = ObserverSpy::default();

        let chain = ChainObserver::new(first.clone(), second.clone());

        let schema = test_table!("entity");

        chain
            .process_event(&Event::InsertRow(binlog_row!(1, 2, 3)), &schema)
            .await
            .unwrap();

        assert_eq!(first.processed_event_count(), 1);
        assert_eq!(second.processed_event_count(), 1);
    }

    #[tokio::test]
    async fn stops_execution_on_first_failed_observer() {
        let second = ObserverSpy::default();
        let chain = ChainObserver::new(FailureObserver, second.clone());

        let schema = test_table!("entity");

        chain
            .process_event(&Event::InsertRow(binlog_row!(1, 2, 3)), &schema)
            .await
            .unwrap_err();

        assert_eq!(second.processed_event_count(), 0);
    }

    #[tokio::test]
    async fn propagates_metadata_into_all_event_observers() {
        let first = ObserverSpy::default();
        let second = ObserverSpy::default();

        let metadata = EventMetadata::new(10, BinlogPosition::new("", 0));

        first
            .clone()
            .with(second.clone())
            .process_metadata(&metadata)
            .await
            .unwrap();

        assert_eq!(first.metadata().len(), 1);
        assert_eq!(second.metadata().len(), 1);
    }
}
