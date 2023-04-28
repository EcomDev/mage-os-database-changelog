use crate::error::Error;
use crate::event::observer::EventObserver;
use crate::event::Event;
use crate::TableSchema;

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
}

#[cfg(test)]
mod tests {
    use crate::error::Error;
    use crate::event::observer::chain_observer::ChainObserver;
    use crate::event::observer::EventObserver;
    use crate::event::Event;
    use crate::test_util::{IntoBinlogValue, ObserverSpy, TestTableSchema};
    use crate::TableSchema;
    use std::io::ErrorKind;

    struct FailureObserver;

    impl EventObserver for FailureObserver {
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
            .process_event(&Event::Insert(binlog_row!(1, 2, 3)), &schema)
            .await
            .unwrap();

        assert_eq!(first.times_executed(), 1);
        assert_eq!(second.times_executed(), 1);
    }

    #[tokio::test]
    async fn stops_execution_on_first_failed_observer() {
        let second = ObserverSpy::default();
        let chain = ChainObserver::new(FailureObserver, second.clone());

        let schema = test_table!("entity");

        chain
            .process_event(&Event::Insert(binlog_row!(1, 2, 3)), &schema)
            .await
            .unwrap_err();

        assert_eq!(second.times_executed(), 0);
    }
}
