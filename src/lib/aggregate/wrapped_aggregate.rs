use crate::aggregate::{Aggregate, AsyncAggregate};
use crate::error::Error;
use crate::log::ItemChange;
use crate::output::Output;

use std::time::Duration;
use tokio::io::AsyncWrite;
use tokio::time::Instant;

pub struct WrappedAggregate<A> {
    inner: A,
    last_flush: Option<Instant>,
}

impl<A> WrappedAggregate<A>
where
    A: Aggregate,
{
    pub fn new(inner: A) -> Self {
        Self {
            inner,
            last_flush: None,
        }
    }
}

impl<A> AsyncAggregate for WrappedAggregate<A>
where
    A: Aggregate,
{
    fn push(&mut self, item: impl Into<ItemChange>) {
        self.inner.push(item);
    }

    async fn write(
        &mut self,
        (max_size, duration): &(usize, Duration),
        output: &impl Output,
        mut write: impl AsyncWrite + Unpin,
    ) -> Result<(), Error> {
        let last_flush = match self.last_flush {
            None => Instant::now(),
            Some(item) => item,
        };

        if self.inner.size() < *max_size && last_flush.elapsed() < *duration {
            self.last_flush = Some(last_flush);
            return Ok(());
        }

        if let Some(item) = self.inner.flush() {
            output.write(&mut write, item).await?;
        }

        self.last_flush = Some(Instant::now());

        Ok(())
    }

    async fn write_eof(
        &mut self,
        output: &impl Output,
        mut write: impl AsyncWrite + Unpin,
    ) -> Result<(), Error> {
        if let Some(item) = self.inner.flush() {
            output.write(&mut write, item).await?;
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::aggregate::{ChangeAggregate, ChangeAggregateEntity};
    use crate::log::ProductChange;
    use crate::output::JsonOutput;
    use crate::replication::{BinlogPosition, EventMetadata};
    use std::io::Cursor;

    #[derive(Default)]
    struct SpyAggregate {
        changes: Vec<ItemChange>,
        flushed: usize,
    }

    impl Aggregate for SpyAggregate {
        fn push(&mut self, item: impl Into<ItemChange>) {
            self.changes.push(item.into());
        }

        fn size(&self) -> usize {
            self.changes.len()
        }

        fn flush(&mut self) -> Option<ChangeAggregate> {
            self.flushed += 1;
            self.changes.clear();
            Some(ChangeAggregate::new(
                ChangeAggregateEntity::Product,
                EventMetadata::new(self.flushed, BinlogPosition::new("file", 0)),
            ))
        }
    }

    #[test]
    fn pushes_items_into_underlying_aggregate() {
        let mut aggregate = WrappedAggregate::new(SpyAggregate::default());
        aggregate.push(ItemChange::ProductChange(ProductChange::Created(1)));
        aggregate.push(ItemChange::ProductChange(ProductChange::Created(2)));
        aggregate.push(ItemChange::ProductChange(ProductChange::Created(3)));

        assert_eq!(
            aggregate.inner.changes,
            vec![
                ItemChange::ProductChange(ProductChange::Created(1)),
                ItemChange::ProductChange(ProductChange::Created(2)),
                ItemChange::ProductChange(ProductChange::Created(3)),
            ]
        );
    }

    #[tokio::test]
    async fn does_not_flush_when_limit_not_reached() {
        let mut aggregate = WrappedAggregate::new(SpyAggregate::default());
        aggregate.push(ItemChange::ProductChange(ProductChange::Created(1)));
        aggregate.push(ItemChange::ProductChange(ProductChange::Created(2)));
        aggregate.push(ItemChange::ProductChange(ProductChange::Created(3)));

        aggregate
            .write(
                &(5, Duration::from_secs(2)),
                &JsonOutput,
                Cursor::new(Vec::<u8>::new()),
            )
            .await
            .unwrap();

        assert_eq!(aggregate.inner.flushed, 0);
    }

    #[tokio::test]
    async fn does_flushes_aggregate_when_size_limit_is_reached() {
        let mut aggregate = WrappedAggregate::new(SpyAggregate::default());
        aggregate.push(ItemChange::ProductChange(ProductChange::Created(1)));
        aggregate.push(ItemChange::ProductChange(ProductChange::Created(2)));
        aggregate.push(ItemChange::ProductChange(ProductChange::Created(3)));
        aggregate.push(ItemChange::ProductChange(ProductChange::Created(4)));

        aggregate
            .write(
                &(2, Duration::from_secs(2)),
                &JsonOutput,
                Cursor::new(Vec::<u8>::new()),
            )
            .await
            .unwrap();

        assert_eq!(aggregate.inner.flushed, 1);
    }

    #[tokio::test]
    async fn outputs_event_into_writer() {
        let mut aggregate = WrappedAggregate::new(SpyAggregate::default());
        aggregate.push(ItemChange::ProductChange(ProductChange::Created(1)));
        aggregate.push(ItemChange::ProductChange(ProductChange::Created(3)));

        let mut output = Cursor::new(Vec::<u8>::new());
        aggregate
            .write(&(2, Duration::from_secs(2)), &JsonOutput, &mut output)
            .await
            .unwrap();

        assert!(output.into_inner().len() > 0);
    }

    #[tokio::test(start_paused = true)]
    async fn does_flushes_aggregate_when_time_limit_elapsed() {
        let mut aggregate = WrappedAggregate::new(SpyAggregate::default());
        aggregate.push(ItemChange::ProductChange(ProductChange::Created(1)));

        aggregate
            .write(
                &(5, Duration::from_secs(2)),
                &JsonOutput,
                Cursor::new(Vec::<u8>::new()),
            )
            .await
            .unwrap();

        tokio::time::advance(Duration::from_secs(3)).await;

        aggregate
            .write(
                &(5, Duration::from_secs(2)),
                &JsonOutput,
                Cursor::new(Vec::<u8>::new()),
            )
            .await
            .unwrap();

        assert_eq!(aggregate.inner.flushed, 1);
    }
}
