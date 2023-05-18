use crate::error::Error;
use crate::log::{ChangeLogSender, ItemChange};

use crate::mapper::ChangeLogMapper;
use crate::replication::{Event, EventMetadata, EventObserver};
use crate::schema::TableSchema;

pub struct MapperObserver<M, S> {
    mapper: M,
    sender: S,
}

impl<M, S> From<(M, S)> for MapperObserver<M, S>
where
    M: ChangeLogMapper<ItemChange>,
    S: ChangeLogSender<Item = ItemChange>,
{
    fn from((mapper, sender): (M, S)) -> Self {
        MapperObserver { mapper, sender }
    }
}

impl<M, S> EventObserver for MapperObserver<M, S>
where
    M: ChangeLogMapper<ItemChange>,
    S: ChangeLogSender<Item = ItemChange>,
{
    async fn process_event(&self, event: &Event, table: &impl TableSchema) -> Result<(), Error> {
        match self.mapper.map_event(event, table)? {
            Some(change) => self.sender.send(change).await?,
            None => (),
        }

        Ok(())
    }

    async fn process_metadata(&self, metadata: &EventMetadata) -> Result<(), Error> {
        self.sender.send(metadata.clone().into()).await
    }
}
