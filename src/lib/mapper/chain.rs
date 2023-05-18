use crate::error::Error;
use crate::log::ItemChange;
use crate::mapper::ChangeLogMapper;
use crate::replication::Event;
use crate::schema::TableSchema;
use std::marker::PhantomData;

pub struct ChainMapper<L, R>(L, R);

impl<L, R> ChainMapper<L, R>
where
    L: ChangeLogMapper<ItemChange>,
    R: ChangeLogMapper<ItemChange>,
{
    pub fn new(left: L, right: R) -> Self {
        Self(left, right)
    }
}

impl<L, R> ChangeLogMapper<ItemChange> for ChainMapper<L, R>
where
    L: ChangeLogMapper<ItemChange>,
    R: ChangeLogMapper<ItemChange>,
{
    fn map_event(
        &self,
        event: &Event,
        schema: &impl TableSchema,
    ) -> Result<Option<ItemChange>, Error> {
        if let Some(item) = self.0.map_event(event, schema)? {
            return Ok(Some(item));
        }

        if let Some(item) = self.1.map_event(event, schema)? {
            return Ok(Some(item));
        }

        Ok(None)
    }
}
