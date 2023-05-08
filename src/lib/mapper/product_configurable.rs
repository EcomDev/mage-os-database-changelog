use crate::error::Error;
use crate::log::ProductChange;
use crate::mapper::ChangeLogMapper;
use crate::replication::Event;
use crate::schema::TableSchema;

pub struct ProductConfigurableMapper;

impl ChangeLogMapper<ProductChange> for ProductConfigurableMapper {
    fn map_event(
        &self,
        event: &Event,
        schema: &impl TableSchema,
    ) -> Result<Option<ProductChange>, Error> {
        Ok(match event {
            Event::InsertRow(row) | Event::DeleteRow(row) => Some(
                ProductChange::CompositeRelation(row.parse("parent_id", schema)?),
            ),
            _ => None,
        })
    }
}
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn maps_insert_and_delete_as_composite_product_change() {
        mapper_test!(
            ProductConfigurableMapper,
            Some(ProductChange::CompositeRelation(2)),
            insert[2, 4],
            ["parent_id", "product_id"]
        );

        mapper_test!(
            ProductConfigurableMapper,
            Some(ProductChange::CompositeRelation(4)),
            insert[4, 5],
            ["parent_id", "product_id"]
        );
    }

    #[test]
    fn ignores_update_event_as_change() {
        mapper_test!(
            ProductConfigurableMapper,
            None,
            update[(1, 2), (2, 3)],
            ["parent_id", "product_id"]
        );
    }
}
