use crate::error::Error;
use crate::log::ProductChange;
use crate::mapper::ChangeLogMapper;
use crate::replication::Event;
use crate::schema::TableSchema;

pub struct ProductAttributeMapper;

impl ChangeLogMapper<ProductChange> for ProductAttributeMapper {
    fn map_event(
        &self,
        event: &Event,
        schema: &impl TableSchema,
    ) -> Result<Option<ProductChange>, Error> {
        Ok(match event {
            Event::InsertRow(row) | Event::DeleteRow(row) => Some(ProductChange::Attribute(
                row.parse("entity_id", schema)?,
                row.parse("attribute_id", schema)?,
            )),
            Event::UpdateRow(row)
                if row.is_changed_column("store_id", schema)
                    || row.is_changed_column("value", schema) =>
            {
                Some(ProductChange::Attribute(
                    row.parse("entity_id", schema)?,
                    row.parse("attribute_id", schema)?,
                ))
            }
            _ => None,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn maps_insert_row_event_as_update_of_attribute() {
        mapper_test!(
            ProductAttributeMapper,
            Some(ProductChange::Attribute(1, 2)),
            insert[1, 2, 3, "test"],
            ["entity_id", "attribute_id", "store_id", "value"]
        );
    }

    #[test]
    fn maps_delete_row_event_as_update_of_attribute() {
        mapper_test!(
            ProductAttributeMapper,
            Some(ProductChange::Attribute(2, 4)),
            delete[2, 4, 3, "test"],
            ["entity_id", "attribute_id", "store_id", "value"]
        );
    }

    #[test]
    fn maps_update_event_to_none_if_not_important_columns_stayed_the_same() {
        mapper_test!(
            ProductAttributeMapper,
            None,
            update[(1, 2, 4, 3, "test"), (2, 2, 4, 3, "test")],
            ["value_id", "entity_id", "attribute_id", "store_id", "value"]
        );
    }

    #[test]
    fn maps_update_event_to_product_attribute_update_if_value_changed() {
        mapper_test!(
            ProductAttributeMapper,
            Some(ProductChange::Attribute(2, 4)),
            update[(2, 4, 1, "test"), (2, 4, 1, "test2")],
            ["entity_id", "attribute_id", "store_id", "value"]
        );
    }

    #[test]
    fn maps_update_event_to_product_attribute_update_if_store_id_changed() {
        mapper_test!(
            ProductAttributeMapper,
            Some(ProductChange::Attribute(2, 4)),
            update[(2, 4, 1, "test"), (2, 4, 3, "test")],
            ["entity_id", "attribute_id", "store_id", "value"]
        );
    }
}
