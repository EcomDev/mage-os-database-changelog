use crate::error::Error;
use crate::log::{FieldUpdate, IntoChangeLog, ProductChange};
use crate::mapper::ChangeLogMapper;
use crate::replication::Event;
use crate::schema::TableSchema;

pub struct ProductMapper;

impl ChangeLogMapper<ProductChange> for ProductMapper {
    fn map_event(
        &self,
        event: &Event,
        schema: &impl TableSchema,
    ) -> Result<Option<ProductChange>, Error> {
        Ok(match event {
            Event::InsertRow(row) => Some(ProductChange::Created(row.parse("entity_id", schema)?)),
            Event::DeleteRow(row) => Some(ProductChange::Deleted(row.parse("entity_id", schema)?)),
            Event::UpdateRow(row) => FieldUpdate::new(row.parse("entity_id", schema)?)
                .process_field_update("attribute_set_id", row, schema)
                .process_field_update("type_id", row, schema)
                .process_field_update("sku", row, schema)
                .process_field_update("has_options", row, schema)
                .process_field_update("required_options", row, schema)
                .into_change_log(),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn maps_insert_row_event_into_created_product_change() {
        mapper_test!(
            ProductMapper,
            Some(ProductChange::Created(3)),
            insert[2, 3, 4],
            ["sku", "entity_id", "type_id"]
        );
    }

    #[test]
    fn maps_delete_row_as_deleted_product() {
        mapper_test!(
            ProductMapper,
            Some(ProductChange::Deleted(3)),
            delete[2, 3, 4],
            ["sku", "entity_id", "type_id"]
        );
    }

    #[test]
    fn maps_updated_row_without_changes_in_relevant_columns_as_empty() {
        mapper_test!(
            ProductMapper,
            None,
            update[("SKU2", 3, "2023-01-01 00:00:00"), ("SKU2", 3, "2023-01-02 00:00:00")],
            ["sku", "entity_id", "updated_at"]
        );
    }

    #[test]
    fn maps_updated_row_for_fields_that_matter() {
        mapper_test!(
            ProductMapper,
            Some(ProductChange::Fields(1, vec![
               "attribute_set_id",
               "type_id",
                "sku",
                "has_options",
                "required_options"
            ].into())),
            update[
                (1, 3, "simple", "SKU1", 0, 0, "2023-01-01 00:00:00"),
                (1, 2, "bundle", "SKU3", 1, 1, "2023-01-02 00:00:00")
            ],
            [
                "entity_id",
                "attribute_set_id",
                "type_id",
                "sku",
                "has_options",
                "required_options",
                "updated_at"
            ]
        );
    }
}
