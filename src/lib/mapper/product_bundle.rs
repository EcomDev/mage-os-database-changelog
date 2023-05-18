use crate::error::Error;
use crate::log::ProductChange;
use crate::mapper::ChangeLogMapper;
use crate::replication::Event;
use crate::schema::TableSchema;

pub struct ProductBundleMapper;

impl ChangeLogMapper<ProductChange> for ProductBundleMapper {
    fn map_event(
        &self,
        event: &Event,
        schema: &impl TableSchema,
    ) -> Result<Option<ProductChange>, Error> {
        Ok(match event {
            Event::InsertRow(row) | Event::DeleteRow(row) => Some(
                ProductChange::CompositeRelation(row.parse("parent_product_id", schema)?),
            ),
            Event::UpdateRow(row)
                if row.is_changed_column("is_default", schema)
                    | row.is_changed_column("selection_price_type", schema)
                    | row.is_changed_column("selection_price_value", schema)
                    | row.is_changed_column("selection_qty", schema) =>
            {
                Some(ProductChange::CompositeRelation(
                    row.parse("parent_product_id", schema)?,
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
    fn maps_insert_and_delete_into_composite_product_change() {
        mapper_test!(
            ProductBundleMapper,
            Some(
                ProductChange::CompositeRelation(3)
            ),
            insert[1, 3, 4],
            ["selection_id", "parent_product_id", "product_id"]
        );

        mapper_test!(
            ProductBundleMapper,
            Some(
                ProductChange::CompositeRelation(2)
            ),
            delete[1, 2, 4],
            ["selection_id", "parent_product_id", "product_id"]
        );
    }

    #[test]
    fn maps_update_of_is_default_property_into_composite_product_change() {
        mapper_test!(
            ProductBundleMapper,
            Some(
                ProductChange::CompositeRelation(3)
            ),
            update[(3, 1), (3, 0)],
            ["parent_product_id", "is_default"]
        );
    }

    #[test]
    fn maps_update_of_selection_price_type_property_into_composite_product_change() {
        mapper_test!(
            ProductBundleMapper,
            Some(
                ProductChange::CompositeRelation(3)
            ),
            update[(3, 1), (3, 0)],
            ["parent_product_id", "selection_price_type"]
        );
    }

    #[test]
    fn maps_update_of_selection_qty_property_into_composite_product_change() {
        mapper_test!(
            ProductBundleMapper,
            Some(
                ProductChange::CompositeRelation(3)
            ),
            update[(3, 2), (3, 4)],
            ["parent_product_id", "selection_qty"]
        );
    }

    #[test]
    fn maps_update_of_selection_price_value_property_into_composite_product_change() {
        mapper_test!(
            ProductBundleMapper,
            Some(
                ProductChange::CompositeRelation(3)
            ),
            update[(3, "10.00"), (3, "99.00")],
            ["parent_product_id", "selection_price_value"]
        );
    }

    #[test]
    fn maps_update_of_unrelated_property_into_none() {
        mapper_test!(
            ProductBundleMapper,
            None,
            update[(3, 1, 1), (3, 0, 0)],
            ["parent_product_id", "position", "selection_can_change_qty"]
        );
    }
}
