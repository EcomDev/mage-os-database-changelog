use crate::error::Error;
use crate::log::ProductChange;
use crate::mapper::ChangeLogMapper;
use crate::replication::Event;
use crate::schema::TableSchema;

pub struct ProductCategoryMapper;

impl ChangeLogMapper<ProductChange> for ProductCategoryMapper {
    fn map_event(
        &self,
        event: &Event,
        schema: &impl TableSchema,
    ) -> Result<Option<ProductChange>, Error> {
        Ok(match event {
            Event::DeleteRow(row) | Event::InsertRow(row) => Some(ProductChange::Category(
                row.parse("product_id", schema)?,
                row.parse("category_id", schema)?,
            )),
            _ => None,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::log::ProductChange;

    #[test]
    fn maps_delete_event_into_updated_category() {
        mapper_test!(
            ProductCategoryMapper,
            Some(ProductChange::Category(1, 4)),
            delete[1, 4],
            ["product_id", "category_id"]
        );
    }

    #[test]
    fn maps_insert_event_into_updated_category() {
        mapper_test!(
            ProductCategoryMapper,
            Some(ProductChange::Category(2, 4)),
            insert[2, 4],
            ["product_id", "category_id"]
        );
    }

    #[test]
    fn ignores_update_event_as_it_has_no_meaning() {
        mapper_test!(
            ProductCategoryMapper,
            None,
            update[(2, 4), (2, 4)],
            ["product_id", "category_id"]
        );
    }
}
