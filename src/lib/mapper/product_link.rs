use crate::error::Error;
use crate::log::ProductChange;
use crate::mapper::ChangeLogMapper;
use crate::replication::Event;
use crate::schema::TableSchema;

pub struct ProductLinkMapper;

impl ChangeLogMapper<ProductChange> for ProductLinkMapper {
    fn map_event(
        &self,
        event: &Event,
        schema: &impl TableSchema,
    ) -> Result<Option<ProductChange>, Error> {
        Ok(match event {
            Event::InsertRow(row) | Event::DeleteRow(row) => Some(ProductChange::LinkRelation(
                row.parse("product_id", schema)?,
                row.parse("link_type_id", schema)?,
            )),
            Event::UpdateRow(row) => Some(ProductChange::LinkRelation(
                row.parse("product_id", schema)?,
                row.parse("link_type_id", schema)?,
            )),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn maps_insert_and_delete_into_link_relation_change() {
        mapper_test!(
            ProductLinkMapper,
            Some(ProductChange::LinkRelation(1, 3)),
            insert[1, 1, 3],
            ["product_id", "linked_product_id", "link_type_id"]
        );

        mapper_test!(
            ProductLinkMapper,
            Some(ProductChange::LinkRelation(2, 3)),
            delete[2, 4, 3],
            ["product_id", "linked_product_id", "link_type_id"]
        );
    }

    #[test]
    fn maps_any_update_into_link_relation_change() {
        mapper_test!(
            ProductLinkMapper,
            Some(ProductChange::LinkRelation(1, 3)),
            update[(1, 1, 3), (1, 1, 3)],
            ["product_id", "linked_product_id", "link_type_id"]
        );
    }
}
