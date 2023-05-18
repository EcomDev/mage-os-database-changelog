use crate::error::Error;
use crate::log::ProductChange;
use crate::mapper::ChangeLogMapper;
use crate::replication::Event;
use crate::schema::TableSchema;

pub struct ProductWebsite;

impl ChangeLogMapper<ProductChange> for ProductWebsite {
    fn map_event(
        &self,
        event: &Event,
        schema: &impl TableSchema,
    ) -> Result<Option<ProductChange>, Error> {
        Ok(match event {
            Event::InsertRow(row) | Event::DeleteRow(row) => Some(ProductChange::Website(
                row.parse("product_id", schema)?,
                row.parse("website_id", schema)?,
            )),
            _ => None,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ignores_update_row_events() {
        mapper_test!(
            ProductWebsite,
            None,
            update[(1, 1), (1, 2)],
            ["product_id", "website_id"]
        );
    }

    #[test]
    fn maps_insert_row_event_as_update_to_website_assignment() {
        mapper_test!(
            ProductWebsite,
            Some(ProductChange::Website(4, 1)),
            insert[4, 1],
            ["product_id", "website_id"]
        );
    }

    #[test]
    fn maps_delete_row_event_as_update_to_website_assignment() {
        mapper_test!(ProductWebsite,
            Some(ProductChange::Website(4, 2)),
            delete[4, 2],
            ["product_id", "website_id"]
        );
    }
}
