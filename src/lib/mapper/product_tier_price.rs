use crate::error::Error;
use crate::log::ProductChange;
use crate::mapper::ChangeLogMapper;
use crate::replication::Event;
use crate::schema::TableSchema;

pub struct ProductTierPriceMapper;

impl ChangeLogMapper<ProductChange> for ProductTierPriceMapper {
    fn map_event(
        &self,
        event: &Event,
        schema: &impl TableSchema,
    ) -> Result<Option<ProductChange>, Error> {
        Ok(match event {
            Event::InsertRow(row) | Event::DeleteRow(row) => {
                Some(ProductChange::TierPrice(row.parse("entity_id", schema)?))
            }
            Event::UpdateRow(row) => {
                Some(ProductChange::TierPrice(row.parse("entity_id", schema)?))
            }
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn maps_insert_event_into_tier_price_field_update() {
        mapper_test!(
            ProductTierPriceMapper,
            Some(ProductChange::TierPrice(2)),
            insert[1, 2],
            ["value_id", "entity_id"]
        );
    }

    #[test]
    fn maps_update_event_without_any_field_match_into_update() {
        mapper_test!(
            ProductTierPriceMapper,
            Some(ProductChange::TierPrice(2)),
            update[(1, 2), (2, 2)],
            ["website_id", "entity_id"]
        );
    }

    #[test]
    fn maps_delete_event_into_tier_price_field_update() {
        mapper_test!(
            ProductTierPriceMapper,
            Some(ProductChange::TierPrice(3)),
            delete[1, 3],
            ["value_id", "entity_id"]
        );
    }
}
