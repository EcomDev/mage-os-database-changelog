use crate::error::Error;
use crate::log::ProductChange;
use crate::mapper::ChangeLogMapper;
use crate::replication::Event;
use crate::schema::TableSchema;

pub struct ProductMediaGalleryValue;

impl ChangeLogMapper<ProductChange> for ProductMediaGalleryValue {
    fn map_event(
        &self,
        event: &Event,
        schema: &impl TableSchema,
    ) -> Result<Option<ProductChange>, Error> {
        Ok(match event {
            Event::InsertRow(row) | Event::DeleteRow(row) => {
                Some(ProductChange::MediaGallery(row.parse("entity_id", schema)?))
            }
            Event::UpdateRow(row)
                if row.is_changed_column("store_id", schema)
                    | row.is_changed_column("label", schema)
                    | row.is_changed_column("disabled", schema) =>
            {
                Some(ProductChange::MediaGallery(row.parse("entity_id", schema)?))
            }
            _ => None,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn maps_insert_event_into_media_gallery_event() {
        mapper_test!(
            ProductMediaGalleryValue,
            Some(ProductChange::MediaGallery(4)),
            insert[2, 4, 3],
            ["store_id", "entity_id", "value_id"]
        );
    }

    #[test]
    fn maps_delete_event_into_media_gallery_event() {
        mapper_test!(
            ProductMediaGalleryValue,
            Some(ProductChange::MediaGallery(5)),
            delete[2, 5, 3],
            ["store_id", "entity_id", "value_id"]
        );
    }

    #[test]
    fn maps_update_event_into_media_gallery_event_when_store_is_updated() {
        mapper_test!(
            ProductMediaGalleryValue,

            Some(ProductChange::MediaGallery(2)),
            update[(2, 1), (2, 0)],
            ["entity_id", "store_id"]
        );
    }

    #[test]
    fn maps_update_event_into_none_when_not_related_field_is_updated() {
        mapper_test!(
            ProductMediaGalleryValue,
            None,
            update[(2, 1), (2, 0)],
            ["entity_id", "position"]
        );
    }

    #[test]
    fn maps_update_event_into_media_gallery_event_when_label_is_updated() {
        mapper_test!(
            ProductMediaGalleryValue,
            Some(ProductChange::MediaGallery(2)),
            update[(2, ""), (2, "Changed")],
            ["entity_id", "label"]
        );
    }

    #[test]
    fn maps_update_event_into_media_gallery_event_when_disable_is_updated() {
        mapper_test!(
            ProductMediaGalleryValue,
            Some(ProductChange::MediaGallery(7)),
            update[(7, 0), (7, 1)],
            ["entity_id", "disabled"]
        );
    }
}
