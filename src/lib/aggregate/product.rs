use crate::aggregate::change_aggregate::{ChangeAggregate, ChangeAggregateDataKey};
use crate::aggregate::Aggregate;
use crate::log::{ItemChange, ProductChange};
use crate::replication::EventMetadata;
use mysql_common::frunk::labelled::chars::{e, s};
use std::collections::{HashMap, HashSet};
use std::mem::size_of_val;

#[derive(Eq, PartialEq, Hash)]
enum AggregateKey {
    Created,
    Deleted,
    Field(&'static str),
    Attribute(usize),
    WebsiteAll,
    WebsiteSpecific(usize),
    CategoryAll,
    CategorySpecific(usize),
    Link(usize),
    Composite,
    MediaGallery,
    TierPrice,
}

impl Into<ChangeAggregateDataKey> for AggregateKey {
    fn into(self) -> ChangeAggregateDataKey {
        match self {
            Self::Attribute(id) => ChangeAggregateDataKey::Attribute(id),
            Self::Created => ChangeAggregateDataKey::Key("@created"),
            Self::Deleted => ChangeAggregateDataKey::Key("@deleted"),
            Self::Field(field) => ChangeAggregateDataKey::Key(field),
            Self::WebsiteAll => ChangeAggregateDataKey::Key("@website"),
            Self::WebsiteSpecific(id) => ChangeAggregateDataKey::KeyAndScopeInt("@website", id),
            Self::CategoryAll => ChangeAggregateDataKey::Key("@category"),
            Self::CategorySpecific(id) => ChangeAggregateDataKey::KeyAndScopeInt("@category", id),
            Self::Link(id) => ChangeAggregateDataKey::KeyAndScopeInt("@link", id),
            Self::Composite => ChangeAggregateDataKey::Key("@composite"),
            Self::MediaGallery => ChangeAggregateDataKey::Key("@media_gallery"),
            Self::TierPrice => ChangeAggregateDataKey::Key("@tier_price"),
        }
    }
}

#[derive(Default)]
pub struct ProductAggregate {
    data: HashMap<AggregateKey, HashSet<usize>>,
    size: usize,
    last_metadata: Option<EventMetadata>,
}

impl ProductAggregate {
    fn aggregate_product(&mut self, key: AggregateKey, entity_id: usize) {
        let is_inserted = self.data.entry(key).or_default().insert(entity_id);

        if is_inserted {
            self.size += 1;
        }
    }

    fn process_product_change(&mut self, change: ProductChange) {
        match change {
            ProductChange::Attribute(entity_id, attribute_id) => {
                self.aggregate_product(AggregateKey::Attribute(attribute_id), entity_id);
            }
            ProductChange::Field(entity_id, field) => {
                self.aggregate_product(AggregateKey::Field(field), entity_id);
            }
            ProductChange::Fields(entity_id, fields) => {
                for field in fields {
                    self.aggregate_product(AggregateKey::Field(field), entity_id);
                }
            }
            ProductChange::Created(entity_id) => {
                self.aggregate_product(AggregateKey::Created, entity_id);
            }
            ProductChange::Deleted(entity_id) => {
                self.aggregate_product(AggregateKey::Deleted, entity_id);
            }

            ProductChange::Website(entity_id, website_id) => {
                self.aggregate_product(AggregateKey::WebsiteAll, entity_id);
                self.aggregate_product(AggregateKey::WebsiteSpecific(website_id), entity_id);
            }
            ProductChange::Category(entity_id, category_id) => {
                self.aggregate_product(AggregateKey::CategoryAll, entity_id);
                self.aggregate_product(AggregateKey::CategorySpecific(category_id), entity_id);
            }
            ProductChange::LinkRelation(entity_id, type_id) => {
                self.aggregate_product(AggregateKey::Link(type_id), entity_id);
            }
            ProductChange::MediaGallery(entity_id) => {
                self.aggregate_product(AggregateKey::MediaGallery, entity_id);
            }
            ProductChange::CompositeRelation(entity_id) => {
                self.aggregate_product(AggregateKey::Composite, entity_id);
            }
            ProductChange::TierPrice(entity_id) => {
                self.aggregate_product(AggregateKey::TierPrice, entity_id);
            }
            _ => {}
        }
    }
}

impl Aggregate for ProductAggregate {
    fn push(&mut self, item: impl Into<ItemChange>) {
        match item.into() {
            ItemChange::Metadata(metadata) => self.last_metadata = Some(metadata),
            ItemChange::ProductChange(product_change) => {
                self.process_product_change(product_change)
            }
        }
    }

    fn size(&self) -> usize {
        self.data
            .iter()
            .fold(0, |value, (_, item)| value + item.len())
    }

    fn flush(&mut self) -> Option<ChangeAggregate> {
        let metadata = match self.last_metadata.take() {
            None => return None,
            Some(metadata) => metadata,
        };

        let mut change_aggregate = ChangeAggregate::new("product", metadata);
        let data = std::mem::take(&mut self.data);

        for (key, value) in data.into_iter() {
            change_aggregate.add_data(key.into(), value);
        }

        Some(change_aggregate)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::aggregate::change_aggregate::ChangeAggregateDataKey;
    use crate::log::ProductChange;
    use crate::replication::BinlogPosition;
    use std::mem::size_of_val;

    #[test]
    fn returns_size_in_bytes_for_data_container() {
        let mut aggregate = ProductAggregate::default();

        aggregate.push(ProductChange::Attribute(1, 1));
        aggregate.push(ProductChange::Attribute(2, 1));
        aggregate.push(ProductChange::Attribute(3, 1));
        aggregate.push(ProductChange::Attribute(3, 2));
        aggregate.push(ProductChange::Attribute(4, 2));
        aggregate.push(ProductChange::Attribute(4, 2));

        assert_eq!(aggregate.size(), 5);
    }

    #[test]
    fn aggregates_attribute_changes() {
        let mut aggregate = ProductAggregate::default();

        aggregate.push(ProductChange::Attribute(2, 1));
        aggregate.push(ProductChange::Attribute(2, 1));
        aggregate.push(ProductChange::Attribute(3, 1));
        aggregate.push(ProductChange::Attribute(1, 2));
        aggregate.push(EventMetadata::new(1, BinlogPosition::new("file", 1)));

        assert_eq!(
            aggregate.flush(),
            Some(
                ChangeAggregate::new(
                    "product",
                    EventMetadata::new(1, BinlogPosition::new("file", 1))
                )
                .with_data(ChangeAggregateDataKey::Attribute(1), [2, 3])
                .with_data(ChangeAggregateDataKey::Attribute(2), [1])
            )
        )
    }

    #[test]
    fn aggregates_key_changes() {
        let mut aggregate = ProductAggregate::default();

        aggregate.push(ProductChange::Field(2, "sku"));
        aggregate.push(ProductChange::Fields(
            2,
            vec!["type_id", "attribute_set_id"].into(),
        ));
        aggregate.push(ProductChange::Field(3, "type_id"));
        aggregate.push(ProductChange::Field(1, "sku"));
        aggregate.push(EventMetadata::new(1, BinlogPosition::new("file", 1)));

        assert_eq!(
            aggregate.flush(),
            Some(
                ChangeAggregate::new(
                    "product",
                    EventMetadata::new(1, BinlogPosition::new("file", 1))
                )
                .with_data(ChangeAggregateDataKey::Key("sku"), [1, 2])
                .with_data(ChangeAggregateDataKey::Key("attribute_set_id"), [2])
                .with_data(ChangeAggregateDataKey::Key("type_id"), [2, 3])
            )
        )
    }

    #[test]
    fn aggregates_create_and_delete_changes() {
        let mut aggregate = ProductAggregate::default();

        aggregate.push(ProductChange::Created(1));
        aggregate.push(ProductChange::Created(2));
        aggregate.push(ProductChange::Deleted(2));
        aggregate.push(ProductChange::Deleted(3));
        aggregate.push(EventMetadata::new(1, BinlogPosition::new("file", 1)));

        assert_eq!(
            aggregate.flush(),
            Some(
                ChangeAggregate::new(
                    "product",
                    EventMetadata::new(1, BinlogPosition::new("file", 1))
                )
                .with_data(ChangeAggregateDataKey::Key("@created"), [1, 2])
                .with_data(ChangeAggregateDataKey::Key("@deleted"), [2, 3])
            )
        )
    }

    #[test]
    fn aggregates_website_changes() {
        let mut aggregate = ProductAggregate::default();

        aggregate.push(ProductChange::Website(1, 1));
        aggregate.push(ProductChange::Website(1, 2));
        aggregate.push(ProductChange::Website(2, 1));
        aggregate.push(ProductChange::Website(3, 1));

        aggregate.push(EventMetadata::new(1, BinlogPosition::new("file", 1)));

        assert_eq!(
            aggregate.flush(),
            Some(
                ChangeAggregate::new(
                    "product",
                    EventMetadata::new(1, BinlogPosition::new("file", 1))
                )
                .with_data(ChangeAggregateDataKey::Key("@website"), [1, 2, 3])
                .with_data(
                    ChangeAggregateDataKey::KeyAndScopeInt("@website", 1),
                    [1, 2, 3]
                )
                .with_data(ChangeAggregateDataKey::KeyAndScopeInt("@website", 2), [1])
            )
        )
    }

    #[test]
    fn aggregates_category_changes() {
        let mut aggregate = ProductAggregate::default();

        aggregate.push(ProductChange::Category(1, 1));
        aggregate.push(ProductChange::Category(1, 2));
        aggregate.push(ProductChange::Category(2, 1));
        aggregate.push(ProductChange::Category(3, 1));

        aggregate.push(EventMetadata::new(1, BinlogPosition::new("file", 1)));

        assert_eq!(
            aggregate.flush(),
            Some(
                ChangeAggregate::new(
                    "product",
                    EventMetadata::new(1, BinlogPosition::new("file", 1))
                )
                .with_data(ChangeAggregateDataKey::Key("@category"), [1, 2, 3])
                .with_data(
                    ChangeAggregateDataKey::KeyAndScopeInt("@category", 1),
                    [1, 2, 3]
                )
                .with_data(ChangeAggregateDataKey::KeyAndScopeInt("@category", 2), [1])
            )
        )
    }

    #[test]
    fn aggregates_link_changes() {
        let mut aggregate = ProductAggregate::default();

        aggregate.push(ProductChange::LinkRelation(1, 1));
        aggregate.push(ProductChange::LinkRelation(1, 2));
        aggregate.push(ProductChange::LinkRelation(2, 1));
        aggregate.push(ProductChange::LinkRelation(3, 1));

        aggregate.push(EventMetadata::new(1, BinlogPosition::new("file", 1)));

        assert_eq!(
            aggregate.flush(),
            Some(
                ChangeAggregate::new(
                    "product",
                    EventMetadata::new(1, BinlogPosition::new("file", 1))
                )
                .with_data(
                    ChangeAggregateDataKey::KeyAndScopeInt("@link", 1),
                    [1, 2, 3]
                )
                .with_data(ChangeAggregateDataKey::KeyAndScopeInt("@link", 2), [1])
            )
        )
    }

    #[test]
    fn aggregates_composite_and_gallery_changes() {
        let mut aggregate = ProductAggregate::default();

        aggregate.push(ProductChange::CompositeRelation(1));
        aggregate.push(ProductChange::CompositeRelation(2));
        aggregate.push(ProductChange::MediaGallery(2));
        aggregate.push(ProductChange::MediaGallery(3));
        aggregate.push(EventMetadata::new(1, BinlogPosition::new("file", 1)));

        assert_eq!(
            aggregate.flush(),
            Some(
                ChangeAggregate::new(
                    "product",
                    EventMetadata::new(1, BinlogPosition::new("file", 1))
                )
                .with_data(ChangeAggregateDataKey::Key("@composite"), [1, 2])
                .with_data(ChangeAggregateDataKey::Key("@media_gallery"), [2, 3])
            )
        )
    }

    #[test]
    fn aggregates_tier_price_changes() {
        let mut aggregate = ProductAggregate::default();

        aggregate.push(ProductChange::TierPrice(1));
        aggregate.push(ProductChange::TierPrice(2));
        aggregate.push(EventMetadata::new(1, BinlogPosition::new("file", 1)));

        assert_eq!(
            aggregate.flush(),
            Some(
                ChangeAggregate::new(
                    "product",
                    EventMetadata::new(1, BinlogPosition::new("file", 1))
                )
                .with_data(ChangeAggregateDataKey::Key("@tier_price"), [1, 2])
            )
        )
    }
}
