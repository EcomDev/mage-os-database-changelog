use crate::log::catalog_inventory_change::CatalogInventoryChange;
use crate::log::CatalogRuleChange;
use crate::log::CatalogRuleProductChange;
use crate::log::ProductChange;
use crate::replication::EventMetadata;

#[derive(PartialEq, Debug, Clone)]
pub enum ItemChange {
    ProductChange(ProductChange),
    CatalogInventoryChange(CatalogInventoryChange),
    CatalogRuleChange(CatalogRuleChange),
    CatalogRuleProductChange(CatalogRuleProductChange),
    Metadata(EventMetadata),
}

impl From<ProductChange> for ItemChange {
    fn from(value: ProductChange) -> Self {
        Self::ProductChange(value)
    }
}

impl From<EventMetadata> for ItemChange {
    fn from(value: EventMetadata) -> Self {
        Self::Metadata(value)
    }
}
