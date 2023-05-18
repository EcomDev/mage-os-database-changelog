use crate::log::ProductChange;
use crate::replication::EventMetadata;


pub enum ItemChange {
    ProductChange(ProductChange),
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
