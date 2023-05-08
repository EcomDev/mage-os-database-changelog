mod magento_two;
mod observer;
mod product;
mod product_attribute;
mod product_bundle;
mod product_category;
mod product_configurable;
mod product_link;
mod product_media_gallery_value;
mod product_tier_price;
mod product_website;

use crate::error::Error;
use crate::replication::Event;
use crate::schema::TableSchema;
pub use magento_two::MagentoTwoMapper;
pub use observer::MapperObserver;
pub use product::ProductMapper;
pub use product_attribute::ProductAttributeMapper;
pub use product_bundle::ProductBundleMapper;
pub use product_configurable::ProductConfigurableMapper;
pub use product_link::ProductLinkMapper;
pub use product_media_gallery_value::ProductMediaGalleryValue;
pub use product_tier_price::ProductTierPriceMapper;
pub use product_website::ProductWebsite;

pub trait ChangeLogMapper<T> {
    fn map_event(&self, event: &Event, schema: &impl TableSchema) -> Result<Option<T>, Error>;
}
