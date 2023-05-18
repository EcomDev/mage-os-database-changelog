use crate::error::Error;
use crate::log::ItemChange;
use crate::log::ItemChange::ProductChange;
use crate::mapper::product_category::ProductCategoryMapper;
use crate::mapper::{
    ChangeLogMapper, ProductAttributeMapper, ProductBundleMapper, ProductConfigurableMapper,
    ProductLinkMapper, ProductMapper, ProductMediaGalleryValue, ProductTierPriceMapper,
    ProductWebsite,
};
use crate::replication::Event;
use crate::schema::TableSchema;

pub struct MagentoTwoMapper;

impl ChangeLogMapper<ItemChange> for MagentoTwoMapper {
    fn map_event(
        &self,
        event: &Event,
        schema: &impl TableSchema,
    ) -> Result<Option<ItemChange>, Error> {
        Ok(match schema.table_name() {
            "catalog_product_entity" => ProductMapper.map_event(event, schema)?.map(ProductChange),
            "catalog_product_entity_datetime"
            | "catalog_product_entity_decimal"
            | "catalog_product_entity_int"
            | "catalog_product_entity_text"
            | "catalog_product_entity_varchar" => ProductAttributeMapper
                .map_event(event, schema)?
                .map(ProductChange),
            "catalog_product_entity_tier_price" => ProductTierPriceMapper
                .map_event(event, schema)?
                .map(ProductChange),
            "catalog_product_website" => {
                ProductWebsite.map_event(event, schema)?.map(ProductChange)
            }
            "catalog_category_product" => ProductCategoryMapper
                .map_event(event, schema)?
                .map(ProductChange),
            "catalog_product_link" => ProductLinkMapper
                .map_event(event, schema)?
                .map(ProductChange),
            "catalog_product_entity_media_gallery_value" => ProductMediaGalleryValue
                .map_event(event, schema)?
                .map(ProductChange),
            "catalog_product_bundle_selection" => ProductBundleMapper
                .map_event(event, schema)?
                .map(ProductChange),
            "catalog_product_super_link" => ProductConfigurableMapper
                .map_event(event, schema)?
                .map(ProductChange),
            _ => None,
        })
    }
}
