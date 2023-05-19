mod catalog_inventory_change;
mod catalog_rule_change;
mod catalog_rule_product_change;
mod change_log;
mod date;
mod field_update;
mod product_change;
mod sender;

pub use catalog_inventory_change::CatalogInventoryChange;
pub use catalog_rule_change::CatalogRuleChange;
pub use catalog_rule_product_change::CatalogRuleProductChange;
pub use change_log::ItemChange;
pub use field_update::FieldUpdate;
pub use product_change::ProductChange;
pub use sender::ChangeLogSender;

pub trait IntoChangeLog<T> {
    fn into_change_log(self) -> Option<T>;
}
