use crate::log::catalog_rule_product_change::CatalogRuleProductChange;
use crate::log::{FieldUpdate, IntoChangeLog};
use crate::MODIFIED_FIELDS_BUFFER_SIZE;
use smallvec::SmallVec;

#[derive(PartialEq, Debug, Clone)]
pub enum CatalogInventoryChange {
    Created(usize, usize),
    Deleted(usize, usize),
    Fields(
        usize,
        usize,
        SmallVec<[&'static str; MODIFIED_FIELDS_BUFFER_SIZE]>,
    ),
}

impl IntoChangeLog<CatalogInventoryChange> for FieldUpdate<(usize, usize)> {
    fn into_change_log(self) -> Option<CatalogInventoryChange> {
        match self {
            Self::Empty(_) => None,
            Self::WithFields((identity, stock_identity), columns) => Some(
                CatalogInventoryChange::Fields(identity, stock_identity, columns),
            ),
        }
    }
}
