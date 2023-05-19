use crate::log::{FieldUpdate, IntoChangeLog};
use crate::MODIFIED_FIELDS_BUFFER_SIZE;
use smallvec::SmallVec;

#[derive(PartialEq, Debug, Clone)]
pub enum CatalogRuleChange {
    Created(usize),
    Deleted(usize),
    Fields(usize, SmallVec<[&'static str; MODIFIED_FIELDS_BUFFER_SIZE]>),
}

impl IntoChangeLog<CatalogRuleChange> for FieldUpdate<usize> {
    fn into_change_log(self) -> Option<CatalogRuleChange> {
        match self {
            Self::Empty(_) => None,
            Self::WithFields(identity, columns) => {
                Some(CatalogRuleChange::Fields(identity, columns))
            }
        }
    }
}
