use crate::log::date::Date;
use crate::log::{FieldUpdate, IntoChangeLog};
use crate::MODIFIED_FIELDS_BUFFER_SIZE;
use smallvec::SmallVec;

#[derive(PartialEq, Debug, Clone)]
pub enum CatalogRuleProductChange {
    Created(usize, Date),
    Deleted(usize, Date),
    Fields(
        usize,
        Date,
        SmallVec<[&'static str; MODIFIED_FIELDS_BUFFER_SIZE]>,
    ),
}

impl IntoChangeLog<CatalogRuleProductChange> for FieldUpdate<(usize, Date)> {
    fn into_change_log(self) -> Option<CatalogRuleProductChange> {
        match self {
            Self::Empty(_) => None,
            Self::WithFields((identity, date), columns) => {
                Some(CatalogRuleProductChange::Fields(identity, date, columns))
            }
        }
    }
}
