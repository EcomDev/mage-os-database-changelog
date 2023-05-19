use crate::log::{FieldUpdate, IntoChangeLog};
use crate::MODIFIED_FIELDS_BUFFER_SIZE;
use smallvec::SmallVec;

#[derive(PartialEq, Debug, Clone)]
pub enum ProductChange {
    Deleted(usize),
    Created(usize),
    Fields(usize, SmallVec<[&'static str; MODIFIED_FIELDS_BUFFER_SIZE]>),
    Attribute(usize, usize),
    MediaGallery(usize),
    LinkRelation(usize, usize),
    Website(usize, usize),
    Category(usize, usize),
    CompositeRelation(usize),
    TierPrice(usize),
    Url(usize, usize),
    CategoryUrl(usize, usize),
}

impl IntoChangeLog<ProductChange> for FieldUpdate<usize> {
    fn into_change_log(self) -> Option<ProductChange> {
        match self {
            Self::Empty(_) => None,
            Self::WithFields(identity, columns) => Some(ProductChange::Fields(identity, columns)),
        }
    }
}
