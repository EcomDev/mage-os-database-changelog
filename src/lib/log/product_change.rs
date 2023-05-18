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
