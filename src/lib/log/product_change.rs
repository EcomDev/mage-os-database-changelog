use crate::log::ItemChange;
use crate::MODIFIED_FIELDS_BUFFER_SIZE;
use smallvec::SmallVec;

#[derive(PartialEq, Debug)]
pub enum ProductChange {
    Deleted(usize),
    Created(usize),
    Fields(usize, SmallVec<[&'static str; MODIFIED_FIELDS_BUFFER_SIZE]>),
    Field(usize, &'static str),
    Attribute(usize, usize),
    MediaGallery(usize),
    LinkRelation(usize, usize),
    Website(usize, usize),
    Category(usize, usize),
    CompositeRelation(usize),
    TierPrice(usize),
}
