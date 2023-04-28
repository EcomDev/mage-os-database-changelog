

#[derive(PartialEq, Debug)]
pub enum ProductChange {
    Deleted(usize),
    Created(usize),
    FieldUpdated(&'static str, usize),
    AttributeUpdated(usize, usize),
    AssignedWebsite(usize, usize),
    CategoryAssignmentChanged(usize, usize),
    CompositeRelationChanged(usize),
}
