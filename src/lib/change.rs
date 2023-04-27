#[derive(PartialEq, Debug)]
pub enum Change {
    Deleted(usize),
    Created(usize),
    FieldUpdated(&'static str, usize),
    AttributeUpdated(usize, usize),
    AssignedWebsite(usize, usize),
    ExternalRelationChanged(usize, usize),
    CompositeRelationChanged(usize),
}
