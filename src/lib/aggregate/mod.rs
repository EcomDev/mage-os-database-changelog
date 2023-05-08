mod change_aggregate;
mod product;

use crate::error::Error;
use crate::log::ItemChange;
pub use change_aggregate::*;
pub use product::*;

pub trait Aggregate {
    fn push(&mut self, item: impl Into<ItemChange>);

    fn size(&self) -> usize;

    fn flush(&mut self) -> Option<ChangeAggregate>;
}
