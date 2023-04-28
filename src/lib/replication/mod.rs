mod binary_table;
mod reader;
mod row;
mod rows;

mod event;
#[macro_use]
mod observer;
#[cfg(test)]
pub(crate) mod test_fixture;

pub const BUFFER_STACK_SIZE: usize = 64;

pub use event::*;
pub use observer::*;
pub use reader::ReplicationReader;
pub use row::BinaryRow;
pub use rows::BinaryRowIter;

pub trait ReplicationObserver {}
