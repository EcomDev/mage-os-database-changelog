


mod binary_table;
mod reader;
mod row;
mod rows;

#[cfg(test)]
pub(crate) mod test_fixture;

pub const BUFFER_STACK_SIZE: usize = 64;

pub use reader::ReplicationReader;
pub use row::BinaryRow;
pub use rows::BinaryRowIter;

pub trait ReplicationObserver {}
