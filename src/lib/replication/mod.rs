mod binary_table;
mod client;
mod row;
mod rows;

mod event;
#[macro_use]
mod observer;
#[cfg(test)]
pub(crate) mod test_fixture;

pub use client::ReplicationClient;
pub use event::*;
pub use observer::*;
pub use row::BinaryRow;
pub use rows::BinaryRowIter;
