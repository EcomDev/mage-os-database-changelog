#![feature(async_fn_in_trait)]

#[cfg(any(feature = "test_util", test))]
#[macro_use]
pub mod test_util;

pub mod event;
pub mod replication;

mod change;
mod error;
mod schema;

pub use schema::{SchemaInformation, TableSchema};
