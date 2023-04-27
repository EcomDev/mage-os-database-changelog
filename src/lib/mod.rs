#![feature(async_fn_in_trait)]

pub mod event;
pub mod replication;

mod change;
mod error;
mod schema;

#[cfg(any(test, feature = "test_util"))]
pub mod test_util;

pub use schema::{SchemaInformation, TableSchema};
