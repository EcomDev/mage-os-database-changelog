#![feature(async_fn_in_trait)]

#[cfg(any(test, docsrs, feature = "test_util"))]
#[macro_use]
pub mod test_util;
#[macro_use]
pub mod replication;
pub mod aggregate;
pub mod database;
pub mod error;
pub mod log;
pub mod mapper;
pub mod output;
pub mod schema;

/// Number of entries in array of `SmallVec` that are used for row representation in tables.
/// When number of values in vector is larger then this value it can result in heap allocations.
/// At the moment value of 64 is chosen as most of the database tables do not exceed this value   
pub const ROW_BUFFER_SIZE: usize = 64;

/// Number of entries in array of `SmallVec` that are used for changelog representations with list
/// of modified columns per entity
pub const MODIFIED_FIELDS_BUFFER_SIZE: usize = 8;

impl log::ChangeLogSender for tokio::sync::mpsc::Sender<log::ItemChange> {
    type Item = log::ItemChange;

    async fn send(&self, change: Self::Item) -> Result<(), error::Error> {
        self.send(change)
            .await
            .map_err(|_| error::Error::Synchronization)
    }
}
