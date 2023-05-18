mod chain_aggregate;
mod change_aggregate;
mod product;
mod wrapped_aggregate;

use crate::aggregate::chain_aggregate::ChainAggregate;
use crate::error::Error;
use crate::log::ItemChange;
use crate::output::Output;
pub use change_aggregate::*;
pub use product::*;
use std::time::Duration;
use tokio::io::AsyncWrite;
pub use wrapped_aggregate::WrappedAggregate;

pub trait Aggregate {
    fn push(&mut self, item: impl Into<ItemChange>);

    fn size(&self) -> usize;

    fn flush(&mut self) -> Option<ChangeAggregate>;
}

pub trait AsyncAggregate {
    fn push(&mut self, item: impl Into<ItemChange>);

    async fn write(
        &mut self,
        limit: &(usize, Duration),
        output: &impl Output,
        write: impl AsyncWrite + Unpin,
    ) -> Result<(), Error>;

    async fn write_eof(
        &mut self,
        output: &impl Output,
        write: impl AsyncWrite + Unpin,
    ) -> Result<(), Error>;

    fn with<R>(self, other: R) -> ChainAggregate<Self, R>
    where
        Self: Sized,
        R: AsyncAggregate,
    {
        ChainAggregate::new(self, other)
    }
}
