use crate::aggregate::AsyncAggregate;
use crate::error::Error;
use crate::log::ItemChange;
use crate::output::Output;

use std::time::Duration;
use tokio::io::AsyncWrite;

pub struct ChainAggregate<L, R>(L, R);

impl<L, R> ChainAggregate<L, R>
where
    L: AsyncAggregate,
    R: AsyncAggregate,
{
    pub fn new(left: L, right: R) -> Self {
        Self(left, right)
    }
}

impl<L, R> AsyncAggregate for ChainAggregate<L, R>
where
    L: AsyncAggregate,
    R: AsyncAggregate,
{
    fn push(&mut self, item: impl Into<ItemChange>) {
        let item = item.into();
        self.0.push(item.clone());
        self.1.push(item.clone());
    }

    async fn write(
        &mut self,
        limit: &(usize, Duration),
        output: &impl Output,
        mut write: impl AsyncWrite + Unpin,
    ) -> Result<(), Error> {
        self.0.write(limit, output, &mut write).await?;
        self.1.write(limit, output, &mut write).await?;

        Ok(())
    }

    async fn write_eof(
        &mut self,
        output: &impl Output,
        mut write: impl AsyncWrite + Unpin,
    ) -> Result<(), Error> {
        self.0.write_eof(output, &mut write).await?;
        self.1.write_eof(output, &mut write).await?;

        Ok(())
    }
}
