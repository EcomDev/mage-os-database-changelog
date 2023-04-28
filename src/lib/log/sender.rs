use crate::error::Error;

pub trait ChangeLogSender {
    type Item;

    async fn send(&self, change: Self::Item) -> Result<(), Error>;
}
