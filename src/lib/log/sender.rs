use crate::error::Error;

pub trait ChangeLogSender: Clone {
    type Item;

    async fn send(&self, change: Self::Item) -> Result<(), Error>;
}
