use crate::error::Error;
use crate::log::ChangeLogSender;
use std::sync::Arc;
use tokio::sync::{Mutex, MutexGuard};

pub struct TestChangeLogSender<T> {
    values: Arc<Mutex<Vec<T>>>,
}

impl<T> Default for TestChangeLogSender<T> {
    fn default() -> Self {
        Self {
            values: Arc::new(Mutex::new(Vec::new())),
        }
    }
}

impl<T> Clone for TestChangeLogSender<T> {
    fn clone(&self) -> Self {
        Self {
            values: Arc::clone(&self.values),
        }
    }
}

impl<T> TestChangeLogSender<T> {
    pub async fn values(&self) -> MutexGuard<'_, Vec<T>> {
        self.values.lock().await
    }
}

impl<T> ChangeLogSender for TestChangeLogSender<T> {
    type Item = T;

    async fn send(&self, change: Self::Item) -> Result<(), Error> {
        let mut values = self.values.lock().await;
        values.push(change);

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use crate::error::Error;
    use crate::log::ChangeLogSender;
    use crate::test_util::TestChangeLogSender;

    #[tokio::test]
    async fn collects_values_on_each_send() -> Result<(), Error> {
        let change_log = TestChangeLogSender::default();

        change_log.clone().send("one").await?;
        change_log.clone().send("two").await?;

        assert_eq!(*change_log.values().await, vec!["one", "two"]);

        Ok(())
    }
}
