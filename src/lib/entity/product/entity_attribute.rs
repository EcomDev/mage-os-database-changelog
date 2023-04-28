use crate::error::Error;
use crate::log::{ChangeLogSender, ProductChange};
use crate::replication::{Event, EventObserver};
use crate::schema::TableSchema;

#[derive(Clone)]
pub struct ProductEntityAttributeObserver<R> {
    recorder: R,
}

impl<R> ProductEntityAttributeObserver<R> {
    pub fn new(log_recorder: R) -> Self {
        Self {
            recorder: log_recorder,
        }
    }
}

impl<R> EventObserver for ProductEntityAttributeObserver<R>
where
    R: ChangeLogSender<Item = ProductChange>,
{
    async fn process_event(&self, event: &Event, table: &impl TableSchema) -> Result<(), Error> {
        match event {
            Event::Insert(row) | Event::Delete(row) => {
                self.recorder
                    .send(ProductChange::AttributeUpdated(
                        row.parse("entity_id", table)?,
                        row.parse("attribute_id", table)?,
                    ))
                    .await?
            }
            Event::Update(update) if update.is_changed_column("value", table) => {
                self.recorder
                    .send(ProductChange::AttributeUpdated(
                        update.parse("entity_id", table)?,
                        update.parse("attribute_id", table)?,
                    ))
                    .await?
            }
            _ => {}
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::replication::UpdateEvent;
    use crate::test_util::{IntoBinlogValue, TestChangeLogSender};

    #[tokio::test]
    async fn when_no_value_changed_product() -> Result<(), Error> {
        let recorder = TestChangeLogSender::default();
        let observer = ProductEntityAttributeObserver::new(recorder.clone());
        process_event!(
            observer,
            test_table!(
                "entity_int",
                "value_id",
                ["value_id", "entity_id", "attribute_id", "store_id", "value"]
            ),
            [
                Event::Update(UpdateEvent::new(
                    binlog_row!(1, 1, 1, 0, "value"),
                    binlog_row!(1, 1, 1, 0, "value")
                )),
                Event::Update(UpdateEvent::new(
                    binlog_row!(2, 1, 1, 0, "value"),
                    binlog_row!(2, 1, 1, 0, "value")
                ))
            ]
        );

        assert_eq!(*recorder.values().await, vec![]);

        Ok(())
    }

    #[tokio::test]
    async fn notifies_of_attribute_update_when_new_record_insert() -> Result<(), Error> {
        let recorder = TestChangeLogSender::default();
        let observer = ProductEntityAttributeObserver::new(recorder.clone());
        process_event!(
            observer,
            test_table!("entity_int", "value_id", ["entity_id", "attribute_id"]),
            [
                Event::Insert(binlog_row!(1, 1)),
                Event::Insert(binlog_row!(2, 1)),
                Event::Insert(binlog_row!(2, 2))
            ]
        );

        assert_eq!(
            *recorder.values().await,
            vec![
                ProductChange::AttributeUpdated(1, 1),
                ProductChange::AttributeUpdated(2, 1),
                ProductChange::AttributeUpdated(2, 2),
            ]
        );

        Ok(())
    }

    #[tokio::test]
    async fn notifies_of_attribute_update_when_record_is_deleted() -> Result<(), Error> {
        let recorder = TestChangeLogSender::default();
        let observer = ProductEntityAttributeObserver::new(recorder.clone());
        process_event!(
            observer,
            test_table!("entity_int", "value_id", ["entity_id", "attribute_id"]),
            [
                Event::Delete(binlog_row!(3, 4)),
                Event::Delete(binlog_row!(3, 5))
            ]
        );

        assert_eq!(
            *recorder.values().await,
            vec![
                ProductChange::AttributeUpdated(3, 4),
                ProductChange::AttributeUpdated(3, 5),
            ]
        );

        Ok(())
    }

    #[tokio::test]
    async fn notifies_of_attribute_update_when_record_value_is_updated() -> Result<(), Error> {
        let recorder = TestChangeLogSender::default();
        let observer = ProductEntityAttributeObserver::new(recorder.clone());
        process_event!(
            observer,
            test_table!(
                "entity_int",
                "value_id",
                ["entity_id", "attribute_id", "value"]
            ),
            [Event::Update(UpdateEvent::new(
                binlog_row!(1, 3, "value_old"),
                binlog_row!(1, 3, "value_new")
            ))]
        );

        assert_eq!(
            *recorder.values().await,
            vec![ProductChange::AttributeUpdated(1, 3),]
        );

        Ok(())
    }
}
