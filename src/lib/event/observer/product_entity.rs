use crate::error::Error;
use crate::event::observer::EventObserver;
use crate::event::Event;
use crate::log::{ChangeLogSender, ProductChange};
use crate::TableSchema;

#[derive(Clone)]
pub struct ProductEntityObserver<R> {
    recorder: R,
}

impl<R> ProductEntityObserver<R> {
    pub fn new(log_recorder: R) -> Self {
        Self {
            recorder: log_recorder,
        }
    }
}

macro_rules! updated_field_macro {
    ($update:expr, $tx:expr, $table:expr, $entity_id:expr,  [$($field:expr),+]) => {
        $(if $update.is_changed_column($field, $table) {
            $tx.send(crate::log::ProductChange::FieldUpdated($field, $entity_id)).await?;
        })+;
    };
}

impl<R> EventObserver for ProductEntityObserver<R>
where
    R: ChangeLogSender<Item = ProductChange>,
{
    async fn process_event(&self, event: &Event, table: &impl TableSchema) -> Result<(), Error> {
        match event {
            Event::Insert(row) => {
                self.recorder
                    .send(ProductChange::Created(row.parse("entity_id", table)?))
                    .await?
            }
            Event::Delete(row) => {
                self.recorder
                    .send(ProductChange::Deleted(row.parse("entity_id", table)?))
                    .await?
            }
            Event::Update(update) => {
                let entity_id = update.parse("entity_id", table)?;
                updated_field_macro!(
                    update,
                    &self.recorder,
                    table,
                    entity_id,
                    [
                        "sku",
                        "attribute_set_id",
                        "type_id",
                        "required_options",
                        "has_options"
                    ]
                );
            }
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::event::{Event, UpdateEvent};
    use crate::test_util::{IntoBinlogValue, TestChangeLogSender};

    #[tokio::test]
    async fn reports_included_columns_on_insert_event() -> Result<(), Error> {
        let recorder = TestChangeLogSender::default();
        let observer = ProductEntityObserver::new(recorder.clone());

        process_event!(
            observer,
            test_table!("entity", "entity_id", ["entity_id"]),
            [
                Event::Insert(binlog_row!(1, "SKU1", 3, "1970-01-01 00:00:00")),
                Event::Insert(binlog_row!(2, "SKU2", 3, "1970-01-01 00:00:00"))
            ]
        );

        assert_eq!(
            *recorder.values().await,
            vec![ProductChange::Created(1), ProductChange::Created(2)]
        );

        Ok(())
    }

    #[tokio::test]
    async fn reports_deleted_entity() -> Result<(), Error> {
        let recorder = TestChangeLogSender::default();
        let observer = ProductEntityObserver::new(recorder.clone());

        process_event!(
            observer,
            test_table!("entity", "entity_id", ["entity_id"]),
            [Event::Delete(binlog_row!(1))]
        );

        assert_eq!(*recorder.values().await, vec![ProductChange::Deleted(1)]);

        Ok(())
    }

    #[tokio::test]
    async fn reports_commonly_updated_entity_columns() -> Result<(), Error> {
        let recorder = TestChangeLogSender::default();
        let observer = ProductEntityObserver::new(recorder.clone());

        process_event!(
            observer,
            test_table!(
                "entity",
                "entity_id",
                ["entity_id", "sku", "attribute_set_id"]
            ),
            [
                Event::Update(UpdateEvent::new(
                    binlog_row!(1, "SKU1", 1),
                    binlog_row!(1, "SKU1C", 1),
                )),
                Event::Update(UpdateEvent::new(
                    binlog_row!(2, "SKU2", 1),
                    binlog_row!(2, "SKU2", 3),
                )),
                Event::Update(UpdateEvent::new(
                    binlog_row!(3, "SKU3", 1),
                    binlog_row!(3, "SKU3C", 3),
                ))
            ]
        );

        assert_eq!(
            *recorder.values().await,
            vec![
                ProductChange::FieldUpdated("sku", 1),
                ProductChange::FieldUpdated("attribute_set_id", 2),
                ProductChange::FieldUpdated("sku", 3),
                ProductChange::FieldUpdated("attribute_set_id", 3),
            ]
        );

        Ok(())
    }

    #[tokio::test]
    async fn reports_less_common_updated_entity_columns() -> Result<(), Error> {
        let recorder = TestChangeLogSender::default();
        let observer = ProductEntityObserver::new(recorder.clone());

        process_event!(
            observer,
            test_table!(
                "entity",
                "entity_id",
                ["entity_id", "type_id", "required_options", "has_options"]
            ),
            [
                Event::Update(UpdateEvent::new(
                    binlog_row!(1, "simple", 0, 0),
                    binlog_row!(1, "configurable", 1, 0),
                )),
                Event::Update(UpdateEvent::new(
                    binlog_row!(2, "bundle", 1, 0),
                    binlog_row!(2, "bundle", 1, 1),
                ))
            ]
        );

        assert_eq!(
            *recorder.values().await,
            vec![
                ProductChange::FieldUpdated("type_id", 1),
                ProductChange::FieldUpdated("required_options", 1),
                ProductChange::FieldUpdated("has_options", 2),
            ]
        );

        Ok(())
    }
}
