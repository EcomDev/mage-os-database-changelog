use crate::change::Change;
use crate::error::Error;
use crate::event::observer::EventObserver;
use crate::event::Event;
use crate::replication::BUFFER_STACK_SIZE;
use crate::TableSchema;
use smallvec::SmallVec;
use std::borrow::Cow;
use tokio::sync::mpsc::UnboundedSender;

#[derive(Clone)]
pub struct ProductEntityObserver {
    send: UnboundedSender<Change>,
}

impl ProductEntityObserver {
    pub fn new(send: UnboundedSender<Change>) -> Self {
        Self { send }
    }
}

impl EventObserver for ProductEntityObserver {
    async fn process_event(&self, event: &Event, table: &impl TableSchema) -> Result<(), Error> {
        match event {
            Event::Insert(row) => self
                .send
                .send(Change::Created(row.parse("entity_id", table)?))
                .unwrap(),
            Event::Delete(row) => self
                .send
                .send(Change::Deleted(row.parse("entity_id", table)?))
                .unwrap(),
            Event::Update(update) => {
                let entity_id = update.parse("entity_id", table)?;
                updated_field_macro!(
                    update,
                    &self.send,
                    table,
                    entity_id,
                    ["sku", "attribute_set_id"]
                );
            }
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use crate::change::Change;
    use crate::error::Error;
    use crate::event::observer::product_entity::ProductEntityObserver;
    use crate::event::observer::EventObserver;
    use crate::event::{Event, UpdateEvent};
    use crate::replication::BUFFER_STACK_SIZE;
    use crate::test_util::{IntoBinlogValue, TestTableSchema};
    use smallvec::SmallVec;
    use std::borrow::Cow;
    use tokio::sync::mpsc::{unbounded_channel, UnboundedReceiver};

    #[tokio::test]
    async fn reports_included_columns_on_insert_event() -> Result<(), Error> {
        let (mut rx, observer) = setup_test();

        process_event!(
            observer,
            test_table!("entity", "entity_id", ["entity_id"]),
            [
                Event::Insert(binlog_row!(1, "SKU1", 3, "1970-01-01 00:00:00")),
                Event::Insert(binlog_row!(2, "SKU2", 3, "1970-01-01 00:00:00"))
            ]
        );

        assert_eq!(
            changes(rx).await,
            vec![Change::Created(1), Change::Created(2)]
        );

        Ok(())
    }

    #[tokio::test]
    async fn reports_deleted_entity() -> Result<(), Error> {
        let (mut rx, observer) = setup_test();

        process_event!(
            observer,
            test_table!("entity", "entity_id", ["entity_id"]),
            [Event::Delete(binlog_row!(1))]
        );

        assert_eq!(changes(rx).await, vec![Change::Deleted(1)]);

        Ok(())
    }

    #[tokio::test]
    async fn reports_only_updated_entity_columns() -> Result<(), Error> {
        let (mut rx, observer) = setup_test();

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
            changes(rx).await,
            vec![
                Change::FieldUpdated("sku", 1),
                Change::FieldUpdated("attribute_set_id", 2),
                Change::FieldUpdated("sku", 3),
                Change::FieldUpdated("attribute_set_id", 3),
            ]
        );

        Ok(())
    }

    fn setup_test() -> (UnboundedReceiver<Change>, ProductEntityObserver) {
        let (tx, rx) = unbounded_channel();
        let observer = ProductEntityObserver::new(tx);
        (rx, observer)
    }

    async fn changes(mut rx: UnboundedReceiver<Change>) -> Vec<Change> {
        let mut changes = vec![];
        while let Some(change) = rx.recv().await {
            changes.push(change);
        }

        changes
    }
}
