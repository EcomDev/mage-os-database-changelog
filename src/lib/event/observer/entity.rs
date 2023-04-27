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
pub struct EntityObserver {
    send: UnboundedSender<Change>,
    columns: SmallVec<[&'static str; BUFFER_STACK_SIZE]>,
    table_name: Cow<'static, str>,
}

impl EntityObserver {
    pub fn new(
        send: UnboundedSender<Change>,
        columns: SmallVec<[&'static str; BUFFER_STACK_SIZE]>,
        table_name: Cow<'static, str>,
    ) -> Self {
        Self {
            send,
            columns,
            table_name,
        }
    }
}

impl EventObserver for EntityObserver {
    async fn process_event(&self, event: &Event, table: &impl TableSchema) -> Result<(), Error> {
        if table.table_name().ne(self.table_name.as_ref()) {
            return Ok(());
        }

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

                for column in &self.columns {
                    if !update.is_changed_column(column, table) {
                        continue;
                    }

                    self.send
                        .send(Change::FieldUpdated(column, entity_id))
                        .unwrap();
                }
            }
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use crate::change::Change;
    use crate::error::Error;
    use crate::event::observer::entity::EntityObserver;
    use crate::event::observer::EventObserver;
    use crate::event::{Event, UpdateEvent};
    use crate::replication::BUFFER_STACK_SIZE;
    use crate::test_util::{IntoBinlogValue, TestTableSchema};
    use smallvec::SmallVec;
    use std::borrow::Cow;
    use tokio::sync::mpsc::{unbounded_channel, UnboundedReceiver};

    macro_rules! columns {
        ($($name:expr),*) => {
            (vec![$($name.into()),*].into())
        }
    }

    #[tokio::test]
    async fn reports_included_columns_on_insert_event() -> Result<(), Error> {
        let (schema, mut rx, observer) = setup_test(columns!(), "entity".into());

        process_event!(
            observer,
            schema,
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
    async fn does_not_report_on_another_table() -> Result<(), Error> {
        let (schema, mut rx, observer) = setup_test(columns!(), "category".into());

        process_event!(
            observer,
            schema,
            [Event::Insert(binlog_row!(
                1,
                "SKU1",
                3,
                "1970-01-01 00:00:00"
            ))]
        );

        assert_eq!(changes(rx).await, vec![]);

        Ok(())
    }

    #[tokio::test]
    async fn reports_deleted_entity() -> Result<(), Error> {
        let (schema, mut rx, observer) = setup_test(columns!(), "entity".into());

        process_event!(observer, schema, [Event::Delete(binlog_row!(1))]);

        assert_eq!(changes(rx).await, vec![Change::Deleted(1)]);

        Ok(())
    }

    #[tokio::test]
    async fn reports_only_updated_entity_columns() -> Result<(), Error> {
        let (schema, mut rx, observer) =
            setup_test(columns!("sku", "attribute_set_id"), "entity".into());

        process_event!(
            observer,
            schema,
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

    fn setup_test(
        columns: SmallVec<[&'static str; BUFFER_STACK_SIZE]>,
        table_name: Cow<'static, str>,
    ) -> (TestTableSchema, UnboundedReceiver<Change>, EntityObserver) {
        let schema = TestTableSchema::new("entity")
            .with_column("entity_id", 0)
            .with_column("sku", 1)
            .with_column("attribute_set_id", 2)
            .with_column("created_at", 3);

        let (tx, rx) = unbounded_channel();

        let observer = EntityObserver::new(tx, columns, table_name);
        (schema, rx, observer)
    }

    async fn changes(mut rx: UnboundedReceiver<Change>) -> Vec<Change> {
        let mut changes = vec![];
        while let Some(change) = rx.recv().await {
            changes.push(change);
        }

        changes
    }
}
