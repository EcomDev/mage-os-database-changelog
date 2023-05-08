use crate::database::Database;
use crate::error::Error;
use crate::replication::binary_table::BinaryTable;
use crate::replication::{
    BinaryRowIter, BinlogPosition, Event, EventMetadata, EventObserver, UpdateRowEvent,
};
use crate::schema::{table_name_without_prefix, SchemaInformation};
use mysql_common::binlog::consts::EventType;
use mysql_common::binlog::events::{Event as BinLogEvent, EventData, RotateEvent, TableMapEvent};
use mysql_common::io::ParseBuf;
use std::collections::HashMap;
use tokio_stream::StreamExt;

pub struct ReplicationClient<D, T> {
    database: Database,
    database_name: D,
    table_prefix: T,
}

impl<D, T> ReplicationClient<D, T>
where
    D: AsRef<str>,
    T: AsRef<str>,
{
    pub fn new(database: Database, database_name: D, table_prefix: T) -> Self {
        Self {
            database,
            database_name,
            table_prefix,
        }
    }

    pub async fn process(
        &self,
        observer: impl EventObserver,
        mut position: BinlogPosition,
    ) -> Result<(), Error> {
        let mut stream = self.database.binlog_stream(&position).await?;
        let mut binary_tables: HashMap<u64, (String, BinaryTable)> = HashMap::new();
        let mut table_schema = SchemaInformation::default();
        table_schema
            .populate(
                &mut self.database.acquire_connection().await?,
                &self.database_name,
                &self.table_prefix,
            )
            .await?;

        while let Some(event) = stream.next().await {
            match event {
                Ok(event) => {
                    position = position.with_position(event.header().log_pos());
                    match event.header().event_type() {
                        Ok(
                            EventType::DELETE_ROWS_EVENT
                            | EventType::WRITE_ROWS_EVENT
                            | EventType::UPDATE_ROWS_EVENT
                            | EventType::PARTIAL_UPDATE_ROWS_EVENT,
                        ) => {
                            self.process_rows_event(
                                &observer,
                                &binary_tables,
                                &table_schema,
                                event,
                                &position,
                            )
                            .await?;
                        }
                        Ok(EventType::TABLE_MAP_EVENT) => {
                            self.process_table_event(&mut binary_tables, event).await?
                        }
                        Ok(EventType::ROTATE_EVENT) => {
                            let rotate_event: RotateEvent = match event.read_event() {
                                Ok(rotate_event) => rotate_event,
                                Err(error) => return Err(Error::from(error)),
                            };

                            position = position
                                .with_file(rotate_event.name())
                                .with_position(rotate_event.position() as u32)
                        }
                        _ => continue,
                    }
                }
                Err(error) => return Err(Error::from(error)),
            };
        }

        Ok(())
    }

    async fn process_rows_event(
        &self,
        observer: &impl EventObserver,
        binary_tables: &HashMap<u64, (String, BinaryTable)>,
        table_info: &SchemaInformation<'_>,
        binlog_event: BinLogEvent,
        position: &BinlogPosition,
    ) -> Result<(), Error> {
        match binlog_event.read_data() {
            Ok(Some(EventData::RowsEvent(event))) => {
                let (table_name, binary_table) = match binary_tables.get(&event.table_id()) {
                    Some(binary_table) => binary_table,
                    None => return Ok(()),
                };

                for row in BinaryRowIter::new(&event, binary_table, ParseBuf(event.rows_data())) {
                    let event = match row {
                        Err(error) => return Err(Error::from(error)),
                        Ok((None, Some(after))) => Event::InsertRow(after),
                        Ok((Some(before), None)) => Event::DeleteRow(before),
                        Ok((Some(before), Some(after))) => {
                            Event::UpdateRow(UpdateRowEvent::new(before, after))
                        }
                        _ => continue,
                    };

                    observer
                        .process_event(&event, &table_info.table_schema(&table_name))
                        .await?
                }

                let metadata = EventMetadata::new(
                    binlog_event.header().timestamp() as usize,
                    position.clone(),
                );

                observer.process_metadata(&metadata).await?;

                Ok(())
            }
            Err(error) => Err(Error::from(error)),
            _ => Ok(()),
        }
    }

    async fn process_table_event(
        &self,
        binary_tables: &mut HashMap<u64, (String, BinaryTable)>,
        binlog_event: BinLogEvent,
    ) -> Result<(), Error> {
        let table_map_event: TableMapEvent = binlog_event.read_event().map_err(Error::Io)?;

        if table_map_event
            .database_name()
            .ne(self.database_name.as_ref())
        {
            return Ok(());
        }

        binary_tables
            .entry(table_map_event.table_id())
            .or_insert_with(|| {
                (
                    table_name_without_prefix(table_map_event.table_name(), &self.table_prefix),
                    BinaryTable::from_table_map_event(&table_map_event),
                )
            });

        Ok(())
    }
}
