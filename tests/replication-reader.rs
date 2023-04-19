#![feature(async_fn_in_trait)]
use mysql_async::binlog::events::{Event, EventData, RotateEvent, RowsEvent, TableMapEvent};
use mysql_async::binlog::EventType;
use mysql_async::prelude::Queryable;
use mysql_async::Error as MySQLError;
use mysql_async::{BinlogDumpFlags, BinlogRequest, BinlogStream, Conn, Row};
use mysql_common::binlog::events::RowsEventData;
use mysql_common::proto::MySerialize;
use mysql_common::value::Value;
use std::borrow::Cow;
use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::error::Error;
use std::fmt::format;
use std::time::Duration;

use mage_os_database_changelog::replication::{ReplicationObserver, ReplicationReader};
use tokio::time::error::Elapsed;
use tokio::time::timeout;
use tokio_stream::{self, StreamExt};

mod fixture;
use fixture::*;

#[derive(Default)]
struct EventsObserver {
    rows: Vec<String>,
    rotates: Vec<String>,
}

impl ReplicationObserver for EventsObserver {}

#[tokio::test]
async fn it_reads_changes_in_mysql() -> Result<(), MySQLError> {
    let mut fixture = Fixture::create_with_database("test_db").await?;

    let binlog_position = fixture.binlog_position().await?;
    fixture
        .insert_into(
            "entity",
            ["entity_id", "name", "description", "price"],
            vec![
                (1, "Product 1", "Product 1 description", 9.99),
                (2, "Product 2", "Product 2 description", 99.99),
            ],
        )
        .await?;

    fixture
        .insert_into(
            "entity_int",
            ["entity_id", "attribute_id", "store_id", "value"],
            vec![(1, 1, 0, 1), (1, 1, 1, 0), (2, 2, 0, 0)],
        )
        .await?;

    fixture
        .insert_into(
            "entity_json",
            ["entity_id", "attribute_id", "store_id", "value"],
            vec![
                (
                    2,
                    2,
                    0,
                    r#"{"flag": false, "featured": true, "labels": ["red", "blue", "purple"]}"#,
                ),
                (
                    2,
                    2,
                    0,
                    r#"{"flag": false, "featured": true, "labels": ["red", "blue", "purple"]}"#,
                ),
            ],
        )
        .await?;

    fixture
        .insert_into(
            "entity_with_multiple_json",
            ["entity_id", "store_id", "first_column", "second_column"],
            vec![
                (
                    1,
                    0,
                    r#"["red", "blue", "purple"]"#,
                    r#"{"flag": false, "featured": true, "labels": ["red", "blue", "purple"]}"#,
                ),
                (
                    2,
                    0,
                    r#"["green", "grey", "black"]"#,
                    r#"{"flag": true, "featured": false, "labels": ["red", "blue", "purple"]}"#,
                ),
            ],
        )
        .await?;

    fixture
        .execute_queries(vec![
            r#"START TRANSACTION"#,
            r#"UPDATE entity SET name = 'Awesome Product 2' WHERE entity_id = 2"#,
            r#"UPDATE entity SET price = 99.8 WHERE entity_id = 1"#,
            r#"DELETE FROM entity_int WHERE entity_id = 1 and store_id = 1 and attribute_id = 1"#,
            r#"UPDATE entity_json SET value = JSON_SET(value, '$.season', '["winter", "spring"]')"#,
            r#"UPDATE entity_with_multiple_json 
                SET second_column = JSON_SET(
                    second_column, '$.featured', 'false'
                ) 
                WHERE entity_id = 1
            "#,
            r#"UPDATE entity_with_multiple_json 
                SET second_column = JSON_SET(
                    second_column, '$.featured', 'true'
                ) 
                WHERE entity_id = 2
            "#,
            r#"UPDATE entity_with_multiple_json 
                SET first_column = JSON_SET(
                    first_column, '$.featured', '["red", "blue"]'
                ) 
                WHERE entity_id = 1
            "#,
            r#"COMMIT"#,
        ])
        .await?;

    let mut binlog_stream = Fixture::create()
        .await?
        .into_binary_log_stream(binlog_position)
        .await?;

    let mut table_binaries = HashMap::new();
    let mut event_rows = HashMap::new();
    let mut events = vec![];

    while let Some(Ok(event)) = binlog_stream.next().await {
        match event.header().event_type() {
            Ok(
                EventType::WRITE_ROWS_EVENT
                | EventType::DELETE_ROWS_EVENT
                | EventType::UPDATE_ROWS_EVENT
                | EventType::PARTIAL_UPDATE_ROWS_EVENT,
            ) => events.push(event),
            Ok(EventType::TABLE_MAP_EVENT) => {
                let table: TableMapEvent = event.read_event().unwrap();
                let table_name = table.table_name().into_owned();
                let mut entry = match table_binaries.entry(table_name) {
                    Entry::Occupied(_) => continue,
                    Entry::Vacant(entry) => entry,
                };

                entry.insert(Vec::from(event.data()));
            }
            _ => {}
        }
    }

    for event in events {
        if let Ok(Some(EventData::RowsEvent(rows_data))) = event.read_data() {
            let table = binlog_stream.get_tme(rows_data.table_id()).unwrap();
            let mut table_event_rows = event_rows
                .entry(format!(
                    "{}-{:?}",
                    table.table_name(),
                    event.header().event_type().unwrap()
                ))
                .or_insert(vec![]);
            table_event_rows.push(Vec::from(event.data()));

            let rows_event = match event.read_data()? {
                Some(EventData::RowsEvent(e)) => e,
                Some(e) => {
                    println!("{e:?}");
                    continue;
                }
                _ => continue,
            };

            for row in rows_event.rows(&table) {
                match row {
                    Ok((Some(left), Some(right))) => {
                        println!(
                            "Table {}: \n Before: {:?} \n After: {:?} \n",
                            table.table_name(),
                            left.unwrap(),
                            right.unwrap()
                        )
                    }
                    Ok((None, Some(right))) => {
                        println!(
                            "Table {}: \n After: {:?} \n",
                            table.table_name(),
                            right.unwrap()
                        )
                    }

                    Ok((Some(left), None)) => {
                        println!(
                            "Table {}: \n Before: {:?} \n",
                            table.table_name(),
                            left.unwrap()
                        )
                    }

                    _ => continue,
                }
            }
        }
    }

    println!("{table_binaries:?}");
    println!("{event_rows:?}");
    Ok(())
}
