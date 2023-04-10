#![feature(async_fn_in_trait)]
use std::borrow::Cow;
use std::collections::HashMap;
use std::error::Error;
use std::fmt::format;
use std::time::Duration;
use mysql_async::{BinlogDumpFlags, BinlogRequest, BinlogStream, Conn, Row};
use mysql_async::binlog::events::{Event, EventData, RotateEvent, RowsEvent, TableMapEvent};
use mysql_async::binlog::EventType;
use mysql_async::Error as MySQLError;
use mysql_async::prelude::Queryable;
use mysql_common::proto::MySerialize;

use tokio::test;
use tokio::time::error::Elapsed;
use tokio::time::timeout;
use tokio_stream::{self, StreamExt};
use mage_os_database_changelog::replication::{ReplicationObserver, ReplicationReader};

async fn create_connection() -> Result<Conn, MySQLError> {
    Conn::from_url(std::env::var("TEST_MYSQL_URL").unwrap()).await
}

async fn execute_queries(queries: Vec<&'static str>) -> Result<(), MySQLError> {
    let mut connection = create_connection().await?;

    for query in queries {
        connection.query_drop(query).await?;
    }

    Ok(())
}

#[derive(Default)]
struct EventsObserver {
    rows: Vec<String>,
    rotates: Vec<String>
}

impl ReplicationObserver for EventsObserver {

}

#[tokio::test]
async fn it_reads_changes_in_mysql() -> Result<(), MySQLError> {
    let reader = ReplicationReader::new(
        "test_db",
        Duration::from_micros(100)
    );

    execute_queries(vec![
        r#"DROP DATABASE IF EXISTS test_db"#,
        r#"CREATE DATABASE test_db"#,
        r#"use test_db"#,
        r#"CREATE TABLE entity (
            entity_id INT UNSIGNED NOT NULL AUTO_INCREMENT,
            name VARCHAR(255) NOT NULL,
            description TEXT NOT NULL,
            price DECIMAL(12, 4) NOT NULL DEFAULT '0.00',
            PRIMARY KEY(entity_id)
        )"#,
        r#"CREATE TABLE entity_int (
            value_id INT UNSIGNED NOT NULL AUTO_INCREMENT,
            attribute_id INT UNSIGNED NOT NULL,
            entity_id INT UNSIGNED NOT NULL,
            store_id INT UNSIGNED NOT NULL,
            value INT,
            PRIMARY KEY(value_id),
            INDEX `attribute_store_entity` (`entity_id`,`attribute_id`,`store_id`)
        )"#,
        r#"CREATE TABLE entity_json (
            value_id INT UNSIGNED NOT NULL AUTO_INCREMENT,
            attribute_id INT UNSIGNED NOT NULL,
            entity_id INT UNSIGNED NOT NULL,
            store_id INT UNSIGNED NOT NULL,
            value JSON,
            PRIMARY KEY(value_id),
            INDEX `attribute_store_entity` (`entity_id`,`attribute_id`,`store_id`)
        )"#,
    ]).await?;

    let binlog_position = current_binlog_position().await;

    execute_queries(vec![
        r#"use test_db"#,
        r#"START TRANSACTION"#,
        r#"INSERT INTO entity (entity_id, name, description, price) VALUES (1, 'Product 1', 'Some description 1', 9.99)"#,
        r#"INSERT INTO entity (entity_id, name, description, price) VALUES (2, 'Product 2', 'Some description 2', 99.99)"#,
        r#"INSERT INTO entity_int (entity_id, attribute_id, store_id, value) VALUES (1, 1, 0, 1)"#,
        r#"INSERT INTO entity_int (entity_id, attribute_id, store_id, value) VALUES (1, 1, 1, 0)"#,
        r#"INSERT INTO entity_int (entity_id, attribute_id, store_id, value) VALUES (2, 2, 0, 0)"#,
        r#"INSERT INTO entity_json (entity_id, attribute_id, store_id, value) VALUES (1, 1, 0, '{"flag": true, "featured": false, "labels": ["red", "blue", "green"]}')"#,
        r#"INSERT INTO entity_json (entity_id, attribute_id, store_id, value) VALUES (2, 2, 0, '{"flag": false, "featured": true, "labels": ["red", "blue", "purple"]}')"#,
        r#"COMMIT"#,
        r#"START TRANSACTION"#,
        r#"UPDATE entity SET name = 'Awesome Product 2' WHERE entity_id = 2"#,
        r#"UPDATE entity SET price = 99.8 WHERE entity_id = 1"#,
        r#"DELETE FROM entity_int WHERE entity_id = 1 and store_id = 1 and attribute_id = 1"#,
        r#"UPDATE entity_json SET value = JSON_SET(value, '$.season', '["winter", "spring"]')"#,
        r#"COMMIT"#,
    ]).await?;

    let mut binlog_stream = create_binary_log_stream(binlog_position).await?;

    let mut table_binaries = HashMap::new();
    let mut event_rows = HashMap::new();
    let mut events = vec![];

    while let Some(Ok(event)) = binlog_stream.next().await {
        match event.header().event_type() {
            Ok(EventType::WRITE_ROWS_EVENT | EventType::DELETE_ROWS_EVENT | EventType::UPDATE_ROWS_EVENT | EventType::PARTIAL_UPDATE_ROWS_EVENT) => events.push(event),
            _ => {}
        }
    }

    for event in events {
       if let Ok(Some(EventData::RowsEvent(rows_data))) = event.read_data() {
            let table = binlog_stream.get_tme(rows_data.table_id()).unwrap();
            table_binaries.entry(table.table_name()).or_insert_with(|| {
                let mut data = Vec::new();
                table.serialize(&mut data);
                data
            });

            let mut table_event_rows = event_rows.entry(table.table_name()).or_insert(vec![]);

            let mut event_rows_data = Vec::new();
            rows_data.serialize(&mut event_rows_data);
            table_event_rows.push(event_rows_data);
        }
    }
    println!("{table_binaries:?}");
    println!("{event_rows:?}");
    Ok(())
}

async fn create_binary_log_stream(binlog_at: (Vec<u8>, u64)) -> Result<BinlogStream, MySQLError> {
    let mut connection = create_connection().await?;

    let (binlog_file, binlog_pos) = binlog_at;

    let replication_request = BinlogRequest::new(42)
        .with_filename(Cow::from(binlog_file))
        .with_pos(binlog_pos)
        .with_flags(BinlogDumpFlags::BINLOG_DUMP_NON_BLOCK);

    connection.get_binlog_stream(replication_request).await
}

async fn current_binlog_position() -> (Vec<u8>, u64) {
    let mut connection = create_connection().await.unwrap();
    let row: Row = connection.query_first("SHOW MASTER STATUS").await.unwrap().unwrap();

    let binlog_file: Vec<u8> = row.get(0).unwrap();
    let binlog_pos: u64 = row.get(1).unwrap();
    (binlog_file, binlog_pos)
}