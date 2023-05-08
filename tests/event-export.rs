use mysql_async::binlog::events::{EventData, TableMapEvent};
use mysql_async::binlog::EventType;

use mysql_async::Error as MySQLError;

use std::collections::btree_map::Entry;
use std::collections::BTreeMap;

use tokio_stream::{self, StreamExt};

mod fixture;
use fixture::*;
use mage_os_database_changelog::error::Error;
use mage_os_database_changelog::replication::BinlogPosition;

#[tokio::test]
async fn writes_to_entity_tables() -> Result<(), Error> {
    let mut fixture = Fixture::create_with_database("test_db").await?;

    let binlog_position = fixture.binlog_position().await?;

    write_entity_data(&mut fixture).await?;

    Ok(print_binlog_events(fixture.copy().await?, binlog_position).await?)
}

#[tokio::test]
async fn updates_to_entity_table() -> Result<(), Error> {
    let mut fixture = Fixture::create_with_database("test_db").await?;

    write_entity_data(&mut fixture).await?;

    let binlog_position = fixture.binlog_position().await?;

    fixture
        .execute_queries(vec![
            r#"START TRANSACTION"#,
            r#"UPDATE entity SET name = 'Awesome Product 2' WHERE entity_id = 2"#,
            r#"UPDATE entity SET price = 99.8 WHERE entity_id = 1"#,
            r#"COMMIT"#,
        ])
        .await?;

    Ok(print_binlog_events(fixture.copy().await?, binlog_position).await?)
}

#[tokio::test]
async fn delete_int_values() -> Result<(), Error> {
    let mut fixture = Fixture::create_with_database("test_db").await?;

    write_entity_data(&mut fixture).await?;

    let binlog_position = fixture.binlog_position().await?;

    fixture
        .execute_queries(vec![
            r#"DELETE FROM entity_int WHERE entity_id = 1 and store_id = 1 and attribute_id = 1"#,
        ])
        .await?;

    Ok(print_binlog_events(fixture.copy().await?, binlog_position).await?)
}

#[tokio::test]
async fn update_single_json_field() -> Result<(), Error> {
    let mut fixture = Fixture::create_with_database("test_db").await?;

    write_entity_data(&mut fixture).await?;

    let binlog_position = fixture.binlog_position().await?;

    fixture
        .execute_queries(vec![
            r#"UPDATE entity_json SET value = JSON_SET(value, '$.season', CAST('["winter", "spring"]' AS JSON))"#,
        ])
        .await?;

    Ok(print_binlog_events(fixture.copy().await?, binlog_position).await?)
}

#[tokio::test]
async fn update_complex_json_table() -> Result<(), Error> {
    let mut fixture = Fixture::create_with_database("test_db").await?;

    write_entity_data(&mut fixture).await?;

    let binlog_position = fixture.binlog_position().await?;

    fixture
        .execute_queries(vec![
            r#"START TRANSACTION"#,
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
                    first_column, '$.color', CAST('["red", "blue"]' AS JSON)
                )
                WHERE entity_id = 1
            "#,
            r#"COMMIT"#,
        ])
        .await?;

    Ok(print_binlog_events(fixture.copy().await?, binlog_position).await?)
}

#[tokio::test]
async fn nullable_entity_write_write() -> Result<(), Error> {
    let mut fixture = Fixture::create_with_database("test_db").await?;

    let binlog_position = fixture.binlog_position().await?;
    fixture
        .insert_into(
            "entity",
            ["entity_id", "name", "description", "price"],
            vec![
                (1, "Product 1", None::<&str>, 9.99),
                (2, "Product 2", None::<&str>, 99.99),
            ],
        )
        .await?;

    Ok(print_binlog_events(fixture.copy().await?, binlog_position).await?)
}

async fn print_binlog_events(
    fixture: Fixture,
    binlog_position: BinlogPosition,
) -> Result<(), Error> {
    let mut binlog_stream = fixture
        .copy()
        .await?
        .into_binary_log_stream(binlog_position)
        .await?;

    let mut table_binaries = BTreeMap::<String, Vec<u8>>::new();
    let mut event_rows = BTreeMap::<String, Vec<Vec<u8>>>::new();
    let mut events = vec![];

    while let Some(Ok(event)) = binlog_stream.next().await {
        match event.header().event_type() {
            Ok(
                EventType::WRITE_ROWS_EVENT
                | EventType::DELETE_ROWS_EVENT
                | EventType::UPDATE_ROWS_EVENT
                | EventType::PARTIAL_UPDATE_ROWS_EVENT,
            ) => {
                let data = match event.read_data() {
                    Ok(Some(EventData::RowsEvent(rows))) => rows,
                    _ => continue,
                };

                let table = binlog_stream.get_tme(data.table_id()).unwrap();

                if !fixture.database_name_filter(table) {
                    continue;
                }

                events.push(event)
            }
            Ok(EventType::TABLE_MAP_EVENT) => {
                let table: TableMapEvent = event.read_event().unwrap();
                if !fixture.database_name_filter(&table) {
                    continue;
                }

                let table_name = format!("{}-{}", table.table_id(), table.table_name());
                let entry = match table_binaries.entry(table_name) {
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
            let table_event_rows = event_rows
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
            let table_name = format!("{}-{}", table.table_id(), table.table_name());
            for row in rows_event.rows(&table) {
                match row {
                    Ok((Some(left), Some(right))) => {
                        println!(
                            "Table {}: \n Before: {:?} \n After: {:?} \n",
                            table_name,
                            left.unwrap(),
                            right.unwrap()
                        )
                    }
                    Ok((None, Some(right))) => {
                        println!("Table {}: \n After: {:?} \n", table_name, right.unwrap())
                    }

                    Ok((Some(left), None)) => {
                        println!("Table {}: \n Before: {:?} \n", table_name, left.unwrap())
                    }
                    Err(err) => {
                        println!("Error: {}", err)
                    }
                    _ => continue,
                }
            }
        }
    }

    println!("{table_binaries:?}");
    println!("{event_rows:?}");
    fixture.cleanup().await?;
    Ok(())
}

async fn write_entity_data(fixture: &mut Fixture) -> Result<(), Error> {
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

    Ok(())
}
