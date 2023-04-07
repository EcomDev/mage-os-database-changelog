use std::borrow::Cow;
use std::error::Error;
use std::time::Duration;
use mysql_async::{BinlogDumpFlags, BinlogRequest, BinlogStream, Conn, Row};
use mysql_async::binlog::events::Event;
use mysql_async::binlog::EventType;
use mysql_async::Error as MySQLError;
use mysql_async::prelude::Queryable;

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
        connection.query_drop(query).await?
    }

    Ok(())
}

struct EventsObserver {

}

impl ReplicationObserver for EventsObserver {
    fn update_row(&mut self) {
        todo!()
    }

    fn write_row(&mut self) {
        todo!()
    }

    fn delete_row(&mut self) {
        todo!()
    }

    fn update_partial_row(&mut self) {
        todo!()
    }
}

#[tokio::test]
async fn it_reads_changes_in_mysql() -> Result<(), MySQLError> {
    let reader = ReplicationReader::new(
        "test_db",
        Duration::from_micros(100)
    );

    let mut observer = EventsObserver {};

    let binlog_stream = create_binary_log_stream().await?;

    execute_queries(vec![
        r#"CREATE DATABASE test_db"#,
        r#"use test_db"#,
        r#"CREATE TABLE test_db (entity_id INT UNSIGNED NOT NULL, value_int INT NOT NULL, PRIMARY KEY(entity_id))"#,
        r#"DROP DATABASE test_db"#,
    ]).await;

    reader.read_binlog_events(binlog_stream, &mut observer).await?;

    /*connection.get_binlog_stream(BinlogRequest::new(32));
    reader.read_events(connection, )*/

    Ok(())
}

async fn create_binary_log_stream() -> Result<BinlogStream, MySQLError> {
    let mut connection = create_connection().await?;
    let row: Row =
        connection.query_first("SHOW MASTER STATUS").await?.unwrap();

    let binlog_file: Vec<u8> = row.get(0).unwrap();
    let binlog_pos: u64 = row.get(1).unwrap();

    let replication_request = BinlogRequest::new(42)
        .with_filename(Cow::from(binlog_file))
        .with_pos(binlog_pos)
        .with_flags(BinlogDumpFlags::BINLOG_DUMP_NON_BLOCK);

    connection.get_binlog_stream(replication_request).await
}