use mysql_async::prelude::*;
use mysql_async::Error;
use mysql_async::{BinlogStream, Conn};
use mysql_common::binlog::events::TableMapEvent;

use mysql_common::packets::binlog_request::BinlogRequest;
use mysql_common::packets::BinlogDumpFlags;

use mysql_common::row::Row;
use mysql_common::value::Value;
use std::borrow::Cow;




use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{OnceLock};

pub struct Fixture {
    conn: Conn,
    database_name: Option<(&'static str, usize)>,
}

static DATABASE_SCHEMA: [&'static str; 4] = [
    r#"CREATE TABLE entity (
            entity_id INT UNSIGNED NOT NULL AUTO_INCREMENT,
            name VARCHAR(255),
            description TEXT,
            price DECIMAL(12, 4) DEFAULT '0.00',
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
    r#"CREATE TABLE entity_with_multiple_json (
            value_id INT UNSIGNED NOT NULL AUTO_INCREMENT,            
            first_column JSON,
            entity_id INT UNSIGNED NOT NULL,
            store_id INT UNSIGNED NOT NULL,
            second_column JSON,
            some_int_flag INT UNSIGNED DEFAULT 0,
            PRIMARY KEY(value_id),
            INDEX `attribute_store_entity` (`entity_id`,`store_id`)
        )"#,
];

static DATABASE_NUMBER: OnceLock<AtomicUsize> = OnceLock::new();

impl Fixture {
    pub async fn create_with_database(prefix_database: &'static str) -> Result<Self, Error> {
        let mut connection = create_connection().await?;
        let database_index = DATABASE_NUMBER.get_or_init(Default::default);
        let database_index = database_index.fetch_add(1, Ordering::Relaxed);

        connection
            .query_drop(format!(
                "DROP DATABASE IF EXISTS {prefix_database}{database_index}"
            ))
            .await?;
        connection
            .query_drop(format!("CREATE DATABASE {prefix_database}{database_index}"))
            .await?;

        connection
            .query_drop(format!("USE {prefix_database}{database_index}"))
            .await?;

        for query in DATABASE_SCHEMA {
            connection.query_drop(query).await?;
        }

        Ok(Self {
            conn: connection,
            database_name: Some((prefix_database, database_index)),
        })
    }

    pub async fn copy(&self) -> Result<Self, Error> {
        Ok(Self {
            conn: create_connection().await?,
            database_name: self.database_name.clone(),
        })
    }

    pub fn database_name_filter(&self, table: &TableMapEvent) -> bool {
        match self.database_name {
            Some((prefix, index)) => table
                .database_name()
                .eq_ignore_ascii_case(&format!("{prefix}{index}")),
            _ => true,
        }
    }

    pub async fn create_connection() -> Result<Conn, Error> {
        create_connection().await
    }

    pub fn database_name(&self) -> Option<Cow<str>> {
        match self.database_name {
            Some((prefix, index)) => Some(Cow::Owned(format!("{prefix}{index}"))),
            None => None,
        }
    }

    pub async fn cleanup(mut self) -> Result<(), Error> {
        match self.database_name {
            Some((prefix, index)) => {
                self.conn
                    .query_drop(format!("DROP DATABASE {prefix}{index}"))
                    .await
            }
            None => Ok(()),
        }
    }

    pub async fn execute_queries(
        &mut self,
        queries: impl IntoIterator<Item = impl AsQuery>,
    ) -> Result<(), Error> {
        for query in queries {
            self.conn.query_drop(query).await?;
        }
        Ok(())
    }

    pub async fn insert_into<const N: usize>(
        &mut self,
        table: &'static str,
        columns: [&'static str; N],
        rows: Vec<impl BatchRow>,
    ) -> Result<(), Error> {
        let column_expr = columns.join("`,`");
        let single_row_expr = format!("({})", columns.map(|_| "?").join(","));

        let rows_expr = rows
            .iter()
            .map(|_| single_row_expr.clone())
            .collect::<Vec<_>>()
            .join(",");

        let query = format!("INSERT INTO `{table}` (`{column_expr}`) VALUES {rows_expr}");

        let rows_count = rows.len();

        let params =
            rows.into_iter()
                .fold(Vec::with_capacity(rows_count * N), |mut params, row| {
                    row.add_to_params(&mut params);
                    params
                });

        self.conn.exec_drop(query, params).await
    }

    pub async fn binlog_position(&mut self) -> Result<(Vec<u8>, u64), Error> {
        let row: Row = self
            .conn
            .query_first("SHOW MASTER STATUS")
            .await
            .unwrap()
            .unwrap();

        let binlog_file: Vec<u8> = row.get(0).unwrap();
        let binlog_pos: u64 = row.get(1).unwrap();
        Ok((binlog_file, binlog_pos))
    }

    pub async fn into_binary_log_stream(
        self,
        (binlog_file, binlog_pos): (Vec<u8>, u64),
    ) -> Result<BinlogStream, Error> {
        let replication_request = BinlogRequest::new(42)
            .with_filename(Cow::from(binlog_file))
            .with_pos(binlog_pos)
            .with_flags(BinlogDumpFlags::BINLOG_DUMP_NON_BLOCK);

        self.conn.get_binlog_stream(replication_request).await
    }
}

async fn create_connection() -> Result<Conn, Error> {
    Conn::from_url(std::env::var("TEST_MYSQL_URL").unwrap()).await
}

trait BatchRow {
    const LENGTH: usize;

    fn add_to_params(self, params: &mut Vec<Value>);
}

impl<T1, T2> BatchRow for (T1, T2)
where
    T1: Into<Value>,
    T2: Into<Value>,
{
    const LENGTH: usize = 2;

    fn add_to_params(self, params: &mut Vec<Value>) {
        params.push(self.0.into());
        params.push(self.1.into());
    }
}

impl<T1, T2, T3> BatchRow for (T1, T2, T3)
where
    T1: Into<Value>,
    T2: Into<Value>,
    T3: Into<Value>,
{
    const LENGTH: usize = 3;

    fn add_to_params(self, params: &mut Vec<Value>) {
        params.push(self.0.into());
        params.push(self.1.into());
        params.push(self.2.into());
    }
}

impl<T1, T2, T3, T4> BatchRow for (T1, T2, T3, T4)
where
    T1: Into<Value>,
    T2: Into<Value>,
    T3: Into<Value>,
    T4: Into<Value>,
{
    const LENGTH: usize = 4;

    fn add_to_params(self, params: &mut Vec<Value>) {
        params.push(self.0.into());
        params.push(self.1.into());
        params.push(self.2.into());
        params.push(self.3.into());
    }
}

impl<T1, T2, T3, T4, T5> BatchRow for (T1, T2, T3, T4, T5)
where
    T1: Into<Value>,
    T2: Into<Value>,
    T3: Into<Value>,
    T4: Into<Value>,
    T5: Into<Value>,
{
    const LENGTH: usize = 5;

    fn add_to_params(self, params: &mut Vec<Value>) {
        params.push(self.0.into());
        params.push(self.1.into());
        params.push(self.2.into());
        params.push(self.3.into());
        params.push(self.4.into());
    }
}
