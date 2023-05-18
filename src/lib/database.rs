use crate::error::Error;
use crate::replication::BinlogPosition;
use std::cmp::max;

use mysql_async::prelude::Queryable;
use mysql_async::{BinlogStream, Conn, Opts, Pool};
use mysql_common::packets::binlog_request::BinlogRequest;
use mysql_common::packets::BinlogDumpFlags;
use mysql_common::row::Row;

#[derive(Clone, Debug)]
pub struct Database {
    pool: Pool,
    dump_options: BinlogDumpFlags,
}

impl Database {
    pub fn new<O>(opts: O) -> Self
    where
        Opts: From<O>,
    {
        Self {
            pool: Pool::new(opts),
            dump_options: BinlogDumpFlags::BINLOG_DUMP_NON_BLOCK,
        }
    }

    pub fn from_pool(pool: Pool) -> Self {
        Self {
            pool,
            dump_options: BinlogDumpFlags::BINLOG_DUMP_NON_BLOCK,
        }
    }

    pub fn with_dump_options(self, options: BinlogDumpFlags) -> Self {
        Self {
            dump_options: options,
            ..self
        }
    }

    pub async fn acquire_connection(&self) -> Result<Conn, Error> {
        self.pool.get_conn().await.map_err(Error::MySQLError)
    }

    pub async fn binlog_stream(&self, position: &BinlogPosition) -> Result<BinlogStream, Error> {
        let mut connection = self.acquire_connection().await?;

        let server_id = connection
            .query_first("SELECT @@server_id")
            .await?
            .unwrap_or(1);

        let server_id = connection
            .query_fold("SHOW SLAVE HOSTS", server_id, |init, row: Row| {
                max(init, row.get::<u32, _>(0).unwrap_or(0))
            })
            .await
            .map_err(|_| Error::BinlogPositionMissing)?;

        Ok(connection
            .get_binlog_stream(
                BinlogRequest::new(server_id + 1)
                    .with_filename(position.file().as_bytes())
                    .with_pos(position.position())
                    .with_flags(self.dump_options),
            )
            .await?)
    }

    pub async fn binlog_position(&mut self) -> Result<BinlogPosition, Error> {
        let row: Row = self
            .acquire_connection()
            .await?
            .query_first("SHOW MASTER STATUS")
            .await?
            .ok_or(Error::BinlogPositionMissing)?;

        Ok(BinlogPosition::new(
            row.get::<String, _>(0)
                .ok_or(Error::BinlogPositionMissing)?,
            row.get(1).ok_or(Error::BinlogPositionMissing)?,
        ))
    }
}
