use crate::error::Error;
use crate::replication::BinlogPosition;
use crate::schema::SchemaInformation;
use mysql_async::prelude::Queryable;
use mysql_async::{BinlogStream, Conn, Opts, Pool};
use mysql_common::packets::binlog_request::BinlogRequest;
use mysql_common::packets::BinlogDumpFlags;

#[derive(Clone, Debug)]
pub struct Database {
    pool: Pool,
}

impl Database {
    pub fn new<O>(opts: O) -> Self
    where
        Opts: From<O>,
    {
        Self {
            pool: Pool::new(opts),
        }
    }

    pub fn from_pool(pool: Pool) -> Self {
        Self { pool }
    }

    pub async fn acquire_connection(&self) -> Result<Conn, Error> {
        self.pool.get_conn().await.map_err(Error::MySQLError)
    }

    pub async fn binlog_stream(&self, position: &BinlogPosition) -> Result<BinlogStream, Error> {
        let mut connection = self.acquire_connection().await?;

        let server_id = connection
            .query_first("SELECT @@server_id;")
            .await
            .map_err(Error::MySQLError)?
            .unwrap_or(1);

        connection
            .get_binlog_stream(
                BinlogRequest::new(server_id)
                    .with_filename(position.file().as_bytes())
                    .with_pos(position.position())
                    .with_flags(BinlogDumpFlags::BINLOG_DUMP_NON_BLOCK),
            )
            .await
            .map_err(Error::MySQLError)
    }
}
