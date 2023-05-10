use bitvec::macros::internal::funty::Fundamental;
use clap::{arg, Parser};
use mage_os_database_changelog::aggregate::{Aggregate, ChangeAggregateDataKey, ProductAggregate};
use mage_os_database_changelog::database::Database;
use mage_os_database_changelog::error::Error;
use mage_os_database_changelog::log::{ChangeLogSender, ItemChange};
use mage_os_database_changelog::mapper::{MagentoTwoMapper, MapperObserver};
use mage_os_database_changelog::replication::{BinlogPosition, ReplicationClient};
use mysql_async::Pool;
use serde_json::{json, to_string};
use tokio::io::{stdout, AsyncWriteExt};
use tokio::sync::mpsc::channel;
use tokio::task::JoinHandle;

const HARDCODED_MAX_LENGTH: usize = 5000;

/// Mage-OS Database Changelog reader
#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    /// Connection URL for database
    #[arg(short, long)]
    url: String,

    /// Name of the database to monitor changes in
    #[arg(short, long)]
    database_name: String,

    /// Table prefix if any
    #[arg(short, long)]
    table_prefix: Option<String>,

    /// Binlog file name
    #[arg(short, long)]
    file: String,

    /// Binlog file position
    #[arg(short, long)]
    position: u32,

    /// Number of times to greet
    #[arg(short, long, default_value_t = 1)]
    count: u8,
}

async fn write_to_stdout(change: &mut impl Aggregate) -> Result<(), Error> {
    let change = match change.flush() {
        Some(value) => value,
        None => return Ok(()),
    };

    let mut output = stdout();

    let mut json = serde_json::Value::Object(Default::default());
    {
        let object = json.as_object_mut().unwrap();
        object.insert("entity".into(), change.entity.into());
        object.insert(
            "metadata".into(),
            json!({
                "timestamp": change.metadata.timestamp(),
                "file": change.metadata.binlog_position().file(),
                "position": change.metadata.binlog_position().position(),
            }),
        );

        for (key, value) in change.data {
            match key {
                ChangeAggregateDataKey::Key(global_key) => {
                    object
                        .entry("field_global")
                        .or_insert(json!({}))
                        .as_object_mut()
                        .unwrap()
                        .insert(global_key.into(), value.into());
                }
                ChangeAggregateDataKey::Attribute(attribute_key) => {
                    object
                        .entry("attributes")
                        .or_insert(json!({}))
                        .as_object_mut()
                        .unwrap()
                        .insert(attribute_key.to_string(), value.into());
                }
                ChangeAggregateDataKey::KeyAndScopeInt(key, scope) => {
                    object
                        .entry("field_scoped")
                        .or_insert(json!({}))
                        .as_object_mut()
                        .unwrap()
                        .entry(scope.to_string())
                        .or_insert(json!({}))
                        .as_object_mut()
                        .unwrap()
                        .insert(key.into(), value.into());
                }
                ChangeAggregateDataKey::KeyAndScopeStr(key, scope) => {
                    object
                        .entry("field_scopded")
                        .or_insert(json!({}))
                        .as_object_mut()
                        .unwrap()
                        .entry(scope.to_string())
                        .or_insert(json!({}))
                        .as_object_mut()
                        .unwrap()
                        .insert(key.into(), value.into());
                }
            }
        }
    }

    output
        .write_all(
            to_string(&json)
                .map_err(|_| Error::Synchronization)?
                .as_bytes(),
        )
        .await?;

    output.write_u8('\n'.as_u8()).await?;

    Ok(())
}

async fn create_writer() -> (
    impl ChangeLogSender<Item = ItemChange>,
    JoinHandle<Result<(), Error>>,
) {
    let (sender, mut receiver) = channel(10000);

    let handle = tokio::spawn(async move {
        let mut aggregate = ProductAggregate::default();

        while let Some(item) = receiver.recv().await {
            aggregate.push(item);

            if aggregate.size() > HARDCODED_MAX_LENGTH {
                write_to_stdout(&mut aggregate).await?
            }
        }

        write_to_stdout(&mut aggregate).await
    });

    (sender, handle)
}

#[tokio::main]
async fn main() -> Result<(), Error> {
    let args = Args::parse();
    let database = Database::from_pool(Pool::from_url(args.url).map_err(Error::MySQLError)?);

    let (sender, handle) = create_writer().await;

    let client = ReplicationClient::new(
        database,
        args.database_name,
        args.table_prefix.unwrap_or_default(),
    );

    client
        .process(
            MapperObserver::from((MagentoTwoMapper, sender)),
            BinlogPosition::new(args.file, args.position),
        )
        .await?;

    handle.await.map_err(|_| Error::Synchronization)?
}
