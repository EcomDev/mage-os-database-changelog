use bitvec::macros::internal::funty::Fundamental;
use clap::{arg, Parser};
use mage_os_database_changelog::aggregate::{Aggregate, ChangeAggregateKey, ProductAggregate};
use mage_os_database_changelog::database::Database;
use mage_os_database_changelog::error::Error;
use mage_os_database_changelog::log::{ChangeLogSender, ItemChange};
use mage_os_database_changelog::mapper::{MagentoTwoMapper, MapperObserver};
use mage_os_database_changelog::output::{JsonOutput, Output};
use mage_os_database_changelog::replication::{BinlogPosition, ReplicationClient};
use mysql_async::Pool;
use tokio::io::stdout;
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

    JsonOutput.write(&mut output, change).await?;

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
