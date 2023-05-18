use crate::aggregate::{Aggregate, ProductAggregate};
use crate::app::{ApplicationConfig, ApplicationOutput};
use crate::database::Database;
use crate::error::Error;
use crate::log::{ChangeLogSender, ItemChange};
use crate::mapper::{ChangeLogMapper, MagentoTwoMapper, MapperObserver};
use crate::output::{Output};
use crate::replication::{BinlogPosition, ReplicationClient};

use mysql_common::packets::BinlogDumpFlags;
use serde_json::json;
use tokio::io::stdout;
use tokio::sync::mpsc::channel;
use tokio::task::JoinHandle;

pub enum ApplicationCommand {
    Position(ApplicationConfig),
    Dump(ApplicationConfig, ApplicationOutput, BinlogPosition),
    Watch(ApplicationConfig, ApplicationOutput, BinlogPosition),
}

pub struct Application<M = MagentoTwoMapper>
where
    M: ChangeLogMapper<ItemChange>,
{
    mapper: M,
}

async fn write_to_stdout(
    output: &ApplicationOutput,
    change: &mut impl Aggregate,
) -> Result<(), Error> {
    let change = match change.flush() {
        Some(value) => value,
        None => return Ok(()),
    };

    let mut stdout = stdout();

    output.write(&mut stdout, change).await?;

    Ok(())
}

async fn create_writer(
    output: ApplicationOutput,
    config: ApplicationConfig,
) -> (
    impl ChangeLogSender<Item = ItemChange>,
    JoinHandle<Result<(), Error>>,
) {
    let (sender, mut receiver) = channel(10000);

    let handle = tokio::spawn(async move {
        let mut aggregate = ProductAggregate::default();

        while let Some(item) = receiver.recv().await {
            aggregate.push(item);

            if aggregate.size() > config.batch_size() {
                write_to_stdout(&output, &mut aggregate).await?
            }
        }

        write_to_stdout(&output, &mut aggregate).await
    });

    (sender, handle)
}

impl Application {
    pub fn new() -> Self {
        Self {
            mapper: MagentoTwoMapper,
        }
    }
    async fn run_binlog_client(
        self,
        database: Database,
        config: ApplicationConfig,
        output: ApplicationOutput,
        position: BinlogPosition,
    ) -> Result<(), Error> {
        let (sender, handle) = create_writer(output, config.clone()).await;

        let client = ReplicationClient::new(database, config.database(), config.table_prefix());

        client
            .process(MapperObserver::from((self.mapper, sender)), position)
            .await?;

        handle.await.map_err(|_| Error::Synchronization)?
    }

    pub async fn run(self, command: ApplicationCommand) -> Result<(), Error> {
        match command {
            ApplicationCommand::Position(config) => {
                let mut database = config.create_database();

                let position = database.binlog_position().await?;

                println!(
                    "{}",
                    serde_json::to_string_pretty(&json!({
                        "file": position.file(),
                        "position": position.position(),
                    }))
                    .map_err(|_| Error::OutputError)?
                );
            }
            ApplicationCommand::Dump(config, output, position) => {
                self.run_binlog_client(config.create_database(), config, output, position)
                    .await?
            }

            ApplicationCommand::Watch(config, output, position) => {
                self.run_binlog_client(
                    config
                        .create_database()
                        .with_dump_options(BinlogDumpFlags::empty()),
                    config,
                    output,
                    position,
                )
                .await?
            }
        };

        Ok(())
    }
}
