use std::borrow::Cow;
use std::time::Duration;
use mysql_async::{BinlogStream, Error};
use mysql_async::binlog::events::{TableMapEvent, Event};
use mysql_async::binlog::EventType;
use tokio::time::timeout;
use tokio_stream::StreamExt;
use crate::replication::ReplicationObserver;

pub struct ReplicationReader<'a>
{
    timeout: Duration,
    database: Cow<'a, str>
}

impl<'a> ReplicationReader<'a> {
    pub fn new(database: impl Into<Cow<'a, str>>, timeout: Duration)
        -> Self
    {
        Self {
            database: database.into(),
            timeout
        }
    }

    pub async fn read_binlog_events(&self, mut binlog_stream: BinlogStream, observer: &mut impl ReplicationObserver)
        -> Result<(), Error> {
        loop {
            let next_event = timeout(self.timeout, binlog_stream.next()).await;

            match next_event {
                Ok(Some(Ok(event))) => {
                    self.process_binlog_event(event, &binlog_stream, observer);
                },
                Ok(Some(Err(error))) => {
                    return Err(error)
                }
                Ok(None) | Err(_)=> break,
            }
        }

        binlog_stream.close().await
    }

    fn process_binlog_event(&self, event: Event, binlog_stream: &BinlogStream, observer: &mut impl ReplicationObserver) {
        println!("{:?}", event);
    }
}