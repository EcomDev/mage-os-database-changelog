use std::borrow::Cow;
use std::time::Duration;
use mysql_async::{BinlogStream, Error};
use mysql_async::binlog::events::{Event};

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

    async fn process_binlog_event(&self, event: Event, _binlog_stream: &BinlogStream, _observer: &mut impl ReplicationObserver) {
        match event.header().event_type() {
            Ok(EventType::WRITE_ROWS_EVENT | EventType::UPDATE_ROWS_EVENT | EventType::DELETE_ROWS_EVENT | EventType::PARTIAL_UPDATE_ROWS_EVENT ) => {
                println!("{:?}", event);
            },
            _ => println!("{:?}", event)
        }


    }

    fn read_event(_event: Event) {

    }
}