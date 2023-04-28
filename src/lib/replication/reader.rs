use std::time::Duration;

pub struct ReplicationReader<D, T> {
    timeout: Duration,
    database: D,
    table_prefix: T,
}

impl<D, T> ReplicationReader<D, T>
where
    D: AsRef<str>,
    T: AsRef<str>,
{
    pub fn new(database: D, table_prefix: T, timeout: Duration) -> Self {
        Self {
            database,
            table_prefix,
            timeout,
        }
    }
}
