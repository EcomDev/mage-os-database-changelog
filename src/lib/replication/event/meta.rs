use std::sync::Arc;

#[derive(Debug, PartialEq, Eq)]
pub struct BinlogPosition {
    file: Arc<str>,
    position: u32,
}

impl Default for BinlogPosition {
    fn default() -> Self {
        Self {
            file: Arc::from(""),
            position: 0,
        }
    }
}

impl Clone for BinlogPosition {
    fn clone(&self) -> Self {
        Self {
            file: Arc::clone(&self.file),
            position: self.position,
        }
    }
}

impl BinlogPosition {
    pub fn new(file: impl Into<Arc<str>>, position: u32) -> Self {
        Self {
            file: file.into(),
            position,
        }
    }

    pub fn position(&self) -> u32 {
        self.position
    }

    pub fn file(&self) -> &str {
        &self.file
    }

    pub fn with_position(self, position: u32) -> Self {
        Self { position, ..self }
    }

    pub fn with_file(self, file: impl Into<Arc<str>>) -> Self {
        Self {
            file: file.into(),
            ..self
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct EventMetadata {
    timestamp: usize,
    binlog_position: BinlogPosition,
}

impl EventMetadata {
    pub fn new(timestamp: usize, binlog_position: BinlogPosition) -> Self {
        Self {
            timestamp,
            binlog_position,
        }
    }

    pub fn timestamp(&self) -> usize {
        self.timestamp
    }

    pub fn binlog_position(&self) -> &BinlogPosition {
        &self.binlog_position
    }
}

#[cfg(test)]
mod tests {
    use crate::replication::event::meta::BinlogPosition;
    use crate::replication::test_fixture::Fixture;
    use mysql_common::binlog::consts::{EventFlags, EventType};
    use mysql_common::binlog::events::BinlogEventHeader;

    #[test]
    fn creates_binlog_file_from_string() {
        let log = BinlogPosition::new("binlogfile.0000", 390);

        assert_eq!(log.file(), "binlogfile.0000");
        assert_eq!(log.position(), 390);
    }

    #[test]
    fn updates_position_on_existing_binlog_item() {
        let log = BinlogPosition::new("binlogfile.0000", 390);

        let log = log.with_position(4001);

        assert_eq!(log.file(), "binlogfile.0000");
        assert_eq!(log.position(), 4001);
    }

    #[test]
    fn updates_file_on_existing_binlog_item() {
        let log = BinlogPosition::new("binlogfile.0000", 390);

        let log = log.with_file("binlogfile.0001");

        assert_eq!(log.file(), "binlogfile.0001");
    }
}
