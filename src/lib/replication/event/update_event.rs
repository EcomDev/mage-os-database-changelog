use crate::error::Error;
use crate::replication::BinaryRow;
use crate::schema::TableSchema;

use mysql_common::value::convert::FromValue;

#[derive(Clone, PartialEq, Debug)]
pub struct UpdateRowEvent {
    before: BinaryRow,
    after: BinaryRow,
}

impl UpdateRowEvent {
    /// Creates a new update event
    pub fn new(before: BinaryRow, after: BinaryRow) -> Self {
        Self { before, after }
    }

    pub fn parse<T>(&self, column: impl AsRef<str>, schema: &impl TableSchema) -> Result<T, Error>
    where
        T: FromValue,
    {
        self.before.parse(column, schema)
    }

    pub fn parse_changed<T>(
        &self,
        column: impl AsRef<str>,
        schema: &impl TableSchema,
    ) -> Result<T, Error>
    where
        T: FromValue,
    {
        self.after.parse(column, schema)
    }

    pub fn is_changed_column(&self, column: impl AsRef<str>, schema: &impl TableSchema) -> bool {
        let position = match schema.column_position(&column) {
            Some(position) => position,
            _ => return false,
        };

        match (self.before.get(position), self.after.get(position)) {
            (Some(left), Some(right)) => left.ne(right),
            (None, None) => false,
            _ => true,
        }
    }
}

#[cfg(test)]
mod tests {

    use crate::replication::UpdateRowEvent;
    use crate::test_util::IntoBinlogValue;

    #[test]
    fn takes_value_from_before_column() {
        let updates = UpdateRowEvent::new(
            binlog_row!("sku1", "Name Before"),
            binlog_row!("sku1", "Name After"),
        );

        let schema = test_table!("entity", ["sku", "name"]);

        assert_eq!(
            updates.parse::<String>("name", &schema).unwrap(),
            "Name Before"
        );
    }

    #[test]
    fn takes_changed_value_from_after_column() {
        let updates = UpdateRowEvent::new(
            binlog_row!("sku1", "Name Before"),
            binlog_row!("sku1", "Name After"),
        );

        let schema = test_table!("entity", ["sku", "name"]);

        assert_eq!(
            updates.parse_changed::<String>("name", &schema).unwrap(),
            "Name After"
        );
    }

    #[test]
    fn takes_only_changed_value_from_after_row() {
        let updates =
            UpdateRowEvent::new(binlog_row!(1, "Name Before"), binlog_row!(1, "Name After"));

        let schema = test_table!("entity", ["entity_id", "name"]);

        assert_eq!(
            vec![
                updates.is_changed_column("entity_id", &schema),
                updates.is_changed_column("name", &schema)
            ],
            vec![false, true]
        );
    }
}
