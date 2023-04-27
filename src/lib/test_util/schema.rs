use crate::schema::TableSchema;
use std::borrow::Cow;
use std::collections::HashMap;

pub struct TestTableSchema {
    table_name: &'static str,
    column_position: HashMap<&'static str, usize>,
    primary_key: Option<&'static str>,
}

impl TestTableSchema {
    pub fn new(table_name: &'static str) -> Self {
        Self {
            table_name,
            column_position: Default::default(),
            primary_key: Default::default(),
        }
    }

    pub fn with_column(mut self, column: &'static str, position: usize) -> Self {
        self.column_position.insert(column, position);
        self
    }

    pub fn with_primary_key(mut self, column: &'static str) -> Self {
        self.primary_key = Some(column);

        self
    }

    pub fn without_primary_key(self) -> Self {
        Self {
            primary_key: None,
            ..self
        }
    }
}

impl TableSchema for TestTableSchema {
    fn table_name(&self) -> &str {
        self.table_name
    }

    fn is_generated_primary_key(&self, column: impl AsRef<str>) -> bool {
        self.primary_key
            .is_some_and(|value| value.eq(column.as_ref()))
    }

    fn has_column(&self, column: impl AsRef<str>) -> bool {
        self.column_position.get(column.as_ref()).is_some()
    }

    fn column_position(&self, column: impl AsRef<str>) -> Option<usize> {
        self.column_position.get(column.as_ref()).copied()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::TableSchema;

    #[test]
    fn empty_table_does_not_have_any_columns() {
        let table = TestTableSchema::new("entity");
        assert_eq!(false, table.has_column("entity_id"));
        assert_eq!(None, table.column_position("entity_id"));
        assert_eq!(false, table.is_generated_primary_key("entity_id"));
    }

    #[test]
    fn when_column_position_specified_column_exists() {
        let table = TestTableSchema::new("entity")
            .with_column("entity_id", 0)
            .with_column("value", 1);

        assert_eq!(table.has_column("entity_id"), true);
    }

    #[test]
    fn returns_different_column_positions() {
        let table = TestTableSchema::new("entity")
            .with_column("entity_id", 0)
            .with_column("attribute_id", 1)
            .with_column("value_id", 2);

        assert_eq!(
            vec![
                table.column_position("entity_id"),
                table.column_position("value_id"),
                table.column_position("row_id"),
            ],
            vec![Some(0), Some(2), None]
        )
    }

    #[test]
    fn returns_primary_key_when_specified() {
        let table = TestTableSchema::new("entity").with_primary_key("entity_id");

        assert_eq!(
            vec![
                table.is_generated_primary_key("entity_id"),
                table.is_generated_primary_key("row_id")
            ],
            vec![true, false]
        );
    }
}
