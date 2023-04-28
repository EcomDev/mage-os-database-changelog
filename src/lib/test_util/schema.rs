use crate::replication::BinaryRow;
use crate::schema::TableSchema;
use mysql_common::binlog::value::BinlogValue;
use phf::{phf_map, Map};
use std::borrow::Cow;
use std::collections::HashMap;

static TEST_TABLE_SCHEMA: Map<&'static str, TestTableSchema> = phf_map! {
    "product" => test_table!(
        "catalog_product_entity",
        "entity_id",
        ["entity_id", "attribute_set_id", "type_id", "sku", "has_options", "required_options", "created_at", "updated_at"]
    ),
    "category" => test_table!(
        "catalog_category_entity",
        "entity_id",
        ["entity_id", "attribute_set_id", "parent_id", "created_at", "updated_at", "path", "position", "level", "children_count"]
    ),

};

#[derive(Clone, Copy)]
pub struct TestTableSchema {
    table_name: &'static str,
    column_position: &'static [&'static str],
    primary_key: Option<&'static str>,
}

impl TestTableSchema {
    pub const fn new(table_name: &'static str, column_position: &'static [&'static str]) -> Self {
        Self::with_primary(table_name, column_position, None)
    }

    pub const fn with_primary(
        table_name: &'static str,
        column_position: &'static [&'static str],
        primary: Option<&'static str>,
    ) -> Self {
        Self {
            table_name,
            column_position,
            primary_key: primary,
        }
    }

    pub fn binary_row(
        &self,
        value: impl IntoIterator<Item = (&'static str, BinlogValue<'static>)>,
    ) -> BinaryRow {
        let mut hash_map: HashMap<&'static str, BinlogValue<'static>> =
            HashMap::from_iter(value.into_iter());

        BinaryRow::new(
            &self
                .column_position
                .iter()
                .map(|col| hash_map.remove(*col))
                .collect::<Vec<_>>(),
        )
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
        self.column_position
            .iter()
            .find(|col| **col == column.as_ref())
            .is_some()
    }

    fn column_position(&self, column: impl AsRef<str>) -> Option<usize> {
        self.column_position
            .iter()
            .position(|col| *col == column.as_ref())
    }
}

pub fn table_schema(table: &'static str) -> &TestTableSchema {
    TEST_TABLE_SCHEMA.get(table).unwrap()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::TableSchema;

    #[test]
    fn empty_table_does_not_have_any_columns() {
        let table = test_table!("entity");
        assert_eq!(false, table.has_column("entity_id"));
        assert_eq!(None, table.column_position("entity_id"));
        assert_eq!(false, table.is_generated_primary_key("entity_id"));
    }

    #[test]
    fn when_column_position_specified_column_exists() {
        let table = test_table!("entity", ["entity_id", "value"]);

        assert_eq!(table.has_column("entity_id"), true);
    }

    #[test]
    fn returns_different_column_positions() {
        let table = test_table!("entity", ["entity_id", "attribute_id", "value_id"]);

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
        let table = test_table!("entity", "entity_id", []);

        assert_eq!(
            vec![
                table.is_generated_primary_key("entity_id"),
                table.is_generated_primary_key("row_id")
            ],
            vec![true, false]
        );
    }
}
