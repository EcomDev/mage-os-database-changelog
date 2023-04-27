use crate::schema::info::SchemaInformation;
use crate::TableSchema;
use mysql_common::frunk::labelled::chars::c;
use std::borrow::Cow;

pub struct InfoSchemaTable<'a> {
    info: &'a SchemaInformation<'a>,
    table_name: Cow<'a, str>,
}

impl<'a> InfoSchemaTable<'a> {
    fn new(info: &'a SchemaInformation, table_name: &'a str) -> Self {
        Self {
            info,
            table_name: Cow::Borrowed(table_name),
        }
    }
}

impl TableSchema for InfoSchemaTable<'_> {
    fn table_name(&self) -> &str {
        self.table_name.as_ref()
    }

    fn is_generated_primary_key(&self, column: impl AsRef<str>) -> bool {
        self.info
            .is_generated_primary_key(self.table_name.as_ref(), column.as_ref())
    }

    fn has_column(&self, column: impl AsRef<str>) -> bool {
        self.info
            .get_column_position(self.table_name.as_ref(), column)
            .is_some()
    }

    fn column_position(&self, column: impl AsRef<str>) -> Option<usize> {
        self.info
            .get_column_position(self.table_name.as_ref(), column)
    }
}

#[cfg(test)]
mod tests {
    use crate::schema::info::SchemaInformation;
    use crate::schema::info_table::InfoSchemaTable;
    use crate::TableSchema;

    #[test]
    fn when_table_does_not_exists_in_schema_info_nothing_is_available_in_the_table() {
        let info = SchemaInformation::default();
        let table = InfoSchemaTable::new(&info, "some_table");

        assert_eq!(table.is_generated_primary_key("entity_id"), false);
        assert_eq!(table.column_position("entity_id"), None);
        assert_eq!(table.has_column("entity_id"), false);
    }

    #[test]
    fn returns_column_position_from_schema_information() {
        let mut info = SchemaInformation::default();
        info.populate_columns(vec![("entity".into(), "entity_id".into(), 0, false)].into_iter());

        let table = InfoSchemaTable::new(&info, "entity");

        assert_eq!(table.column_position("entity_id"), Some(0));
    }

    #[test]
    fn column_exists_when_its_position_is_found_in_schema_information() {
        let mut info = SchemaInformation::default();
        info.populate_columns(vec![("entity".into(), "entity_id".into(), 0, false)].into_iter());

        let table = InfoSchemaTable::new(&info, "entity");

        assert_eq!(table.has_column("entity_id"), true);
    }

    #[test]
    fn reports_primary_key_from_schema_information_data() {
        let mut info = SchemaInformation::default();
        info.populate_columns(
            vec![
                ("entity".into(), "entity_id".into(), 0, true),
                ("entity".into(), "sku".into(), 1, false),
                ("entity_int".into(), "value_id".into(), 0, true),
                ("entity_int".into(), "attribute_id".into(), 1, false),
                ("entity_int".into(), "store_id".into(), 2, false),
            ]
            .into_iter(),
        );

        let first_table = InfoSchemaTable::new(&info, "entity");
        let second_table = InfoSchemaTable::new(&info, "entity");

        assert_eq!(
            vec![
                first_table.is_generated_primary_key("entity_id"),
                first_table.is_generated_primary_key("row_id"),
                second_table.is_generated_primary_key("entity_id"),
            ],
            vec![true, false, true]
        )
    }
}
