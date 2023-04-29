mod info;
mod info_table;

pub use info::SchemaInformation;

/// Table schema provider
///
/// Provides information required for mapping binary rows values to actual columns
///
/// Here is an example how it is used inside of the changelog observers to access column values
/// ```rust
/// use mage_os_database_changelog::{binlog_row, test_table, replication::BinaryRow};
/// use mage_os_database_changelog::test_util::IntoBinlogValue;
///
/// let row: BinaryRow = binlog_row!(1, 2, "value1");
/// let schema = test_table!("entity_int", ["entity_id", "attribute_id", "value"]);
///
/// assert_eq!(row.parse::<usize>("entity_id", &schema).unwrap(), 1);
/// assert_eq!(row.parse::<usize>("attribute_id", &schema).unwrap(), 2);
/// assert_eq!(row.parse::<String>("value", &schema).unwrap(), "value1");
/// ```
pub trait TableSchema {
    fn table_name(&self) -> &str;

    fn is_generated_primary_key(&self, column: impl AsRef<str>) -> bool;

    fn has_column(&self, column: impl AsRef<str>) -> bool;

    fn column_position(&self, column: impl AsRef<str>) -> Option<usize>;
}
