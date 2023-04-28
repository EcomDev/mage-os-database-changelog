

mod info;
mod info_table;

pub use info::SchemaInformation;

pub trait TableSchema {
    fn table_name(&self) -> &str;

    fn is_generated_primary_key(&self, column: impl AsRef<str>) -> bool;

    fn has_column(&self, column: impl AsRef<str>) -> bool;

    fn column_position(&self, column: impl AsRef<str>) -> Option<usize>;
}
