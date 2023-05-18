//! Table schema for mapping of binary log rows into actual column names
mod info;
mod info_table;
mod table_name;

pub use info::SchemaInformation;
pub(crate) use table_name::table_name_without_prefix;

/// Table schema provider
///
/// Provides information required for mapping binary rows values to actual columns
pub trait TableSchema {
    /// Table name in database
    fn table_name(&self) -> &str;

    /// Checks if specified column is a single auto_incremented column in this table
    fn is_generated_primary_key(&self, column: impl AsRef<str>) -> bool;

    /// Checks if specified column is a part of the table
    fn has_column(&self, column: impl AsRef<str>) -> bool;

    /// Returns position of column in the table definition
    fn column_position(&self, column: impl AsRef<str>) -> Option<usize>;
}
