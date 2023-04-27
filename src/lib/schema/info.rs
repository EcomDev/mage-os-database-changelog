use mysql_async::prelude::Queryable;
use mysql_async::{Conn, Error};
use mysql_common::params::Params;
use std::borrow::Cow;
use std::collections::HashMap;

#[doc(hidden)]
#[derive(Default)]
pub struct SchemaInformation<'a> {
    column_position: HashMap<(Cow<'a, str>, Cow<'a, str>), usize>,
    generated_primary_key: HashMap<Cow<'a, str>, Cow<'a, str>>,
}

impl<'a> SchemaInformation<'a> {
    pub async fn populate<T, P>(
        &mut self,
        connection: &mut Conn,
        database_name: T,
        table_prefix: P,
    ) -> Result<(), Error>
    where
        T: AsRef<str>,
        P: AsRef<str>,
    {
        let mut table_info = connection.exec_iter(
            "SELECT TABLE_NAME, COLUMN_NAME, ORDINAL_POSITION - 1, COLUMN_KEY = 'PRI' AND EXTRA LIKE '%auto_increment%' FROM information_schema.COLUMNS WHERE TABLE_SCHEMA = ?",
            Params::Positional(vec![database_name.as_ref().into()])
        ).await?;

        self.populate_columns(
            table_info
                .map(|mut row| {
                    let mut table_name: Cow<str> = Cow::Owned(row.take(0).unwrap());
                    if table_name.starts_with(table_prefix.as_ref()) {
                        table_name.to_mut().drain(0..table_prefix.as_ref().len());
                    }
                    (
                        table_name,
                        Cow::Owned(row.take(1).unwrap()),
                        row.take(2).unwrap(),
                        row.take(3).unwrap(),
                    )
                })
                .await?
                .into_iter(),
        );

        Ok(())
    }

    pub(crate) fn clear(&mut self) {
        self.column_position.clear();
        self.generated_primary_key.clear();
    }

    pub(crate) fn populate_columns(
        &mut self,
        source: impl Iterator<Item = (Cow<'a, str>, Cow<'a, str>, usize, bool)>,
    ) {
        match source.size_hint() {
            (_, Some(length)) => self.column_position.reserve(length),
            _ => {}
        }

        for (table_name, column_name, position, is_generated_primary_key) in source {
            if is_generated_primary_key {
                self.generated_primary_key
                    .entry(table_name.clone())
                    .or_insert(column_name.clone());
            }

            *self
                .column_position
                .entry((table_name, column_name))
                .or_default() = position;
        }
    }

    pub fn get_column_position<T, C>(&self, table: T, column: C) -> Option<usize>
    where
        T: AsRef<str>,
        C: AsRef<str>,
    {
        let lookup_key = (
            Cow::Borrowed(table.as_ref()),
            Cow::Borrowed(column.as_ref()),
        );

        self.column_position.get(&lookup_key).copied()
    }

    pub fn is_generated_primary_key<T, C>(&self, table: T, column: C) -> bool
    where
        T: AsRef<str>,
        C: AsRef<str>,
    {
        let lookup_key = Cow::Borrowed(table.as_ref());

        match self.generated_primary_key.get(&lookup_key) {
            None => false,
            Some(primary_key) => primary_key.eq(column.as_ref()),
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::schema::info::SchemaInformation;

    #[test]
    fn populates_schema_for_column_positions() {
        let mut schema = SchemaInformation::default();

        schema.populate_columns(
            vec![
                ("entity".into(), "entity_id".into(), 0, true),
                ("entity".into(), "sku".into(), 1, false),
                ("entity_int".into(), "entity_id".into(), 0, true),
                ("entity_int".into(), "store_id".into(), 2, false),
            ]
            .into_iter(),
        );

        assert_eq!(
            vec![
                schema.get_column_position("entity", "sku"),
                schema.get_column_position("entity", "entity_id"),
                schema.get_column_position("entity_int", "store_id"),
                schema.get_column_position("entity_not_exists", "entity_id"),
            ],
            vec![Some(1), Some(0), Some(2), None]
        );
    }

    #[test]
    fn populates_schema_with_single_primary_keys() {
        let mut schema = SchemaInformation::default();
        schema.populate_columns(
            vec![
                ("entity".into(), "entity_id".into(), 0, true),
                ("entity".into(), "sku".into(), 1, false),
                ("entity_int".into(), "value_id".into(), 0, true),
                ("entity_int".into(), "entity_id".into(), 0, false),
                ("entity_int".into(), "store_id".into(), 2, false),
            ]
            .into_iter(),
        );

        assert_eq!(
            vec![
                schema.is_generated_primary_key("entity", "sku"),
                schema.is_generated_primary_key("entity", "entity_id"),
                schema.is_generated_primary_key("entity_int", "value_id"),
                schema.is_generated_primary_key("entity_int", "entity_id"),
            ],
            vec![false, true, true, false]
        );
    }
}
