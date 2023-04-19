use bitvec::prelude::*;
use mysql_common::binlog::events::{RowsEventData, TableMapEvent};
use mysql_common::binlog::value::BinlogValue;
use mysql_common::io::ParseBuf;
use mysql_common::proto::MyDeserialize;

use crate::replication::binary_table::BinaryTable;
use crate::replication::BUFFER_STACK_SIZE;
use smallvec::SmallVec;

#[derive(Debug, PartialEq)]
pub struct SimpleBinaryRow {
    values: SmallVec<[Option<BinlogValue<'static>>; BUFFER_STACK_SIZE]>,
}

impl SimpleBinaryRow {
    pub fn new(values: &[Option<BinlogValue<'static>>]) -> Self {
        Self {
            values: values.into(),
        }
    }
}

pub struct SimpleBinaryRows<'a> {
    rows_event: &'a RowsEventData<'a>,
    table: &'a BinaryTable,
    data: ParseBuf<'a>,
}

impl<'a> SimpleBinaryRows<'a> {
    fn new(rows_event: &'a RowsEventData<'a>, table: &'a BinaryTable, data: ParseBuf<'a>) -> Self {
        SimpleBinaryRows {
            rows_event,
            table,
            data,
        }
    }
}

impl<'a> Iterator for SimpleBinaryRows<'a> {
    type Item = std::io::Result<(Option<SimpleBinaryRow>, Option<SimpleBinaryRow>)>;

    fn next(&mut self) -> Option<Self::Item> {
        let mut left_row = None;
        let mut right_row = None;

        if self.data.is_empty() {
            return None;
        }

        if let Some(columns) = self.rows_event.columns_after_image() {
            let ctx = (columns, false, self.table);

            right_row = match self.data.parse(ctx) {
                Ok(right_row) => Some(right_row),
                Err(err) => return Some(Err(err)),
            };
        }

        Some(Ok((left_row, right_row)))
    }
}

impl<'de> MyDeserialize<'de> for SimpleBinaryRow {
    const SIZE: Option<usize> = None;
    /// Content:
    ///
    /// * number of columns
    /// * column bitmap - bit is set if column is in the row
    /// * have shared image - `true` means, that this is a partial event
    ///   and this is an after image row. Therefore we need to parse a shared image
    /// * corresponding table map event
    type Ctx = (&'de BitSlice<u8>, bool, &'de BinaryTable);

    fn deserialize(
        (columns, have_shared_image, table_info): Self::Ctx,
        buf: &mut ParseBuf<'de>,
    ) -> std::io::Result<Self> {
        let mut values = SmallVec::with_capacity(table_info.num_columns());

        let nullable_columns = table_info.null_column_bits(columns, buf)?;

        for index in 0..table_info.num_columns() {
            let column_type = table_info.get_column_type(index)?;
            let column_metadata = table_info.get_column_metadata(index)?;

            values.push(match columns.get(index).as_deref() {
                Some(&true) => Some(
                    buf.parse::<BinlogValue>((
                        column_type,
                        column_metadata,
                        table_info.is_unsigned(index),
                        false,
                    ))?
                    .into_owned(),
                ),
                _ => None,
            })
        }

        return Ok(Self { values });
    }
}

#[cfg(test)]
mod tests {
    use crate::replication::binary_table::BinaryTable;
    use crate::replication::rows::{SimpleBinaryRow, SimpleBinaryRows};
    use crate::replication::test_fixture::Fixture;
    use mysql_async::Value;
    use mysql_common::binlog::value::BinlogValue;
    use mysql_common::io::ParseBuf;

    #[test]
    fn converts_single_write_event_for_entity() {
        let fixture = Fixture::default();

        let event = fixture.row_event("write_entity");
        let table = BinaryTable::from_table_map_event(&fixture.table_event("entity"));

        let binding = ParseBuf(event.rows_data());
        let simple_binary_rows = SimpleBinaryRows::new(&event, &table, ParseBuf(event.rows_data()));

        let rows = simple_binary_rows
            .map(|r| r.unwrap_or_default())
            .collect::<Vec<_>>();

        assert_eq!(
            vec![
                (
                    None,
                    Some(SimpleBinaryRow::new(&[
                        binlog_value(1),
                        binlog_value("Product 1"),
                        binlog_value("Product 1 description"),
                        binlog_value("9.9900")
                    ]))
                ),
                (
                    None,
                    Some(SimpleBinaryRow::new(&[
                        binlog_value(2),
                        binlog_value("Product 2"),
                        binlog_value("Product 2 description"),
                        binlog_value("99.9900")
                    ]))
                )
            ],
            rows
        );
    }

    fn binlog_value<T>(value: T) -> Option<BinlogValue<'static>>
    where
        Value: From<T>,
    {
        Some(BinlogValue::Value(value.into()))
    }
}
