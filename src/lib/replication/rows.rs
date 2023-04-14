use bitvec::prelude::*;
use mysql_common::binlog::events::{RowsEventData, TableMapEvent};
use mysql_common::binlog::value::BinlogValue;
use mysql_common::io::ParseBuf;
use mysql_common::proto::MyDeserialize;

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
    table: &'a TableMapEvent<'a>,
    data: ParseBuf<'a>,
}

impl<'a> SimpleBinaryRows<'a> {
    fn new(
        rows_event: &'a RowsEventData<'a>,
        table: &'a TableMapEvent<'a>,
        data: ParseBuf<'a>,
    ) -> Self {
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

        if let Some(columns) = self.rows_event.columns_after_image() {
            let ctx = (self.rows_event.num_columns(), columns, false, self.table);

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
    type Ctx = (u64, &'de BitSlice<u8>, bool, &'de TableMapEvent<'de>);

    fn deserialize(
        (num_columns, columns, have_shared_image, table_info): Self::Ctx,
        buf: &mut ParseBuf<'de>,
    ) -> std::io::Result<Self> {
        let mut values = (0..num_columns).map(|_| None).collect();

        let num_bits = columns.count_ones();
        let bitmap_len = (num_bits + 7) / 8;
        let bitmap_buf: &[u8] = buf.parse(bitmap_len)?;
        let null_bitmap = BitVec::<u8>::from_slice(bitmap_buf);

        return Ok(Self { values });
    }
}

#[cfg(test)]
mod tests {
    use crate::replication::rows::{SimpleBinaryRow, SimpleBinaryRows};
    use crate::replication::test_fixture::{row_event, table_event};
    use mysql_async::Value;
    use mysql_common::binlog::consts::BinlogVersion;
    use mysql_common::binlog::events::FormatDescriptionEvent;
    use mysql_common::binlog::value::BinlogValue;
    use mysql_common::io::ParseBuf;

    #[test]
    fn converts_single_write_event_for_entity() {
        let fde = FormatDescriptionEvent::new(BinlogVersion::Version4);

        let event = row_event("write_entity", &fde);
        let table = table_event("entity", &fde);
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
                        binlog_value(9.99)
                    ]))
                ),
                (
                    None,
                    Some(SimpleBinaryRow::new(&[
                        binlog_value(2),
                        binlog_value("Product 2"),
                        binlog_value("Product 2 description"),
                        binlog_value(99.99)
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
