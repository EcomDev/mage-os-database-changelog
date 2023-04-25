use bitvec::prelude::*;
use mysql_common::binlog::events::{RowsEventData, TableMapEvent};
use mysql_common::binlog::row::BinlogRowValueOptions;
use mysql_common::binlog::value::BinlogValue;
use mysql_common::frunk::labelled::chars::b;
use mysql_common::io::ParseBuf;
use mysql_common::misc::raw::int::LenEnc;
use mysql_common::misc::raw::RawInt;
use mysql_common::proto::MyDeserialize;
use mysql_common::value::Value;

use crate::replication::binary_table::{BinaryTable, MappedBitSet};
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

    pub fn matches(
        &self,
        other: impl Iterator<Item = impl PartialEq<Option<BinlogValue<'static>>>>,
    ) -> bool {
        match other.size_hint() {
            (_, Some(length)) if self.values.len() == length => {}
            _ => return false,
        }

        let (flag, _) = other.fold((true, 0), |(flag, index), item| {
            let flag = flag && item.eq(&self.values[index]);
            (flag, index + 1)
        });

        flag
    }

    pub fn values(&self) -> &SmallVec<[Option<BinlogValue<'static>>; BUFFER_STACK_SIZE]> {
        &self.values
    }
}

pub struct SimpleBinaryRowIter<'a> {
    rows_event: &'a RowsEventData<'a>,
    table: &'a BinaryTable,
    data: ParseBuf<'a>,
}

impl<'a> SimpleBinaryRowIter<'a> {
    fn new(rows_event: &'a RowsEventData<'a>, table: &'a BinaryTable, data: ParseBuf<'a>) -> Self {
        SimpleBinaryRowIter {
            rows_event,
            table,
            data,
        }
    }
}

impl<'a> SimpleBinaryRowIter<'a> {
    fn parse_row_image(
        &mut self,
        supports_partial: bool,
        columns: Option<&'a BitSlice<u8>>,
    ) -> std::io::Result<Option<SimpleBinaryRow>> {
        match columns {
            Some(columns) => {
                let ctx = (columns, supports_partial, self.table);

                match self.data.parse(ctx) {
                    Ok(row) => Ok(Some(row)),
                    Err(err) => Err(err),
                }
            }
            _ => Ok(None),
        }
    }
}

impl<'a> Iterator for SimpleBinaryRowIter<'a> {
    type Item = std::io::Result<(Option<SimpleBinaryRow>, Option<SimpleBinaryRow>)>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.data.is_empty() {
            return None;
        }

        let is_partial_update = match self.rows_event {
            RowsEventData::PartialUpdateRowsEvent(_) => true,
            _ => false,
        };

        let left_row = match self.parse_row_image(false, self.rows_event.columns_before_image()) {
            Ok(row) => row,
            Err(error) => return Some(Err(error)),
        };

        let right_row =
            match self.parse_row_image(is_partial_update, self.rows_event.columns_after_image()) {
                Ok(row) => row,
                Err(error) => return Some(Err(error)),
            };

        Some(Ok((left_row, right_row)))
    }
}

impl<'de> MyDeserialize<'de> for SimpleBinaryRow {
    const SIZE: Option<usize> = None;

    type Ctx = (&'de BitSlice<u8>, bool, &'de BinaryTable);

    fn deserialize(
        (columns, have_shared_image, table_info): Self::Ctx,
        buf: &mut ParseBuf<'de>,
    ) -> std::io::Result<Self> {
        let mut values = SmallVec::with_capacity(table_info.num_columns());

        let partial_columns: MappedBitSet = match have_shared_image {
            true => {
                let value_options = *buf.parse::<RawInt<LenEnc>>(())?;

                match value_options & BinlogRowValueOptions::PARTIAL_JSON_UPDATES as u64 {
                    0.. => table_info.partial_column_bits(buf)?,
                    _ => MappedBitSet::default(),
                }
            }
            false => MappedBitSet::default(),
        };

        let nullable_columns = table_info.null_column_bits(columns, buf)?;
        for index in 0..table_info.num_columns() {
            let column_type = table_info.get_column_type(index)?;
            let column_metadata = table_info.get_column_metadata(index)?;

            values.push(
                match (
                    nullable_columns.is_set(index),
                    columns.get(index).as_deref(),
                ) {
                    (false, Some(&true)) => Some(
                        buf.parse::<BinlogValue>((
                            column_type,
                            column_metadata,
                            table_info.is_unsigned(index),
                            partial_columns.is_set(index),
                        ))?
                        .into_owned(),
                    ),
                    (true, _) => Some(BinlogValue::Value(Value::NULL)),
                    other => None,
                },
            )
        }

        return Ok(Self { values });
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::replication::binary_table::BinaryTable;
    use crate::replication::test_fixture::Fixture;
    use crate::test_util::*;
    use crate::*;
    use mysql_async::Value;
    use mysql_common::binlog::jsondiff::JsonDiffOperation;
    use mysql_common::binlog::value::BinlogValue;
    use mysql_common::io::ParseBuf;
    use serde_json::json;

    #[test]
    fn converts_write_entity_event_into_rows() {
        let fixture = Fixture::default();

        let (table, event) = fixture.row_event("write_entity");
        let table = BinaryTable::from_table_map_event(&table);

        let simple_binary_rows =
            SimpleBinaryRowIter::new(&event, &table, ParseBuf(event.rows_data()));

        assert_after_binlog_row!(
            simple_binary_rows,
            binlog_row!(1, "Product 1", "Product 1 description", "9.9900"),
            binlog_row!(2, "Product 2", "Product 2 description", "99.9900")
        );
    }

    #[test]
    fn converts_write_entity_event_with_null_into_rows() {
        let fixture = Fixture::default();

        let (table, event) = fixture.row_event("write_entity_with_null");

        let table = BinaryTable::from_table_map_event(&table);

        let simple_binary_rows =
            SimpleBinaryRowIter::new(&event, &table, ParseBuf(event.rows_data()));

        assert_after_binlog_row!(
            simple_binary_rows,
            binlog_row!(1, "Product 1", binlog_null!(), "9.9900"),
            binlog_row!(2, "Product 2", binlog_null!(), "99.9900")
        );
    }

    #[test]
    fn converts_delete_entity_int_event_into_rows() {
        let fixture = Fixture::default();

        let (table, event) = fixture.row_event("delete_entity_int");
        let table = BinaryTable::from_table_map_event(&table);

        let simple_binary_rows =
            SimpleBinaryRowIter::new(&event, &table, ParseBuf(event.rows_data()));

        assert_before_binlog_row!(simple_binary_rows, binlog_row!(2, 1, 1, 1, 0));
    }

    #[test]
    fn converts_update_entity_event_into_rows() {
        let fixture = Fixture::default();

        let (table, event) = fixture.row_event("update_entity");

        let table = BinaryTable::from_table_map_event(&table);

        let simple_binary_rows =
            SimpleBinaryRowIter::new(&event, &table, ParseBuf(event.rows_data()));

        assert_binlog_row!(
            simple_binary_rows,
            (
                binlog_row!(2, "Product 2", "Product 2 description", "99.9900"),
                binlog_row!(2, "Awesome Product 2", "Product 2 description", "99.9900")
            )
        );
    }

    #[test]
    fn converts_update_entity_json_event_into_rows() {
        let fixture = Fixture::default();

        let (table, event) = fixture.row_event("update_entity_json");

        let table = BinaryTable::from_table_map_event(&table);

        let simple_binary_rows =
            SimpleBinaryRowIter::new(&event, &table, ParseBuf(event.rows_data()));

        assert_binlog_row!(
            simple_binary_rows,
            (
                partial_binlog_row!(
                    1,
                    2,
                    2,
                    0,
                    json!({"flag": false, "labels": ["red", "blue", "purple"], "featured": true})
                ),
                partial_binlog_row!(
                    1,
                    2,
                    2,
                    0,
                    json!({"flag": false, "labels": ["red", "blue", "purple"], "featured": true, "season":  ["winter", "spring"]})
                )
            ),
            (
                partial_binlog_row!(
                    2,
                    2,
                    2,
                    0,
                    json!({"flag": false, "labels": ["red", "blue", "purple"], "featured": true})
                ),
                partial_binlog_row!(
                    2,
                    2,
                    2,
                    0,
                    json!({"flag": false, "labels": ["red", "blue", "purple"], "featured": true, "season":  ["winter", "spring"]})
                )
            )
        );
    }

    #[test]
    fn converts_extended_minimal_update_entity_json_event_into_rows() {
        let fixture = Fixture::default();

        let (table, event) = fixture.row_event("partial_update_entity_json");

        let table = BinaryTable::from_table_map_event(&table);

        let simple_binary_rows =
            SimpleBinaryRowIter::new(&event, &table, ParseBuf(event.rows_data()));

        assert_binlog_row!(
            simple_binary_rows,
            (
                partial_binlog_row!(
                    1,
                    binlog_none!(),
                    binlog_none!(),
                    binlog_none!(),
                    binlog_none!()
                ),
                partial_binlog_row!(
                    binlog_none!(),
                    binlog_none!(),
                    binlog_none!(),
                    binlog_none!(),
                    binlog_json!(
                        "$.season",
                        JsonDiffOperation::INSERT,
                        json!(["winter", "spring"])
                    )
                )
            ),
            (
                partial_binlog_row!(
                    2,
                    binlog_none!(),
                    binlog_none!(),
                    binlog_none!(),
                    binlog_none!()
                ),
                partial_binlog_row!(
                    binlog_none!(),
                    binlog_none!(),
                    binlog_none!(),
                    binlog_none!(),
                    binlog_json!(
                        "$.season",
                        JsonDiffOperation::INSERT,
                        json!(["winter", "spring"])
                    )
                )
            )
        );
    }
}
