use bitvec::prelude::*;
use mysql_common::binlog::events::RowsEventData;

use mysql_common::io::ParseBuf;

use crate::replication::binary_table::BinaryTable;
use crate::replication::row::BinaryRow;

pub struct BinaryRowIter<'a> {
    rows_event: &'a RowsEventData<'a>,
    table: &'a BinaryTable,
    data: ParseBuf<'a>,
}

impl<'a> BinaryRowIter<'a> {
    pub(crate) fn new(
        rows_event: &'a RowsEventData<'a>,
        table: &'a BinaryTable,
        data: ParseBuf<'a>,
    ) -> Self {
        BinaryRowIter {
            rows_event,
            table,
            data,
        }
    }
}

impl<'a> BinaryRowIter<'a> {
    fn parse_row_image(
        &mut self,
        supports_partial: bool,
        columns: Option<&'a BitSlice<u8>>,
    ) -> std::io::Result<Option<BinaryRow>> {
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

impl<'a> Iterator for BinaryRowIter<'a> {
    type Item = std::io::Result<(Option<BinaryRow>, Option<BinaryRow>)>;

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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::replication::binary_table::BinaryTable;
    use crate::replication::test_fixture::Fixture;
    use crate::test_util::*;

    use mysql_common::binlog::jsondiff::JsonDiffOperation;

    use mysql_common::io::ParseBuf;
    use serde_json::json;

    #[test]
    fn converts_write_entity_event_into_rows() {
        let fixture = Fixture::default();

        let (table, event) = fixture.row_event("write_entity");
        let table = BinaryTable::from_table_map_event(&table);

        let simple_binary_rows = BinaryRowIter::new(&event, &table, ParseBuf(event.rows_data()));

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

        let simple_binary_rows = BinaryRowIter::new(&event, &table, ParseBuf(event.rows_data()));

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

        let simple_binary_rows = BinaryRowIter::new(&event, &table, ParseBuf(event.rows_data()));

        assert_before_binlog_row!(simple_binary_rows, binlog_row!(2, 1, 1, 1, 0));
    }

    #[test]
    fn converts_update_entity_event_into_rows() {
        let fixture = Fixture::default();

        let (table, event) = fixture.row_event("update_entity");

        let table = BinaryTable::from_table_map_event(&table);

        let simple_binary_rows = BinaryRowIter::new(&event, &table, ParseBuf(event.rows_data()));

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

        let simple_binary_rows = BinaryRowIter::new(&event, &table, ParseBuf(event.rows_data()));

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

        let simple_binary_rows = BinaryRowIter::new(&event, &table, ParseBuf(event.rows_data()));

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
