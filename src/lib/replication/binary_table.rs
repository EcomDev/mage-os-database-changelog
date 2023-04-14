use super::BUFFER_STACK_SIZE;
use bitvec::prelude::BitVec;
use mysql_common::binlog::events::{OptionalMetadataField, TableMapEvent};
use mysql_common::constants::ColumnType;
use smallvec::SmallVec;
use std::sync::Arc;

pub struct BinaryTable {
    column_types: SmallVec<[Option<ColumnType>; BUFFER_STACK_SIZE]>,
    column_is_unsigned: BitVec<u8>,
    column_is_json: BitVec<u8>,
    column_metadata: SmallVec<[Option<Vec<u8>>; BUFFER_STACK_SIZE]>,
}

impl BinaryTable {}

impl BinaryTable {
    pub fn from_table_map_event(table_map_event: &TableMapEvent) -> Self {
        let column_count = table_map_event.columns_count() as usize;

        let mut column_types = SmallVec::with_capacity(column_count);
        let mut column_metadata = SmallVec::with_capacity(column_count);
        let mut column_is_unsigned = BitVec::with_capacity(column_count);
        let mut column_is_json = BitVec::with_capacity(column_count);
        let unsigned_bitmask = table_map_event.iter_optional_meta().find_map(|v| match v {
            Ok(OptionalMetadataField::Signedness(bitmask)) => Some(bitmask),
            _ => None,
        });
        let mut unsigned_index = 0;
        for index in 0..column_count {
            let column_type = table_map_event.get_column_type(index).unwrap_or(None);
            let is_unsigned = match (&column_type, unsigned_bitmask) {
                (Some(column_type), Some(bitmask)) if column_type.is_numeric_type() => {
                    let result = *bitmask.get(unsigned_index).as_deref().unwrap_or(&false);
                    unsigned_index += 1;
                    result
                }
                _ => false,
            };

            column_types.push(column_type);
            column_metadata.push(
                table_map_event
                    .get_column_metadata(index)
                    .map(|slice| Vec::from(slice)),
            );
            column_is_unsigned.push(is_unsigned);
            column_is_json.push(column_type == Some(ColumnType::MYSQL_TYPE_JSON));
        }

        Self {
            column_metadata,
            column_types,
            column_is_json,
            column_is_unsigned,
        }
    }

    pub fn get_column_type(&self, index: usize) -> Option<ColumnType> {
        match self.column_types.get(index) {
            Some(column_type) => column_type.to_owned(),
            _ => None,
        }
    }

    pub fn get_column_metadata(&self, index: usize) -> Option<&[u8]> {
        match self.column_metadata.get(index) {
            Some(Some(column_meta)) => Some(column_meta.as_slice()),
            _ => None,
        }
    }

    pub fn is_unsigned(&self, index: usize) -> bool {
        match self.column_is_unsigned.get(index).as_deref() {
            Some(value) => *value,
            None => false,
        }
    }

    pub fn is_json(&self, index: usize) -> bool {
        match self.column_is_json.get(index).as_deref() {
            Some(value) => *value,
            None => false,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::replication::test_fixture::table_event;
    use mysql_common::binlog::consts::BinlogVersion;
    use mysql_common::binlog::events::FormatDescriptionEvent;
    use mysql_common::frunk::labelled::IntoLabelledGeneric;

    #[test]
    fn copies_column_type_from_table_map_event() {
        let fde = FormatDescriptionEvent::new(BinlogVersion::Version4);
        let event = table_event("entity", &fde);

        let table = BinaryTable::from_table_map_event(&event);

        assert_eq!(
            vec![
                Some(ColumnType::MYSQL_TYPE_LONG),
                Some(ColumnType::MYSQL_TYPE_VARCHAR),
                Some(ColumnType::MYSQL_TYPE_BLOB),
                Some(ColumnType::MYSQL_TYPE_NEWDECIMAL),
                None
            ],
            vec![
                table.get_column_type(0),
                table.get_column_type(1),
                table.get_column_type(2),
                table.get_column_type(3),
                table.get_column_type(4),
            ]
        );
    }

    #[test]
    fn copies_column_metadata_from_table_map_event() {
        let fde = FormatDescriptionEvent::new(BinlogVersion::Version4);
        let event = table_event("entity", &fde);

        let table = BinaryTable::from_table_map_event(&event);

        assert_eq!(
            vec![
                table.get_column_metadata(0),
                table.get_column_metadata(1),
                table.get_column_metadata(2),
                table.get_column_metadata(3),
            ],
            vec![
                Some(vec![].as_slice()),
                Some(vec![252, 3].as_slice()),
                Some(vec![2].as_slice()),
                Some(vec![12, 4].as_slice())
            ]
        );
    }

    #[test]
    fn maps_unsigned_flag_into_each_column_of_event() {
        let fde = FormatDescriptionEvent::new(BinlogVersion::Version4);
        let event = table_event("entity", &fde);

        let table = BinaryTable::from_table_map_event(&event);

        assert_eq!(
            vec![
                table.is_unsigned(0),
                table.is_unsigned(1),
                table.is_unsigned(2),
                table.is_unsigned(3),
                table.is_unsigned(4)
            ],
            vec![true, false, false, false, false]
        );
    }

    #[test]
    fn maps_is_json_flag_into_each_column_of_event() {
        let fde = FormatDescriptionEvent::new(BinlogVersion::Version4);
        let event = table_event("entity_json", &fde);

        let table = BinaryTable::from_table_map_event(&event);

        assert_eq!(
            vec![
                table.is_json(0),
                table.is_json(1),
                table.is_json(2),
                table.is_json(3),
                table.is_json(4),
                table.is_json(5)
            ],
            vec![false, false, false, false, true, false]
        );
    }
}
