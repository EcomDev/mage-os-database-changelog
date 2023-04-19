use super::BUFFER_STACK_SIZE;
use bitvec::index::BitMask;
use bitvec::macros::internal::funty::Fundamental;
use bitvec::order::Lsb0;
use bitvec::prelude::{BitSlice, BitVec};
use bitvec::slice::BitSliceIndex;
use mysql_common::binlog::events::{OptionalMetadataField, TableMapEvent};
use mysql_common::constants::ColumnType;
use mysql_common::io::ParseBuf;
use smallvec::SmallVec;
use std::fmt::format;
use std::hash::Hash;
use std::io::{Error as IoError, ErrorKind, Result as IoResult};
use std::ops::Deref;
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

    pub fn get_column_type(&self, index: usize) -> IoResult<ColumnType> {
        match self.column_types.get(index) {
            Some(Some(column_type)) => Ok(column_type.to_owned()),
            _ => Err(IoError::new(
                ErrorKind::InvalidData,
                format!("Column {index} is not available in table map event"),
            )),
        }
    }

    pub fn get_column_metadata(&self, index: usize) -> IoResult<&[u8]> {
        match self.column_metadata.get(index) {
            Some(Some(column_meta)) => Ok(column_meta.as_slice()),
            _ => Err(IoError::new(
                ErrorKind::InvalidData,
                format!("Column {index} does not have metadata in table map event"),
            )),
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

    #[inline]
    pub fn num_columns(&self) -> usize {
        self.column_types.len()
    }

    pub fn partial_column_bits(&self, buffer: &mut ParseBuf) -> IoResult<MappedBitSet> {
        let shared_image: &[u8] = buffer.parse((self.column_is_json.count_ones() + 7) / 8)?;

        let mut shared_image_bits = BitSlice::<u8>::from_slice(shared_image).into_iter();

        let mut marked_is_json = self.column_is_json.clone();

        for index in 0..marked_is_json.len() {
            match marked_is_json.get_mut(index) {
                Some(mut item) => {
                    if *item {
                        match shared_image_bits.next().as_deref() {
                            Some(&false) => item.set(false),
                            _ => {}
                        }
                    }
                }

                _ => {}
            }
        }

        Ok(MappedBitSet(marked_is_json))
    }

    pub fn null_column_bits(
        &self,
        row_set_columns: &BitSlice<u8>,
        buffer: &mut ParseBuf,
    ) -> IoResult<MappedBitSet> {
        let total_columns = self.column_types.len();

        let nullable_bits: &[u8] = buffer.parse((row_set_columns.count_ones() + 7) / 8)?;
        let mut nullable_bit_slice = BitSlice::<u8>::from_slice(nullable_bits)
            .into_iter()
            .map(|v| *v);

        let mut null_columns = row_set_columns.to_bitvec();

        for index in 0..null_columns.len() {
            let mut item = null_columns.get_mut(index).unwrap();

            if *item {
                let is_null = nullable_bit_slice.next().unwrap_or(false);
                item.set(is_null);
            }
        }

        Ok(MappedBitSet(null_columns))
    }
}

pub struct MappedBitSet(BitVec<u8>);

impl MappedBitSet {
    pub fn is_set(&self, index: usize) -> bool {
        if self.0.is_empty() {
            return false;
        }

        match self.0.get(index) {
            Some(flag) => *flag,
            _ => false,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::replication::test_fixture::{table_event, Fixture};
    use bitvec::macros::internal::funty::Fundamental;
    use mysql_common::binlog::consts::BinlogVersion;
    use mysql_common::binlog::events::FormatDescriptionEvent;
    use mysql_common::frunk::labelled::IntoLabelledGeneric;
    use std::io::ErrorKind;

    #[test]
    fn copies_column_type_from_table_map_event() {
        let fixture = Fixture::default();

        let table = BinaryTable::from_table_map_event(&fixture.table_event("entity"));

        assert_eq!(
            vec![
                table.get_column_type(0).unwrap(),
                table.get_column_type(1).unwrap(),
                table.get_column_type(2).unwrap(),
                table.get_column_type(3).unwrap(),
            ],
            vec![
                ColumnType::MYSQL_TYPE_LONG,
                ColumnType::MYSQL_TYPE_VARCHAR,
                ColumnType::MYSQL_TYPE_BLOB,
                ColumnType::MYSQL_TYPE_NEWDECIMAL
            ],
        );
    }

    #[test]
    fn errors_out_when_column_type_is_not_available() {
        let fixture = Fixture::default();
        let table = BinaryTable::from_table_map_event(&fixture.table_event("entity"));

        assert_eq!(
            table.get_column_type(100).unwrap_err().kind(),
            ErrorKind::InvalidData
        );
    }

    #[test]
    fn copies_column_metadata_from_table_map_event() {
        let fixture = Fixture::default();

        let table = BinaryTable::from_table_map_event(&fixture.table_event("entity"));

        assert_eq!(
            vec![
                table.get_column_metadata(0).unwrap(),
                table.get_column_metadata(1).unwrap(),
                table.get_column_metadata(2).unwrap(),
                table.get_column_metadata(3).unwrap(),
            ],
            vec![
                vec![].as_slice(),
                vec![252, 3].as_slice(),
                vec![2].as_slice(),
                vec![12, 4].as_slice()
            ]
        );
    }

    #[test]
    fn errors_out_when_column_metadata_is_not_available() {
        let fixture = Fixture::default();
        let table = BinaryTable::from_table_map_event(&fixture.table_event("entity"));

        assert_eq!(
            table.get_column_metadata(100).unwrap_err().kind(),
            ErrorKind::InvalidData
        );
    }

    #[test]
    fn maps_unsigned_flag_into_each_column_of_event() {
        let fixture = Fixture::default();

        let table = BinaryTable::from_table_map_event(&fixture.table_event("entity"));

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
        let fixture = Fixture::default();

        let table = BinaryTable::from_table_map_event(&fixture.table_event("entity_json"));

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

    #[test]
    fn maps_single_partial_column_in_json_table() {
        let fixture = Fixture::default();

        let table = BinaryTable::from_table_map_event(&fixture.table_event("entity_json"));
        let mut buffer = ParseBuf(&[0b00000001]);

        let partial_column_bits = table.partial_column_bits(&mut buffer).unwrap();

        assert_eq!(
            vec![
                partial_column_bits.is_set(0),
                partial_column_bits.is_set(4),
                partial_column_bits.is_set(5),
            ],
            vec![false, true, false]
        )
    }

    #[test]
    fn tells_if_no_update_in_current_buffer_for_json_field() {
        let fixture = Fixture::default();

        let table = BinaryTable::from_table_map_event(&fixture.table_event("entity_json"));
        let mut buffer = ParseBuf(&[0b00000000]);

        let partial_column_bits = table.partial_column_bits(&mut buffer).unwrap();

        assert_eq!(partial_column_bits.is_set(4), false);
    }

    #[test]
    fn when_no_columns_in_image_returns_false() {
        let fixture = Fixture::default();

        let table = BinaryTable::from_table_map_event(&fixture.table_event("entity"));
        let mut buffer = ParseBuf(&[0b00000000]);
        let null_bits = table
            .null_column_bits(BitSlice::empty(), &mut buffer)
            .unwrap();

        assert_eq!(
            vec![null_bits.is_set(0), null_bits.is_set(2)],
            vec![false, false]
        );
    }

    #[test]
    fn marks_column_as_null_when_all_columns_in_image_present() {
        let fixture = Fixture::default();

        let table = BinaryTable::from_table_map_event(&fixture.table_event("entity"));
        let mut buffer = ParseBuf(&[0b00001100]);
        let null_bits = table
            .null_column_bits(&BitSlice::<u8>::from_slice(&[0b00001111]), &mut buffer)
            .unwrap();

        assert_eq!(
            vec![
                null_bits.is_set(0),
                null_bits.is_set(1),
                null_bits.is_set(2),
                null_bits.is_set(3),
            ],
            vec![false, false, true, true]
        )
    }
}
