use crate::error::Error;
use crate::replication::binary_table::{BinaryTable, MappedBitSet};
use crate::replication::BUFFER_STACK_SIZE;
use crate::TableSchema;
use bitvec::slice::BitSlice;
use mysql_common::binlog::row::BinlogRowValueOptions;
use mysql_common::binlog::value::BinlogValue;
use mysql_common::io::ParseBuf;
use mysql_common::misc::raw::int::LenEnc;
use mysql_common::misc::raw::RawInt;
use mysql_common::proto::MyDeserialize;
use mysql_common::value::convert::FromValue;
use mysql_common::value::Value;
use smallvec::SmallVec;
use std::any::type_name;

#[derive(Debug, PartialEq)]
pub struct BinaryRow {
    values: SmallVec<[Option<BinlogValue<'static>>; BUFFER_STACK_SIZE]>,
}

impl<'de> MyDeserialize<'de> for BinaryRow {
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
                    _other => None,
                },
            )
        }

        return Ok(Self { values });
    }
}

impl BinaryRow {
    pub fn new(values: &[Option<BinlogValue<'static>>]) -> Self {
        Self {
            values: values.into(),
        }
    }

    pub(crate) fn matches(
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

    pub(crate) fn values(&self) -> &SmallVec<[Option<BinlogValue<'static>>; BUFFER_STACK_SIZE]> {
        &self.values
    }

    pub fn take(&mut self, index: usize) -> Option<BinlogValue<'static>> {
        match self.values.get_mut(index) {
            Some(value) => value.take(),
            None => None,
        }
    }

    pub fn get(&self, index: usize) -> Option<&BinlogValue<'static>> {
        match self.values.get(index) {
            Some(Some(value)) => Some(value),
            _ => None,
        }
    }

    pub fn parse<T>(&self, column: impl AsRef<str>, schema: &impl TableSchema) -> Result<T, Error>
    where
        T: FromValue,
    {
        let column_position = schema
            .column_position(&column)
            .ok_or_else(|| Error::ColumnNotFound(column.as_ref().to_string()))?;

        let column_value = self
            .get(column_position)
            .ok_or_else(|| Error::ColumnNotFound(column.as_ref().to_string()))?;

        match column_value {
            BinlogValue::Value(value) => match T::from_value_opt(value.clone()) {
                Ok(value) => Ok(value),
                Err(err) => Err(Error::ColumnParseError(
                    err.0,
                    type_name::<T>(),
                    column.as_ref().to_string(),
                )),
            },
            _ => Err(Error::ColumnNotSupported(
                type_name::<T>(),
                column.as_ref().to_string(),
            )),
        }
    }
}

#[cfg(test)]
mod tests {
    
    use crate::test_util::{IntoBinlogValue, TestTableSchema};
    use mysql_common::binlog::value::BinlogValue;

    #[test]
    fn takes_value_from_row_by_schema() {
        let table = table_schema();

        let row = binlog_row!(1, "Name 1", "sku1", "10.00");

        assert_eq!(row.parse::<String>("name", &table).unwrap(), "Name 1");
    }

    #[test]
    fn errors_when_column_is_not_in_schema() {
        let table = table_schema();

        let row = binlog_row!(1, "Name 1", "sku1", "10.00");

        assert_eq!(
            row.parse::<String>("updated_at", &table)
                .unwrap_err()
                .to_string(),
            "Cannot find value for updated_at column"
        );
    }

    #[test]
    fn errors_when_column_data_is_not_present() {
        let table = table_schema();

        let row = binlog_row!(1, binlog_none!(), "sku1", "10.00");

        assert_eq!(
            row.parse::<String>("name", &table).unwrap_err().to_string(),
            "Cannot find value for name column"
        );
    }

    #[test]
    fn errors_when_column_value_is_not_parsable_into_type() {
        let table = table_schema();

        let row = binlog_row!(1, "Name 1", "sku1", "10.00");

        assert_eq!(
            row.parse::<u32>("name", &table).unwrap_err().to_string(),
            "Cannot parse Bytes(\"Name 1\") value as u32 in name column"
        );
    }

    #[test]
    fn allows_to_get_value_by_index() {
        let row = binlog_row!(1, "Name 1", "sku1", "10.00");

        assert_eq!(
            vec![row.get(0), row.get(1), row.get(4),],
            vec![
                Some(&BinlogValue::Value(1.into())),
                Some(&BinlogValue::Value("Name 1".into())),
                None
            ]
        )
    }

    fn table_schema() -> TestTableSchema {
        test_table!("entity", "entity_id", ["entity_id", "name", "sku", "price"])
    }
}
