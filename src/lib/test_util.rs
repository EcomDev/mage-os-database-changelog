use crate::replication::{SimpleBinaryRow, BUFFER_STACK_SIZE};
use mysql_common::binlog::jsonb;
use mysql_common::binlog::jsondiff::{JsonDiff, JsonDiffOperation};
use mysql_common::binlog::value::{self, BinlogValue};
use mysql_common::value::Value;
use smallvec::SmallVec;
use std::borrow::Cow;
use std::fmt::{Debug, Display, Formatter, Write};

#[doc(hidden)]
pub struct NullValue;

#[doc(hidden)]
pub struct NoneValue;

#[doc(hidden)]
#[derive(Debug, PartialEq, Clone)]
pub struct JsonMatch {
    path: &'static str,
    operation: JsonDiffOperation,
    value: serde_json::Value,
}

#[doc(hidden)]
#[derive(Debug, PartialEq, Clone)]
pub enum CompareValue {
    None,
    Value(Value),
    Jsonb(serde_json::Value),
    JsonbDiff(JsonMatch),
}

#[doc(hidden)]
#[derive(Debug, PartialEq)]
pub struct CompareBinaryRow {
    values: SmallVec<[CompareValue; BUFFER_STACK_SIZE]>,
}

#[doc(hidden)]
pub enum MatchingBinaryRow {
    None,
    SimpleBinaryRow(SimpleBinaryRow),
    CompareBinaryRow(CompareBinaryRow),
}

impl Debug for MatchingBinaryRow {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::None => f.write_str("None"),
            Self::SimpleBinaryRow(row) => {
                f.write_str("[")?;
                for value in row.values() {
                    match value {
                        Some(BinlogValue::Jsonb(json)) => {
                            let json: serde_json::Value = json.clone().try_into().unwrap();
                            f.write_str("Some(")?;
                            Debug::fmt(&json, f)?;
                            f.write_str(")")?
                        }
                        other => value.fmt(f)?,
                    }

                    f.write_str(",")?
                }
                f.write_str("]")
            }
            Self::CompareBinaryRow(row) => {
                f.write_str("[")?;
                for value in &row.values {
                    match value {
                        CompareValue::Jsonb(json) => {
                            f.write_str("Some(")?;
                            Debug::fmt(&json, f)?;
                            f.write_str(")")?;
                        }
                        CompareValue::None => f.write_str("None")?,
                        value => {
                            f.write_str("Some(")?;
                            value.fmt(f)?;
                            f.write_str(")")?
                        }
                    }

                    f.write_str(",")?
                }
                f.write_str("]")
            }
        }
    }
}

impl PartialEq<MatchingBinaryRow> for MatchingBinaryRow {
    fn eq(&self, other: &MatchingBinaryRow) -> bool {
        match (self, other) {
            (MatchingBinaryRow::None, MatchingBinaryRow::None) => true,
            (
                MatchingBinaryRow::CompareBinaryRow(compare_row),
                MatchingBinaryRow::SimpleBinaryRow(binary_row),
            )
            | (
                MatchingBinaryRow::SimpleBinaryRow(binary_row),
                MatchingBinaryRow::CompareBinaryRow(compare_row),
            ) => compare_row.eq(binary_row),
            (
                MatchingBinaryRow::SimpleBinaryRow(left),
                MatchingBinaryRow::SimpleBinaryRow(right),
            ) => left.eq(right),
            (
                MatchingBinaryRow::CompareBinaryRow(left),
                MatchingBinaryRow::CompareBinaryRow(right),
            ) => left.eq(right),
            _ => false,
        }
    }
}

impl From<SimpleBinaryRow> for MatchingBinaryRow {
    fn from(value: SimpleBinaryRow) -> Self {
        MatchingBinaryRow::SimpleBinaryRow(value)
    }
}

impl From<CompareBinaryRow> for MatchingBinaryRow {
    fn from(value: CompareBinaryRow) -> Self {
        MatchingBinaryRow::CompareBinaryRow(value)
    }
}
impl CompareBinaryRow {
    pub fn new(values: &[CompareValue]) -> Self {
        Self {
            values: values.into(),
        }
    }
}

impl JsonMatch {
    pub fn new(path: &'static str, operation: JsonDiffOperation, value: serde_json::Value) -> Self {
        Self {
            path,
            operation,
            value,
        }
    }
}

impl PartialEq<SimpleBinaryRow> for CompareBinaryRow {
    fn eq(&self, other: &SimpleBinaryRow) -> bool {
        // Check if the lengths of the values are the same
        other.matches(self.values.iter())
    }
}

impl PartialEq<Option<BinlogValue<'static>>> for &CompareValue {
    fn eq(&self, other: &Option<BinlogValue<'static>>) -> bool {
        match (self, other) {
            (CompareValue::None, None) => true,
            (CompareValue::Value(left), Some(BinlogValue::Value(right))) => left.eq(right),
            (CompareValue::Jsonb(left), Some(BinlogValue::Jsonb(right))) => {
                let right: serde_json::Value = right.clone().try_into().unwrap();
                left.eq(&right)
            }
            (CompareValue::JsonbDiff(left), Some(BinlogValue::JsonDiff(right))) => {
                let right = right.first().unwrap();
                left.eq(right)
            }
            _ => false,
        }
    }
}

impl PartialEq<JsonDiff<'_>> for JsonMatch {
    fn eq(&self, other: &JsonDiff<'_>) -> bool {
        if other.path_str().ne(self.path) || other.operation().ne(&self.operation) {
            return false;
        }

        let value: serde_json::Value = other.value().unwrap().clone().try_into().unwrap();

        if self.value.ne(&value) {
            return false;
        }

        true
    }
}

impl From<JsonMatch> for CompareValue {
    fn from(value: JsonMatch) -> Self {
        Self::JsonbDiff(value)
    }
}

impl From<serde_json::Value> for CompareValue {
    fn from(value: serde_json::Value) -> Self {
        Self::Jsonb(value)
    }
}

impl<T> From<T> for CompareValue
where
    T: IntoBinlogValue,
{
    fn from(value: T) -> Self {
        match value.into_binlog_value() {
            Some(BinlogValue::Value(value)) => CompareValue::Value(value),
            _ => CompareValue::None,
        }
    }
}

pub trait IntoBinlogValue {
    fn into_binlog_value(self) -> Option<BinlogValue<'static>>;
}

impl IntoBinlogValue for NullValue {
    fn into_binlog_value(self) -> Option<BinlogValue<'static>> {
        Some(BinlogValue::Value(Value::NULL))
    }
}

impl IntoBinlogValue for NoneValue {
    fn into_binlog_value(self) -> Option<BinlogValue<'static>> {
        None
    }
}

macro_rules! impl_into_binlog {
    ($T:ty) => {
        impl IntoBinlogValue for $T {
            fn into_binlog_value(self) -> Option<BinlogValue<'static>> {
                Some(BinlogValue::Value(self.into()))
            }
        }
    };
}

impl_into_binlog!(u32);
impl_into_binlog!(i32);
impl_into_binlog!(u64);
impl_into_binlog!(i64);
impl_into_binlog!(&str);

#[macro_export]
macro_rules! binlog_null {
    () => {
        (crate::test_util::NullValue)
    };
}

#[macro_export]
macro_rules! binlog_none {
    () => {
        (crate::test_util::NoneValue)
    };
}

#[macro_export]
macro_rules! binlog_json {
    ($path:expr, $operation:expr, $value:expr) => {
        (crate::test_util::JsonMatch::new($path, $operation, $value))
    };
}

#[macro_export]
macro_rules! binlog_row {
    ($($value:expr),+) => {
        (crate::replication::SimpleBinaryRow::new(&[$($value.into_binlog_value()),+]))
    };
}

#[macro_export]
macro_rules! partial_binlog_row {
    ($($value:expr),+) => {
        (crate::test_util::CompareBinaryRow::new(&[$($value.into()),+]))
    };
}

#[macro_export]
macro_rules! assert_equals_binlog_iter {
    ($actual:expr, $($expected:expr),+) => {
        use crate::test_util::MatchingBinaryRow;
        let mut expected_rows: Vec<(MatchingBinaryRow, MatchingBinaryRow)> = Vec::new();
        $(expected_rows.push($expected);)+;

        assert_eq!(
            $actual.map(|v| match v.unwrap() {
                (Some(left), Some(right)) => (left.into(), right.into()),
                (None, Some(right)) => (MatchingBinaryRow::None, right.into()),
                (Some(left), None) => (left.into(), MatchingBinaryRow::None),
                (None, None) => (MatchingBinaryRow::None, MatchingBinaryRow::None)
            }).collect::<Vec<(MatchingBinaryRow, MatchingBinaryRow)>>(),
            expected_rows
        );
    };
}

#[macro_export]
macro_rules! assert_after_binlog_row {
    ($actual:expr, $($expected:expr),+) => {
        crate::assert_equals_binlog_iter!($actual, $((crate::test_util::MatchingBinaryRow::None, $expected.into())),+)
    };
}

#[macro_export]
macro_rules! assert_before_binlog_row {
    ($actual:expr, $($expected:expr),+) => {
        crate::assert_equals_binlog_iter!($actual, $(($expected.into(), crate::test_util::MatchingBinaryRow::None)),+)
    };
}

#[macro_export]
macro_rules! assert_binlog_row {
    ($actual:expr, $($expected:expr),+) => {
        crate::assert_equals_binlog_iter!($actual, $(($expected.0.into(), $expected.1.into())),+)
    };
}
