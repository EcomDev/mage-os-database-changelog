mod match_binary_row;
mod schema;

pub use match_binary_row::*;
pub use schema::TestTableSchema;

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
        (crate::replication::BinaryRow::new(&[$($value.into_binlog_value()),+]))
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
