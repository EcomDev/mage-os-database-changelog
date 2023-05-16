#[macro_export]
macro_rules! binlog_null {
    () => {
        ($crate::test_util::NullValue)
    };
}

#[macro_export]
macro_rules! binlog_none {
    () => {
        ($crate::test_util::NoneValue)
    };
}

#[macro_export]
macro_rules! binlog_json {
    ($path:expr, $operation:expr, $value:expr) => {
        ($crate::test_util::JsonMatch::new($path, $operation, $value))
    };
}

#[macro_export]
macro_rules! binlog_row {
    ($($value:expr),*) => {
        ($crate::replication::BinaryRow::new(&[$($crate::test_util::IntoBinlogValue::into_binlog_value($value)),*]))
    };
}

macro_rules! partial_binlog_row {
    ($($value:expr),+) => {
        ($crate::test_util::CompareBinaryRow::new(&[$($value.into()),+]))
    };
}

macro_rules! assert_equals_binlog_iter {
    ($actual:expr, $($expected:expr),+) => {
        use $crate::test_util::MatchingBinaryRow;
        let mut expected_rows: Vec<(MatchingBinaryRow, MatchingBinaryRow)> = Vec::new();
        $(expected_rows.push($expected);)+

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

macro_rules! assert_after_binlog_row {
    ($actual:expr, $($expected:expr),+) => {
        assert_equals_binlog_iter!($actual, $((crate::test_util::MatchingBinaryRow::None, $expected.into())),+)
    };
}

macro_rules! assert_before_binlog_row {
    ($actual:expr, $($expected:expr),+) => {
        assert_equals_binlog_iter!($actual, $(($expected.into(), crate::test_util::MatchingBinaryRow::None)),+)
    };
}

macro_rules! assert_binlog_row {
    ($actual:expr, $($expected:expr),+) => {
        assert_equals_binlog_iter!($actual, $(($expected.0.into(), $expected.1.into())),+)
    };
}

#[macro_export]
macro_rules! test_table {
    ($name:expr) => {
        $crate::test_util::TestTableSchema::new($name, &[])
    };
    ($name:expr, $primary:expr, [$($column:expr),*]) => {
        $crate::test_util::TestTableSchema::with_primary($name,
            &[$($column),*],
            Some($primary)
        )
    };
    ($name:expr, [$($column:expr),*]) => {
        $crate::test_util::TestTableSchema::new($name,
            &[$($column),*]
        )
    };
    [$($column:expr),*] => {
        $crate::test_util::TestTableSchema::new(
            "table_name",
            &[$($column),*]
        )
    }
}

macro_rules! mapper_test {
    ($mapper:expr, $expected:expr, insert[$($value:expr),+], [$($column:expr),+]) => {
        let table = test_table![$($column),+];
        let event = $crate::replication::Event::InsertRow(binlog_row!($($value),+));
        mapper_test!($mapper, $expected, event, table);
    };
    ($mapper:expr, $expected:expr, update[($($before:expr),+), ($($after:expr),+)], [$($column:expr),+]) => {
        let table = test_table![$($column),+];
        let event = $crate::replication::Event::UpdateRow($crate::replication::UpdateRowEvent::new(
            binlog_row!($($before),+),
            binlog_row!($($after),+),
        ));
        mapper_test!($mapper, $expected, event, table);
    };
    ($mapper:expr, $expected:expr, delete[$($value:expr),+], [$($column:expr),+]) => {
        let table = test_table![$($column),+];
        let event = $crate::replication::Event::DeleteRow(binlog_row!($($value),+));
        mapper_test!($mapper, $expected, event, table);
    };
    ($mapper:expr, $expected:expr, $event:expr, $table:expr) => {
        assert_eq!(
            $crate::mapper::ChangeLogMapper::map_event(
                    &$mapper,
                    &$event,
                    &$table
                ).unwrap(),
            $expected
        );
    };
}

macro_rules! output_test {
    ($formatter:expr, $aggregate:expr, $expected:expr) => {
        let mut buffer = Cursor::new(Vec::new());
        $crate::output::Output::write(&$formatter, &mut buffer, $aggregate)
            .await
            .unwrap();

        assert_eq!($expected, buffer.into_inner().into())
    };
}
