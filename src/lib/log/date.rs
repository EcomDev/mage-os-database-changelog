use mysql_common::prelude::FromValue;
use mysql_common::value::convert::ParseIr;
use mysql_common::{FromValueError, Value};

#[derive(PartialEq, Debug, Clone, Hash, Eq)]
pub struct Date(u16, u8, u8);

#[doc(hidden)]
pub enum DateIntermediate {
    /// Type instance is ready without parsing.
    Ready(Date),
    /// Type instance is successfully parsed from this value.
    Parsed(Date, Value),
}

impl FromValue for Date {
    type Intermediate = DateIntermediate;
}

impl From<DateIntermediate> for Date {
    fn from(value: DateIntermediate) -> Self {
        match value {
            DateIntermediate::Ready(v) | DateIntermediate::Parsed(v, _) => v,
        }
    }
}

impl From<DateIntermediate> for Value {
    fn from(value: DateIntermediate) -> Self {
        match value {
            DateIntermediate::Ready(v) => v.into(),
            DateIntermediate::Parsed(_, v) => v,
        }
    }
}

impl TryFrom<Value> for DateIntermediate {
    type Error = FromValueError;

    fn try_from(value: Value) -> Result<Self, Self::Error> {
        match &value {
            Value::Date(year, month, day, ..) => {
                Ok(DateIntermediate::Parsed(Date(*year, *month, *day), value))
            }
            _ => Err(FromValueError(value)),
        }
    }
}

impl From<Date> for Value {
    fn from(value: Date) -> Self {
        Value::Date(value.0, value.1, value.2, 0, 0, 0, 0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use mysql_common::value::Value;

    #[test]
    fn can_be_created_from_date_binary_log_value() {
        let value = Date::from_value_opt(Value::Date(2023, 01, 01, 0, 0, 0, 0)).unwrap();

        assert_eq!(value, Date(2023, 01, 01));
    }

    #[test]
    fn errors_on_creation_not_from_date_value() {
        let value = Date::from_value_opt(Value::Bytes(b"123".to_vec())).unwrap_err();

        assert_eq!(value, FromValueError(Value::Bytes(b"123".to_vec())));
    }

    #[test]
    fn creates_value_from_datetime() {
        let value = Value::from(Date(2023, 01, 03));

        assert_eq!(value, Value::Date(2023, 01, 03, 0, 0, 0, 0));
    }
}
