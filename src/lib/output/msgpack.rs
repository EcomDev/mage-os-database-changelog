use crate::aggregate::*;
use crate::error::Error;
use crate::output::Output;
use rmp::encode::buffer::ByteBuf;
use rmp::encode::{write_array_len, write_str, write_uint, ValueWriteError};
use std::convert::Infallible;
use tokio::io::{AsyncWrite, AsyncWriteExt};

/// MessagePack output for aggregate events
pub struct MessagePack;

impl Output for MessagePack {
    async fn write<T: AsyncWrite + Unpin>(
        &self,
        writer: &mut T,
        aggregate: ChangeAggregate,
    ) -> Result<(), Error> {
        let mut buffer = ByteBuf::with_capacity(4096);
        write_header(&aggregate, &mut buffer)?;

        for (key, value) in aggregate.data {
            write_data_key(&mut buffer, key)?;
            write_data_value(&mut buffer, value)?;
        }

        writer.write_all(buffer.as_slice()).await?;
        Ok(())
    }
}

impl From<ValueWriteError<Infallible>> for Error {
    fn from(_value: ValueWriteError<Infallible>) -> Self {
        Error::OutputError
    }
}

fn entity_to_byte(entity: ChangeAggregateEntity) -> u8 {
    match entity {
        ChangeAggregateEntity::Product => 1,
        ChangeAggregateEntity::Category => 2,
        ChangeAggregateEntity::Inventory => 3,
    }
}

fn write_header(aggregate: &ChangeAggregate, buffer: &mut ByteBuf) -> Result<(), Error> {
    write_uint(buffer, entity_to_byte(aggregate.entity) as u64)?;
    write_uint(buffer, aggregate.metadata.timestamp() as u64)?;
    write_str(buffer, aggregate.metadata.binlog_position().file())?;
    write_uint(
        buffer,
        aggregate.metadata.binlog_position().position() as u64,
    )?;
    write_uint(buffer, aggregate.data.len() as u64)?;
    Ok(())
}

fn write_data_key(buffer: &mut ByteBuf, key: ChangeAggregateKey) -> Result<(), Error> {
    match key {
        ChangeAggregateKey::Key(field) => {
            write_uint(buffer, 1)?;
            write_str(buffer, field)?;
        }
        ChangeAggregateKey::KeyAndScopeInt(field, scope) => {
            write_uint(buffer, 2)?;
            write_str(buffer, field)?;
            write_uint(buffer, scope as u64)?;
        }
        ChangeAggregateKey::KeyAndScopeStr(field, scope) => {
            write_uint(buffer, 3)?;
            write_str(buffer, field)?;
            write_str(buffer, scope)?;
        }
        ChangeAggregateKey::Attribute(attribute_id) => {
            write_uint(buffer, 4)?;
            write_uint(buffer, attribute_id as u64)?;
        }
    }
    Ok(())
}

fn write_data_value(buffer: &mut ByteBuf, value: ChangeAggregateValue) -> Result<(), Error> {
    match value {
        ChangeAggregateValue::Id(ids) => {
            write_array_len(buffer, ids.len() as u32)?;
            for id in ids {
                write_uint(buffer, id as u64)?;
            }
        }
        ChangeAggregateValue::String(ids) => {
            write_array_len(buffer, ids.len() as u32)?;
            for id in ids {
                write_str(buffer, id.as_ref())?;
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::replication::{BinlogPosition, EventMetadata};
    use std::io::Cursor;

    #[derive(Debug, PartialEq)]
    struct ExpectedValue(Vec<u8>);

    impl ExpectedValue {
        fn new(value: &'static [u8]) -> Self {
            Self(value.into())
        }
    }

    impl From<Vec<u8>> for ExpectedValue {
        fn from(value: Vec<u8>) -> Self {
            return Self(value);
        }
    }

    #[tokio::test]
    async fn encodes_empty_event() {
        output_test!(
            MessagePack,
            ChangeAggregate::new(
                ChangeAggregateEntity::Product,
                EventMetadata::new(10, BinlogPosition::new("bin.0000", 4)),
            ),
            ExpectedValue::new(b"\x01\x0a\xa8bin.0000\x04\x00")
        );
    }

    #[tokio::test]
    async fn outputs_events_with_string_key() {
        output_test!(
            MessagePack,
            ChangeAggregate::new(
                ChangeAggregateEntity::Product,
                EventMetadata::new(10, BinlogPosition::new("bin.0000", 4)),
            )
            .with_data(ChangeAggregateKey::Key("sku"), [1, 2, 3]),
            ExpectedValue::new(b"\x01\x0a\xa8bin.0000\x04\x01\x01\xa3sku\x93\x01\x02\x03")
        );
    }

    #[tokio::test]
    async fn outputs_events_with_string_key_and_integer_scope() {
        output_test!(
            MessagePack,
            ChangeAggregate::new(
                ChangeAggregateEntity::Product,
                EventMetadata::new(10, BinlogPosition::new("bin.0000", 4)),
            )
            .with_data(
                ChangeAggregateKey::KeyAndScopeInt("websites", 10),
                [1, 2, 3]
            ),
            ExpectedValue::new(b"\x01\x0a\xa8bin.0000\x04\x01\x02\xa8websites\x0a\x93\x01\x02\x03")
        );
    }

    #[tokio::test]
    async fn outputs_events_with_string_key_and_string_scope() {
        output_test!(
            MessagePack,
            ChangeAggregate::new(
                ChangeAggregateEntity::Product,
                EventMetadata::new(10, BinlogPosition::new("bin.0000", 4)),
            )
            .with_data(
                ChangeAggregateKey::KeyAndScopeStr("websites", "default"),
                [1, 2, 3]
            ),
            ExpectedValue::new(
                b"\x01\x0a\xa8bin.0000\x04\x01\x03\xa8websites\xa7default\x93\x01\x02\x03"
            )
        );
    }

    #[tokio::test]
    async fn outputs_events_with_attributes() {
        output_test!(
            MessagePack,
            ChangeAggregate::new(
                ChangeAggregateEntity::Product,
                EventMetadata::new(10, BinlogPosition::new("bin.0000", 4)),
            )
            .with_data(ChangeAggregateKey::Attribute(1), [4, 5, 6])
            .with_data(ChangeAggregateKey::Attribute(2), [1, 2, 3]),
            ExpectedValue::new(
                b"\x01\x0a\xa8bin.0000\x04\x02\x04\x01\x93\x04\x05\x06\x04\x02\x93\x01\x02\x03"
            )
        );
    }

    #[tokio::test]
    async fn outputs_events_with_string_ids() {
        output_test!(
            MessagePack,
            ChangeAggregate::new(
                ChangeAggregateEntity::Product,
                EventMetadata::new(10, BinlogPosition::new("bin.0000", 4)),
            )
            .with_data(ChangeAggregateKey::Attribute(1), ["sku1"])
            .with_data(ChangeAggregateKey::Attribute(2), ["sku2"]),
            ExpectedValue::new(
                b"\x01\x0a\xa8bin.0000\x04\x02\x04\x01\x91\xa4sku1\x04\x02\x91\xa4sku2"
            )
        );
    }
}
