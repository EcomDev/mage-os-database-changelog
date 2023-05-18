use crate::aggregate::{ChangeAggregate, ChangeAggregateEntity, ChangeAggregateKey};
use crate::error::Error;
use crate::output::Output;

use serde_json::{json, to_vec, Value};
use tokio::io::{AsyncWrite, AsyncWriteExt};

/// JSON output for aggregate events
pub struct JsonOutput;

fn entity_to_str(entity: ChangeAggregateEntity) -> &'static str {
    match entity {
        ChangeAggregateEntity::Product => "product",
        ChangeAggregateEntity::Category => "category",
        ChangeAggregateEntity::Inventory => "inventory",
    }
}

impl Output for JsonOutput {
    async fn write<T: AsyncWrite + Unpin>(
        &self,
        writer: &mut T,
        aggregate: ChangeAggregate,
    ) -> Result<(), Error> {
        let mut json_object = json!({
            "entity": entity_to_str(aggregate.entity),
            "metadata": {
                "timestamp": aggregate.metadata.timestamp(),
                "file": aggregate.metadata.binlog_position().file(),
                "position": aggregate.metadata.binlog_position().position()
            }
        });

        for (key, value) in aggregate.data {
            match key {
                ChangeAggregateKey::Key(key) => {
                    populate_global_level_key(&mut json_object, "global", key, value)?
                }
                ChangeAggregateKey::KeyAndScopeInt(key, id) => {
                    populate_scoped_level_key(&mut json_object, "scoped", key, id, value)?
                }
                ChangeAggregateKey::KeyAndScopeStr(key, id) => {
                    populate_scoped_level_key(&mut json_object, "scoped", key, id, value)?
                }
                ChangeAggregateKey::Attribute(id) => {
                    populate_global_level_key(&mut json_object, "attribute", id, value)?
                }
            }
        }

        writer
            .write_all(&to_vec(&json_object).map_err(Error::Json)?)
            .await
            .map_err(Error::Io)?;

        writer.write_all(b"\n").await.map_err(Error::Io)?;

        Ok(())
    }
}

fn populate_global_level_key(
    json: &mut Value,
    group: impl ToString,
    key: impl ToString,
    value: impl Into<Value>,
) -> Result<(), Error> {
    json.as_object_mut()
        .ok_or(Error::OutputError)?
        .entry(group.to_string())
        .or_insert(json!({}))
        .as_object_mut()
        .ok_or(Error::OutputError)?
        .insert(key.to_string(), value.into());

    Ok(())
}

fn populate_scoped_level_key(
    json: &mut Value,
    group: impl ToString,
    key_first: impl ToString,
    key_second: impl ToString,
    value: impl Into<Value>,
) -> Result<(), Error> {
    json.as_object_mut()
        .ok_or(Error::OutputError)?
        .entry(group.to_string())
        .or_insert(json!({}))
        .as_object_mut()
        .ok_or(Error::OutputError)?
        .entry(key_first.to_string())
        .or_insert(json!({}))
        .as_object_mut()
        .ok_or(Error::OutputError)?
        .insert(key_second.to_string(), value.into());

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::aggregate::{ChangeAggregateEntity, ChangeAggregateKey};
    use crate::replication::{BinlogPosition, EventMetadata};
    use serde_json::from_slice;
    use std::io::Cursor;

    #[derive(Debug, PartialEq)]
    struct ExpectedValue(Value);

    impl From<Vec<u8>> for ExpectedValue {
        fn from(value: Vec<u8>) -> Self {
            match value.iter().position(|v| *v == b'\n') {
                Some(position) => Self(from_slice(&value[0..position]).unwrap()),
                None => unimplemented!(),
            }
        }
    }

    #[tokio::test]
    async fn outputs_empty_event() {
        output_test!(
            JsonOutput,
            ChangeAggregate::new(
                ChangeAggregateEntity::Product,
                EventMetadata::new(10, BinlogPosition::new("bin.0000", 4)),
            ),
            ExpectedValue(json!({
                "entity": "product",
                "metadata": {"timestamp": 10, "file": "bin.0000", "position": 4}
            }))
        );
    }

    #[tokio::test]
    async fn outputs_events_with_string_key() {
        output_test!(
            JsonOutput,
            ChangeAggregate::new(
                ChangeAggregateEntity::Product,
                EventMetadata::new(10, BinlogPosition::new("bin.0000", 4)),
            )
            .with_data(ChangeAggregateKey::Key("sku"), [1, 2, 3]),
            ExpectedValue(json!({
                "entity": "product",
                "metadata": {"timestamp": 10, "file": "bin.0000", "position": 4},
                "global": {
                    "sku": [1,2,3]
                }
            }))
        );
    }

    #[tokio::test]
    async fn outputs_events_with_string_key_object_keyed_by_int() {
        output_test!(
            JsonOutput,
            ChangeAggregate::new(
                ChangeAggregateEntity::Product,
                EventMetadata::new(10, BinlogPosition::new("bin.0000", 4)),
            )
            .with_data(
                ChangeAggregateKey::KeyAndScopeInt("websites", 10),
                [1, 2, 3]
            ),
            ExpectedValue(json!({
                "entity": "product",
                "metadata": {"timestamp": 10, "file": "bin.0000", "position": 4},
                "scoped": {
                    "websites": {
                        "10": [1,2,3]
                    }
                }
            }))
        );
    }

    #[tokio::test]
    async fn outputs_events_with_string_key_object_keyed_by_string() {
        output_test!(
            JsonOutput,
            ChangeAggregate::new(
                ChangeAggregateEntity::Product,
                EventMetadata::new(10, BinlogPosition::new("bin.0000", 4)),
            )
            .with_data(
                ChangeAggregateKey::KeyAndScopeStr("stock", "warehouse_one"),
                [4, 5, 6]
            ),
            ExpectedValue(json!({
                "entity": "product",
                "metadata": {"timestamp": 10, "file": "bin.0000", "position": 4},
                "scoped": {
                    "stock": {
                        "warehouse_one": [4, 5, 6]
                    }
                }
            }))
        );
    }

    #[tokio::test]
    async fn outputs_events_with_attribute_map() {
        output_test!(
            JsonOutput,
            ChangeAggregate::new(
                ChangeAggregateEntity::Product,
                EventMetadata::new(10, BinlogPosition::new("bin.0000", 4)),
            )
            .with_data(ChangeAggregateKey::Attribute(1), [4, 5, 6])
            .with_data(ChangeAggregateKey::Attribute(2), [1, 2, 3]),
            ExpectedValue(json!({
                "entity": "product",
                "metadata": {"timestamp": 10, "file": "bin.0000", "position": 4},
                "attribute": {
                    "1": [4, 5, 6],
                    "2": [1, 2, 3],
                }
            }))
        );
    }
}
