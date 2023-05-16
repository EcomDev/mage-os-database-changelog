use crate::replication::{BinlogPosition, EventMetadata};
use crate::MODIFIED_FIELDS_BUFFER_SIZE;
use mysql_common::frunk::labelled::chars::u;
use serde_json::Value;
use smallvec::SmallVec;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;

#[derive(Debug, PartialEq)]
pub enum ChangeAggregateKey {
    Attribute(usize),
    Key(&'static str),
    KeyAndScopeInt(&'static str, usize),
    KeyAndScopeStr(&'static str, &'static str),
}

#[derive(Debug, PartialEq)]
pub enum ChangeAggregateValue {
    Id(Vec<usize>),
    String(Vec<Arc<str>>),
}

impl Into<Value> for ChangeAggregateValue {
    fn into(self) -> Value {
        match self {
            Self::Id(value) => Value::Array(value.into_iter().map(Into::into).collect()),
            Self::String(value) => {
                Value::Array(value.into_iter().map(|v| v.to_string().into()).collect())
            }
        }
    }
}

fn as_string_string_vec<I>(value: I) -> ChangeAggregateValue
where
    I: IntoIterator,
    I::Item: Into<Arc<str>>,
{
    let mut value: Vec<_> = value.into_iter().map(Into::into).collect();
    value.sort();
    ChangeAggregateValue::String(value)
}

impl<const S: usize> From<[&'static str; S]> for ChangeAggregateValue {
    fn from(value: [&'static str; S]) -> Self {
        as_string_string_vec(value)
    }
}

impl From<HashSet<String>> for ChangeAggregateValue {
    fn from(value: HashSet<String>) -> Self {
        as_string_string_vec(value)
    }
}

impl From<HashSet<usize>> for ChangeAggregateValue {
    fn from(value: HashSet<usize>) -> Self {
        let mut value: Vec<_> = value.into_iter().collect();
        value.sort();
        Self::Id(value)
    }
}

impl<const S: usize> From<[usize; S]> for ChangeAggregateValue {
    fn from(value: [usize; S]) -> Self {
        Self::Id(value.into_iter().collect())
    }
}

#[derive(Debug, PartialEq, Copy, Clone)]
pub enum ChangeAggregateEntity {
    Product,
    Category,
    Inventory,
}

#[derive(Debug)]
pub struct ChangeAggregate {
    pub entity: ChangeAggregateEntity,
    pub metadata: EventMetadata,
    pub data: SmallVec<[(ChangeAggregateKey, ChangeAggregateValue); MODIFIED_FIELDS_BUFFER_SIZE]>,
}

impl PartialEq for ChangeAggregate {
    fn eq(&self, other: &Self) -> bool {
        self.entity == other.entity
            && self.metadata == other.metadata
            && self.data.len() == other.data.len()
            && self.data.iter().all(|v| other.data.contains(v))
    }
}

impl ChangeAggregate {
    pub fn new(entity: ChangeAggregateEntity, metadata: EventMetadata) -> Self {
        Self {
            entity,
            metadata,
            data: SmallVec::new(),
        }
    }

    pub fn with_data<T>(mut self, key: ChangeAggregateKey, value: T) -> Self
    where
        T: Into<ChangeAggregateValue>,
    {
        self.add_data(key, value);
        self
    }

    pub fn add_data<T>(&mut self, key: ChangeAggregateKey, value: T)
    where
        T: Into<ChangeAggregateValue>,
    {
        self.data.push((key, value.into()));
    }
}
