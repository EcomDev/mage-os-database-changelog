use crate::replication::{BinlogPosition, EventMetadata};
use mysql_common::frunk::labelled::chars::u;
use serde_json::Value;
use smallvec::SmallVec;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;

#[derive(Debug, Eq, PartialEq, Hash)]
pub enum ChangeAggregateDataKey {
    Attribute(usize),
    Key(&'static str),
    KeyAndScopeInt(&'static str, usize),
    KeyAndScopeStr(&'static str, &'static str),
}

#[derive(Debug, PartialEq, Eq)]
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

#[derive(Debug, PartialEq)]
pub struct ChangeAggregate {
    pub entity: &'static str,
    pub metadata: EventMetadata,
    pub data: HashMap<ChangeAggregateDataKey, ChangeAggregateValue>,
}

impl ChangeAggregate {
    pub fn new(entity: &'static str, metadata: EventMetadata) -> Self {
        Self {
            entity,
            metadata,
            data: HashMap::new(),
        }
    }

    pub fn with_data<T>(mut self, key: ChangeAggregateDataKey, value: T) -> Self
    where
        T: Into<ChangeAggregateValue>,
    {
        self.add_data(key, value);
        self
    }

    pub fn add_data<T>(&mut self, key: ChangeAggregateDataKey, value: T)
    where
        T: Into<ChangeAggregateValue>,
    {
        self.data.insert(key, value.into());
    }
}
