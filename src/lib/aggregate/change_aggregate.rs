use crate::replication::{BinlogPosition, EventMetadata};
use mysql_common::frunk::labelled::chars::u;
use smallvec::SmallVec;
use std::collections::HashMap;

#[derive(Debug, Eq, PartialEq, Hash)]
pub enum ChangeAggregateDataKey {
    Attribute(usize),
    Key(&'static str),
    KeyAndScopeInt(&'static str, usize),
    KeyAndScopeStr(&'static str, &'static str),
}

#[derive(Debug)]
pub struct ChangeAggregate {
    pub entity: &'static str,
    pub metadata: EventMetadata,
    pub data: HashMap<ChangeAggregateDataKey, Vec<usize>>,
}

impl PartialEq for ChangeAggregate {
    fn eq(&self, other: &Self) -> bool {
        if self.entity != other.entity {
            return false;
        }

        if self.metadata != other.metadata {
            return false;
        }

        if self.data.len() != other.data.len() {
            return false;
        }

        for (key, value) in &self.data {
            let other_value = match other.data.get(key) {
                None => return false,
                Some(other_value) => other_value,
            };

            if value.len() != other_value.len() {
                return false;
            }

            if !value
                .iter()
                .fold(true, |flag, value| flag && other_value.contains(value))
            {
                return false;
            }
        }

        true
    }
}

impl ChangeAggregate {
    pub fn new(entity: &'static str, metadata: EventMetadata) -> Self {
        Self {
            entity,
            metadata,
            data: HashMap::new(),
        }
    }

    pub fn with_data(
        mut self,
        key: ChangeAggregateDataKey,
        value: impl IntoIterator<Item = usize>,
    ) -> Self {
        self.add_data(key, value);
        self
    }

    pub fn add_data(
        &mut self,
        key: ChangeAggregateDataKey,
        value: impl IntoIterator<Item = usize>,
    ) {
        self.data.insert(key, value.into_iter().collect());
    }
}
