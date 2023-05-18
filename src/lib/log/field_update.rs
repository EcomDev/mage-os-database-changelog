use crate::log::{IntoChangeLog, ProductChange};
use crate::replication::UpdateRowEvent;
use crate::schema::TableSchema;
use crate::MODIFIED_FIELDS_BUFFER_SIZE;
use smallvec::SmallVec;


pub enum FieldUpdate<T> {
    Empty(T),
    WithFields(T, SmallVec<[&'static str; MODIFIED_FIELDS_BUFFER_SIZE]>),
}

impl<T> FieldUpdate<T> {
    pub fn new(identity: T) -> Self {
        Self::Empty(identity)
    }

    pub fn process_field_update(
        self,
        column: &'static str,
        event: &UpdateRowEvent,
        table: &impl TableSchema,
    ) -> FieldUpdate<T> {
        if event.is_changed_column(column, table) {
            return match self {
                Self::Empty(identity) => {
                    let mut columns = SmallVec::new();
                    columns.push(column);
                    Self::WithFields(identity, columns)
                }
                Self::WithFields(identity, mut columns) => {
                    columns.push(column);
                    Self::WithFields(identity, columns)
                }
            };
        }

        self
    }
}

impl IntoChangeLog<ProductChange> for FieldUpdate<usize> {
    fn into_change_log(self) -> Option<ProductChange> {
        match self {
            Self::Empty(_) => None,
            Self::WithFields(identity, columns) => Some(ProductChange::Fields(identity, columns)),
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::log::{FieldUpdate, IntoChangeLog, ProductChange};
    use crate::replication::{UpdateRowEvent};
    
    use smallvec::SmallVec;

    #[test]
    fn converts_to_none_when_no_fields_update_for_product_change() {
        let field_change = FieldUpdate::new(123);
        assert_eq!(field_change.into_change_log(), None::<ProductChange>);
    }

    #[test]
    fn creates_product_change_log_when_fields_are_provided() {
        let field_change = FieldUpdate::new(123);
        let event = UpdateRowEvent::new(binlog_row!(1, 2, 3, 4), binlog_row!(1, 4, 6, 4));

        let table = test_table!(
            "entity",
            ["entity_id", "field_one", "field_two", "field_three"]
        );

        assert_eq!(
            field_change
                .process_field_update("entity_id", &event, &table)
                .process_field_update("field_one", &event, &table)
                .process_field_update("field_two", &event, &table)
                .process_field_update("field_three", &event, &table)
                .into_change_log(),
            Some(ProductChange::Fields(
                123,
                SmallVec::from_vec(vec!["field_one", "field_two"])
            ))
        );
    }
}
