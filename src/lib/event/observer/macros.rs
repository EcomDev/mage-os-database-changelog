macro_rules! process_event(
    ($observer:expr, $schema:expr, [$($event:expr),+]) => {
        let observer = $observer;
        let schema = $schema;
        $(observer.process_event(&$event, &schema).await?;)+

        drop(observer);

    }
);

macro_rules! updated_field_macro {
    ($update:expr, $tx:expr, $table:expr, $entity_id:expr,  [$($field:expr),+]) => {
        $(if $update.is_changed_column($field, $table) {
            $tx.send(crate::change::Change::FieldUpdated($field, $entity_id)).unwrap();
        })+;
    };
}
