#[cfg(test)]
mod tests {
    use crate::replication::test_fixture::{row_event, table_event};
    use mysql_common::binlog::consts::BinlogVersion;
    use mysql_common::binlog::events::FormatDescriptionEvent;

    #[test]
    fn it_works_with_table_map() {
        let fde = FormatDescriptionEvent::new(BinlogVersion::Version4);

        let event = row_event("partial_update_entity_json", &fde);
        let table = table_event("entity_json", &fde);
        println!("{:?}", event.rows(&table))
    }
}
