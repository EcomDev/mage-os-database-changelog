#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use mysql_common::binlog::BinlogCtx;
    use mysql_common::binlog::events::TableMapEvent;
    use mysql_common::io::ParseBuf;
    use mysql_common::proto::MyDeserialize;

    const TABLE_MAP_ENTITY: HashMap<&str, TableMapEvent> = HashMap::from([
        ("entity_int", TableMapEvent::deserialize(() as BinlogCtx, &mut ParseBuf(&[167, 0, 0, 0, 0, 0, 1, 0, 7, 116, 101, 115, 116, 95, 100, 98, 0, 10, 101, 110, 116, 105, 116, 121, 95, 105, 110, 116, 0, 5, 3, 3, 3, 3, 3, 0, 16])).unwrap()),
        ("entity", TableMapEvent::deserialize(() as BinlogCtx, &mut ParseBuf(&[166, 0, 0, 0, 0, 0, 1, 0, 7, 116, 101, 115, 116, 95, 100, 98, 0, 6, 101, 110, 116, 105, 116, 121, 0, 4, 3, 15, 252, 246, 5, 255, 0, 2, 12, 4, 0])).unwrap()),
        ("entity_json", TableMapEvent::deserialize(() as BinlogCtx, &mut ParseBuf(&[168, 0, 0, 0, 0, 0, 1, 0, 7, 116, 101, 115, 116, 95, 100, 98, 0, 11, 101, 110, 116, 105, 116, 121, 95, 106, 115, 111, 110, 0, 5, 3, 3, 3, 3, 245, 1, 4, 16])).unwrap()),
    ]);

    #[test]
    fn it_works_with_table_map() {
        println!("{:?}", TABLE_MAP_ENTITY.get("entity_int").unwrap());
    }

}