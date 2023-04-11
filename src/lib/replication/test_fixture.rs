use mysql_common::binlog::consts::{BinlogVersion, EventType};
use mysql_common::binlog::events::{
    BinlogEventHeader, FormatDescriptionEvent, RowsEvent, RowsEventData, TableMapEvent,
    WriteRowsEvent,
};
use mysql_common::binlog::BinlogCtx;
use mysql_common::frunk::labelled::chars::n;
use mysql_common::io::ParseBuf;
use phf::phf_map;
use phf::Map;
use std::cell::RefCell;

static TABLE_DEFINITION_ENTITY: &[u8] = &[
    178, 0, 0, 0, 0, 0, 1, 0, 7, 116, 101, 115, 116, 95, 100, 98, 0, 6, 101, 110, 116, 105, 116,
    121, 0, 4, 3, 15, 252, 246, 5, 255, 0, 2, 12, 4, 0,
];

static TABLE_DEFINITION_INT: &[u8] = &[
    179, 0, 0, 0, 0, 0, 1, 0, 7, 116, 101, 115, 116, 95, 100, 98, 0, 10, 101, 110, 116, 105, 116,
    121, 95, 105, 110, 116, 0, 5, 3, 3, 3, 3, 3, 0, 16,
];

static TABLE_DEFINITION_JSON: &[u8] = &[
    180, 0, 0, 0, 0, 0, 1, 0, 7, 116, 101, 115, 116, 95, 100, 98, 0, 11, 101, 110, 116, 105, 116,
    121, 95, 106, 115, 111, 110, 0, 5, 3, 3, 3, 3, 245, 1, 4, 16,
];

static TABLE_DEFINITION: Map<&'static str, &[u8]> = phf_map! {
    "entity" => TABLE_DEFINITION_ENTITY,
    "entity_int" => TABLE_DEFINITION_INT,
    "entity_json" => TABLE_DEFINITION_JSON
};

static PARTIAL_UPDATE_ENTITY_JSON: &[u8] = &[
    86, 0, 0, 0, 0, 0, 1, 0, 2, 0, 5, 1, 16, 0, 1, 0, 0, 0, 1, 1, 0, 33, 0, 0, 0, 1, 8, 36, 46,
    115, 101, 97, 115, 111, 110, 22, 12, 20, 91, 34, 119, 105, 110, 116, 101, 114, 34, 44, 32, 34,
    115, 112, 114, 105, 110, 103, 34, 93, 0, 2, 0, 0, 0, 1, 1, 0, 33, 0, 0, 0, 1, 8, 36, 46, 115,
    101, 97, 115, 111, 110, 22, 12, 20, 91, 34, 119, 105, 110, 116, 101, 114, 34, 44, 32, 34, 115,
    112, 114, 105, 110, 103, 34, 93,
];

static PARTIAL_UPDATE_ENTITY: &[u8] = &[
    83, 0, 0, 0, 0, 0, 1, 0, 2, 0, 4, 1, 2, 0, 2, 0, 0, 0, 0, 0, 17, 0, 65, 119, 101, 115, 111,
    109, 101, 32, 80, 114, 111, 100, 117, 99, 116, 32, 50,
];

static WRITE_ENTITY_JSON: &[u8] = &[
    86, 0, 0, 0, 0, 0, 1, 0, 2, 0, 5, 31, 0, 1, 0, 0, 0, 2, 0, 0, 0, 2, 0, 0, 0, 0, 0, 0, 0, 73, 0,
    0, 0, 0, 3, 0, 72, 0, 25, 0, 4, 0, 29, 0, 6, 0, 35, 0, 8, 0, 4, 2, 0, 2, 43, 0, 4, 1, 0, 102,
    108, 97, 103, 108, 97, 98, 101, 108, 115, 102, 101, 97, 116, 117, 114, 101, 100, 3, 0, 29, 0,
    12, 13, 0, 12, 17, 0, 12, 22, 0, 3, 114, 101, 100, 4, 98, 108, 117, 101, 6, 112, 117, 114, 112,
    108, 101, 0, 2, 0, 0, 0, 2, 0, 0, 0, 2, 0, 0, 0, 0, 0, 0, 0, 73, 0, 0, 0, 0, 3, 0, 72, 0, 25,
    0, 4, 0, 29, 0, 6, 0, 35, 0, 8, 0, 4, 2, 0, 2, 43, 0, 4, 1, 0, 102, 108, 97, 103, 108, 97, 98,
    101, 108, 115, 102, 101, 97, 116, 117, 114, 101, 100, 3, 0, 29, 0, 12, 13, 0, 12, 17, 0, 12,
    22, 0, 3, 114, 101, 100, 4, 98, 108, 117, 101, 6, 112, 117, 114, 112, 108, 101,
];

static WRITE_ENTITY: &[u8] = &[
    83, 0, 0, 0, 0, 0, 1, 0, 2, 0, 4, 15, 0, 1, 0, 0, 0, 9, 0, 80, 114, 111, 100, 117, 99, 116, 32,
    49, 21, 0, 80, 114, 111, 100, 117, 99, 116, 32, 49, 32, 100, 101, 115, 99, 114, 105, 112, 116,
    105, 111, 110, 128, 0, 0, 9, 38, 172, 0, 2, 0, 0, 0, 9, 0, 80, 114, 111, 100, 117, 99, 116, 32,
    50, 21, 0, 80, 114, 111, 100, 117, 99, 116, 32, 50, 32, 100, 101, 115, 99, 114, 105, 112, 116,
    105, 111, 110, 128, 0, 0, 99, 38, 172,
];

static DELETE_ENTITY_INT: &[u8] = &[
    111, 0, 0, 0, 0, 0, 1, 0, 2, 0, 5, 255, 224, 2, 0, 0, 0, 1, 0, 0, 0, 1, 0, 0, 0, 1, 0, 0, 0, 0,
    0, 0, 0,
];
static MINIMAL_DELETE_ENTITY_INT: &[u8] = &[138, 0, 0, 0, 0, 0, 1, 0, 2, 0, 5, 1, 254, 2, 0, 0, 0];
static PARTIAL_UPDATE_ROWS: &[u8] = &[
    86, 0, 0, 0, 0, 0, 1, 0, 2, 0, 5, 255, 255, 0, 1, 0, 0, 0, 2, 0, 0, 0, 2, 0, 0, 0, 0, 0, 0, 0,
    73, 0, 0, 0, 0, 3, 0, 72, 0, 25, 0, 4, 0, 29, 0, 6, 0, 35, 0, 8, 0, 4, 2, 0, 2, 43, 0, 4, 1, 0,
    102, 108, 97, 103, 108, 97, 98, 101, 108, 115, 102, 101, 97, 116, 117, 114, 101, 100, 3, 0, 29,
    0, 12, 13, 0, 12, 17, 0, 12, 22, 0, 3, 114, 101, 100, 4, 98, 108, 117, 101, 6, 112, 117, 114,
    112, 108, 101, 0, 1, 0, 0, 0, 2, 0, 0, 0, 2, 0, 0, 0, 0, 0, 0, 0, 107, 0, 0, 0, 0, 4, 0, 106,
    0, 32, 0, 4, 0, 36, 0, 6, 0, 42, 0, 6, 0, 48, 0, 8, 0, 4, 2, 0, 2, 56, 0, 12, 85, 0, 4, 1, 0,
    102, 108, 97, 103, 108, 97, 98, 101, 108, 115, 115, 101, 97, 115, 111, 110, 102, 101, 97, 116,
    117, 114, 101, 100, 3, 0, 29, 0, 12, 13, 0, 12, 17, 0, 12, 22, 0, 3, 114, 101, 100, 4, 98, 108,
    117, 101, 6, 112, 117, 114, 112, 108, 101, 20, 91, 34, 119, 105, 110, 116, 101, 114, 34, 44,
    32, 34, 115, 112, 114, 105, 110, 103, 34, 93, 0, 2, 0, 0, 0, 2, 0, 0, 0, 2, 0, 0, 0, 0, 0, 0,
    0, 73, 0, 0, 0, 0, 3, 0, 72, 0, 25, 0, 4, 0, 29, 0, 6, 0, 35, 0, 8, 0, 4, 2, 0, 2, 43, 0, 4, 1,
    0, 102, 108, 97, 103, 108, 97, 98, 101, 108, 115, 102, 101, 97, 116, 117, 114, 101, 100, 3, 0,
    29, 0, 12, 13, 0, 12, 17, 0, 12, 22, 0, 3, 114, 101, 100, 4, 98, 108, 117, 101, 6, 112, 117,
    114, 112, 108, 101, 0, 2, 0, 0, 0, 2, 0, 0, 0, 2, 0, 0, 0, 0, 0, 0, 0, 107, 0, 0, 0, 0, 4, 0,
    106, 0, 32, 0, 4, 0, 36, 0, 6, 0, 42, 0, 6, 0, 48, 0, 8, 0, 4, 2, 0, 2, 56, 0, 12, 85, 0, 4, 1,
    0, 102, 108, 97, 103, 108, 97, 98, 101, 108, 115, 115, 101, 97, 115, 111, 110, 102, 101, 97,
    116, 117, 114, 101, 100, 3, 0, 29, 0, 12, 13, 0, 12, 17, 0, 12, 22, 0, 3, 114, 101, 100, 4, 98,
    108, 117, 101, 6, 112, 117, 114, 112, 108, 101, 20, 91, 34, 119, 105, 110, 116, 101, 114, 34,
    44, 32, 34, 115, 112, 114, 105, 110, 103, 34, 93,
];

static UPDATE_ROWS: &[u8] = &[
    83, 0, 0, 0, 0, 0, 1, 0, 2, 0, 4, 255, 255, 0, 2, 0, 0, 0, 9, 0, 80, 114, 111, 100, 117, 99,
    116, 32, 50, 21, 0, 80, 114, 111, 100, 117, 99, 116, 32, 50, 32, 100, 101, 115, 99, 114, 105,
    112, 116, 105, 111, 110, 128, 0, 0, 99, 38, 172, 0, 2, 0, 0, 0, 17, 0, 65, 119, 101, 115, 111,
    109, 101, 32, 80, 114, 111, 100, 117, 99, 116, 32, 50, 21, 0, 80, 114, 111, 100, 117, 99, 116,
    32, 50, 32, 100, 101, 115, 99, 114, 105, 112, 116, 105, 111, 110, 128, 0, 0, 99, 38, 172,
];

static MINIMAL_UPDATE_ENTITY: &[u8] = &[
    137, 0, 0, 0, 0, 0, 1, 0, 2, 0, 4, 1, 2, 254, 2, 0, 0, 0, 254, 17, 65, 119, 101, 115, 111, 109,
    101, 32, 80, 114, 111, 100, 117, 99, 116, 32, 50,
];

static TABLE_ROWS_EVENTS: Map<&'static str, (EventType, &[u8])> = phf_map! {
    "partial_update_entity_json" => (EventType::PARTIAL_UPDATE_ROWS_EVENT, PARTIAL_UPDATE_ENTITY_JSON),
    "partial_update_entity" => (EventType::PARTIAL_UPDATE_ROWS_EVENT, PARTIAL_UPDATE_ENTITY),
    "write_entity_json" => (EventType::WRITE_ROWS_EVENT, WRITE_ENTITY_JSON),
    "write_entity" => (EventType::WRITE_ROWS_EVENT, WRITE_ENTITY),

    "delete_entity_int" => (EventType::DELETE_ROWS_EVENT, DELETE_ENTITY_INT),
    "minimal_delete_entity_int" => (EventType::DELETE_ROWS_EVENT, MINIMAL_DELETE_ENTITY_INT),
    "update_entity_json" => (EventType::UPDATE_ROWS_EVENT, PARTIAL_UPDATE_ROWS),
    "update_entity" => (EventType::UPDATE_ROWS_EVENT, UPDATE_ROWS),
    "minimal_update_entity" => (EventType::UPDATE_ROWS_EVENT, MINIMAL_UPDATE_ENTITY),
};

pub fn row_event<'a>(name: &'static str, fde: &'a FormatDescriptionEvent<'a>) -> RowsEventData<'a> {
    let (event_type, event_data) = TABLE_ROWS_EVENTS.get(name).unwrap();
    let event_size = BinlogEventHeader::LEN + event_data.len();
    let event_data = &mut ParseBuf(event_data);
    let ctx = BinlogCtx::new(event_size, fde);

    match event_type {
        EventType::WRITE_ROWS_EVENT => {
            RowsEventData::WriteRowsEvent(event_data.parse(ctx).unwrap())
        }
        EventType::UPDATE_ROWS_EVENT => {
            RowsEventData::UpdateRowsEvent(event_data.parse(ctx).unwrap())
        }
        EventType::PARTIAL_UPDATE_ROWS_EVENT => {
            RowsEventData::PartialUpdateRowsEvent(event_data.parse(ctx).unwrap())
        }
        EventType::DELETE_ROWS_EVENT => {
            RowsEventData::DeleteRowsEvent(event_data.parse(ctx).unwrap())
        }
        _ => unreachable!(),
    }
}

pub fn table_event<'a>(
    name: &'static str,
    fde: &'a FormatDescriptionEvent<'a>,
) -> TableMapEvent<'a> {
    let event_data = TABLE_DEFINITION.get(name).unwrap();
    let event_size = BinlogEventHeader::LEN + event_data.len();
    let event_data = &mut ParseBuf(event_data);
    let ctx = BinlogCtx::new(event_size, fde);

    event_data.parse(ctx).unwrap()
}
