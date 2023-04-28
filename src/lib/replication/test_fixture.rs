use mysql_common::binlog::consts::{BinlogVersion, EventType};
use mysql_common::binlog::events::{
    BinlogEventHeader, FormatDescriptionEvent, RowsEventData, TableMapEvent,
};
use mysql_common::binlog::BinlogCtx;

use mysql_common::io::ParseBuf;
use phf::phf_map;
use phf::Map;

static TABLE_DEFINITION_ENTITY: &[u8] = &[
    24, 1, 0, 0, 0, 0, 1, 0, 8, 116, 101, 115, 116, 95, 100, 98, 48, 0, 6, 101, 110, 116, 105, 116,
    121, 0, 4, 3, 15, 252, 246, 5, 252, 3, 2, 12, 4, 14, 1, 1, 128, 2, 3, 252, 255, 0,
];

static TABLE_DEFINITION_ENTITY_MINIMAL: &[u8] = &[
    89, 0, 0, 0, 0, 0, 1, 0, 7, 116, 101, 115, 116, 95, 100, 98, 0, 6, 101, 110, 116, 105, 116,
    121, 0, 4, 3, 15, 252, 246, 5, 252, 3, 2, 12, 4, 14, 1, 1, 128, 2, 3, 252, 255, 0,
];

static TABLE_DEFINITION_ENTITY_WITH_NULL: &[u8] = &[
    129, 0, 0, 0, 0, 0, 1, 0, 7, 116, 101, 115, 116, 95, 100, 98, 0, 6, 101, 110, 116, 105, 116,
    121, 0, 4, 3, 15, 252, 246, 5, 255, 0, 2, 12, 4, 14,
];

static TABLE_DEFINITION_INT: &[u8] = &[
    90, 0, 0, 0, 0, 0, 1, 0, 7, 116, 101, 115, 116, 95, 100, 98, 0, 10, 101, 110, 116, 105, 116,
    121, 95, 105, 110, 116, 0, 5, 3, 3, 3, 3, 3, 0, 16, 1, 1, 240,
];

static TABLE_DEFINITION_JSON: &[u8] = &[
    174, 1, 0, 0, 0, 0, 1, 0, 8, 116, 101, 115, 116, 95, 100, 98, 52, 0, 11, 101, 110, 116, 105,
    116, 121, 95, 106, 115, 111, 110, 0, 5, 3, 3, 3, 3, 245, 1, 4, 16, 1, 1, 240,
];

static TABLE_DEFINITION_PARTIAL_JSON: &[u8] = &[
    233, 0, 0, 0, 0, 0, 1, 0, 8, 116, 101, 115, 116, 95, 100, 98, 52, 0, 11, 101, 110, 116, 105,
    116, 121, 95, 106, 115, 111, 110, 0, 5, 3, 3, 3, 3, 245, 1, 4, 16, 1, 1, 240,
];

static TABLE_DEFINITION_MULTIPLE_JSON: &[u8] = &[
    138, 0, 0, 0, 0, 0, 1, 0, 8, 116, 101, 115, 116, 95, 100, 98, 48, 0, 25, 101, 110, 116, 105,
    116, 121, 95, 119, 105, 116, 104, 95, 109, 117, 108, 116, 105, 112, 108, 101, 95, 106, 115,
    111, 110, 0, 6, 3, 245, 3, 3, 245, 3, 2, 4, 4, 50, 1, 1, 240,
];

static TABLE_DEFINITION: Map<&'static str, &[u8]> = phf_map! {
    "entity" => TABLE_DEFINITION_ENTITY,
    "entity_with_null" => TABLE_DEFINITION_ENTITY_WITH_NULL,
    "entity_int" => TABLE_DEFINITION_INT,
    "entity_json" => TABLE_DEFINITION_JSON,
    "partial_entity_json" => TABLE_DEFINITION_JSON,
    "entity_with_multiple_json" => TABLE_DEFINITION_MULTIPLE_JSON
};

static PARTIAL_UPDATE_ENTITY: &[u8] = &[
    89, 0, 0, 0, 0, 0, 1, 0, 2, 0, 4, 1, 2, 0, 2, 0, 0, 0, 0, 0, 17, 0, 65, 119, 101, 115, 111,
    109, 101, 32, 80, 114, 111, 100, 117, 99, 116, 32, 50,
];

static WRITE_ENTITY: &[u8] = &[
    110, 1, 0, 0, 0, 0, 1, 0, 2, 0, 4, 255, 0, 1, 0, 0, 0, 9, 0, 80, 114, 111, 100, 117, 99, 116,
    32, 49, 21, 0, 80, 114, 111, 100, 117, 99, 116, 32, 49, 32, 100, 101, 115, 99, 114, 105, 112,
    116, 105, 111, 110, 128, 0, 0, 9, 38, 172, 0, 2, 0, 0, 0, 9, 0, 80, 114, 111, 100, 117, 99,
    116, 32, 50, 21, 0, 80, 114, 111, 100, 117, 99, 116, 32, 50, 32, 100, 101, 115, 99, 114, 105,
    112, 116, 105, 111, 110, 128, 0, 0, 99, 38, 172,
];

static WRITE_ENTITY_WITH_NULL: &[u8] = &[
    127, 0, 0, 0, 0, 0, 1, 0, 2, 0, 4, 255, 244, 1, 0, 0, 0, 9, 80, 114, 111, 100, 117, 99, 116,
    32, 49, 128, 0, 0, 9, 38, 172, 244, 2, 0, 0, 0, 9, 80, 114, 111, 100, 117, 99, 116, 32, 50,
    128, 0, 0, 99, 38, 172,
];

static DELETE_ENTITY_INT: &[u8] = &[
    111, 0, 0, 0, 0, 0, 1, 0, 2, 0, 5, 255, 224, 2, 0, 0, 0, 1, 0, 0, 0, 1, 0, 0, 0, 1, 0, 0, 0, 0,
    0, 0, 0,
];
static UPDATE_ROWS_JSON: &[u8] = &[
    111, 0, 0, 0, 0, 0, 1, 0, 2, 0, 5, 255, 255, 224, 1, 0, 0, 0, 2, 0, 0, 0, 2, 0, 0, 0, 0, 0, 0,
    0, 73, 0, 0, 0, 0, 3, 0, 72, 0, 25, 0, 4, 0, 29, 0, 6, 0, 35, 0, 8, 0, 4, 2, 0, 2, 43, 0, 4, 1,
    0, 102, 108, 97, 103, 108, 97, 98, 101, 108, 115, 102, 101, 97, 116, 117, 114, 101, 100, 3, 0,
    29, 0, 12, 13, 0, 12, 17, 0, 12, 22, 0, 3, 114, 101, 100, 4, 98, 108, 117, 101, 6, 112, 117,
    114, 112, 108, 101, 224, 1, 0, 0, 0, 2, 0, 0, 0, 2, 0, 0, 0, 0, 0, 0, 0, 110, 0, 0, 0, 0, 4, 0,
    109, 0, 32, 0, 4, 0, 36, 0, 6, 0, 42, 0, 6, 0, 48, 0, 8, 0, 4, 2, 0, 2, 56, 0, 2, 85, 0, 4, 1,
    0, 102, 108, 97, 103, 108, 97, 98, 101, 108, 115, 115, 101, 97, 115, 111, 110, 102, 101, 97,
    116, 117, 114, 101, 100, 3, 0, 29, 0, 12, 13, 0, 12, 17, 0, 12, 22, 0, 3, 114, 101, 100, 4, 98,
    108, 117, 101, 6, 112, 117, 114, 112, 108, 101, 2, 0, 24, 0, 12, 10, 0, 12, 17, 0, 6, 119, 105,
    110, 116, 101, 114, 6, 115, 112, 114, 105, 110, 103, 224, 2, 0, 0, 0, 2, 0, 0, 0, 2, 0, 0, 0,
    0, 0, 0, 0, 73, 0, 0, 0, 0, 3, 0, 72, 0, 25, 0, 4, 0, 29, 0, 6, 0, 35, 0, 8, 0, 4, 2, 0, 2, 43,
    0, 4, 1, 0, 102, 108, 97, 103, 108, 97, 98, 101, 108, 115, 102, 101, 97, 116, 117, 114, 101,
    100, 3, 0, 29, 0, 12, 13, 0, 12, 17, 0, 12, 22, 0, 3, 114, 101, 100, 4, 98, 108, 117, 101, 6,
    112, 117, 114, 112, 108, 101, 224, 2, 0, 0, 0, 2, 0, 0, 0, 2, 0, 0, 0, 0, 0, 0, 0, 110, 0, 0,
    0, 0, 4, 0, 109, 0, 32, 0, 4, 0, 36, 0, 6, 0, 42, 0, 6, 0, 48, 0, 8, 0, 4, 2, 0, 2, 56, 0, 2,
    85, 0, 4, 1, 0, 102, 108, 97, 103, 108, 97, 98, 101, 108, 115, 115, 101, 97, 115, 111, 110,
    102, 101, 97, 116, 117, 114, 101, 100, 3, 0, 29, 0, 12, 13, 0, 12, 17, 0, 12, 22, 0, 3, 114,
    101, 100, 4, 98, 108, 117, 101, 6, 112, 117, 114, 112, 108, 101, 2, 0, 24, 0, 12, 10, 0, 12,
    17, 0, 6, 119, 105, 110, 116, 101, 114, 6, 115, 112, 114, 105, 110, 103,
];

static PARTIAL_UPDATE_ENTITY_JSON: &[u8] = &[
    233, 0, 0, 0, 0, 0, 1, 0, 2, 0, 5, 1, 16, 0, 1, 0, 0, 0, 1, 1, 0, 36, 0, 0, 0, 1, 8, 36, 46,
    115, 101, 97, 115, 111, 110, 25, 2, 2, 0, 24, 0, 12, 10, 0, 12, 17, 0, 6, 119, 105, 110, 116,
    101, 114, 6, 115, 112, 114, 105, 110, 103, 0, 2, 0, 0, 0, 1, 1, 0, 36, 0, 0, 0, 1, 8, 36, 46,
    115, 101, 97, 115, 111, 110, 25, 2, 2, 0, 24, 0, 12, 10, 0, 12, 17, 0, 6, 119, 105, 110, 116,
    101, 114, 6, 115, 112, 114, 105, 110, 103,
];

static UPDATE_ROWS_ENTITY: &[u8] = &[
    71, 1, 0, 0, 0, 0, 1, 0, 2, 0, 4, 255, 255, 0, 2, 0, 0, 0, 9, 0, 80, 114, 111, 100, 117, 99,
    116, 32, 50, 21, 0, 80, 114, 111, 100, 117, 99, 116, 32, 50, 32, 100, 101, 115, 99, 114, 105,
    112, 116, 105, 111, 110, 128, 0, 0, 99, 38, 172, 0, 2, 0, 0, 0, 17, 0, 65, 119, 101, 115, 111,
    109, 101, 32, 80, 114, 111, 100, 117, 99, 116, 32, 50, 21, 0, 80, 114, 111, 100, 117, 99, 116,
    32, 50, 32, 100, 101, 115, 99, 114, 105, 112, 116, 105, 111, 110, 128, 0, 0, 99, 38, 172,
];

static MINIMAL_UPDATE_ENTITY: &[u8] = &[
    137, 0, 0, 0, 0, 0, 1, 0, 2, 0, 4, 1, 2, 254, 2, 0, 0, 0, 254, 17, 65, 119, 101, 115, 111, 109,
    101, 32, 80, 114, 111, 100, 117, 99, 116, 32, 50,
];

static TABLE_ROWS_EVENTS: Map<&'static str, (&[u8], EventType, &[u8])> = phf_map! {
    "partial_update_entity_json" => (TABLE_DEFINITION_PARTIAL_JSON, EventType::PARTIAL_UPDATE_ROWS_EVENT, PARTIAL_UPDATE_ENTITY_JSON),
    "partial_update_entity" => (TABLE_DEFINITION_ENTITY, EventType::PARTIAL_UPDATE_ROWS_EVENT, PARTIAL_UPDATE_ENTITY),
    "write_entity" => (TABLE_DEFINITION_ENTITY, EventType::WRITE_ROWS_EVENT, WRITE_ENTITY),
    "write_entity_with_null" => (TABLE_DEFINITION_ENTITY_WITH_NULL, EventType::WRITE_ROWS_EVENT, WRITE_ENTITY_WITH_NULL),
    "delete_entity_int" => (TABLE_DEFINITION_INT, EventType::DELETE_ROWS_EVENT, DELETE_ENTITY_INT),
    "update_entity_json" => (TABLE_DEFINITION_JSON, EventType::UPDATE_ROWS_EVENT, UPDATE_ROWS_JSON),
    "update_entity" => (TABLE_DEFINITION_ENTITY, EventType::UPDATE_ROWS_EVENT, UPDATE_ROWS_ENTITY),
    "minimal_update_entity" => (TABLE_DEFINITION_ENTITY_MINIMAL, EventType::PARTIAL_UPDATE_ROWS_EVENT, MINIMAL_UPDATE_ENTITY),
};

pub(crate) struct Fixture {
    fde: FormatDescriptionEvent<'static>,
}

impl Default for Fixture {
    fn default() -> Self {
        Self {
            fde: FormatDescriptionEvent::new(BinlogVersion::Version4),
        }
    }
}

impl Fixture {
    pub fn row_event(&self, name: &'static str) -> (TableMapEvent<'_>, RowsEventData<'_>) {
        row_event(name, &self.fde)
    }

    pub fn table_event(&self, name: &'static str) -> TableMapEvent<'_> {
        table_event(name, &self.fde)
    }
}

fn row_event<'a>(
    name: &'static str,
    fde: &'a FormatDescriptionEvent<'a>,
) -> (TableMapEvent<'a>, RowsEventData<'a>) {
    let (table_event, event_type, event_data) = TABLE_ROWS_EVENTS.get(name).unwrap();
    let event_size = BinlogEventHeader::LEN + event_data.len();
    let event_data = &mut ParseBuf(event_data);
    let ctx = BinlogCtx::new(event_size, fde);

    let rows_event = match event_type {
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
    };

    let table_event = parse_table_event(&fde, table_event);
    return (table_event, rows_event);
}

fn table_event<'a>(name: &'static str, fde: &'a FormatDescriptionEvent<'a>) -> TableMapEvent<'a> {
    let event_data = TABLE_DEFINITION.get(name).unwrap();

    parse_table_event(fde, *event_data)
}

fn parse_table_event<'a>(
    fde: &'a FormatDescriptionEvent,
    event_data: &'a [u8],
) -> TableMapEvent<'a> {
    let event_size = BinlogEventHeader::LEN + event_data.len();
    let event_data = &mut ParseBuf(event_data);
    let ctx = BinlogCtx::new(event_size, fde);

    event_data.parse(ctx).unwrap()
}
