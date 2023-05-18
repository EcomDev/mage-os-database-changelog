use mage_os_database_changelog::log::{
    ChangeLogMapper, FieldUpdate, IntoChangeLog, ItemChange, ProductChange,
};
use mage_os_database_changelog::replication::Event;
use mage_os_database_changelog::replication::Event::UpdateRow;
use mage_os_database_changelog::schema::TableSchema;

struct MyAwesomeTableNameMatchMapper;

impl ChangeLogMapper<ItemChange> for MyAwesomeTableNameMatchMapper {
    fn map_event(
        &self,
        event: &Event,
        schema: &impl TableSchema,
    ) -> Result<Option<ItemChange>, Error> {
        Ok(match schema.table_name() {
            "my_awesome_table_for_product" => MyAwesomeTableMapper
                .map_event(event, schema)?
                .map(ItemChange::ProductChange),
            _ => None,
        })
    }
}

pub struct MyAwesomeTableMapper;

impl ChangeLogMapper<ProductChange> for MyAwesomeTableMapper {
    fn map_event(
        &self,
        event: &Event,
        schema: &impl TableSchema,
    ) -> Result<Option<ProductChange>, Error> {
        Ok(match event {
            UpdateRow(row) => FieldUpdate::new(row.parse("product_id", schema)?)
                .process_field_update("not_awesome_field", row, schema)
                .process_field_update("super_awesome_field", row, schema)
                .into_change_log(),
            _ => None,
        })
    }
}

use mage_os_database_changelog::app::{command_from_cli, Application};
use mage_os_database_changelog::error::Error;

#[tokio::main]
async fn main() -> Result<(), Error> {
    Application::new()
        .with_mapper(MyAwesomeTableNameMatchMapper)
        .run(command_from_cli())
        .await
}
