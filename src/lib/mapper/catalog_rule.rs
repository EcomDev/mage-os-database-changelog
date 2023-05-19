use crate::error::Error;
use crate::log::IntoChangeLog;
use crate::log::{CatalogRuleChange, FieldUpdate};
use crate::mapper::ChangeLogMapper;
use crate::replication::Event;
use crate::schema::TableSchema;
pub struct CatalogRuleMapper;

impl ChangeLogMapper<CatalogRuleChange> for CatalogRuleMapper {
    fn map_event(
        &self,
        event: &Event,
        schema: &impl TableSchema,
    ) -> Result<Option<CatalogRuleChange>, Error> {
        Ok(match event {
            Event::InsertRow(row) => {
                Some(CatalogRuleChange::Created(row.parse("rule_id", schema)?))
            }
            Event::DeleteRow(row) => {
                Some(CatalogRuleChange::Deleted(row.parse("rule_id", schema)?))
            }
            Event::UpdateRow(row) => FieldUpdate::new(row.parse("rule_id", schema)?)
                .process_field_update("from_date", row, schema)
                .process_field_update("to_date", row, schema)
                .process_field_update("is_active", row, schema)
                .process_field_update("conditions_serialized", row, schema)
                .process_field_update("actions_serialized", row, schema)
                .process_field_update("simple_action", row, schema)
                .process_field_update("discount_amount", row, schema)
                .process_field_update("stop_rules_processing", row, schema)
                .process_field_update("sort_order", row, schema)
                .into_change_log(),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn reports_added_new_catalog_rules() {
        mapper_test!(
            CatalogRuleMapper,
            Some(
                CatalogRuleChange::Created(3)
            ),
            insert["Test", 3],
            ["name", "rule_id"]
        );
    }

    #[test]
    fn reports_deleted_catalog_rules() {
        mapper_test!(
            CatalogRuleMapper,
            Some(
                CatalogRuleChange::Deleted(3)
            ),
            delete["Test", 3],
            ["name", "rule_id"]
        );
    }

    #[test]
    fn ignores_updates_to_fields_that_do_not_affect_calculation() {
        mapper_test!(
            CatalogRuleMapper,
            None,
            update[(3, "Name Old", "Description Old"), (3, "Name New", "Description New")],
            ["rule_id", "name", "description"]
        );
    }

    #[test]
    fn report_updates_to_fields_that_affect_rule_activity() {
        mapper_test!(
            CatalogRuleMapper,
            Some(
                CatalogRuleChange::Fields(3, vec!["from_date", "to_date", "is_active"].into())
            ),
            update[(3, binlog_null!(), "2023-01-01", 1), (3, "2023-02-03", "2023-05-30", 0)],
            ["rule_id", "from_date", "to_date", "is_active"]
        );
    }

    #[test]
    fn report_updates_to_fields_that_affect_rule_conditions_and_actions() {
        mapper_test!(
            CatalogRuleMapper,
            Some(
                CatalogRuleChange::Fields(4, vec!["conditions_serialized", "actions_serialized", "simple_action",
                "discount_amount", "stop_rules_processing", "sort_order"].into())
            ),
            update[
                (4, binlog_null!(), binlog_null!(), binlog_null!(), binlog_null!(), binlog_null!(), binlog_null!(), binlog_null!()),
                (4, "", "", "", "", "", "", "")
            ],
            [
                "rule_id", "conditions_serialized", "actions_serialized", "simple_action",
                "discount_amount", "stop_rules_processing", "sort_order"
            ]
        );
    }
}
