mod fixture;
use fixture::Fixture;
use mage_os_database_changelog::error::Error;
use mage_os_database_changelog::schema::SchemaInformation;

#[tokio::test]
async fn populates_columns_from_information_schema() -> Result<(), Error> {
    let fixture = Fixture::create_with_database("test_database").await?;
    let database_name = fixture.database_name().unwrap();
    let mut connection = Fixture::create_connection().await?;
    let mut schema = SchemaInformation::default();
    schema.populate(&mut connection, database_name, "").await?;

    fixture.cleanup().await?;

    assert_eq!(
        vec![
            schema.get_column_position("entity", "entity_id"),
            schema.get_column_position("entity_int", "attribute_id"),
            schema.get_column_position("entity_not_real", "attribute_id"),
            schema.get_column_position("entity_json", "value"),
        ],
        vec![Some(0), Some(1), None, Some(4)]
    );

    assert_eq!(
        vec![
            schema.is_generated_primary_key("entity", "entity_id"),
            schema.is_generated_primary_key("entity_int", "attribute_id"),
        ],
        vec![true, false]
    );

    Ok(())
}

#[tokio::test]
async fn takes_into_account_table_prefix_for_column_names() -> Result<(), Error> {
    let fixture = Fixture::create_with_database("test_database").await?;
    let database_name = fixture.database_name().unwrap();
    let mut connection = Fixture::create_connection().await?;
    let mut schema = SchemaInformation::default();
    schema
        .populate(&mut connection, database_name, "enti")
        .await?;

    fixture.cleanup().await?;

    assert_eq!(
        vec![
            schema.get_column_position("ty", "entity_id"),
            schema.get_column_position("ty_int", "attribute_id"),
            schema.get_column_position("entity", "entity_id"),
        ],
        vec![Some(0), Some(1), None]
    );

    assert_eq!(
        vec![
            schema.is_generated_primary_key("ty", "entity_id"),
            schema.is_generated_primary_key("ty_int", "attribute_id"),
        ],
        vec![true, false]
    );

    Ok(())
}
