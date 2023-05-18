mod fixture;

use fixture::Fixture;
use mage_os_database_changelog::binlog_row;
use mage_os_database_changelog::error::Error;
use mage_os_database_changelog::replication::{
    Event, EventMetadata, ReplicationClient, UpdateRowEvent,
};
use mage_os_database_changelog::test_util::ObserverSpy;
use std::future::Future;

#[tokio::test]
async fn reports_insert_events() -> Result<(), Error> {
    let fixture = Fixture::create_with_database("test_database").await?;
    let client = client(&fixture);
    verify_client_events(
        client,
        fixture,
        |mut fixture| async move {
            fixture
                .insert_into(
                    "entity",
                    ["name", "description", "price"],
                    vec![
                        ("Product 3", "Product 3 description", 9.99),
                        ("Product 4", "Product 4 description", 99.99),
                    ],
                )
                .await
        },
        vec![
            Event::InsertRow(binlog_row!(
                1,
                "Product 3",
                "Product 3 description",
                "9.9900"
            )),
            Event::InsertRow(binlog_row!(
                2,
                "Product 4",
                "Product 4 description",
                "99.9900"
            )),
        ],
    )
    .await
}

#[tokio::test]
async fn reports_delete_events() -> Result<(), Error> {
    let mut fixture = Fixture::create_with_database("test_database").await?;
    fixture
        .insert_into(
            "entity",
            ["name", "description", "price"],
            vec![
                ("Product 3", "Product 3 description", 9.99),
                ("Product 4", "Product 4 description", 99.99),
            ],
        )
        .await?;

    let client = client(&fixture);
    verify_client_events(
        client,
        fixture,
        |mut fixture| async move {
            fixture
                .execute_queries(vec!["DELETE FROM entity WHERE entity_id = 1"])
                .await
        },
        vec![Event::DeleteRow(binlog_row!(
            1,
            "Product 3",
            "Product 3 description",
            "9.9900"
        ))],
    )
    .await
}

#[tokio::test]
async fn reports_update_events() -> Result<(), Error> {
    let mut fixture = Fixture::create_with_database("test_database").await?;
    fixture
        .insert_into(
            "entity",
            ["name", "description", "price"],
            vec![
                ("Product 3", "Product 3 description", 9.99),
                ("Product 4", "Product 4 description", 99.99),
            ],
        )
        .await?;

    let client = client(&fixture);
    verify_client_events(
        client,
        fixture,
        |mut fixture| async move {
            fixture
                .execute_queries(vec![
                    "UPDATE entity SET name ='Updated Name' WHERE entity_id = 1",
                ])
                .await
        },
        vec![Event::UpdateRow(UpdateRowEvent::new(
            binlog_row!(1, "Product 3", "Product 3 description", "9.9900"),
            binlog_row!(1, "Updated Name", "Product 3 description", "9.9900"),
        ))],
    )
    .await
}

#[tokio::test]
async fn ignores_events_not_related_to_own_data_base() -> Result<(), Error> {
    let fixture = Fixture::create_with_database("test_database").await?;
    let mut another_fixture = Fixture::create_with_database("test_database").await?;

    another_fixture
        .insert_into(
            "entity",
            ["name", "description", "price"],
            vec![
                ("Product 3", "Product 3 description", 9.99),
                ("Product 4", "Product 4 description", 99.99),
            ],
        )
        .await?;

    let client = client(&fixture);
    verify_client_events(
        client,
        another_fixture,
        |mut fixture| async move {
            fixture
                .execute_queries(vec![
                    "UPDATE entity SET name ='Updated Name' WHERE entity_id = 1",
                ])
                .await
        },
        vec![],
    )
    .await
}

#[tokio::test]
async fn notifies_metadata_on_each_separate_event() -> Result<(), Error> {
    let fixture = Fixture::create_with_database("test_database").await?;
    let client = client(&fixture);
    let observer = perform_client_action(client, fixture, |mut fixture| async move {
        fixture
            .insert_into(
                "entity",
                ["name", "description", "price"],
                vec![
                    ("Product 3", "Product 3 description", 9.99),
                    ("Product 4", "Product 4 description", 99.99),
                ],
            )
            .await?;

        fixture
            .execute_queries(vec![
                "UPDATE entity SET name ='Updated Name' WHERE entity_id = 1",
            ])
            .await
    })
    .await?;

    assert_eq!(observer.metadata().len(), 2);

    Ok(())
}

#[tokio::test]
async fn changes_binlog_filename_on_flush_event() -> Result<(), Error> {
    let fixture = Fixture::create_with_database("test_database").await?;
    let client = client(&fixture);
    let observer = perform_client_action(client, fixture, |mut fixture| async move {
        fixture
            .insert_into(
                "entity",
                ["name", "description", "price"],
                vec![("Product 3", "Product 3 description", 9.99)],
            )
            .await?;

        fixture.execute_queries(vec!["FLUSH LOGS"]).await?;

        fixture
            .execute_queries(vec![
                "UPDATE entity SET name ='Updated Name' WHERE entity_id = 1",
            ])
            .await
    })
    .await?;

    assert_eq!(
        unique_metadata_values(observer, |metadata| metadata
            .binlog_position()
            .file()
            .to_string()),
        2
    );

    Ok(())
}

#[tokio::test]
async fn updates_position_in_each_metadata() -> Result<(), Error> {
    let fixture = Fixture::create_with_database("test_database").await?;
    let client = client(&fixture);
    let observer = perform_client_action(client, fixture, |mut fixture| async move {
        fixture
            .insert_into(
                "entity",
                ["name", "description", "price"],
                vec![("Product 3", "Product 3 description", 9.99)],
            )
            .await?;

        fixture
            .execute_queries(vec![
                "UPDATE entity SET name ='Updated Name' WHERE entity_id = 1",
            ])
            .await
    })
    .await?;

    assert_eq!(
        unique_metadata_values(observer, |metadata| metadata.binlog_position().position()),
        2
    );

    Ok(())
}

fn unique_metadata_values<T>(observer: ObserverSpy, getter: impl Fn(&EventMetadata) -> T) -> usize
where
    T: PartialEq,
{
    let mut values: Vec<T> = observer.metadata().iter().map(getter).collect();
    values.dedup();
    values.len()
}

fn create_client(
    fixture: &Fixture,
    table_prefix: &'static str,
) -> ReplicationClient<String, &'static str> {
    let client = ReplicationClient::new(
        Fixture::create_database(),
        fixture.database_name().unwrap().into_owned(),
        table_prefix,
    );
    client
}

fn client(fixture: &Fixture) -> ReplicationClient<String, &'static str> {
    create_client(fixture, "")
}

async fn verify_client_events<F, Fut>(
    client: ReplicationClient<String, &'static str>,
    fixture: Fixture,
    action: F,
    events: Vec<Event>,
) -> Result<(), Error>
where
    F: FnOnce(Fixture) -> Fut,
    Fut: Future<Output = Result<(), Error>>,
{
    let observer = perform_client_action(client, fixture, action).await?;

    assert_eq!(observer.events(), events);

    Ok(())
}

async fn perform_client_action<F, Fut>(
    client: ReplicationClient<String, &str>,
    fixture: Fixture,
    action: F,
) -> Result<ObserverSpy, Error>
where
    F: FnOnce(Fixture) -> Fut,
    Fut: Future<Output = Result<(), Error>>,
{
    let position = Fixture::binlog_position().await?;

    action(fixture).await?;

    let observer = ObserverSpy::default();
    client.process(observer.clone(), position).await?;
    Ok(observer)
}
