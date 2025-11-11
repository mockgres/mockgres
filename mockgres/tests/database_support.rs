mod common;

use std::sync::Arc;

use tokio_postgres::error::SqlState;
use tokio_postgres::{Client, NoTls, SimpleQueryMessage};

#[tokio::test(flavor = "multi_thread")]
async fn database_routing_accepts_only_configured_name() {
    let config = mockgres::ServerConfig {
        database_name: "demo_db".to_string(),
    };
    let handler = Arc::new(mockgres::Mockgres::with_config(config.clone()));
    let (addr, server_task, shutdown) = common::spawn_server(handler).await;

    let conn_str = format!(
        "host={} port={} user=postgres dbname={}",
        addr.ip(),
        addr.port(),
        config.database_name
    );
    let (client, connection) = tokio_postgres::connect(&conn_str, NoTls)
        .await
        .expect("connect to configured database");
    let bg = tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("connection error: {e}");
        }
    });

    let rows = client
        .simple_query("select current_database()")
        .await
        .expect("select current_database()");
    let db_value = rows
        .iter()
        .find_map(|m| match m {
            SimpleQueryMessage::Row(row) => row.get(0).map(|v| v.to_string()),
            _ => None,
        })
        .expect("row in result set");
    assert_eq!(db_value, config.database_name);

    let wrong_conn_str = format!(
        "host={} port={} user=postgres dbname={}",
        addr.ip(),
        addr.port(),
        "other_db"
    );
    let err = match tokio_postgres::connect(&wrong_conn_str, NoTls).await {
        Ok(_) => panic!("connection should fail for unknown database"),
        Err(e) => e,
    };
    let db_err = err.as_db_error().expect("db error");
    assert_eq!(db_err.code(), &SqlState::INVALID_CATALOG_NAME);
    assert!(
        db_err.message().contains("does not exist"),
        "expected helpful message, got {}",
        db_err.message()
    );

    drop(client);
    let _ = shutdown.send(());
    let _ = server_task.await;
    let _ = bg.await;
}

#[tokio::test(flavor = "multi_thread")]
async fn database_ddl_commands_are_rejected() {
    let ctx = common::start().await;

    expect_feature_not_supported(
        &ctx.client,
        "create database foo",
        "CREATE DATABASE is not supported",
    )
    .await;
    expect_feature_not_supported(&ctx.client, "drop database foo", "DROP DATABASE").await;
    expect_feature_not_supported(&ctx.client, "alter database foo", "ALTER DATABASE").await;
}

async fn expect_feature_not_supported(client: &Client, sql: &str, snippet: &str) {
    let err = client
        .batch_execute(sql)
        .await
        .expect_err("statement should fail");
    let db_err = err.as_db_error().expect("db error");
    assert_eq!(db_err.code(), &SqlState::FEATURE_NOT_SUPPORTED);
    assert!(
        db_err.message().contains(snippet),
        "message '{}' did not contain expected snippet '{}'",
        db_err.message(),
        snippet
    );
}
