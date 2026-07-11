mod common;

use std::net::SocketAddr;
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
    assert_eq!(
        connection.parameter("server_version"),
        Some(mockgres::POSTGRES_COMPAT_VERSION)
    );
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
async fn create_database_enables_isolated_connections() {
    let handler = Arc::new(mockgres::Mockgres::default());
    let (addr, server_task, shutdown) = common::spawn_server(handler).await;
    let (admin, admin_bg) = connect_client(addr, "postgres").await;

    admin
        .batch_execute(
            "create table database_local(id int primary key);
             insert into database_local values (1);",
        )
        .await
        .expect("seed bootstrap database");

    let affected = admin
        .execute(
            "create database regression
             template=template0 encoding='UTF8'
             locale='C' locale_provider='builtin'",
            &[],
        )
        .await
        .expect("create regression database");
    assert_eq!(affected, 0);

    let duplicate = admin
        .execute("create database regression", &[])
        .await
        .expect_err("duplicate database should fail");
    assert_eq!(
        duplicate.as_db_error().expect("database error").code(),
        &SqlState::DUPLICATE_DATABASE
    );

    let (regression, regression_bg) = connect_client(addr, "regression").await;
    assert_eq!(
        first_cell(&regression, "select current_database()").await,
        "regression"
    );

    let missing_table = regression
        .query("select id from database_local", &[])
        .await
        .expect_err("databases should have isolated catalogs");
    assert_eq!(
        missing_table.as_db_error().expect("database error").code(),
        &SqlState::UNDEFINED_TABLE
    );

    regression
        .batch_execute(
            "create table database_local(id int primary key);
             insert into database_local values (2);",
        )
        .await
        .expect("seed created database");
    assert_eq!(
        first_cell(&admin, "select id from database_local").await,
        "1"
    );
    assert_eq!(
        first_cell(&regression, "select id from database_local").await,
        "2"
    );

    drop(regression);
    drop(admin);
    let _ = shutdown.send(());
    let _ = server_task.await;
    let _ = regression_bg.await;
    let _ = admin_bg.await;
}

#[tokio::test(flavor = "multi_thread")]
async fn create_database_is_rejected_inside_transaction() {
    let ctx = common::start().await;

    ctx.client.batch_execute("begin").await.expect("begin");
    let err = ctx
        .client
        .execute("create database transacted", &[])
        .await
        .expect_err("CREATE DATABASE in a transaction should fail");
    assert_eq!(
        err.as_db_error().expect("database error").code(),
        &SqlState::ACTIVE_SQL_TRANSACTION
    );
    ctx.client
        .batch_execute("rollback")
        .await
        .expect("rollback");
}

#[tokio::test(flavor = "multi_thread")]
async fn database_metadata_changes_are_accepted_for_regression_compatibility() {
    let ctx = common::start().await;

    for statement in [
        "create database metadata_target",
        "alter database metadata_target connection_limit 5",
        "drop database metadata_target",
    ] {
        ctx.client
            .execute(statement, &[])
            .await
            .unwrap_or_else(|error| panic!("{statement}: {error}"));
    }
}

async fn connect_client(addr: SocketAddr, database: &str) -> (Client, tokio::task::JoinHandle<()>) {
    let conn_str = format!(
        "host={} port={} user=postgres dbname={database}",
        addr.ip(),
        addr.port()
    );
    let (client, connection) = tokio_postgres::connect(&conn_str, NoTls)
        .await
        .unwrap_or_else(|err| panic!("connect to {database}: {err}"));
    let bg = tokio::spawn(async move {
        if let Err(err) = connection.await {
            eprintln!("connection error: {err}");
        }
    });
    (client, bg)
}

async fn first_cell(client: &Client, sql: &str) -> String {
    let rows = client.simple_query(sql).await.expect("simple query");
    rows.iter()
        .find_map(|message| match message {
            SimpleQueryMessage::Row(row) => row.get(0).map(str::to_string),
            _ => None,
        })
        .expect("first cell")
}
