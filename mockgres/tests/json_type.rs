mod common;

use common::start;
use tokio_postgres::error::SqlState;
use tokio_postgres::types::{Json, Type};

#[tokio::test(flavor = "multi_thread")]
async fn create_insert_select_json() {
    let ctx = start().await;

    ctx.client
        .batch_execute("create table t (v json)")
        .await
        .expect("create table");
    ctx.client
        .execute("insert into t (v) values ($1)", &[&r#"{"a":1}"#])
        .await
        .expect("insert json");

    let rows = ctx
        .client
        .query("select v from t", &[])
        .await
        .expect("select");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].columns()[0].type_(), &Type::JSON);
    let v: Json<serde_json::Value> = rows[0].get(0);
    assert_eq!(v.0, serde_json::json!({"a": 1}));

    let _ = ctx.shutdown.send(());
}

#[tokio::test(flavor = "multi_thread")]
async fn create_insert_select_jsonb() {
    let ctx = start().await;

    ctx.client
        .batch_execute("create table t (v jsonb)")
        .await
        .expect("create table");
    ctx.client
        .execute("insert into t (v) values ($1)", &[&r#"{"a":1}"#])
        .await
        .expect("insert jsonb");

    let rows = ctx
        .client
        .query("select v from t", &[])
        .await
        .expect("select");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].columns()[0].type_(), &Type::JSONB);
    let v: Json<serde_json::Value> = rows[0].get(0);
    assert_eq!(v.0, serde_json::json!({"a": 1}));

    let _ = ctx.shutdown.send(());
}

#[tokio::test(flavor = "multi_thread")]
async fn reject_invalid_json() {
    let ctx = start().await;

    ctx.client
        .batch_execute("create table t (v json)")
        .await
        .expect("create table");

    let err = ctx
        .client
        .execute("insert into t (v) values ('{\"a\": }')", &[])
        .await
        .expect_err("invalid json should error");
    assert_eq!(err.code(), Some(&SqlState::INVALID_TEXT_REPRESENTATION));

    let _ = ctx.shutdown.send(());
}

#[tokio::test(flavor = "multi_thread")]
async fn reject_invalid_jsonb() {
    let ctx = start().await;

    ctx.client
        .batch_execute("create table t (v jsonb)")
        .await
        .expect("create table");

    let err = ctx
        .client
        .execute("insert into t (v) values ('{\"a\": }')", &[])
        .await
        .expect_err("invalid jsonb should error");
    assert_eq!(err.code(), Some(&SqlState::INVALID_TEXT_REPRESENTATION));

    let _ = ctx.shutdown.send(());
}

#[tokio::test(flavor = "multi_thread")]
async fn pg_type_reports_json_oid() {
    let ctx = start().await;

    let rows = ctx
        .client
        .query(
            "select oid from pg_catalog.pg_type where typname = 'json'",
            &[],
        )
        .await
        .expect("pg_type query");
    assert_eq!(rows.len(), 1);
    let oid: i32 = rows[0].get(0);
    assert_eq!(oid, 114);

    let _ = ctx.shutdown.send(());
}
