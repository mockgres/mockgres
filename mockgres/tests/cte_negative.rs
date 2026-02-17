mod common;

fn assert_unsupported(err: &tokio_postgres::Error, expected_message: &str) {
    let db_err = err.as_db_error().expect("expected db error");
    assert_eq!(db_err.code().code(), "0A000");
    assert_eq!(db_err.message(), expected_message);
}

#[tokio::test(flavor = "multi_thread")]
async fn recursive_cte_is_rejected() {
    let ctx = common::start().await;
    let err = ctx
        .client
        .query("with recursive c(n) as (select 1) select n from c", &[])
        .await
        .expect_err("expected recursive CTE error");
    assert_unsupported(&err, "WITH RECURSIVE is not supported");
    let _ = ctx.shutdown.send(());
}

#[tokio::test(flavor = "multi_thread")]
async fn data_modifying_cte_body_with_returning_is_supported() {
    let ctx = common::start().await;
    ctx.client
        .batch_execute(
            "create table t(id int primary key);
             insert into t values (1), (2), (3)",
        )
        .await
        .unwrap();
    let rows = ctx
        .client
        .query(
            "with c as (delete from t returning id) select id from c order by id",
            &[],
        )
        .await
        .unwrap();
    let ids: Vec<i32> = rows.into_iter().map(|row| row.get(0)).collect();
    assert_eq!(ids, vec![1, 2, 3]);
    let _ = ctx.shutdown.send(());
}

#[tokio::test(flavor = "multi_thread")]
async fn cte_materialization_variant_is_rejected() {
    let ctx = common::start().await;
    let err = ctx
        .client
        .query(
            "with c as materialized (select 1 as id) select id from c",
            &[],
        )
        .await
        .expect_err("expected CTE materialization error");
    assert_unsupported(&err, "CTE materialization options are not supported");
    let _ = ctx.shutdown.send(());
}

#[tokio::test(flavor = "multi_thread")]
async fn cte_alias_count_mismatch_is_rejected() {
    let ctx = common::start().await;
    let err = ctx
        .client
        .query("with c(a, b) as (select 1) select a from c", &[])
        .await
        .expect_err("expected alias mismatch error");
    let db_err = err.as_db_error().expect("expected db error");
    assert_eq!(db_err.code().code(), "42601");
    assert_eq!(
        db_err.message(),
        "CTE \"c\" has 1 columns but 2 column aliases were provided"
    );
    let _ = ctx.shutdown.send(());
}

#[tokio::test(flavor = "multi_thread")]
async fn cte_forward_reference_is_supported() {
    let ctx = common::start().await;
    let row = ctx
        .client
        .query_one(
            "with second as (select id from first), first as (select 1 as id) select id from second",
            &[],
        )
        .await
        .unwrap();
    let id: i32 = row.get(0);
    assert_eq!(id, 1);
    let _ = ctx.shutdown.send(());
}

#[tokio::test(flavor = "multi_thread")]
async fn circular_cte_dependencies_are_rejected() {
    let ctx = common::start().await;
    let err = ctx
        .client
        .query(
            "with first as (select id from second), second as (select id from first) select id from first",
            &[],
        )
        .await
        .expect_err("expected circular dependency error");
    assert_unsupported(&err, "circular CTE dependencies are not supported");
    let _ = ctx.shutdown.send(());
}
