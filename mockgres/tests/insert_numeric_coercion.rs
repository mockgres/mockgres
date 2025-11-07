mod common;

use tokio_postgres::Row;

#[tokio::test(flavor = "multi_thread")]
async fn insert_int_into_float8_and_mixed_types() {
    let ctx = common::start().await;

    ctx.client
        .execute("create table t(a float8 not null)", &[])
        .await
        .expect("create");

    // extended protocol (execute) but insert must use constants in your engine
    ctx.client
        .execute("insert into t values (1),(2),(3.5)", &[])
        .await
        .expect("insert");

    // extended protocol select
    let rows: Vec<Row> = ctx
        .client
        .query("select a from t order by 1 asc", &[])
        .await
        .expect("select ok");

    let vals: Vec<f64> = rows.iter().map(|r| r.get::<_, f64>(0)).collect();
    assert_eq!(vals, vec![1.0, 2.0, 3.5]);

    let _ = ctx.shutdown.send(());
}

#[tokio::test(flavor = "multi_thread")]
async fn reject_float_into_int4_on_insert() {
    let ctx = common::start().await;

    ctx.client
        .execute("create table t(a int4 not null)", &[])
        .await
        .expect("create");

    // insert must use constants; this should fail (float into int4)
    let err = ctx
        .client
        .execute("insert into t values (1.5)", &[])
        .await
        .expect_err("should error");

    // keep assertion loose: just ensure we got a db error
    let msg = err.to_string().to_lowercase();
    assert!(msg.contains("type") && msg.contains("mismatch"));

    let _ = ctx.shutdown.send(());
}
