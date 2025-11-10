mod common;

use tokio_postgres::error::SqlState;
use tokio_postgres::Row;

#[tokio::test(flavor = "multi_thread")]
async fn nan_equality_and_comparison_via_filters() {
    let ctx = common::start().await;

    ctx.client
        .execute("create table t(v float8)", &[])
        .await
        .expect("create");

    // insert constants only: 1, 2, NaN, 3, NaN
    ctx.client
        .execute(
            "insert into t values (1.0),(2.0),(float8 'NaN'),(3.0),(float8 'NaN')",
            &[],
        )
        .await
        .expect("insert");

    // v = v should be true even for NaN with our postgres-like rule
    let rows: Vec<Row> = ctx
        .client
        .query("select v from t where v = v", &[])
        .await
        .expect("select v=v");
    assert_eq!(rows.len(), 5);

    // compare against a param in where (extended protocol)
    let stmt = ctx
        .client
        .prepare("select v from t where v > $1")
        .await
        .expect("prepare with param");
    let rows: Vec<Row> = ctx
        .client
        .query(&stmt, &[&123.0_f64])
        .await
        .expect("v > param");
    assert_eq!(rows.len(), 2, "only the two NaNs are > 123.0");
    for row in rows {
        let v: Option<f64> = row.get(0);
        assert!(v.map(|x| x.is_nan()).unwrap_or(false));
    }

    let _ = ctx.shutdown.send(());
}

#[tokio::test(flavor = "multi_thread")]
async fn order_by_with_nan_and_nulls_extended() {
    let ctx = common::start().await;

    ctx.client
        .execute("create table t(v float8)", &[])
        .await
        .expect("create");

    // constants: 2.0, null, NaN, 1.0
    ctx.client
        .execute(
            "insert into t values (2.0),(null),(float8 'NaN'),(1.0)",
            &[],
        )
        .await
        .expect("insert");

    // asc: expect 1.0, 2.0, NaN, NULL (nulls last)
    let asc_rows: Vec<Row> = ctx
        .client
        .query("select v from t order by 1 asc", &[])
        .await
        .expect("asc");
    let asc_vals: Vec<Option<f64>> = asc_rows
        .iter()
        .map(|r| r.get::<_, Option<f64>>(0))
        .collect();

    assert!(asc_vals[0] == Some(1.0));
    assert!(asc_vals[1] == Some(2.0));
    assert!(asc_vals[2].map(|x| x.is_nan()).unwrap_or(false));
    assert!(asc_vals[3].is_none());

    // desc: default nulls first, then NaN, then numbers
    let desc_rows: Vec<Row> = ctx
        .client
        .query("select v from t order by 1 desc", &[])
        .await
        .expect("desc");
    let desc_vals: Vec<Option<f64>> = desc_rows
        .iter()
        .map(|r| r.get::<_, Option<f64>>(0))
        .collect();

    assert!(desc_vals[0].is_none());
    assert!(desc_vals[1].map(|x| x.is_nan()).unwrap_or(false));
    assert!(desc_vals[2] == Some(2.0));
    assert!(desc_vals[3] == Some(1.0));

    let _ = ctx.shutdown.send(());
}

#[tokio::test(flavor = "multi_thread")]
async fn insert_nan_requires_explicit_literal() {
    let ctx = common::start().await;
    ctx.client
        .execute("create table nan_insert(v float8)", &[])
        .await
        .expect("create table");

    ctx.client
        .execute("insert into nan_insert values (float8 'NaN')", &[])
        .await
        .expect("literal NaN insert");

    let err = ctx
        .client
        .execute("insert into nan_insert values (nan)", &[])
        .await
        .expect_err("bare identifier nan should fail");
    assert_eq!(err.code(), Some(&SqlState::INTERNAL_ERROR));
    assert!(
        err.to_string()
            .contains("INSERT expressions cannot reference columns"),
        "unexpected error message: {err}"
    );

    let _ = ctx.shutdown.send(());
}
