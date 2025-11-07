mod common;

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
        .execute("insert into t values (1.0),(2.0),(nan),(3.0),(nan)", &[])
        .await
        .expect("insert");

    // v = v should be true even for NaN with our postgres-like rule
    let rows: Vec<Row> = ctx
        .client
        .query("select count(*)::int8 from t where v = v", &[])
        .await
        .expect("count v=v");
    let cnt_v_eq_v: i64 = rows[0].get(0);
    assert_eq!(cnt_v_eq_v, 5);

    // compare against a param in where (extended protocol)
    let rows: Vec<Row> = ctx
        .client
        .query("select count(*)::int8 from t where v > $1", &[&123.0_f64])
        .await
        .expect("count v>param");
    let cnt_gt: i64 = rows[0].get(0);
    assert_eq!(cnt_gt, 2, "only the two NaNs are > 123.0");

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
        .execute("insert into t values (2.0),(null),(nan),(1.0)", &[])
        .await
        .expect("insert");

    // asc: expect 1.0, 2.0, NaN, NULL (nulls last)
    let asc_rows: Vec<Row> = ctx
        .client
        .query("select v from t order by 1 asc", &[])
        .await
        .expect("asc");
    let asc_vals: Vec<Option<f64>> = asc_rows.iter().map(|r| r.get::<_, Option<f64>>(0)).collect();

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
    let desc_vals: Vec<Option<f64>> = desc_rows.iter().map(|r| r.get::<_, Option<f64>>(0)).collect();

    assert!(desc_vals[0].is_none());
    assert!(desc_vals[1].map(|x| x.is_nan()).unwrap_or(false));
    assert!(desc_vals[2] == Some(2.0));
    assert!(desc_vals[3] == Some(1.0));

    let _ = ctx.shutdown.send(());
}
