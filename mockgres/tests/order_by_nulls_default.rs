// tests/order_by_nulls_default.rs
mod common;

use tokio_postgres::Row;

#[tokio::test(flavor = "multi_thread")]
async fn default_nulls_last_for_asc_first_for_desc() {
    let ctx = common::start().await;

    ctx.client
        .execute("create table t(x int4)", &[])
        .await
        .expect("create");

    // constants for insert: 2, null, 1
    ctx.client
        .execute("insert into t values (2),(null),(1)", &[])
        .await
        .expect("insert");

    // asc: nulls last
    let asc_rows: Vec<Row> = ctx
        .client
        .query("select x from t order by 1 asc", &[])
        .await
        .expect("asc ok");
    let asc_vals: Vec<Option<i32>> = asc_rows
        .iter()
        .map(|r| r.get::<_, Option<i32>>(0))
        .collect();
    assert_eq!(asc_vals, vec![Some(1), Some(2), None]);

    // desc: nulls first
    let desc_rows: Vec<Row> = ctx
        .client
        .query("select x from t order by 1 desc", &[])
        .await
        .expect("desc ok");
    let desc_vals: Vec<Option<i32>> = desc_rows
        .iter()
        .map(|r| r.get::<_, Option<i32>>(0))
        .collect();
    assert_eq!(desc_vals, vec![None, Some(2), Some(1)]);

    let _ = ctx.shutdown.send(());
}
