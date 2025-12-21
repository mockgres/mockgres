mod common;

use tokio_postgres::Row;

#[tokio::test(flavor = "multi_thread")]
async fn where_column_does_not_appear_in_output() {
    let ctx = common::start().await;

    ctx.client
        .execute("create table t(a int4 not null, b int4)", &[])
        .await
        .expect("create");

    // constants for insert
    ctx.client
        .execute("insert into t values (1,10),(2,20),(3,30)", &[])
        .await
        .expect("insert");

    // extended query with param in WHERE
    let rows: Vec<Row> = ctx
        .client
        .query("select a from t where b >= 20 order by 1", &[])
        .await
        .expect("query ok");

    for r in &rows {
        dbg!(r);
        assert_eq!(r.len(), 1);
    }
    let vals: Vec<i32> = rows.iter().map(|r| r.get::<_, i32>(0)).collect();
    assert_eq!(vals, vec![2, 3]);

    let _ = ctx.shutdown.send(());
}
