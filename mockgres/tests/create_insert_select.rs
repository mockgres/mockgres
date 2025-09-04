mod common;

#[tokio::test(flavor = "multi_thread")]
async fn create_insert_select_star_extended() {
    let ctx = common::start().await;

    ctx.client
        .execute("CREATE TABLE t(a INT, b INT, PRIMARY KEY(a));", &[])
        .await
        .expect("create table");

    ctx.client
        .execute("INSERT INTO t VALUES (2, 20), (1, 10);", &[])
        .await
        .expect("insert");

    let pg_rows = ctx
        .client
        .query("SELECT * FROM t;", &[])
        .await
        .expect("select");

    let mut rows: Vec<(i32, i32)> = pg_rows
        .into_iter()
        .map(|row| {
            let a: i32 = row.get(0);
            let b: i32 = row.get(1);
            (a, b)
        })
        .collect();

    rows.sort_by_key(|(a, _)| *a);
    assert_eq!(rows, vec![(1, 10), (2, 20)]);

    let _ = ctx.shutdown.send(());
}
