mod common;

#[tokio::test(flavor = "multi_thread")]
async fn in_predicate_supports_subquery() {
    let ctx = common::start().await;

    ctx.client
        .batch_execute("create table nums(n int primary key)")
        .await
        .unwrap();
    ctx.client
        .batch_execute("insert into nums values (1),(2),(3),(4)")
        .await
        .unwrap();

    let rows = ctx
        .client
        .query(
            "select n from nums where n in (select n from nums where n <= 2) order by n",
            &[],
        )
        .await
        .unwrap();
    let vals: Vec<i32> = rows.into_iter().map(|r| r.get(0)).collect();
    assert_eq!(vals, vec![1, 2]);

    let _ = ctx.shutdown.send(());
}
