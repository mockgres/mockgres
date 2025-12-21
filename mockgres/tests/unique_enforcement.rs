mod common;

#[tokio::test(flavor = "multi_thread")]
async fn unique_index_blocks_duplicates_and_allows_nulls() {
    let ctx = common::start().await;
    ctx.client
        .batch_execute("create table u(id int primary key, a int, b int)")
        .await
        .unwrap();
    ctx.client
        .batch_execute("create unique index ux_a on u(a)")
        .await
        .unwrap();

    ctx.client
        .batch_execute("insert into u values (1, 10, 1)")
        .await
        .unwrap();
    let err = ctx
        .client
        .batch_execute("insert into u values (2, 10, 2)")
        .await
        .expect_err("dup");
    common::assert_db_error_contains(&err, "unique constraint ux_a");

    ctx.client
        .batch_execute("insert into u values (3, NULL, 3)")
        .await
        .unwrap();
    ctx.client
        .batch_execute("insert into u values (4, NULL, 4)")
        .await
        .unwrap();

    let err = ctx
        .client
        .batch_execute("update u set a = 10 where id = 3")
        .await
        .expect_err("upd dup");
    common::assert_db_error_contains(&err, "unique constraint ux_a");

    ctx.client.batch_execute("drop index ux_a").await.unwrap();
    ctx.client
        .batch_execute("create unique index ux_ab on u(a, b)")
        .await
        .unwrap();

    ctx.client
        .batch_execute("insert into u values (5, 20, 5)")
        .await
        .unwrap();
    let err = ctx
        .client
        .batch_execute("insert into u values (6, 20, 5)")
        .await
        .expect_err("dup ab");
    common::assert_db_error_contains(&err, "unique constraint ux_ab");

    ctx.client
        .batch_execute("insert into u values (7, 20, NULL)")
        .await
        .unwrap();
    ctx.client
        .batch_execute("insert into u values (8, 20, NULL)")
        .await
        .unwrap();
    ctx.client
        .batch_execute("insert into u values (9, NULL, 5)")
        .await
        .unwrap();
    ctx.client
        .batch_execute("insert into u values (10, NULL, 5)")
        .await
        .unwrap();

    let _ = ctx.shutdown.send(());
}
