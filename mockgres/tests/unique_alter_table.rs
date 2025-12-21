mod common;

#[tokio::test(flavor = "multi_thread")]
async fn alter_table_add_and_drop_unique_constraints() {
    let ctx = common::start().await;

    ctx.client
        .batch_execute("create table u(id int primary key, a int, b int)")
        .await
        .unwrap();

    ctx.client
        .batch_execute("alter table u add unique (a)")
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
        .expect_err("duplicate on a");
    common::assert_db_error_contains(&err, "unique constraint u_a_key");

    ctx.client
        .batch_execute("alter table u drop constraint u_a_key")
        .await
        .unwrap();
    ctx.client
        .batch_execute("insert into u values (2, 10, 2)")
        .await
        .unwrap();

    ctx.client
        .batch_execute("alter table u add constraint uniq_ab unique (a, b)")
        .await
        .unwrap();
    ctx.client
        .batch_execute("insert into u values (3, 20, 3)")
        .await
        .unwrap();
    let err = ctx
        .client
        .batch_execute("insert into u values (4, 20, 3)")
        .await
        .expect_err("duplicate on (a,b)");
    common::assert_db_error_contains(&err, "unique constraint uniq_ab");

    ctx.client
        .batch_execute("alter table u drop constraint uniq_ab")
        .await
        .unwrap();
    ctx.client
        .batch_execute("insert into u values (4, 20, 3)")
        .await
        .unwrap();

    ctx.client
        .batch_execute("alter table u drop constraint if exists nonexistent_constraint")
        .await
        .unwrap();

    ctx.client
        .batch_execute("alter table u add constraint uniq_b unique (b)")
        .await
        .unwrap();
    ctx.client.batch_execute("drop index uniq_b").await.unwrap();
    ctx.client
        .batch_execute("insert into u values (5, 30, 3)")
        .await
        .unwrap();

    let _ = ctx.shutdown.send(());
}
