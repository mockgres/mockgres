mod common;

#[tokio::test(flavor = "multi_thread")]
async fn unique_constraints_from_create_table() {
    let ctx = common::start().await;

    ctx.client
        .batch_execute("create table t_unique_col(a int unique, b int, id int primary key)")
        .await
        .unwrap();
    ctx.client
        .batch_execute("insert into t_unique_col values (10, 1, 1)")
        .await
        .unwrap();
    let err = ctx
        .client
        .batch_execute("insert into t_unique_col values (10, 2, 2)")
        .await
        .expect_err("duplicate unique column insert");
    assert!(
        err.to_string()
            .contains("duplicate key value violates unique constraint"),
        "unexpected error: {err}"
    );
    ctx.client
        .batch_execute("insert into t_unique_col values (NULL, 3, 3)")
        .await
        .unwrap();
    ctx.client
        .batch_execute("insert into t_unique_col values (NULL, 4, 4)")
        .await
        .unwrap();

    ctx.client
        .batch_execute(
            "create table t_unique_table(id int primary key, a int, b int, unique (a, b))",
        )
        .await
        .unwrap();
    ctx.client
        .batch_execute("insert into t_unique_table values (1, 5, 6)")
        .await
        .unwrap();
    let err = ctx
        .client
        .batch_execute("insert into t_unique_table values (2, 5, 6)")
        .await
        .expect_err("duplicate unique table insert");
    assert!(
        err.to_string()
            .contains("duplicate key value violates unique constraint"),
        "unexpected error: {err}"
    );
    ctx.client
        .batch_execute("insert into t_unique_table values (3, 5, NULL)")
        .await
        .unwrap();
    ctx.client
        .batch_execute("insert into t_unique_table values (4, NULL, 6)")
        .await
        .unwrap();

    let _ = ctx.shutdown.send(());
}
