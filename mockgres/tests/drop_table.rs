mod common;

#[tokio::test(flavor = "multi_thread")]
async fn drop_table_removes_catalog() {
    let ctx = common::start().await;

    ctx.client
        .execute("create table drop_me(id int primary key)", &[])
        .await
        .expect("create table");
    ctx.client
        .execute("insert into drop_me values (1)", &[])
        .await
        .expect("insert row");

    ctx.client
        .execute("drop table drop_me", &[])
        .await
        .expect("drop table");

    ctx.client
        .execute("create table drop_me(id int primary key)", &[])
        .await
        .expect("recreate after drop");

    let _ = ctx.shutdown.send(());
}

#[tokio::test(flavor = "multi_thread")]
async fn drop_table_if_exists_suppresses_missing_error() {
    let ctx = common::start().await;

    let err = ctx
        .client
        .execute("drop table doesnt_exist", &[])
        .await
        .expect_err("drop missing table");
    assert!(
        err.to_string().contains("no such table"),
        "unexpected error: {err}"
    );

    ctx.client
        .execute("drop table if exists doesnt_exist", &[])
        .await
        .expect("drop if exists succeeds");

    let _ = ctx.shutdown.send(());
}
