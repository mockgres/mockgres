mod common;

#[tokio::test(flavor = "multi_thread")]
async fn create_and_drop_index_noop() {
    let ctx = common::start().await;

    ctx.client
        .execute(
            "create table things(
                id int primary key,
                qty int not null,
                note text
            )",
            &[],
        )
        .await
        .expect("create table");

    ctx.client
        .execute("create index idx_things_qty on things (qty, id)", &[])
        .await
        .expect("create index");

    let dup_err = ctx
        .client
        .execute("create index idx_things_qty on things (qty)", &[])
        .await
        .expect_err("duplicate index create should fail");
    assert!(
        dup_err.to_string().contains("already exists"),
        "unexpected duplicate error: {dup_err}"
    );

    ctx.client
        .execute(
            "create index if not exists idx_things_qty on things (qty)",
            &[],
        )
        .await
        .expect("idempotent create");

    ctx.client
        .execute("drop index idx_things_qty", &[])
        .await
        .expect("drop index");

    let drop_err = ctx
        .client
        .execute("drop index idx_things_qty", &[])
        .await
        .expect_err("second drop should error");
    assert!(
        drop_err.to_string().contains("does not exist"),
        "unexpected drop error: {drop_err}"
    );

    ctx.client
        .execute("drop index if exists idx_things_qty", &[])
        .await
        .expect("drop if exists");

    let bad_col = ctx
        .client
        .execute("create index idx_things_missing on things (missing)", &[])
        .await
        .expect_err("unknown column should fail");
    assert!(
        bad_col.to_string().contains("unknown column"),
        "unexpected missing column error: {bad_col}"
    );

    let _ = ctx.shutdown.send(());
}
