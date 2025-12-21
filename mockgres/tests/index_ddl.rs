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
    common::assert_db_error_contains(&dup_err, "already exists");

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
    common::assert_db_error_contains(&drop_err, "does not exist");

    ctx.client
        .execute("drop index if exists idx_things_qty", &[])
        .await
        .expect("drop if exists");

    let bad_col = ctx
        .client
        .execute("create index idx_things_missing on things (missing)", &[])
        .await
        .expect_err("unknown column should fail");
    common::assert_db_error_contains(&bad_col, "unknown column");

    let _ = ctx.shutdown.send(());
}
