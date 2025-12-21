mod common;

#[tokio::test(flavor = "multi_thread")]
async fn basic_update_applies_set() {
    let ctx = common::start().await;

    ctx.client
        .batch_execute("create table items(id int primary key, qty int)")
        .await
        .expect("create table");
    ctx.client
        .execute("insert into items values (1, 10)", &[])
        .await
        .expect("seed row");
    ctx.client
        .execute(
            "insert into items values (1, 20)\n             on conflict (id) do update set qty = excluded.qty",
            &[],
        )
        .await
        .expect("conflict update");

    let qty: i32 = ctx
        .client
        .query_one("select qty from items where id = 1", &[])
        .await
        .expect("select qty")
        .get(0);
    assert_eq!(qty, 20);

    let _ = ctx.shutdown.send(());
}

#[tokio::test(flavor = "multi_thread")]
async fn where_clause_can_skip_update() {
    let ctx = common::start().await;

    ctx.client
        .batch_execute("create table items(id int primary key, qty int)")
        .await
        .expect("create table");
    ctx.client
        .execute("insert into items values (1, 20)", &[])
        .await
        .expect("seed row");
    ctx.client
        .execute(
            "insert into items values (1, 30)\n             on conflict (id) do update set qty = excluded.qty where false",
            &[],
        )
        .await
        .expect("conflict with skipped update");

    let qty: i32 = ctx
        .client
        .query_one("select qty from items where id = 1", &[])
        .await
        .expect("select qty")
        .get(0);
    assert_eq!(qty, 20);

    let _ = ctx.shutdown.send(());
}

#[tokio::test(flavor = "multi_thread")]
async fn multi_row_mix_of_insert_and_update() {
    let ctx = common::start().await;

    ctx.client
        .batch_execute("create table items(id int primary key, qty int)")
        .await
        .expect("create table");
    ctx.client
        .execute("insert into items values (1, 10)", &[])
        .await
        .expect("seed row");

    ctx.client
        .execute(
            "insert into items values (1, 5), (2, 10), (1, 99)\n             on conflict (id) do update set qty = excluded.qty",
            &[],
        )
        .await
        .expect("batch insert/update");

    let rows: Vec<(i32, i32)> = ctx
        .client
        .query("select id, qty from items order by id", &[])
        .await
        .expect("select rows")
        .into_iter()
        .map(|r| (r.get(0), r.get(1)))
        .collect();
    assert_eq!(rows, vec![(1, 99), (2, 10)]);

    let _ = ctx.shutdown.send(());
}

#[tokio::test(flavor = "multi_thread")]
async fn update_uses_expressions() {
    let ctx = common::start().await;

    ctx.client
        .batch_execute("create table items(id int primary key, qty int)")
        .await
        .expect("create table");
    ctx.client
        .execute("insert into items values (1, 10)", &[])
        .await
        .expect("seed row");

    ctx.client
        .execute(
            "insert into items values (1, 5)\n             on conflict (id) do update set qty = items.qty + excluded.qty",
            &[],
        )
        .await
        .expect("conflict update");

    let qty: i32 = ctx
        .client
        .query_one("select qty from items where id = 1", &[])
        .await
        .expect("select qty")
        .get(0);
    assert_eq!(qty, 15);

    let _ = ctx.shutdown.send(());
}

#[tokio::test(flavor = "multi_thread")]
async fn non_target_conflicts_error() {
    let ctx = common::start().await;

    ctx.client
        .batch_execute("create table t(a int unique, b int unique)")
        .await
        .expect("create table");
    ctx.client
        .execute("insert into t values (1, 1)", &[])
        .await
        .expect("seed row");

    // conflict on target column a -> update succeeds
    ctx.client
        .execute(
            "insert into t values (1, 2) on conflict (a) do update set b = excluded.b",
            &[],
        )
        .await
        .expect("update on targeted conflict");

    // conflict on b (non-target) should error
    let err = ctx
        .client
        .execute(
            "insert into t values (2, 2) on conflict (a) do update set b = excluded.b",
            &[],
        )
        .await
        .expect_err("conflict on non-target should fail");
    common::assert_db_error_contains(
        &err,
        "duplicate key value violates unique constraint",
    );

    let _ = ctx.shutdown.send(());
}

#[tokio::test(flavor = "multi_thread")]
async fn returning_reports_updates_and_inserts() {
    let ctx = common::start().await;

    ctx.client
        .batch_execute("create table items(id int primary key, qty int)")
        .await
        .expect("create table");
    ctx.client
        .execute("insert into items values (1, 1)", &[])
        .await
        .expect("seed row");

    let rows: Vec<(i32, i32)> = ctx
        .client
        .query(
            "insert into items values (1, 5), (3, 7)\n             on conflict (id) do update set qty = excluded.qty\n             returning id, qty",
            &[],
        )
        .await
        .expect("insert with returning")
        .into_iter()
        .map(|r| (r.get(0), r.get(1)))
        .collect();
    let mut sorted = rows;
    sorted.sort_by_key(|(id, _)| *id);
    assert_eq!(sorted, vec![(1, 5), (3, 7)]);

    let _ = ctx.shutdown.send(());
}
