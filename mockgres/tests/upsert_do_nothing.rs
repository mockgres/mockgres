mod common;

#[tokio::test(flavor = "multi_thread")]
async fn pk_conflict_without_target_is_ignored() {
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
            "insert into items values (1, 20) on conflict do nothing",
            &[],
        )
        .await
        .expect("conflict insert");

    let count: i64 = ctx
        .client
        .query_one("select count(*) from items", &[])
        .await
        .expect("count rows")
        .get(0);
    assert_eq!(count, 1);

    let qty: i32 = ctx
        .client
        .query_one("select qty from items where id = 1", &[])
        .await
        .expect("select qty")
        .get(0);
    assert_eq!(qty, 10);

    let _ = ctx.shutdown.send(());
}

#[tokio::test(flavor = "multi_thread")]
async fn pk_conflict_named_constraint_is_ignored() {
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
            "insert into items values (1, 30) on conflict on constraint items_pkey do nothing",
            &[],
        )
        .await
        .expect("conflict insert");

    let count: i64 = ctx
        .client
        .query_one("select count(*) from items", &[])
        .await
        .expect("count rows")
        .get(0);
    assert_eq!(count, 1);

    let qty: i32 = ctx
        .client
        .query_one("select qty from items where id = 1", &[])
        .await
        .expect("select qty")
        .get(0);
    assert_eq!(qty, 10);

    let _ = ctx.shutdown.send(());
}

#[tokio::test(flavor = "multi_thread")]
async fn unique_index_target_only_skips_matching_conflict() {
    let ctx = common::start().await;

    ctx.client
        .batch_execute("create table gadgets(id int primary key, code text unique, qty int)")
        .await
        .expect("create table");
    ctx.client
        .execute("insert into gadgets values (1, 'A', 5)", &[])
        .await
        .expect("seed row");
    ctx.client
        .execute(
            "insert into gadgets values (2, 'A', 10) on conflict (code) do nothing",
            &[],
        )
        .await
        .expect("conflict insert");

    let rows: Vec<(i32, String, i32)> = ctx
        .client
        .query("select id, code, qty from gadgets order by id", &[])
        .await
        .expect("select rows")
        .into_iter()
        .map(|r| (r.get(0), r.get(1), r.get(2)))
        .collect();
    assert_eq!(rows, vec![(1, "A".to_string(), 5)]);

    let _ = ctx.shutdown.send(());
}

#[tokio::test(flavor = "multi_thread")]
async fn conflict_on_non_target_unique_errors() {
    let ctx = common::start().await;

    ctx.client
        .batch_execute(
            "create table multi_unique(id int primary key, u1 int unique, u2 int unique)",
        )
        .await
        .expect("create table");
    ctx.client
        .execute("insert into multi_unique values (1, 1, 2)", &[])
        .await
        .expect("seed row");

    // conflict on u1, target u1 -> skip
    ctx.client
        .execute(
            "insert into multi_unique values (2, 1, 2) on conflict (u1) do nothing",
            &[],
        )
        .await
        .expect("conflict on u1 skipped");

    // conflict on u2, target u1 -> should error
    let err = ctx
        .client
        .execute(
            "insert into multi_unique values (3, 3, 2) on conflict (u1) do nothing",
            &[],
        )
        .await
        .expect_err("conflict on u2 should error");
    common::assert_db_error_contains(
        &err,
        "duplicate key value violates unique constraint",
    );

    let _ = ctx.shutdown.send(());
}

#[tokio::test(flavor = "multi_thread")]
async fn multi_row_insert_skips_only_conflicting_rows() {
    let ctx = common::start().await;

    ctx.client
        .batch_execute("create table gadgets(id int primary key, code text unique, qty int)")
        .await
        .expect("create table");
    ctx.client
        .execute("insert into gadgets values (1, 'A', 5)", &[])
        .await
        .expect("seed row");

    ctx.client
        .execute(
            "insert into gadgets values (3, 'B', 50), (4, 'A', 99) on conflict (code) do nothing",
            &[],
        )
        .await
        .expect("conflict insert");

    let rows: Vec<(i32, String, i32)> = ctx
        .client
        .query("select id, code, qty from gadgets order by id", &[])
        .await
        .expect("select rows")
        .into_iter()
        .map(|r| (r.get(0), r.get(1), r.get(2)))
        .collect();
    assert_eq!(
        rows,
        vec![(1, "A".to_string(), 5), (3, "B".to_string(), 50),]
    );

    let _ = ctx.shutdown.send(());
}

#[tokio::test(flavor = "multi_thread")]
async fn returning_reports_only_inserted_rows() {
    let ctx = common::start().await;

    ctx.client
        .batch_execute("create table gadgets(id int primary key, code text unique, qty int)")
        .await
        .expect("create table");
    ctx.client
        .execute("insert into gadgets values (1, 'A', 5)", &[])
        .await
        .expect("seed row");

    let rows: Vec<(i32, String)> = ctx
        .client
        .query(
            "insert into gadgets (id, code, qty)\n             values (10, 'C', 1), (11, 'A', 2)\n             on conflict (code) do nothing\n             returning id, code",
            &[],
        )
        .await
        .expect("insert with returning")
        .into_iter()
        .map(|r| (r.get(0), r.get(1)))
        .collect();
    assert_eq!(rows, vec![(10, "C".to_string())]);

    let _ = ctx.shutdown.send(());
}
