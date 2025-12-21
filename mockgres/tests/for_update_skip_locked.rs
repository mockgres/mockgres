mod common;

use tokio::time::{Duration, sleep};
use tokio_postgres::error::SqlState;

#[tokio::test(flavor = "multi_thread")]
async fn for_update_skip_locked_splits_between_sessions() {
    let ctx = common::start().await;
    ctx.client
        .execute("create table jobs(id int primary key, ready bool)", &[])
        .await
        .expect("create jobs");
    ctx.client
        .execute(
            "insert into jobs values (1, true), (2, true), (3, true), (4, true)",
            &[],
        )
        .await
        .expect("seed jobs");
    let client_b = ctx.new_client().await;

    ctx.client.execute("begin", &[]).await.expect("begin a");
    client_b.execute("begin", &[]).await.expect("begin b");

    let rows_a = fetch_ids(
        &ctx.client,
        "select id from jobs where ready limit 2 for update skip locked",
    )
    .await;
    let rows_b = fetch_ids(
        &client_b,
        "select id from jobs where ready limit 2 for update skip locked",
    )
    .await;

    assert_eq!(rows_a.len(), 2, "session A should lock first two rows");
    assert_eq!(rows_b.len(), 2, "session B should get remaining rows");
    for id in &rows_a {
        assert!(
            !rows_b.contains(id),
            "row {id} should not be visible to session B"
        );
    }

    ctx.client
        .execute("rollback", &[])
        .await
        .expect("rollback a");
    client_b.execute("rollback", &[]).await.expect("rollback b");
    let _ = ctx.shutdown.send(());
}

#[tokio::test(flavor = "multi_thread")]
async fn for_update_skip_locked_respects_ordering() {
    let ctx = common::start().await;
    ctx.client
        .execute("create table jobs(id int primary key, ready bool)", &[])
        .await
        .expect("create jobs");
    ctx.client
        .execute(
            "insert into jobs values (1, true), (2, true), (3, true), (4, true)",
            &[],
        )
        .await
        .expect("seed jobs");
    let client_b = ctx.new_client().await;

    ctx.client.execute("begin", &[]).await.expect("begin a");
    client_b.execute("begin", &[]).await.expect("begin b");

    let rows_a = fetch_ids(
        &ctx.client,
        "select id from jobs where ready order by id limit 2 for update skip locked",
    )
    .await;
    let rows_b = fetch_ids(
        &client_b,
        "select id from jobs where ready order by id limit 2 for update skip locked",
    )
    .await;

    assert_eq!(rows_a, vec![1, 2], "session A should lock first rows by id");
    assert_eq!(
        rows_b,
        vec![3, 4],
        "session B should see remaining ordered rows"
    );

    ctx.client
        .execute("rollback", &[])
        .await
        .expect("rollback a");
    client_b.execute("rollback", &[]).await.expect("rollback b");
    let _ = ctx.shutdown.send(());
}

#[tokio::test(flavor = "multi_thread")]
async fn update_conflicts_with_locked_row() {
    let ctx = common::start().await;
    ctx.client
        .execute(
            "create table accounts(id int primary key, balance int)",
            &[],
        )
        .await
        .expect("create accounts");
    ctx.client
        .execute("insert into accounts values (1, 10)", &[])
        .await
        .expect("seed accounts");
    let blocker = ctx.new_client().await;

    ctx.client
        .execute("begin", &[])
        .await
        .expect("begin locker");
    ctx.client
        .query_one("select id from accounts where id = 1 for update", &[])
        .await
        .expect("lock row");

    let err = blocker
        .execute("update accounts set balance = 5 where id = 1", &[])
        .await
        .expect_err("update should fail while locked");
    common::assert_db_error_contains(&err, "lock");

    ctx.client
        .execute("rollback", &[])
        .await
        .expect("rollback locker");
    let _ = ctx.shutdown.send(());
}

#[tokio::test(flavor = "multi_thread")]
async fn for_update_nowait_errors_immediately() {
    let ctx = common::start().await;
    ctx.client
        .execute("create table locks(id int primary key)", &[])
        .await
        .expect("create locks");
    ctx.client
        .execute("insert into locks values (1)", &[])
        .await
        .expect("seed locks");

    let other = ctx.new_client().await;

    ctx.client
        .execute("begin", &[])
        .await
        .expect("begin locker");
    ctx.client
        .query("select id from locks for update", &[])
        .await
        .expect("lock row");

    let err = other
        .query("select id from locks for update nowait", &[])
        .await
        .expect_err("NOWAIT should error");
    assert_eq!(
        err.code(),
        Some(&SqlState::LOCK_NOT_AVAILABLE),
        "expected 55P03"
    );

    ctx.client
        .execute("rollback", &[])
        .await
        .expect("rollback locker");
    let _ = ctx.shutdown.send(());
}

#[tokio::test(flavor = "multi_thread")]
async fn for_update_blocks_until_row_is_free() {
    let ctx = common::start().await;
    ctx.client
        .execute("create table blocking(id int primary key)", &[])
        .await
        .expect("create blocking");
    ctx.client
        .execute("insert into blocking values (1)", &[])
        .await
        .expect("seed blocking");
    let blocked_client = ctx.new_client().await;

    ctx.client
        .execute("begin", &[])
        .await
        .expect("begin holder");
    ctx.client
        .query("select id from blocking for update", &[])
        .await
        .expect("lock row");

    let runner = tokio::spawn(async move {
        blocked_client
            .query("select id from blocking for update", &[])
            .await
    });

    sleep(Duration::from_millis(50)).await;
    assert!(
        !runner.is_finished(),
        "second FOR UPDATE should block while lock held"
    );

    ctx.client
        .execute("commit", &[])
        .await
        .expect("commit holder");

    let rows = runner
        .await
        .expect("join spawned query")
        .expect("blocking select succeeds");
    assert_eq!(rows.len(), 1, "second reader runs after commit");

    let _ = ctx.shutdown.send(());
}

#[tokio::test(flavor = "multi_thread")]
async fn locks_release_after_rollback() {
    let ctx = common::start().await;
    ctx.client
        .execute("create table msgs(id int primary key)", &[])
        .await
        .expect("create msgs");
    ctx.client
        .execute("insert into msgs values (1)", &[])
        .await
        .expect("seed msgs");
    let client_b = ctx.new_client().await;

    ctx.client.execute("begin", &[]).await.expect("begin a");
    ctx.client
        .query("select id from msgs for update", &[])
        .await
        .expect("lock row");

    let rows_b = fetch_ids(&client_b, "select id from msgs for update skip locked").await;
    assert!(
        rows_b.is_empty(),
        "row should be locked until transaction ends"
    );

    ctx.client
        .execute("rollback", &[])
        .await
        .expect("rollback a");

    let rows_after = fetch_ids(&client_b, "select id from msgs for update").await;
    assert_eq!(
        rows_after,
        vec![1],
        "lock should be released after rollback"
    );
    let _ = ctx.shutdown.send(());
}

#[tokio::test(flavor = "multi_thread")]
async fn join_with_for_update_not_supported() {
    let ctx = common::start().await;
    ctx.client
        .execute("create table parents(id int primary key)", &[])
        .await
        .expect("create parents");
    ctx.client
        .execute(
            "create table children(id int primary key, parent_id int references parents(id))",
            &[],
        )
        .await
        .expect("create children");
    ctx.client
        .execute("insert into parents values (1)", &[])
        .await
        .expect("insert parent");
    ctx.client
        .execute("insert into children values (1, 1)", &[])
        .await
        .expect("insert child");

    let err = ctx
        .client
        .query("select parents.id from parents, children for update", &[])
        .await
        .expect_err("joins with FOR UPDATE should be rejected");
    common::assert_db_error_contains(
        &err,
        "FOR UPDATE is only supported for single-table SELECT",
    );

    let _ = ctx.shutdown.send(());
}

#[tokio::test(flavor = "multi_thread")]
async fn repeat_select_returns_same_rows_for_owner() {
    let ctx = common::start().await;
    ctx.client
        .execute("create table queue(id int primary key)", &[])
        .await
        .expect("create queue");
    ctx.client
        .execute("insert into queue values (1), (2)", &[])
        .await
        .expect("seed queue");
    ctx.client.execute("begin", &[]).await.expect("begin queue");

    let first = fetch_ids(
        &ctx.client,
        "select id from queue order by id for update skip locked",
    )
    .await;
    let second = fetch_ids(
        &ctx.client,
        "select id from queue order by id for update skip locked",
    )
    .await;
    assert_eq!(first, second, "same session should see locked rows");

    ctx.client
        .execute("rollback", &[])
        .await
        .expect("rollback queue txn");
    let _ = ctx.shutdown.send(());
}

async fn fetch_ids(client: &tokio_postgres::Client, sql: &str) -> Vec<i32> {
    client
        .query(sql, &[])
        .await
        .expect("query rows")
        .iter()
        .map(|row| row.get(0))
        .collect()
}
