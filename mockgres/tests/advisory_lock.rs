use std::time::Duration;

use tokio_postgres::SimpleQueryMessage;

mod common;

#[tokio::test(flavor = "multi_thread")]
async fn advisory_lock_returns_void() {
    let ctx = common::start().await;
    let msgs = ctx
        .client
        .simple_query("select pg_advisory_lock(42)")
        .await
        .expect("lock");
    let row = msgs
        .iter()
        .find_map(|m| match m {
            SimpleQueryMessage::Row(r) => Some(r),
            _ => None,
        })
        .expect("lock row");
    assert!(row.get(0).is_none(), "void result should be null");

    let msgs = ctx
        .client
        .simple_query("select pg_advisory_unlock(42)")
        .await
        .expect("unlock");
    let row = msgs
        .iter()
        .find_map(|m| match m {
            SimpleQueryMessage::Row(r) => Some(r),
            _ => None,
        })
        .expect("unlock row");
    assert_eq!(row.get(0), Some("t"), "unlock should return true");
}

#[tokio::test(flavor = "multi_thread")]
async fn advisory_lock_blocks_until_unlock() {
    let ctx = common::start().await;
    ctx.client
        .simple_query("select pg_advisory_lock(7)")
        .await
        .expect("lock");

    let client_b = ctx.new_client().await;
    let lock_task = tokio::spawn(async move {
        client_b
            .simple_query("select pg_advisory_lock(7)")
            .await
            .expect("lock in client b");
    });

    tokio::time::sleep(Duration::from_millis(200)).await;
    assert!(
        !lock_task.is_finished(),
        "second lock should block while held"
    );

    ctx.client
        .simple_query("select pg_advisory_unlock(7)")
        .await
        .expect("unlock");

    let _ = tokio::time::timeout(Duration::from_secs(2), lock_task)
        .await
        .expect("lock release should unblock client b");
}
