mod common;

#[tokio::test(flavor = "multi_thread")]
async fn rollback_discards_changes() {
    let ctx = common::start().await;

    ctx.client
        .execute("create table tx_items(id int primary key)", &[])
        .await
        .expect("create table");

    ctx.client.execute("begin", &[]).await.expect("begin");
    ctx.client
        .execute("insert into tx_items values (1)", &[])
        .await
        .expect("insert in tx");

    // Visible inside the transaction.
    let inside = ctx
        .client
        .query("select id from tx_items", &[])
        .await
        .expect("query inside tx");
    assert_eq!(inside.len(), 1);

    ctx.client.execute("rollback", &[]).await.expect("rollback");

    let rows = ctx
        .client
        .query("select id from tx_items", &[])
        .await
        .expect("query after rollback");
    assert!(rows.is_empty(), "rollback should discard inserted rows");

    let _ = ctx.shutdown.send(());
}

#[tokio::test(flavor = "multi_thread")]
async fn commit_persists_and_errors_on_misuse() {
    let ctx = common::start().await;

    ctx.client
        .execute("create table tx_books(id int primary key, title text)", &[])
        .await
        .expect("create table");

    ctx.client.execute("begin", &[]).await.expect("begin");
    ctx.client
        .execute(
            "insert into tx_books values (1, 'draft'), (2, 'final')",
            &[],
        )
        .await
        .expect("insert data");
    ctx.client.execute("commit", &[]).await.expect("commit");

    let rows = ctx
        .client
        .query("select id from tx_books order by id", &[])
        .await
        .expect("load rows");
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0].get::<_, i32>(0), 1);
    assert_eq!(rows[1].get::<_, i32>(0), 2);

    // COMMIT without BEGIN should error.
    let err = ctx.client.execute("commit", &[]).await.expect_err("no txn");
    let msg = err
        .as_db_error()
        .expect("expected db error")
        .message()
        .to_lowercase();
    assert!(
        msg.contains("no transaction"),
        "unexpected commit error: {msg:?}"
    );

    ctx.client.execute("begin", &[]).await.expect("begin 2");
    let err = ctx
        .client
        .execute("begin", &[])
        .await
        .expect_err("double begin");
    let msg = err
        .as_db_error()
        .expect("expected db error")
        .message()
        .to_lowercase();
    assert!(
        msg.contains("already in progress"),
        "unexpected begin error: {msg:?}"
    );
    ctx.client
        .execute("rollback", &[])
        .await
        .expect("rollback 2");

    let _ = ctx.shutdown.send(());
}
