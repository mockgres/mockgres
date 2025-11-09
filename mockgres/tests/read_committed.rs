mod common;

#[tokio::test(flavor = "multi_thread")]
async fn other_sessions_only_see_committed_rows() {
    let ctx = common::start().await;

    ctx.client
        .execute("create table mvcc_vis(id int primary key)", &[])
        .await
        .expect("create table");

    let client_b = ctx.new_client().await;

    ctx.client.execute("begin", &[]).await.expect("begin");
    ctx.client
        .execute("insert into mvcc_vis values (1)", &[])
        .await
        .expect("insert");

    let inside = ctx
        .client
        .query("select count(*) from mvcc_vis", &[])
        .await
        .expect("inside tx");
    assert_eq!(inside[0].get::<_, i64>(0), 1, "writer sees own insert");

    let outside = client_b
        .query("select count(*) from mvcc_vis", &[])
        .await
        .expect("outside read");
    assert_eq!(
        outside[0].get::<_, i64>(0),
        0,
        "others must not see uncommitted rows"
    );

    ctx.client.execute("commit", &[]).await.expect("commit");

    let after = client_b
        .query("select count(*) from mvcc_vis", &[])
        .await
        .expect("after commit");
    assert_eq!(
        after[0].get::<_, i64>(0),
        1,
        "committed rows become visible"
    );

    let _ = ctx.shutdown.send(());
}

#[tokio::test(flavor = "multi_thread")]
async fn statements_take_fresh_snapshots_in_read_committed() {
    let ctx = common::start().await;

    ctx.client
        .execute(
            "create table mvcc_notes(id int primary key, note text)",
            &[],
        )
        .await
        .expect("create table");
    ctx.client
        .execute("insert into mvcc_notes values (1, 'draft')", &[])
        .await
        .expect("seed row");

    let client_b = ctx.new_client().await;
    client_b.execute("begin", &[]).await.expect("begin reader");

    let first = client_b
        .query("select note from mvcc_notes where id = 1", &[])
        .await
        .expect("initial read");
    assert_eq!(first[0].get::<_, &str>(0), "draft");

    ctx.client
        .execute("update mvcc_notes set note = 'published' where id = 1", &[])
        .await
        .expect("writer update");

    let second = client_b
        .query("select note from mvcc_notes where id = 1", &[])
        .await
        .expect("second read");
    assert_eq!(
        second[0].get::<_, &str>(0),
        "published",
        "new statement sees committed change"
    );

    client_b
        .execute("rollback", &[])
        .await
        .expect("rollback reader");

    let _ = ctx.shutdown.send(());
}
