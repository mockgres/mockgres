mod common;

#[tokio::test(flavor = "multi_thread")]
async fn do_nothing_without_target_skips_any_unique_violation() {
    let ctx = common::start().await;

    ctx.client
        .batch_execute("create table conflict_any(id int primary key, code int unique)")
        .await
        .expect("create table");
    ctx.client
        .execute("insert into conflict_any values (1, 10)", &[])
        .await
        .expect("seed row");

    ctx.client
        .execute(
            "insert into conflict_any values (1, 20), (2, 10), (3, 30) on conflict do nothing",
            &[],
        )
        .await
        .expect("conflict insert");

    let rows: Vec<(i32, i32)> = ctx
        .client
        .query("select id, code from conflict_any order by id", &[])
        .await
        .expect("select rows")
        .into_iter()
        .map(|row| (row.get(0), row.get(1)))
        .collect();
    assert_eq!(rows, vec![(1, 10), (3, 30)]);

    let _ = ctx.shutdown.send(());
}

#[tokio::test(flavor = "multi_thread")]
async fn do_nothing_only_applies_to_targeted_constraint() {
    let ctx = common::start().await;

    ctx.client
        .batch_execute("create table conflict_target(id int primary key, code int unique)")
        .await
        .expect("create table");
    ctx.client
        .execute("insert into conflict_target values (1, 7)", &[])
        .await
        .expect("seed row");

    let skipped = ctx
        .client
        .execute(
            "insert into conflict_target values (2, 7) on conflict (code) do nothing",
            &[],
        )
        .await
        .expect("targeted conflict");
    assert_eq!(skipped, 0);

    let err = ctx
        .client
        .execute(
            "insert into conflict_target values (1, 8) on conflict (code) do nothing",
            &[],
        )
        .await;
    assert!(err.is_err(), "primary key conflict should error");

    let _ = ctx.shutdown.send(());
}

#[tokio::test(flavor = "multi_thread")]
async fn do_nothing_on_named_constraint_resolves_primary_key() {
    let ctx = common::start().await;

    ctx.client
        .batch_execute("create table conflict_named(id int primary key, note text)")
        .await
        .expect("create table");
    ctx.client
        .execute("insert into conflict_named values (1, 'a')", &[])
        .await
        .expect("seed row");

    let skipped = ctx
        .client
        .execute(
            "insert into conflict_named values (1, 'b') on conflict on constraint conflict_named_pkey do nothing",
            &[],
        )
        .await
        .expect("named constraint conflict");
    assert_eq!(skipped, 0);

    let rows: Vec<(i32, String)> = ctx
        .client
        .query("select id, note from conflict_named order by id", &[])
        .await
        .expect("select rows")
        .into_iter()
        .map(|row| (row.get(0), row.get(1)))
        .collect();
    assert_eq!(rows, vec![(1, "a".to_string())]);

    let _ = ctx.shutdown.send(());
}
