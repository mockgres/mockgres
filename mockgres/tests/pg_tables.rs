mod common;

use common::start;

#[tokio::test(flavor = "multi_thread")]
async fn pg_tables_contains_created_table() {
    let ctx = start().await;
    ctx.client.execute("create schema app", &[]).await.unwrap();
    ctx.client
        .execute("create table app.users(id int primary key)", &[])
        .await
        .unwrap();

    let row = ctx
        .client
        .query_one(
            "select schemaname, tablename, tableowner, tablespace, hasindexes, hasrules, hastriggers, rowsecurity \
             from pg_catalog.pg_tables \
             where schemaname = 'app' and tablename = 'users'",
            &[],
        )
        .await
        .unwrap();

    let schemaname: String = row.get(0);
    let tablename: String = row.get(1);
    let tableowner: String = row.get(2);
    let tablespace: Option<String> = row.get(3);
    let hasindexes: bool = row.get(4);
    let hasrules: bool = row.get(5);
    let hastriggers: bool = row.get(6);
    let rowsecurity: bool = row.get(7);

    assert_eq!(schemaname, "app");
    assert_eq!(tablename, "users");
    assert_eq!(tableowner, "mockgres");
    assert_eq!(tablespace, None);
    assert!(hasindexes);
    assert!(!hasrules);
    assert!(!hastriggers);
    assert!(!rowsecurity);

    let _ = ctx.shutdown.send(());
}

#[tokio::test(flavor = "multi_thread")]
async fn pg_tables_tracks_indexes_and_search_path_resolution() {
    let ctx = start().await;
    ctx.client
        .execute("create table idx_probe(id int, v int)", &[])
        .await
        .unwrap();

    let before = ctx
        .client
        .query_one(
            "select hasindexes from pg_catalog.pg_tables where schemaname = 'public' and tablename = 'idx_probe'",
            &[],
        )
        .await
        .unwrap();
    assert!(!before.get::<_, bool>(0));

    ctx.client
        .execute("create index idx_probe_v_idx on idx_probe (v)", &[])
        .await
        .unwrap();
    let after_create = ctx
        .client
        .query_one(
            "select hasindexes from pg_catalog.pg_tables where schemaname = 'public' and tablename = 'idx_probe'",
            &[],
        )
        .await
        .unwrap();
    assert!(after_create.get::<_, bool>(0));

    ctx.client
        .execute("drop index idx_probe_v_idx", &[])
        .await
        .unwrap();
    let after_drop = ctx
        .client
        .query_one(
            "select hasindexes from pg_catalog.pg_tables where schemaname = 'public' and tablename = 'idx_probe'",
            &[],
        )
        .await
        .unwrap();
    assert!(!after_drop.get::<_, bool>(0));

    ctx.client
        .execute("set search_path = pg_catalog", &[])
        .await
        .unwrap();
    let rows = ctx
        .client
        .query(
            "select count(*) from pg_tables where schemaname = 'public' and tablename = 'idx_probe'",
            &[],
        )
        .await
        .unwrap();
    assert_eq!(rows[0].get::<_, i64>(0), 1);

    let _ = ctx.shutdown.send(());
}
