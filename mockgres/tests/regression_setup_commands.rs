mod common;

use tokio_postgres::error::SqlState;

fn assert_sqlstate(error: &tokio_postgres::Error, expected: &SqlState) {
    let db_error = error.as_db_error().expect("expected database error");
    assert_eq!(db_error.code(), expected, "unexpected error: {db_error:?}");
}

#[tokio::test(flavor = "multi_thread")]
async fn regression_setup_settings_are_stateful_and_validate_values() {
    let ctx = common::start().await;

    assert_eq!(
        common::simple_first_cell(&ctx.client, "show synchronous_commit").await,
        "on"
    );
    ctx.client
        .execute("set synchronous_commit = remote_write", &[])
        .await
        .expect("set synchronous_commit");
    assert_eq!(
        common::simple_first_cell(&ctx.client, "show synchronous_commit").await,
        "remote_write"
    );
    ctx.client
        .execute("reset synchronous_commit", &[])
        .await
        .expect("reset synchronous_commit");
    assert_eq!(
        common::simple_first_cell(&ctx.client, "show synchronous_commit").await,
        "on"
    );

    assert_eq!(
        common::simple_first_cell(&ctx.client, "show allow_in_place_tablespaces").await,
        "off"
    );
    ctx.client
        .execute("set allow_in_place_tablespaces = true", &[])
        .await
        .expect("enable in-place tablespaces");
    assert_eq!(
        common::simple_first_cell(&ctx.client, "show allow_in_place_tablespaces").await,
        "on"
    );
    ctx.client
        .execute("set allow_in_place_tablespaces = default", &[])
        .await
        .expect("reset in-place tablespaces");
    assert_eq!(
        common::simple_first_cell(&ctx.client, "show allow_in_place_tablespaces").await,
        "off"
    );

    let error = ctx
        .client
        .execute("set synchronous_commit = sometimes", &[])
        .await
        .expect_err("invalid synchronous_commit should fail");
    assert_sqlstate(&error, &SqlState::INVALID_PARAMETER_VALUE);

    let error = ctx
        .client
        .execute("set allow_in_place_tablespaces = maybe", &[])
        .await
        .expect_err("invalid boolean setting should fail");
    assert_sqlstate(&error, &SqlState::INVALID_PARAMETER_VALUE);

    let _ = ctx.shutdown.send(());
}

#[tokio::test(flavor = "multi_thread")]
async fn schema_grants_validate_targets_and_are_accepted_as_authorization_noops() {
    let ctx = common::start().await;

    ctx.client
        .execute("grant all on schema public to public", &[])
        .await
        .expect("grant schema privileges");
    ctx.client
        .execute("revoke all on schema public from public", &[])
        .await
        .expect("revoke schema privileges");

    let error = ctx
        .client
        .execute("grant usage on schema missing_schema to public", &[])
        .await
        .expect_err("missing schema should fail");
    assert_sqlstate(&error, &SqlState::INVALID_SCHEMA_NAME);

    let error = ctx
        .client
        .execute("grant select on table missing_table to public", &[])
        .await
        .expect_err("non-schema grants should remain unsupported");
    assert_sqlstate(&error, &SqlState::FEATURE_NOT_SUPPORTED);

    let _ = ctx.shutdown.send(());
}

#[tokio::test(flavor = "multi_thread")]
async fn tablespaces_are_tracked_and_obey_nontransactional_rules() {
    let ctx = common::start().await;

    let error = ctx
        .client
        .execute("create tablespace empty_location location ''", &[])
        .await
        .expect_err("empty location should require the compatibility setting");
    assert_sqlstate(&error, &SqlState::INVALID_OBJECT_DEFINITION);

    let error = ctx
        .client
        .execute(
            "create tablespace relative_location location 'relative'",
            &[],
        )
        .await
        .expect_err("relative tablespace location should fail");
    assert_sqlstate(&error, &SqlState::INVALID_OBJECT_DEFINITION);

    ctx.client
        .execute("set allow_in_place_tablespaces = on", &[])
        .await
        .expect("enable in-place tablespaces");
    ctx.client
        .execute("create tablespace regress_tblspace location ''", &[])
        .await
        .expect("create regression tablespace");

    let error = ctx
        .client
        .execute("create tablespace regress_tblspace location ''", &[])
        .await
        .expect_err("duplicate tablespace should fail");
    assert_sqlstate(&error, &SqlState::DUPLICATE_OBJECT);

    ctx.client.execute("begin", &[]).await.expect("begin");
    let error = ctx
        .client
        .execute("drop tablespace regress_tblspace", &[])
        .await
        .expect_err("drop tablespace in transaction should fail");
    assert_sqlstate(&error, &SqlState::ACTIVE_SQL_TRANSACTION);
    ctx.client.execute("rollback", &[]).await.expect("rollback");

    ctx.client
        .execute("drop tablespace regress_tblspace", &[])
        .await
        .expect("drop regression tablespace");
    ctx.client
        .execute("drop tablespace if exists regress_tblspace", &[])
        .await
        .expect("drop missing tablespace with if exists");

    let error = ctx
        .client
        .execute("drop tablespace regress_tblspace", &[])
        .await
        .expect_err("drop missing tablespace should fail");
    assert_sqlstate(&error, &SqlState::UNDEFINED_OBJECT);

    let error = ctx
        .client
        .execute("drop tablespace pg_default", &[])
        .await
        .expect_err("built-in tablespaces cannot be dropped");
    assert_sqlstate(&error, &SqlState::INSUFFICIENT_PRIVILEGE);

    ctx.client.execute("begin", &[]).await.expect("begin");
    let error = ctx
        .client
        .execute("create tablespace in_transaction location ''", &[])
        .await
        .expect_err("create tablespace in transaction should fail");
    assert_sqlstate(&error, &SqlState::ACTIVE_SQL_TRANSACTION);
    ctx.client.execute("rollback", &[]).await.expect("rollback");

    let _ = ctx.shutdown.send(());
}

#[tokio::test(flavor = "multi_thread")]
async fn vacuum_and_analyze_validate_relations_and_transaction_context() {
    let ctx = common::start().await;
    ctx.client
        .execute("create table maintenance_target (id int4)", &[])
        .await
        .expect("create table");

    ctx.client
        .execute("vacuum maintenance_target", &[])
        .await
        .expect("vacuum table");
    ctx.client
        .execute("vacuum analyze maintenance_target", &[])
        .await
        .expect("vacuum analyze table");
    ctx.client
        .execute("analyze maintenance_target", &[])
        .await
        .expect("analyze table");

    let error = ctx
        .client
        .execute("vacuum missing_table", &[])
        .await
        .expect_err("vacuum missing table should fail");
    assert_sqlstate(&error, &SqlState::UNDEFINED_TABLE);

    ctx.client.execute("begin", &[]).await.expect("begin");
    ctx.client
        .execute("analyze maintenance_target", &[])
        .await
        .expect("analyze is allowed in a transaction");
    let error = ctx
        .client
        .execute("vacuum maintenance_target", &[])
        .await
        .expect_err("vacuum in transaction should fail");
    assert_sqlstate(&error, &SqlState::ACTIVE_SQL_TRANSACTION);
    ctx.client.execute("rollback", &[]).await.expect("rollback");

    let _ = ctx.shutdown.send(());
}

#[tokio::test(flavor = "multi_thread")]
async fn planner_settings_and_storage_introspection_are_accepted_as_noops() {
    let ctx = common::start().await;

    ctx.client.execute("begin", &[]).await.expect("begin");

    for setting in [
        "set local enable_seqscan = false",
        "set local enable_indexonlyscan = false",
        "set local enable_bitmapscan = false",
        "set geqo = on",
        "set geqo_threshold = 2",
    ] {
        ctx.client
            .execute(setting, &[])
            .await
            .unwrap_or_else(|error| panic!("{setting} failed: {error:?}"));
    }
    ctx.client.execute("rollback", &[]).await.expect("rollback");

    let row = ctx
        .client
        .query_one("select pg_relation_size('mock_index')", &[])
        .await
        .expect("query no-op relation size");
    assert_eq!(row.get::<_, i64>(0), 0);
    let row = ctx
        .client
        .query_one(
            "select current_setting('max_prepared_transactions'), pg_numa_available(), getdatabaseencoding(), pg_char_to_encoding('UTF8'), 'Linux-GNU' ~* 'linux-gnu'",
            &[],
        )
        .await
        .expect("query compatibility settings");
    assert_eq!(row.get::<_, &str>(0), "0");
    assert!(!row.get::<_, bool>(1));
    assert_eq!(row.get::<_, &str>(2), "UTF8");
    assert_eq!(row.get::<_, i32>(3), 6);
    assert!(row.get::<_, bool>(4));
    assert!(
        ctx.client
            .query("select 1 where 0 != pg_relation_size('mock_index')", &[])
            .await
            .expect("filter a SELECT without FROM")
            .is_empty()
    );

    ctx.client
        .execute("do $$ begin null; end $$", &[])
        .await
        .expect("execute procedural no-op");

    let _ = ctx.shutdown.send(());
}
