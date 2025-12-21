mod common;

use tokio_postgres::SimpleQueryMessage;

#[tokio::test(flavor = "multi_thread")]
async fn schema_search_path_resolution() {
    let ctx = common::start().await;

    ctx.client.execute("create schema s1", &[]).await.unwrap();
    ctx.client.execute("create schema s2", &[]).await.unwrap();
    ctx.client
        .execute("create table s1.t(a int)", &[])
        .await
        .unwrap();
    ctx.client
        .execute("create table s2.t(a int)", &[])
        .await
        .unwrap();
    ctx.client
        .execute("insert into s1.t values (1)", &[])
        .await
        .unwrap();
    ctx.client
        .execute("insert into s2.t values (1), (2)", &[])
        .await
        .unwrap();

    ctx.client
        .execute("set search_path = s1, s2", &[])
        .await
        .unwrap();
    let rows = ctx
        .client
        .query("select count(*) from t", &[])
        .await
        .unwrap();
    assert_eq!(rows[0].get::<_, i64>(0), 1);

    ctx.client
        .execute("set search_path = s2, s1", &[])
        .await
        .unwrap();
    let rows = ctx
        .client
        .query("select count(*) from t", &[])
        .await
        .unwrap();
    assert_eq!(rows[0].get::<_, i64>(0), 2);

    let rows = ctx
        .client
        .query("select count(*) from s1.t", &[])
        .await
        .unwrap();
    assert_eq!(rows[0].get::<_, i64>(0), 1);
    let rows = ctx
        .client
        .query("select count(*) from s2.t", &[])
        .await
        .unwrap();
    assert_eq!(rows[0].get::<_, i64>(0), 2);

    let _ = ctx.shutdown.send(());
}

#[tokio::test(flavor = "multi_thread")]
async fn create_table_uses_search_path() {
    let ctx = common::start().await;
    ctx.client.execute("create schema sp", &[]).await.unwrap();
    ctx.client
        .execute("set search_path = sp, public", &[])
        .await
        .unwrap();
    ctx.client
        .execute("create table x(a int)", &[])
        .await
        .unwrap();
    ctx.client
        .execute("insert into sp.x values (1)", &[])
        .await
        .unwrap();
    let rows = ctx
        .client
        .query("select count(*) from sp.x", &[])
        .await
        .unwrap();
    assert_eq!(rows[0].get::<_, i64>(0), 1);
    let _ = ctx.shutdown.send(());
}

#[tokio::test(flavor = "multi_thread")]
async fn cross_schema_foreign_keys() {
    let ctx = common::start().await;
    ctx.client.execute("create schema p", &[]).await.unwrap();
    ctx.client.execute("create schema c", &[]).await.unwrap();
    ctx.client
        .execute("create table p.parents(id int primary key)", &[])
        .await
        .unwrap();
    ctx.client
        .execute(
            "create table c.children(pid int references p.parents(id))",
            &[],
        )
        .await
        .unwrap();
    ctx.client
        .execute("insert into p.parents values (1)", &[])
        .await
        .unwrap();
    ctx.client
        .execute("insert into c.children values (1)", &[])
        .await
        .unwrap();
    let rows = ctx
        .client
        .query("select count(*) from c.children", &[])
        .await
        .unwrap();
    assert_eq!(rows[0].get::<_, i64>(0), 1);
    let _ = ctx.shutdown.send(());
}

#[tokio::test(flavor = "multi_thread")]
async fn cross_schema_fk_resolves_via_search_path() {
    let ctx = common::start().await;
    ctx.client
        .execute("create schema parents", &[])
        .await
        .unwrap();
    ctx.client
        .execute("create schema children", &[])
        .await
        .unwrap();
    ctx.client
        .execute("create table parents.p(id int primary key)", &[])
        .await
        .unwrap();
    ctx.client
        .execute("set search_path = children, parents", &[])
        .await
        .unwrap();
    ctx.client
        .execute("create table children.c(pid int references p(id))", &[])
        .await
        .unwrap();
    ctx.client
        .execute("insert into parents.p values (1)", &[])
        .await
        .unwrap();
    ctx.client
        .execute("insert into children.c values (1)", &[])
        .await
        .unwrap();
    let err = ctx
        .client
        .execute("insert into children.c values (2)", &[])
        .await
        .unwrap_err();
    common::assert_db_error_contains(&err, "violates foreign key");
    let _ = ctx.shutdown.send(());
}

#[tokio::test(flavor = "multi_thread")]
async fn drop_schema_restrict_and_cascade() {
    let ctx = common::start().await;
    ctx.client.execute("create schema ds", &[]).await.unwrap();
    ctx.client
        .execute("create table ds.t(a int)", &[])
        .await
        .unwrap();

    let err = ctx.client.execute("drop schema ds", &[]).await.unwrap_err();
    common::assert_db_error_contains(&err, "not empty");

    ctx.client
        .execute("drop schema ds cascade", &[])
        .await
        .unwrap();
    let rows = ctx
        .client
        .simple_query("select 1 from pg_catalog.pg_namespace where nspname = 'ds'")
        .await
        .unwrap();
    let found_row = rows
        .iter()
        .any(|msg| matches!(msg, SimpleQueryMessage::Row(_)));
    assert!(
        !found_row,
        "pg_namespace still contains schema entry after drop"
    );
    let _ = ctx.shutdown.send(());
}

#[tokio::test(flavor = "multi_thread")]
async fn rename_schema_updates_search_path_and_functions() {
    let ctx = common::start().await;
    ctx.client
        .execute("create schema to_rename", &[])
        .await
        .unwrap();
    ctx.client
        .execute("alter schema to_rename rename to renamed", &[])
        .await
        .unwrap();
    ctx.client
        .execute("set search_path = renamed, public", &[])
        .await
        .unwrap();
    let rows = ctx.client.simple_query("show search_path").await.unwrap();
    let value = rows
        .iter()
        .find_map(|msg| match msg {
            SimpleQueryMessage::Row(row) => row.get(0).map(|s| s.to_string()),
            _ => None,
        })
        .expect("expected row");
    assert_eq!(value, "renamed, public");

    let rows = ctx
        .client
        .query("select current_schema()", &[])
        .await
        .unwrap();
    assert_eq!(rows[0].get::<_, String>(0), "renamed");

    let rows = ctx
        .client
        .query("select current_schemas(false)", &[])
        .await
        .unwrap();
    assert_eq!(rows[0].get::<_, String>(0), "{renamed,public}");

    let _ = ctx.shutdown.send(());
}
