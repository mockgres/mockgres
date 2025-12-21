mod common;

#[tokio::test(flavor = "multi_thread")]
async fn cascade_deletes_direct_children() {
    let ctx = common::start().await;

    ctx.client
        .execute("create table parents(id int primary key)", &[])
        .await
        .expect("create parents");
    ctx.client
        .execute(
            "create table children(id int primary key, parent_id int references parents(id) on delete cascade)",
            &[],
        )
        .await
        .expect("create children");
    ctx.client
        .execute("insert into parents values (1)", &[])
        .await
        .expect("insert parent");
    ctx.client
        .execute("insert into children values (10, 1), (11, 1)", &[])
        .await
        .expect("insert children");

    ctx.client
        .execute("delete from parents where id = 1", &[])
        .await
        .expect("delete parent");

    let remaining: i64 = ctx
        .client
        .query_one("select count(*) from children", &[])
        .await
        .expect("count children")
        .get(0);
    assert_eq!(remaining, 0, "children should be removed by cascade");

    let _ = ctx.shutdown.send(());
}

#[tokio::test(flavor = "multi_thread")]
async fn cascade_traverses_grandchildren() {
    let ctx = common::start().await;

    ctx.client
        .execute("create table parents(id int primary key)", &[])
        .await
        .expect("create parents");
    ctx.client
        .execute(
            "create table children(id int primary key, parent_id int references parents(id) on delete cascade)",
            &[],
        )
        .await
        .expect("create children");
    ctx.client
        .execute(
            "create table grandchildren(id int primary key, child_id int references children(id) on delete cascade)",
            &[],
        )
        .await
        .expect("create grandchildren");

    ctx.client
        .execute("insert into parents values (1)", &[])
        .await
        .expect("insert parent");
    ctx.client
        .execute("insert into children values (10, 1)", &[])
        .await
        .expect("insert child");
    ctx.client
        .execute("insert into grandchildren values (100, 10)", &[])
        .await
        .expect("insert grandchild");

    ctx.client
        .execute("delete from parents where id = 1", &[])
        .await
        .expect("delete parent");

    let child_count: i64 = ctx
        .client
        .query_one("select count(*) from children", &[])
        .await
        .expect("count children")
        .get(0);
    let grand_count: i64 = ctx
        .client
        .query_one("select count(*) from grandchildren", &[])
        .await
        .expect("count grandchildren")
        .get(0);
    assert_eq!(child_count, 0, "children should be removed");
    assert_eq!(grand_count, 0, "grandchildren should be removed");

    let _ = ctx.shutdown.send(());
}

#[tokio::test(flavor = "multi_thread")]
async fn restrict_fk_blocks_cascade() {
    let ctx = common::start().await;

    ctx.client
        .execute("create table parents(id int primary key)", &[])
        .await
        .expect("create parents");
    ctx.client
        .execute(
            "create table cascade_child(id int primary key, parent_id int references parents(id) on delete cascade)",
            &[],
        )
        .await
        .expect("create cascade child");
    ctx.client
        .execute(
            "create table restrict_child(id int primary key, parent_id int references parents(id))",
            &[],
        )
        .await
        .expect("create restrict child");

    ctx.client
        .execute("insert into parents values (1)", &[])
        .await
        .expect("insert parent");
    ctx.client
        .execute("insert into cascade_child values (10, 1)", &[])
        .await
        .expect("insert cascade child");
    ctx.client
        .execute("insert into restrict_child values (20, 1)", &[])
        .await
        .expect("insert restrict child");

    let err = ctx
        .client
        .execute("delete from parents where id = 1", &[])
        .await
        .expect_err("restrict child should block delete");
    common::assert_db_error_contains(&err, "foreign key");

    ctx.client
        .execute("delete from restrict_child where id = 20", &[])
        .await
        .expect("delete restrict child");
    ctx.client
        .execute("delete from parents where id = 1", &[])
        .await
        .expect("delete parent after restrict removed");

    let cascade_remaining: i64 = ctx
        .client
        .query_one("select count(*) from cascade_child", &[])
        .await
        .expect("count cascade child")
        .get(0);
    assert_eq!(cascade_remaining, 0, "cascade child rows should be removed");

    let _ = ctx.shutdown.send(());
}

#[tokio::test(flavor = "multi_thread")]
async fn cascade_handles_self_references() {
    let ctx = common::start().await;

    ctx.client
        .execute(
            "create table nodes(id int primary key, parent_id int references nodes(id) on delete cascade)",
            &[],
        )
        .await
        .expect("create nodes");
    ctx.client
        .execute("insert into nodes values (1, null)", &[])
        .await
        .expect("insert root");
    ctx.client
        .execute("insert into nodes values (2, 1)", &[])
        .await
        .expect("insert child");
    ctx.client
        .execute("insert into nodes values (3, 2)", &[])
        .await
        .expect("insert grandchild");

    ctx.client
        .execute("delete from nodes where id = 1", &[])
        .await
        .expect("delete root");

    let remaining: i64 = ctx
        .client
        .query_one("select count(*) from nodes", &[])
        .await
        .expect("count nodes")
        .get(0);
    assert_eq!(remaining, 0, "entire tree should be removed");

    let _ = ctx.shutdown.send(());
}
