mod common;

#[tokio::test(flavor = "multi_thread")]
async fn composite_primary_key_enforces_uniqueness() {
    let ctx = common::start().await;

    ctx.client
        .execute(
            "create table pk_demo(a int, b int, primary key (a, b))",
            &[],
        )
        .await
        .expect("create pk_demo");

    ctx.client
        .execute("insert into pk_demo values (1, 2)", &[])
        .await
        .expect("insert first row");

    let err = ctx
        .client
        .execute("insert into pk_demo values (1, 2)", &[])
        .await
        .expect_err("duplicate composite key");
    assert!(
        err.to_string().contains("duplicate key"),
        "unexpected error: {err}"
    );

    let _ = ctx.shutdown.send(());
}

#[tokio::test(flavor = "multi_thread")]
async fn composite_foreign_key_requires_parent() {
    let ctx = common::start().await;

    ctx.client
        .execute(
            "create table parents(a int, b int, primary key (a, b))",
            &[],
        )
        .await
        .expect("create parents");
    ctx.client
        .execute(
            "create table children(x int, y int, foreign key (x, y) references parents(a, b))",
            &[],
        )
        .await
        .expect("create children");

    let err = ctx
        .client
        .execute("insert into children values (1, 1)", &[])
        .await
        .expect_err("insert without parent");
    assert!(
        err.to_string().contains("foreign key"),
        "unexpected error: {err}"
    );

    ctx.client
        .execute("insert into parents values (1, 1)", &[])
        .await
        .expect("insert parent");
    ctx.client
        .execute("insert into children values (1, 1)", &[])
        .await
        .expect("insert child");

    let _ = ctx.shutdown.send(());
}

#[tokio::test(flavor = "multi_thread")]
async fn update_primary_key_collision_rejected() {
    let ctx = common::start().await;

    ctx.client
        .execute(
            "create table pk_updates(a int, b int, primary key (a, b))",
            &[],
        )
        .await
        .expect("create pk_updates");
    ctx.client
        .execute("insert into pk_updates values (1, 1), (2, 2)", &[])
        .await
        .expect("insert initial rows");

    let err = ctx
        .client
        .execute("update pk_updates set a = 1, b = 1 where a = 2", &[])
        .await
        .expect_err("update into duplicate key");
    assert!(
        err.to_string().contains("duplicate key"),
        "unexpected error: {err}"
    );

    let _ = ctx.shutdown.send(());
}

#[tokio::test(flavor = "multi_thread")]
async fn update_foreign_key_requires_existing_parent() {
    let ctx = common::start().await;

    ctx.client
        .execute(
            "create table updates_parent(a int, b int, primary key (a, b))",
            &[],
        )
        .await
        .expect("create parents");
    ctx.client
        .execute(
            "create table updates_child(id int primary key, x int, y int, foreign key (x, y) references updates_parent(a, b))",
            &[],
        )
        .await
        .expect("create child");
    ctx.client
        .execute("insert into updates_parent values (1, 1)", &[])
        .await
        .expect("insert parent");
    ctx.client
        .execute("insert into updates_child values (1, 1, 1)", &[])
        .await
        .expect("insert child");

    let err = ctx
        .client
        .execute("update updates_child set x = 2 where id = 1", &[])
        .await
        .expect_err("update to missing parent");
    assert!(
        err.to_string().contains("foreign key"),
        "unexpected error: {err}"
    );

    let _ = ctx.shutdown.send(());
}

#[tokio::test(flavor = "multi_thread")]
async fn delete_parent_blocked_by_child_composite_fk() {
    let ctx = common::start().await;

    ctx.client
        .execute(
            "create table del_parent(a int, b int, primary key (a, b))",
            &[],
        )
        .await
        .expect("create parent");
    ctx.client
        .execute(
            "create table del_child(id int primary key, x int, y int, foreign key (x, y) references del_parent(a, b))",
            &[],
        )
        .await
        .expect("create child");
    ctx.client
        .execute("insert into del_parent values (1, 1)", &[])
        .await
        .expect("insert parent");
    ctx.client
        .execute("insert into del_child values (1, 1, 1)", &[])
        .await
        .expect("insert child");

    let err = ctx
        .client
        .execute("delete from del_parent where a = 1 and b = 1", &[])
        .await
        .expect_err("delete referenced parent");
    assert!(
        err.to_string().contains("foreign key"),
        "unexpected error: {err}"
    );

    let _ = ctx.shutdown.send(());
}
