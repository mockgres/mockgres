mod common;

#[tokio::test(flavor = "multi_thread")]
async fn insert_requires_existing_parent() {
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

    let err = ctx
        .client
        .execute("insert into children values (1, 42)", &[])
        .await
        .expect_err("insert without parent");
    common::assert_db_error_contains(&err, "foreign key");

    ctx.client
        .execute("insert into parents values (42)", &[])
        .await
        .expect("insert parent");
    ctx.client
        .execute("insert into children values (1, 42)", &[])
        .await
        .expect("insert child");

    let _ = ctx.shutdown.send(());
}

#[tokio::test(flavor = "multi_thread")]
async fn delete_parent_blocked_by_child() {
    let ctx = common::start().await;

    ctx.client
        .execute("create table parent(id int primary key)", &[])
        .await
        .expect("create parent");
    ctx.client
        .execute(
            "create table child(id int primary key, parent_id int references parent(id))",
            &[],
        )
        .await
        .expect("create child");
    ctx.client
        .execute("insert into parent values (1)", &[])
        .await
        .expect("insert parent");
    ctx.client
        .execute("insert into child values (1, 1)", &[])
        .await
        .expect("insert child");

    let err = ctx
        .client
        .execute("delete from parent where id = 1", &[])
        .await
        .expect_err("delete parent with child");
    common::assert_db_error_contains(&err, "foreign key");

    ctx.client
        .execute("delete from child where parent_id = 1", &[])
        .await
        .expect("delete child");
    ctx.client
        .execute("delete from parent where id = 1", &[])
        .await
        .expect("delete parent");

    let _ = ctx.shutdown.send(());
}

#[tokio::test(flavor = "multi_thread")]
async fn drop_parent_blocked_when_child_exists() {
    let ctx = common::start().await;

    ctx.client
        .execute("create table p(id int primary key)", &[])
        .await
        .expect("create p");
    ctx.client
        .execute(
            "create table c(id int primary key, pid int references p(id))",
            &[],
        )
        .await
        .expect("create c");
    ctx.client
        .execute("insert into p values (1)", &[])
        .await
        .expect("insert p");
    ctx.client
        .execute("insert into c values (1, 1)", &[])
        .await
        .expect("insert c");

    let err = ctx
        .client
        .execute("drop table p", &[])
        .await
        .expect_err("drop parent");
    common::assert_db_error_contains(&err, "referenced");

    let _ = ctx.shutdown.send(());
}
