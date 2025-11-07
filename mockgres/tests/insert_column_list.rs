mod common;

#[tokio::test(flavor = "multi_thread")]
async fn insert_with_column_list_and_defaults() {
    let ctx = common::start().await;

    ctx.client
        .execute(
            "create table gadgets(
                id int primary key,
                name text default 'n/a',
                qty int not null default 0
            )",
            &[],
        )
        .await
        .expect("create table");

    ctx.client
        .execute("insert into gadgets (id, qty) values (1, 5)", &[])
        .await
        .expect("insert subset");

    let row = ctx
        .client
        .query_one("select name, qty from gadgets where id = 1", &[])
        .await
        .expect("select row");
    assert_eq!(row.get::<_, String>(0), "n/a");
    assert_eq!(row.get::<_, i32>(1), 5);

    ctx.client
        .execute(
            "insert into gadgets (id, qty, name) values (2, 2 + 3, upper('beta'))",
            &[],
        )
        .await
        .expect("insert expressions");

    let second = ctx
        .client
        .query_one("select name, qty from gadgets where id = 2", &[])
        .await
        .expect("select second");
    assert_eq!(second.get::<_, String>(0), "BETA");
    assert_eq!(second.get::<_, i32>(1), 5);

    let err = ctx
        .client
        .execute("insert into gadgets (id) values (3)", &[])
        .await
        .expect_err("should fail missing NOT NULL column");
    assert!(
        err.to_string().contains("not null"),
        "unexpected error: {err}"
    );

    let _ = ctx.shutdown.send(());
}
