mod common;

#[tokio::test(flavor = "multi_thread")]
async fn cross_join_with_where_clause() {
    let ctx = common::start().await;

    ctx.client
        .execute(
            "create table authors(author_id int primary key, name text)",
            &[],
        )
        .await
        .expect("create authors");
    ctx.client
        .execute(
            "create table books(book_id int primary key, writer_id int, title text)",
            &[],
        )
        .await
        .expect("create books");
    ctx.client
        .execute("insert into authors values (1, 'Ada')", &[])
        .await
        .expect("insert author");
    ctx.client
        .execute(
            "insert into books values (10, 1, 'Analysis of Engines')",
            &[],
        )
        .await
        .expect("insert book");

    let rows = ctx
        .client
        .query(
            "select name, title from authors, books where authors.author_id = books.writer_id",
            &[],
        )
        .await
        .expect("join query");

    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].get::<_, String>(0), "Ada");
    assert_eq!(rows[0].get::<_, String>(1), "Analysis of Engines");

    let _ = ctx.shutdown.send(());
}

#[tokio::test(flavor = "multi_thread")]
async fn ambiguous_column_reference_errors() {
    let ctx = common::start().await;

    ctx.client
        .execute("create table a(id int primary key)", &[])
        .await
        .expect("create a");
    ctx.client
        .execute("create table b(id int primary key)", &[])
        .await
        .expect("create b");

    let err = ctx
        .client
        .simple_query("select id from a, b")
        .await
        .expect_err("ambiguous select should fail");
    common::assert_db_error_contains(&err, "ambiguous");

    let _ = ctx.shutdown.send(());
}
