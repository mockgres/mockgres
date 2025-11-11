mod common;

#[tokio::test(flavor = "multi_thread")]
async fn inner_join_on_selects_matching_pairs() {
    let ctx = common::start().await;
    ctx.client
        .batch_execute("create table authors(author_pk int primary key, name text)")
        .await
        .unwrap();
    ctx.client
        .batch_execute("create table books(book_pk int primary key, author_fk int, title text)")
        .await
        .unwrap();
    ctx.client
        .batch_execute("insert into authors values (1,'Ada'), (2,'Bob')")
        .await
        .unwrap();
    ctx.client
        .batch_execute(
            "insert into books values (10,1,'A'), (11,2,'B'), (12,2,'C'), (13,null,'Orphan')",
        )
        .await
        .unwrap();

    let rows = ctx
        .client
        .query(
            "select name, title
           from authors
           inner join books on authors.author_pk = books.author_fk
           order by title",
            &[],
        )
        .await
        .unwrap();

    let got: Vec<(String, String)> = rows.into_iter().map(|r| (r.get(0), r.get(1))).collect();
    assert_eq!(
        got,
        vec![
            ("Ada".into(), "A".into()),
            ("Bob".into(), "B".into()),
            ("Bob".into(), "C".into()),
        ]
    );

    let _ = ctx.shutdown.send(());
}

#[tokio::test(flavor = "multi_thread")]
async fn nested_inner_joins_and_where_are_conjoined() {
    let ctx = common::start().await;
    ctx.client
        .batch_execute("create table a(a_id int primary key)")
        .await
        .unwrap();
    ctx.client
        .batch_execute("create table b(b_id int primary key, a_ref int)")
        .await
        .unwrap();
    ctx.client
        .batch_execute("create table c(c_id int primary key, b_ref int)")
        .await
        .unwrap();
    ctx.client
        .batch_execute("insert into a values (1),(2)")
        .await
        .unwrap();
    ctx.client
        .batch_execute("insert into b values (10,1),(11,2)")
        .await
        .unwrap();
    ctx.client
        .batch_execute("insert into c values (100,10),(101,11)")
        .await
        .unwrap();

    let rows = ctx
        .client
        .query(
            "select c.c_id
           from a join b on a.a_id = b.a_ref
                    join c on b.b_id = c.b_ref
          where c.c_id = 100",
            &[],
        )
        .await
        .unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].get::<_, i32>(0), 100);

    let _ = ctx.shutdown.send(());
}

#[tokio::test(flavor = "multi_thread")]
async fn non_inner_join_is_rejected_for_now() {
    let ctx = common::start().await;
    ctx.client
        .batch_execute("create table x(i int)")
        .await
        .unwrap();
    ctx.client
        .batch_execute("create table y(j int)")
        .await
        .unwrap();
    let err = ctx
        .client
        .simple_query("select * from x left join y on true")
        .await
        .unwrap_err();
    assert!(err.to_string().contains("only INNER JOIN is supported"));
    let _ = ctx.shutdown.send(());
}
