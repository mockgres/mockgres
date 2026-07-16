mod common;

#[tokio::test(flavor = "multi_thread")]
async fn position_supports_text_columns_unicode_and_edge_cases() {
    let ctx = common::start().await;

    ctx.client
        .batch_execute(
            "create table phrases (
                id int primary key,
                haystack text,
                needle text
            );
            insert into phrases values
                (1, 'mockgres', 'gres'),
                (2, 'café', 'fé'),
                (3, 'a😀b', '😀'),
                (4, 'mockgres', 'missing'),
                (5, 'mockgres', ''),
                (6, null, 'x'),
                (7, 'mockgres', null);",
        )
        .await
        .expect("create and populate phrases");

    let rows = ctx
        .client
        .query(
            "select id, position(needle in haystack)
             from phrases
             order by id",
            &[],
        )
        .await
        .expect("query text positions");

    let positions = rows
        .iter()
        .map(|row| row.get::<_, Option<i32>>(1))
        .collect::<Vec<_>>();
    assert_eq!(
        positions,
        vec![Some(5), Some(3), Some(2), Some(0), Some(1), None, None]
    );

    let empty: i32 = ctx
        .client
        .query_one("select position('' in '')", &[])
        .await
        .expect("query empty strings")
        .get(0);
    assert_eq!(empty, 1);

    let _ = ctx.shutdown.send(());
}

#[tokio::test(flavor = "multi_thread")]
async fn position_supports_bytea() {
    let ctx = common::start().await;

    let row = ctx
        .client
        .query_one(
            "select
                position('\\x5678'::bytea in '\\x1234567890'::bytea),
                position(''::bytea in '\\x1234'::bytea),
                position('\\xffff'::bytea in '\\x1234'::bytea),
                position(null::bytea in '\\x1234'::bytea)",
            &[],
        )
        .await
        .expect("query bytea positions");

    assert_eq!(row.get::<_, i32>(0), 3);
    assert_eq!(row.get::<_, i32>(1), 1);
    assert_eq!(row.get::<_, i32>(2), 0);
    assert_eq!(row.get::<_, Option<i32>>(3), None);

    let _ = ctx.shutdown.send(());
}
