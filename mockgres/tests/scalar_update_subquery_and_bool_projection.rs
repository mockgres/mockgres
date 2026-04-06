mod common;

#[tokio::test(flavor = "multi_thread")]
async fn update_set_scalar_subquery_is_supported() {
    let ctx = common::start().await;

    ctx.client
        .batch_execute(
            "create table src_tbl(id bigint primary key, val bigint not null);
             create table dst_tbl(id bigint primary key, src_id bigint not null, copied bigint not null);
             insert into src_tbl(id, val) values (1, 9);
             insert into dst_tbl(id, src_id, copied) values (10, 1, 0), (11, 1, 0), (12, 2, 0);",
        )
        .await
        .expect("setup tables");

    let updated = ctx
        .client
        .execute(
            "update dst_tbl
             set copied = (select val from src_tbl where id = $1)
             where src_id = $1",
            &[&1_i64],
        )
        .await
        .expect("update with scalar subquery");
    assert_eq!(updated, 2);

    let rows = ctx
        .client
        .query("select id, copied from dst_tbl order by id", &[])
        .await
        .expect("select updated rows");

    let actual: Vec<(i64, i64)> = rows
        .iter()
        .map(|r| (r.get::<_, i64>(0), r.get::<_, i64>(1)))
        .collect();
    assert_eq!(actual, vec![(10, 9), (11, 9), (12, 0)]);

    let _ = ctx.shutdown.send(());
}

#[tokio::test(flavor = "multi_thread")]
async fn boolean_predicate_projection_is_supported() {
    let ctx = common::start().await;

    ctx.client
        .batch_execute(
            "create table flag_tbl(id bigint primary key, marker bigint);
             insert into flag_tbl(id, marker) values
               (1, null),
               (2, 3);",
        )
        .await
        .expect("setup flag table");

    let rows = ctx
        .client
        .query(
            "select marker is not null and marker <= 5 as ready
             from flag_tbl
             order by id",
            &[],
        )
        .await
        .expect("select predicate projection");

    let actual: Vec<bool> = rows.iter().map(|r| r.get(0)).collect();
    assert_eq!(actual, vec![false, true]);

    let _ = ctx.shutdown.send(());
}
