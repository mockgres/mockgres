mod common;

#[tokio::test(flavor = "multi_thread")]
async fn qualified_group_columns_remain_distinct_across_tables() {
    let ctx = common::start().await;

    ctx.client
        .batch_execute(
            "create table left_values(c0 text);
             create table right_values(c0 text);
             insert into left_values values ('left');
             insert into right_values values ('right');",
        )
        .await
        .expect("create grouped join fixture");

    let rows = ctx
        .client
        .query(
            "select right_values.c0
             from left_values, right_values
             group by left_values.c0, right_values.c0, right_values.c0
             having right_values.c0 = 'right'",
            &[],
        )
        .await
        .expect("group qualified columns with duplicate names");

    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].get::<_, String>(0), "right");

    let _ = ctx.shutdown.send(());
}
