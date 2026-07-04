mod common;

#[tokio::test(flavor = "multi_thread")]
async fn count_distinct_with_filter_and_not_null_predicate() {
    let ctx = common::start().await;

    ctx.client
        .batch_execute(
            "create table t_distinct_values (
                partition_key text not null,
                item_value text
            );

            insert into t_distinct_values(partition_key, item_value) values
                ('selected', 'alpha'),
                ('selected', 'alpha'),
                ('selected', 'beta'),
                ('selected', NULL),
                ('other', 'alpha');",
        )
        .await
        .expect("setup distinct aggregate rows");

    let distinct_count: i64 = ctx
        .client
        .query_one(
            "select count(distinct item_value)
             from t_distinct_values
             where partition_key = 'selected'
               and item_value is not null",
            &[],
        )
        .await
        .expect("count distinct values")
        .get(0);

    assert_eq!(distinct_count, 2);

    let _ = ctx.shutdown.send(());
}
