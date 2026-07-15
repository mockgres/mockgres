mod common;

use tokio_postgres::error::SqlState;

#[tokio::test(flavor = "multi_thread")]
async fn sum_overflow_returns_an_error_without_crashing() {
    let ctx = common::start().await;

    ctx.client
        .batch_execute(
            "create table overflow_values(value bigint);
             insert into overflow_values values (9223372036854775807), (1);",
        )
        .await
        .expect("create overflowing aggregate fixture");

    let error = ctx
        .client
        .query_one("select sum(value) from overflow_values", &[])
        .await
        .expect_err("overflowing sum should return an error");
    assert_eq!(
        error.as_db_error().expect("database error").code(),
        &SqlState::NUMERIC_VALUE_OUT_OF_RANGE
    );

    let one = ctx
        .client
        .query_one("select 1", &[])
        .await
        .expect("server remains available after aggregate overflow")
        .get::<_, i32>(0);
    assert_eq!(one, 1);

    let _ = ctx.shutdown.send(());
}

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
