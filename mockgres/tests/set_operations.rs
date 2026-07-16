mod common;

async fn setup() -> common::TestCtx {
    let ctx = common::start().await;
    ctx.client
        .batch_execute(
            "create table set_left(v bigint);
             create table set_right(v bigint);
             insert into set_left values (1), (1), (2), (null);
             insert into set_right values (1), (3), (null), (null);",
        )
        .await
        .expect("setup set-operation inputs");
    ctx
}

async fn optional_ints(ctx: &common::TestCtx, query: &str) -> Vec<Option<i64>> {
    ctx.client
        .query(query, &[])
        .await
        .expect("execute set operation")
        .iter()
        .map(|row| row.get(0))
        .collect()
}

#[tokio::test(flavor = "multi_thread")]
async fn union_preserves_or_removes_duplicates() {
    let ctx = setup().await;

    let all = optional_ints(
        &ctx,
        "select v from set_left union all select v from set_right order by v",
    )
    .await;
    assert_eq!(
        all,
        vec![
            Some(1),
            Some(1),
            Some(1),
            Some(2),
            Some(3),
            None,
            None,
            None,
        ]
    );

    let distinct = optional_ints(
        &ctx,
        "select v from set_left union select v from set_right order by v",
    )
    .await;
    assert_eq!(distinct, vec![Some(1), Some(2), Some(3), None]);

    let _ = ctx.shutdown.send(());
}

#[tokio::test(flavor = "multi_thread")]
async fn intersect_uses_postgres_duplicate_and_null_semantics() {
    let ctx = setup().await;

    let distinct = optional_ints(
        &ctx,
        "select v from set_left intersect select v from set_right order by v",
    )
    .await;
    assert_eq!(distinct, vec![Some(1), None]);

    let all = optional_ints(
        &ctx,
        "select v from set_left intersect all select v from set_right order by v",
    )
    .await;
    assert_eq!(all, vec![Some(1), None]);

    let _ = ctx.shutdown.send(());
}

#[tokio::test(flavor = "multi_thread")]
async fn except_uses_postgres_duplicate_and_null_semantics() {
    let ctx = setup().await;

    let distinct = optional_ints(
        &ctx,
        "select v from set_left except select v from set_right order by v",
    )
    .await;
    assert_eq!(distinct, vec![Some(2)]);

    let all = optional_ints(
        &ctx,
        "select v from set_left except all select v from set_right order by v",
    )
    .await;
    assert_eq!(all, vec![Some(1), Some(2)]);

    let _ = ctx.shutdown.send(());
}

#[tokio::test(flavor = "multi_thread")]
async fn set_operations_work_in_subqueries_and_ctes() {
    let ctx = setup().await;

    let count: i64 = ctx
        .client
        .query_one(
            "select count(*) from (
                 select v from set_left
                 union all
                 select v from set_right
             ) combined",
            &[],
        )
        .await
        .expect("count UNION ALL subquery")
        .get(0);
    assert_eq!(count, 8);

    let values = optional_ints(
        &ctx,
        "with combined as (
             select v from set_left
             union
             select v from set_right
         )
         select v from combined order by v limit 2",
    )
    .await;
    assert_eq!(values, vec![Some(1), Some(2)]);

    let _ = ctx.shutdown.send(());
}
