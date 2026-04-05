mod common;

#[tokio::test(flavor = "multi_thread")]
async fn delete_supports_target_alias_qualified_columns() {
    let ctx = common::start().await;

    ctx.client
        .batch_execute(
            "create table t_alias (id integer, c integer);
             insert into t_alias (id, c) values (1, 10), (2, 20);",
        )
        .await
        .expect("setup t_alias");

    let deleted = ctx
        .client
        .execute("delete from t_alias ta where ta.id = 1", &[])
        .await
        .expect("delete with alias-qualified predicate");
    assert_eq!(deleted, 1);

    let rows = ctx
        .client
        .query("select id from t_alias order by id", &[])
        .await
        .expect("select remaining rows");
    let ids: Vec<i32> = rows.iter().map(|r| r.get(0)).collect();
    assert_eq!(ids, vec![2]);

    let _ = ctx.shutdown.send(());
}

#[tokio::test(flavor = "multi_thread")]
async fn bool_and_aggregate_is_supported() {
    let ctx = common::start().await;

    ctx.client
        .batch_execute(
            "create table t_bool (b boolean);
             insert into t_bool (b) values (true), (false);",
        )
        .await
        .expect("setup t_bool");

    let all_true: bool = ctx
        .client
        .query_one(
            "select coalesce(bool_and(b), false) as all_true from t_bool",
            &[],
        )
        .await
        .expect("select bool_and")
        .get(0);
    assert!(!all_true);

    let _ = ctx.shutdown.send(());
}

#[tokio::test(flavor = "multi_thread")]
async fn case_expression_inside_sum_is_supported() {
    let ctx = common::start().await;

    ctx.client
        .batch_execute(
            "create table t_case (b boolean);
             insert into t_case (b) values (true), (false), (true);",
        )
        .await
        .expect("setup t_case");

    let true_count: i64 = ctx
        .client
        .query_one(
            "select coalesce(sum(case when b then 1 else 0 end), 0) as true_count
             from t_case",
            &[],
        )
        .await
        .expect("select sum(case...)")
        .get(0);
    assert_eq!(true_count, 2);

    let _ = ctx.shutdown.send(());
}

#[tokio::test(flavor = "multi_thread")]
async fn order_by_column_not_in_select_list_is_supported() {
    let ctx = common::start().await;

    ctx.client
        .batch_execute(
            "create table t_order (name text, rank integer);
             insert into t_order (name, rank) values ('b', 2), ('a', 1);",
        )
        .await
        .expect("setup t_order");

    let rows = ctx
        .client
        .query("select name from t_order order by rank asc, name asc", &[])
        .await
        .expect("select ordered rows");
    let names: Vec<String> = rows.iter().map(|r| r.get(0)).collect();
    assert_eq!(names, vec!["a", "b"]);

    let _ = ctx.shutdown.send(());
}
