mod common;

use tokio_postgres::SimpleQueryMessage;

#[tokio::test(flavor = "multi_thread")]
async fn prepared_params_are_inferred_across_cte_and_outer_query() {
    let ctx = common::start().await;
    ctx.client
        .batch_execute(
            "create table items(id int primary key); insert into items values (1),(2),(3)",
        )
        .await
        .unwrap();

    let stmt = ctx
        .client
        .prepare(
            "with c as (
                select id from items where id >= $1
             )
             select id from c where id <= $2 order by id",
        )
        .await
        .unwrap();

    let rows = ctx.client.query(&stmt, &[&2_i32, &3_i32]).await.unwrap();
    let got: Vec<i32> = rows.into_iter().map(|r| r.get(0)).collect();
    assert_eq!(got, vec![2, 3]);

    let _ = ctx.shutdown.send(());
}

#[tokio::test(flavor = "multi_thread")]
async fn describe_with_cte_reports_outer_schema_only() {
    let ctx = common::start().await;
    let stmt = ctx
        .client
        .prepare("with c as (select 1 as a, 2 as b) select a from c")
        .await
        .unwrap();
    assert_eq!(stmt.columns().len(), 1);
    assert_eq!(stmt.columns()[0].name(), "a");

    let _ = ctx.shutdown.send(());
}

#[tokio::test(flavor = "multi_thread")]
async fn cte_query_results_match_between_simple_and_extended_protocols() {
    let ctx = common::start().await;
    ctx.client
        .batch_execute(
            "create table items(id int primary key); insert into items values (1),(2),(3)",
        )
        .await
        .unwrap();

    let extended_rows = ctx
        .client
        .query(
            "with c as (select id from items where id >= 2) select id from c order by id",
            &[],
        )
        .await
        .unwrap();
    let extended: Vec<i32> = extended_rows.into_iter().map(|r| r.get(0)).collect();

    let simple_rows = ctx
        .client
        .simple_query("with c as (select id from items where id >= 2) select id from c order by id")
        .await
        .unwrap();
    let simple: Vec<i32> = simple_rows
        .into_iter()
        .filter_map(|m| match m {
            SimpleQueryMessage::Row(r) => Some(
                r.get(0)
                    .expect("first column")
                    .parse::<i32>()
                    .expect("int row"),
            ),
            _ => None,
        })
        .collect();
    assert_eq!(extended, simple);

    let _ = ctx.shutdown.send(());
}

#[tokio::test(flavor = "multi_thread")]
async fn connection_recovers_after_cte_errors_in_both_protocol_paths() {
    let ctx = common::start().await;

    let err = ctx
        .client
        .query("with recursive c(n) as (select 1) select n from c", &[])
        .await
        .expect_err("expected extended CTE error");
    assert!(err.as_db_error().is_some());

    let ok: i32 = ctx.client.query_one("select 42", &[]).await.unwrap().get(0);
    assert_eq!(ok, 42);

    let simple_err = ctx
        .client
        .simple_query("with recursive c(n) as (select 1) select n from c")
        .await
        .expect_err("expected simple CTE error");
    assert!(simple_err.as_db_error().is_some());

    let simple_ok = ctx.client.simple_query("select 7").await.unwrap();
    let row = simple_ok
        .into_iter()
        .find_map(|m| match m {
            SimpleQueryMessage::Row(r) => Some(r),
            _ => None,
        })
        .expect("row");
    assert_eq!(row.get(0), Some("7"));

    let _ = ctx.shutdown.send(());
}
