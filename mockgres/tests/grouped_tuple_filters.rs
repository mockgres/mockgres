mod common;

#[tokio::test(flavor = "multi_thread")]
async fn grouped_aggregate_over_requested_value_pairs() {
    let ctx = common::start().await;

    ctx.client
        .batch_execute(
            "create table foo_events (
                foo_id int,
                bar_id int,
                amount double precision,
                is_enabled boolean
            );

            insert into foo_events values
              (1, 1, 10, false),
              (1, 2, 999, false),
              (2, 1, 888, false),
              (2, 2, 20, false),
              (1, 1, 5, true);",
        )
        .await
        .unwrap();

    let requested_rows = ctx
        .client
        .query(
            "with requested(foo_id, bar_id) as (
              values (1::int4, 1::int4), (2::int4, 2::int4)
            )
            select requested.foo_id, requested.bar_id, coalesce(sum(foo_events.amount), 0)
            from requested
            left join foo_events
              on foo_events.foo_id = requested.foo_id
             and foo_events.bar_id = requested.bar_id
             and foo_events.is_enabled = false
            group by requested.foo_id, requested.bar_id",
            &[],
        )
        .await
        .unwrap();

    let requested_got: Vec<(i32, i32, f64)> = requested_rows
        .into_iter()
        .map(|r| (r.get(0), r.get(1), r.get(2)))
        .collect();
    assert_eq!(requested_got, vec![(1, 1, 10.0), (2, 2, 20.0)]);

    let tuple_in_rows = ctx
        .client
        .query(
            "select foo_id, bar_id, coalesce(sum(amount), 0)
            from foo_events
            where is_enabled = false
              and (foo_id, bar_id) in ((1, 1), (2, 2))
            group by foo_id, bar_id",
            &[],
        )
        .await
        .unwrap();

    let tuple_in_got: Vec<(i32, i32, f64)> = tuple_in_rows
        .into_iter()
        .map(|r| (r.get(0), r.get(1), r.get(2)))
        .collect();
    assert_eq!(tuple_in_got, vec![(1, 1, 10.0), (2, 2, 20.0)]);

    let _ = ctx.shutdown.send(());
}
