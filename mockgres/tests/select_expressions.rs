mod common;

#[tokio::test(flavor = "multi_thread")]
async fn select_arithmetic_and_text_functions() {
    let ctx = common::start().await;

    ctx.client
        .execute(
            "create table widgets(
                id int primary key,
                qty int not null,
                note text
            )",
            &[],
        )
        .await
        .expect("create table");

    ctx.client
        .execute(
            "insert into widgets values
                (1, 10, 'alpha'),
                (2, 5, NULL)",
            &[],
        )
        .await
        .expect("insert rows");

    let rows = ctx
        .client
        .query(
            "select id,
                    qty + 1 as qty_plus,
                    upper(coalesce(note, 'blank')) as note_up,
                    length(coalesce(note, '')) as note_len
             from widgets
             order by id",
            &[],
        )
        .await
        .expect("select expressions");

    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0].get::<_, i32>(0), 1);
    assert_eq!(rows[0].get::<_, i32>(1), 11);
    assert_eq!(rows[0].get::<_, String>(2), "ALPHA");
    assert_eq!(rows[0].get::<_, i32>(3), 5);

    assert_eq!(rows[1].get::<_, i32>(0), 2);
    assert_eq!(rows[1].get::<_, i32>(1), 6);
    assert_eq!(rows[1].get::<_, String>(2), "BLANK");
    assert_eq!(rows[1].get::<_, i32>(3), 0);

    let ones = ctx
        .client
        .query("select 1 from widgets where qty > 5 order by id", &[])
        .await
        .expect("select constants");
    assert_eq!(ones.len(), 1);
    assert_eq!(ones[0].get::<_, i32>(0), 1);

    let _ = ctx.shutdown.send(());
}

#[tokio::test(flavor = "multi_thread")]
async fn select_coalesce_wrapped_sum_aggregate() {
    let ctx = common::start().await;

    ctx.client
        .batch_execute(
            "create table observed_segments(
                channel_id text not null,
                variant text not null,
                is_content bool not null,
                duration_seconds int not null,
                observed_at int not null
            );
            insert into observed_segments values
                ('channel-a', 'v1', false, 10, 100),
                ('channel-a', 'v1', false, 20, 120),
                ('channel-a', 'v1', true, 99, 130),
                ('channel-a', 'v2', false, 50, 140)",
        )
        .await
        .unwrap();

    let total: i64 = ctx
        .client
        .query_one(
            "select coalesce(sum(duration_seconds), 0)
             from observed_segments
             where channel_id = $1
               and variant = $2
               and is_content = false",
            &[&"channel-a", &"v1"],
        )
        .await
        .unwrap()
        .get(0);
    assert_eq!(total, 30);

    let lookback_total: i64 = ctx
        .client
        .query_one(
            "select coalesce(sum(duration_seconds), 0)
             from observed_segments
             where channel_id = $1
               and variant = $2
               and is_content = false
               and observed_at >= $3",
            &[&"channel-a", &"v1", &200_i32],
        )
        .await
        .unwrap()
        .get(0);
    assert_eq!(lookback_total, 0);

    let _ = ctx.shutdown.send(());
}
