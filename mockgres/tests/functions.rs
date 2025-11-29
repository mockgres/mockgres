mod common;

#[tokio::test(flavor = "multi_thread")]
async fn numeric_funcs_and_greatest() {
    let ctx = common::start().await;

    let abs_row = ctx
        .client
        .query_one(
            "select abs(-5) as ai,
                    abs(cast(-1.5 as double precision)) as af",
            &[],
        )
        .await
        .unwrap();
    assert_eq!(abs_row.get::<_, i32>(0), 5);
    assert!((abs_row.get::<_, f64>(1) - 1.5_f64).abs() < 1e-9);

    let log10_row = ctx
        .client
        .query_one("select log(100::float8) as log10_val", &[])
        .await
        .unwrap();
    assert!((log10_row.get::<_, f64>(0) - 2.0_f64).abs() < 1e-9);

    let log_base_row = ctx
        .client
        .query_one("select log(2, 8) as log_base", &[])
        .await
        .unwrap();
    assert!((log_base_row.get::<_, f64>(0) - 3.0_f64).abs() < 1e-9);

    let ln_row = ctx
        .client
        .query_one("select ln(1.0) as ln_val", &[])
        .await
        .unwrap();
    assert!((ln_row.get::<_, f64>(0) - 0.0_f64).abs() < 1e-9);

    let g_row = ctx
        .client
        .query_one(
            "select greatest(null, 1, 3, 2) as g1,
                    greatest(null::int, null::int) as gnull",
            &[],
        )
        .await
        .unwrap();
    assert_eq!(g_row.get::<_, i32>(0), 3);
    let gnull: Option<i32> = g_row.get(1);
    assert!(gnull.is_none());

    let _ = ctx.shutdown.send(());
}

#[tokio::test(flavor = "multi_thread")]
async fn interval_and_epoch_functions_work() {
    let ctx = common::start().await;

    let rows = ctx
        .client
        .query(
            "select extract(epoch from '1970-01-02 00:00:00+00'::timestamptz) as epoch,
                    ('2024-01-02 00:00:00+00'::timestamptz - interval '1 day') as ts_minus,
                    (interval '5 minutes' + interval '3 hours')::text as interval_sum",
            &[],
        )
        .await
        .unwrap();
    let row = &rows[0];
    assert_eq!(row.get::<_, f64>(0), 86_400_f64);
    assert_eq!(
        row.get::<_, chrono::DateTime<chrono::Utc>>(1).to_rfc3339(),
        "2024-01-01T00:00:00+00:00"
    );
    assert!(row.get::<_, String>(2).contains("03:05:00")); // format may include day annotation

    let _ = ctx.shutdown.send(());
}
