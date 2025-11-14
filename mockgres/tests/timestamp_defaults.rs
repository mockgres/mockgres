mod common;

use chrono::{DateTime, NaiveDateTime, TimeZone, Utc};
use tokio::time::{Duration, sleep};

#[tokio::test(flavor = "multi_thread")]
async fn inserts_fill_and_updates_refresh_timestamps() {
    let ctx = common::start().await;

    ctx.client
        .execute("SET TIME ZONE 'UTC'", &[])
        .await
        .expect("set time zone");

    ctx.client
        .execute(
            "CREATE TABLE ts_demo (
                id INT PRIMARY KEY,
                created_at_tz TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP NOT NULL,
                updated_at_tz TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP NOT NULL,
                created_at_ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP NOT NULL,
                updated_at_ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP NOT NULL
            )",
            &[],
        )
        .await
        .expect("create table");

    ctx.client
        .execute("INSERT INTO ts_demo (id) VALUES (1), (2)", &[])
        .await
        .expect("insert defaults");

    let rows = ctx
        .client
        .query(
            "SELECT id, created_at_tz, updated_at_tz, created_at_ts, updated_at_ts
             FROM ts_demo ORDER BY id",
            &[],
        )
        .await
        .expect("select inserted rows");
    assert_eq!(rows.len(), 2);

    let first_created_tz: DateTime<Utc> = rows[0].get(1);
    let first_updated_tz: DateTime<Utc> = rows[0].get(2);
    let first_created_ts: NaiveDateTime = rows[0].get(3);
    let first_updated_ts: NaiveDateTime = rows[0].get(4);

    let second_created_tz: DateTime<Utc> = rows[1].get(1);
    let second_updated_tz: DateTime<Utc> = rows[1].get(2);
    let second_created_ts: NaiveDateTime = rows[1].get(3);
    let second_updated_ts: NaiveDateTime = rows[1].get(4);

    assert_eq!(first_created_tz, first_updated_tz, "row 1 tz fields differ");
    assert_eq!(
        second_created_tz, second_updated_tz,
        "row 2 tz fields differ"
    );
    assert_eq!(first_created_ts, first_updated_ts, "row 1 ts fields differ");
    assert_eq!(
        second_created_ts, second_updated_ts,
        "row 2 ts fields differ"
    );
    assert_eq!(
        first_created_tz, second_created_tz,
        "multi-row statement should share timestamptz timestamp"
    );
    assert_eq!(
        first_created_ts, second_created_ts,
        "multi-row statement should share timestamp without tz"
    );

    ctx.client
        .execute(
            "INSERT INTO ts_demo (id, created_at_tz, created_at_ts)
             VALUES (
                3,
                TIMESTAMPTZ '2000-01-01 00:00:00+00',
                TIMESTAMP '1999-12-31 20:00:00'
             )",
            &[],
        )
        .await
        .expect("insert explicit timestamp");
    let explicit_row = ctx
        .client
        .query_one(
            "SELECT created_at_tz, created_at_ts FROM ts_demo WHERE id = 3",
            &[],
        )
        .await
        .expect("select explicit row");
    let explicit_created_tz: DateTime<Utc> = explicit_row.get(0);
    let explicit_created_ts: NaiveDateTime = explicit_row.get(1);
    let expected_tz = Utc.from_utc_datetime(
        &NaiveDateTime::parse_from_str("2000-01-01 00:00:00", "%Y-%m-%d %H:%M:%S").unwrap(),
    );
    let expected_ts =
        NaiveDateTime::parse_from_str("1999-12-31 20:00:00", "%Y-%m-%d %H:%M:%S").unwrap();
    assert_eq!(explicit_created_tz, expected_tz);
    assert_eq!(explicit_created_ts, expected_ts);

    let before_row = ctx
        .client
        .query_one(
            "SELECT updated_at_tz, updated_at_ts FROM ts_demo WHERE id = 1",
            &[],
        )
        .await
        .expect("select row1 before update");
    let before_update_tz: DateTime<Utc> = before_row.get(0);
    let before_update_ts: NaiveDateTime = before_row.get(1);

    sleep(Duration::from_millis(5)).await;
    ctx.client
        .execute(
            "UPDATE ts_demo
             SET updated_at_tz = CURRENT_TIMESTAMP,
                 updated_at_ts = CURRENT_TIMESTAMP
             WHERE id = 1",
            &[],
        )
        .await
        .expect("update row1 with current_timestamp");
    let after_row = ctx
        .client
        .query_one(
            "SELECT updated_at_tz, updated_at_ts FROM ts_demo WHERE id = 1",
            &[],
        )
        .await
        .expect("select row1 after update");
    let after_update_tz: DateTime<Utc> = after_row.get(0);
    let after_update_ts: NaiveDateTime = after_row.get(1);
    assert!(
        after_update_tz >= before_update_tz,
        "row 1 tz didn't advance"
    );
    assert!(
        after_update_ts >= before_update_ts,
        "row 1 ts didn't advance"
    );

    sleep(Duration::from_millis(5)).await;
    ctx.client
        .execute(
            "UPDATE ts_demo
             SET updated_at_tz = now(),
                 updated_at_ts = now()
             WHERE id IN (1, 2, 3)",
            &[],
        )
        .await
        .expect("multi-row update");
    let refreshed_rows = ctx
        .client
        .query(
            "SELECT id, updated_at_tz, updated_at_ts FROM ts_demo WHERE id IN (1, 2, 3) ORDER BY id",
            &[],
        )
        .await
        .expect("select refreshed rows");
    let refreshed_tz: Vec<DateTime<Utc>> = refreshed_rows.iter().map(|row| row.get(1)).collect();
    let refreshed_ts: Vec<NaiveDateTime> = refreshed_rows.iter().map(|row| row.get(2)).collect();
    assert_eq!(refreshed_tz.len(), 3);
    assert_eq!(refreshed_ts.len(), 3);
    assert!(
        refreshed_tz.windows(2).all(|pair| pair[0] == pair[1]),
        "multi-row update should apply a statement-stable timestamptz"
    );
    assert!(
        refreshed_ts.windows(2).all(|pair| pair[0] == pair[1]),
        "multi-row update should apply a statement-stable timestamp"
    );

    ctx.client
        .execute("DROP TABLE ts_demo", &[])
        .await
        .expect("drop table");
    let _ = ctx.shutdown.send(());
}
