mod common;

use chrono::{NaiveDate, NaiveDateTime};
use tokio_postgres::Row;

#[tokio::test(flavor = "multi_thread")]
async fn date_timestamp_bytea_roundtrip() {
    let ctx = common::start().await;

    ctx.client
        .execute(
            "create table events(
                id int primary key,
                occurred date default '2024-01-01',
                created_at timestamp default '2024-01-01 12:00:00',
                payload bytea,
                note text default 'n/a'
            )",
            &[],
        )
        .await
        .expect("create events");

    ctx.client
        .execute(
            "insert into events values (1, '2023-12-31', '2023-12-31 23:59:00', '\\x616263', 'first')",
            &[],
        )
        .await
        .expect("insert explicit");

    ctx.client
        .execute(
            "insert into events values (2, DEFAULT, DEFAULT, '\\x646566', DEFAULT)",
            &[],
        )
        .await
        .expect("insert defaults");

    let rows: Vec<Row> = ctx
        .client
        .query(
            "select id, occurred, created_at, payload, note from events order by id",
            &[],
        )
        .await
        .expect("select events");

    assert_eq!(rows.len(), 2);
    let first_date: NaiveDate = rows[0].get(1);
    let first_ts: NaiveDateTime = rows[0].get(2);
    assert_eq!(rows[0].get::<_, i32>(0), 1);
    assert_eq!(first_date.to_string(), "2023-12-31");
    assert_eq!(first_ts.to_string(), "2023-12-31 23:59:00");
    assert_eq!(rows[0].get::<_, Vec<u8>>(3), b"abc");
    assert_eq!(rows[0].get::<_, String>(4), "first");

    let second_date: NaiveDate = rows[1].get(1);
    let second_ts: NaiveDateTime = rows[1].get(2);
    assert_eq!(rows[1].get::<_, i32>(0), 2);
    assert_eq!(second_date.to_string(), "2024-01-01");
    assert_eq!(second_ts.to_string(), "2024-01-01 12:00:00");
    assert_eq!(rows[1].get::<_, Vec<u8>>(3), b"def");
    assert_eq!(rows[1].get::<_, String>(4), "n/a");

    let stmt = ctx
        .client
        .prepare("insert into events values ($1, $2, $3, $4, $5)")
        .await
        .expect("prepare");
    let param_date = NaiveDate::from_ymd_opt(2024, 5, 5).expect("valid date");
    let param_ts = NaiveDate::from_ymd_opt(2024, 5, 5)
        .expect("valid date")
        .and_hms_micro_opt(5, 5, 5, 0)
        .expect("valid timestamp");
    let param_payload = b"123".to_vec();
    ctx.client
        .execute(
            &stmt,
            &[&3, &param_date, &param_ts, &param_payload, &"param"],
        )
        .await
        .expect("insert via params");

    let row = ctx
        .client
        .query_one(
            "select occurred, created_at, payload from events where id = 3",
            &[],
        )
        .await
        .expect("select param row");
    let occurred: NaiveDate = row.get(0);
    let created: NaiveDateTime = row.get(1);
    assert_eq!(occurred.to_string(), "2024-05-05");
    assert_eq!(created.to_string(), "2024-05-05 05:05:05");
    assert_eq!(row.get::<_, Vec<u8>>(2), b"123");

    let _ = ctx.shutdown.send(());
}

#[tokio::test(flavor = "multi_thread")]
async fn insert_default_keyword() {
    let ctx = common::start().await;

    ctx.client
        .execute(
            "create table defaults_demo(
                id int primary key,
                description text not null default 'pending',
                active bool not null default true
            )",
            &[],
        )
        .await
        .expect("create defaults table");

    ctx.client
        .execute(
            "insert into defaults_demo values (1, DEFAULT, DEFAULT)",
            &[],
        )
        .await
        .expect("insert default row");

    let row = ctx
        .client
        .query_one(
            "select description, active from defaults_demo where id = 1",
            &[],
        )
        .await
        .expect("select defaults");
    let desc: Option<String> = row.get(0);
    assert_eq!(desc.as_deref(), Some("pending"));
    assert!(row.get::<_, bool>(1));

    let _ = ctx.shutdown.send(());
}

#[tokio::test(flavor = "multi_thread")]
async fn binary_params_for_date_and_timestamp() {
    let ctx = common::start().await;
    ctx.client
        .execute(
            "create table ts_params(
                id int primary key,
                occurred date not null,
                happened_at timestamp not null
            )",
            &[],
        )
        .await
        .expect("create ts_params");

    let stmt = ctx
        .client
        .prepare("insert into ts_params values ($1, $2, $3)")
        .await
        .expect("prepare ts_params insert");
    let day = NaiveDate::from_ymd_opt(2024, 2, 3).expect("valid date");
    let ts = NaiveDate::from_ymd_opt(2024, 2, 3)
        .expect("valid date")
        .and_hms_micro_opt(4, 5, 6, 789)
        .expect("valid timestamp");
    ctx.client
        .execute(&stmt, &[&1, &day, &ts])
        .await
        .expect("insert via params");

    let row = ctx
        .client
        .query_one(
            "select occurred, happened_at from ts_params where id = 1",
            &[],
        )
        .await
        .expect("select inserted row");
    let stored_day: NaiveDate = row.get(0);
    let stored_ts: NaiveDateTime = row.get(1);
    assert_eq!(stored_day, day, "date round trips via binary params");
    assert_eq!(stored_ts, ts, "timestamp round trips via binary params");

    let _ = ctx.shutdown.send(());
}

#[tokio::test(flavor = "multi_thread")]
async fn default_expressions_are_evaluated() {
    let ctx = common::start().await;

    ctx.client
        .execute(
            "create table default_expr_demo(
                id int primary key,
                label text not null default upper('ab' || 'cd'),
                score int not null default (1 + 2)
            )",
            &[],
        )
        .await
        .expect("create table");

    for id in [1, 2] {
        ctx.client
            .execute(
                "insert into default_expr_demo values ($1, DEFAULT, DEFAULT)",
                &[&id],
            )
            .await
            .expect("insert row");
    }

    let rows = ctx
        .client
        .query(
            "select id, label, score from default_expr_demo order by id",
            &[],
        )
        .await
        .expect("select values");

    assert_eq!(rows.len(), 2);
    for (idx, row) in rows.iter().enumerate() {
        assert_eq!(row.get::<_, i32>(0), (idx + 1) as i32);
        assert_eq!(row.get::<_, String>(1), "ABCD");
        assert_eq!(row.get::<_, i32>(2), 3);
    }

    let _ = ctx.shutdown.send(());
}
