mod common;

use tokio_postgres::SimpleQueryMessage;

fn collect_text(messages: &[SimpleQueryMessage]) -> Vec<String> {
    messages
        .iter()
        .filter_map(|msg| match msg {
            SimpleQueryMessage::Row(row) => row.get(0).map(|s| s.to_string()),
            _ => None,
        })
        .collect()
}

#[tokio::test(flavor = "multi_thread")]
async fn timestamptz_respects_session_timezone() {
    let ctx = common::start().await;

    ctx.client
        .execute("create table tz_data (ts timestamptz)", &[])
        .await
        .expect("create timestamptz table");
    ctx.client
        .execute("set time zone '+02'", &[])
        .await
        .expect("set initial time zone");
    ctx.client
        .execute(
            "insert into tz_data (ts) values ('2025-01-01 12:00:00')",
            &[],
        )
        .await
        .expect("insert local timestamptz");
    ctx.client
        .execute(
            "insert into tz_data (ts) values ('2025-01-01 12:00:00+00')",
            &[],
        )
        .await
        .expect("insert utc timestamptz");

    ctx.client
        .execute("set time zone 'UTC'", &[])
        .await
        .expect("set time zone UTC");
    let utc_rows = ctx
        .client
        .simple_query("select ts::text from tz_data order by ts")
        .await
        .expect("select timestamptz UTC");
    assert_eq!(
        collect_text(&utc_rows),
        vec![
            String::from("2025-01-01 10:00:00+00:00"),
            String::from("2025-01-01 12:00:00+00:00"),
        ]
    );

    ctx.client
        .execute("set time zone '-08'", &[])
        .await
        .expect("set time zone -08");
    let pacific_rows = ctx
        .client
        .simple_query("select ts::text from tz_data order by ts")
        .await
        .expect("select timestamptz pacific");
    assert_eq!(
        collect_text(&pacific_rows),
        vec![
            String::from("2025-01-01 02:00:00-08:00"),
            String::from("2025-01-01 04:00:00-08:00"),
        ]
    );

    let now_rows = ctx
        .client
        .simple_query("select now()::text")
        .await
        .expect("select now text");
    let now_texts = collect_text(&now_rows);
    assert_eq!(now_texts.len(), 1);
    assert!(
        now_texts[0].ends_with("-08:00"),
        "expected now() to honor session tz, got {}",
        now_texts[0]
    );

    ctx.client
        .execute("drop table tz_data", &[])
        .await
        .expect("drop timestamptz table");
    let _ = ctx.shutdown.send(());
}
