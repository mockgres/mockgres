mod common;

use tokio_postgres::SimpleQueryMessage;

#[tokio::test(flavor = "multi_thread")]
async fn show_and_set_commands() {
    let ctx = common::start().await;

    let messages = ctx
        .client
        .simple_query("show server_version")
        .await
        .expect("show server_version");
    let value = messages
        .iter()
        .find_map(|msg| match msg {
            SimpleQueryMessage::Row(row) => row.get(0).map(|s| s.to_string()),
            _ => None,
        })
        .expect("server_version row");
    assert_eq!(value, "15.0");

    let search_path = ctx
        .client
        .simple_query("show search_path")
        .await
        .expect("show search_path");
    let path = search_path
        .iter()
        .find_map(|msg| match msg {
            SimpleQueryMessage::Row(row) => row.get(0).map(|s| s.to_string()),
            _ => None,
        })
        .expect("search_path row");
    assert_eq!(path, "public");

    ctx.client
        .execute("set search_path = public", &[])
        .await
        .expect("set search_path");

    let search_path = ctx
        .client
        .simple_query("show search_path")
        .await
        .expect("show search_path after set");
    let path = search_path
        .iter()
        .find_map(|msg| match msg {
            SimpleQueryMessage::Row(row) => row.get(0).map(|s| s.to_string()),
            _ => None,
        })
        .expect("search_path row after set");
    assert_eq!(path, "public");

    ctx.client
        .execute("set client_min_messages = warning", &[])
        .await
        .expect("set client_min_messages");

    let time_zone = ctx
        .client
        .simple_query("show time zone")
        .await
        .expect("show time zone default");
    let tz = time_zone
        .iter()
        .find_map(|msg| match msg {
            SimpleQueryMessage::Row(row) => row.get(0).map(|s| s.to_string()),
            _ => None,
        })
        .expect("time zone row");
    assert_eq!(tz, "UTC");

    ctx.client
        .execute("set time zone '+05:30'", &[])
        .await
        .expect("set time zone");

    let time_zone = ctx
        .client
        .simple_query("show time zone")
        .await
        .expect("show time zone after set");
    let tz = time_zone
        .iter()
        .find_map(|msg| match msg {
            SimpleQueryMessage::Row(row) => row.get(0).map(|s| s.to_string()),
            _ => None,
        })
        .expect("time zone row after set");
    assert_eq!(tz, "+05:30");

    ctx.client
        .execute("set time zone default", &[])
        .await
        .expect("reset time zone");

    let time_zone = ctx
        .client
        .simple_query("show time zone")
        .await
        .expect("show time zone after reset");
    let tz = time_zone
        .iter()
        .find_map(|msg| match msg {
            SimpleQueryMessage::Row(row) => row.get(0).map(|s| s.to_string()),
            _ => None,
        })
        .expect("time zone row after reset");
    assert_eq!(tz, "UTC");

    let _ = ctx.shutdown.send(());
}

#[tokio::test(flavor = "multi_thread")]
async fn show_standard_conforming_strings() {
    let ctx = common::start().await;
    let rows = ctx
        .client
        .simple_query("show standard_conforming_strings")
        .await
        .expect("show standard_conforming_strings");
    let val = rows
        .iter()
        .find_map(|msg| match msg {
            SimpleQueryMessage::Row(row) => row.get(0).map(|s| s.to_string()),
            _ => None,
        })
        .expect("row");
    assert_eq!(val.to_ascii_lowercase(), "on");
    let _ = ctx.shutdown.send(());
}

#[tokio::test(flavor = "multi_thread")]
async fn show_and_set_transaction_isolation() {
    let ctx = common::start().await;

    let iso = ctx
        .client
        .simple_query("show transaction_isolation")
        .await
        .expect("show transaction_isolation");
    let val = iso
        .iter()
        .find_map(|msg| match msg {
            SimpleQueryMessage::Row(row) => row.get(0).map(|s| s.to_string()),
            _ => None,
        })
        .expect("isolation row");
    assert_eq!(val, "read committed");

    ctx.client
        .execute("set default_transaction_isolation = 'read committed'", &[])
        .await
        .expect("set default_transaction_isolation");

    let err = ctx
        .client
        .execute("set transaction_isolation = 'serializable'", &[])
        .await
        .expect_err("unsupported isolation should error");
    let message = err
        .as_db_error()
        .expect("expected db error")
        .message()
        .to_ascii_lowercase();
    assert!(
        message.contains("not supported"),
        "unexpected error: {message:?}"
    );

    ctx.client.execute("begin", &[]).await.expect("begin");
    ctx.client
        .execute("set transaction_isolation = 'read committed'", &[])
        .await
        .expect("set tx isolation");
    let iso = ctx
        .client
        .simple_query("show transaction_isolation")
        .await
        .expect("show tx iso in txn");
    let val = iso
        .iter()
        .find_map(|msg| match msg {
            SimpleQueryMessage::Row(row) => row.get(0).map(|s| s.to_string()),
            _ => None,
        })
        .expect("isolation row in txn");
    assert_eq!(val, "read committed");
    ctx.client.execute("commit", &[]).await.expect("commit");

    let _ = ctx.shutdown.send(());
}

#[tokio::test(flavor = "multi_thread")]
async fn show_and_set_lock_timeout() {
    let ctx = common::start().await;

    let value = ctx
        .client
        .simple_query("show lock_timeout")
        .await
        .expect("show lock_timeout default");
    let initial = value
        .iter()
        .find_map(|msg| match msg {
            SimpleQueryMessage::Row(row) => row.get(0).map(|s| s.to_string()),
            _ => None,
        })
        .expect("lock_timeout row");
    assert_eq!(initial, "0");

    ctx.client
        .execute("set lock_timeout = '50ms'", &[])
        .await
        .expect("set lock_timeout");
    let value = ctx
        .client
        .simple_query("show lock_timeout")
        .await
        .expect("show lock_timeout after 50ms");
    let after_ms = value
        .iter()
        .find_map(|msg| match msg {
            SimpleQueryMessage::Row(row) => row.get(0).map(|s| s.to_string()),
            _ => None,
        })
        .expect("lock_timeout row after 50ms");
    assert_eq!(after_ms, "50ms");

    ctx.client
        .execute("set lock_timeout = 1000", &[])
        .await
        .expect("set lock_timeout integer");
    let value = ctx
        .client
        .simple_query("show lock_timeout")
        .await
        .expect("show lock_timeout after integer set");
    let after_int = value
        .iter()
        .find_map(|msg| match msg {
            SimpleQueryMessage::Row(row) => row.get(0).map(|s| s.to_string()),
            _ => None,
        })
        .expect("lock_timeout row after integer");
    assert_eq!(after_int, "1s");

    ctx.client
        .execute("set lock_timeout = default", &[])
        .await
        .expect("reset lock_timeout");
    let value = ctx
        .client
        .simple_query("show lock_timeout")
        .await
        .expect("show lock_timeout reset");
    let reset = value
        .iter()
        .find_map(|msg| match msg {
            SimpleQueryMessage::Row(row) => row.get(0).map(|s| s.to_string()),
            _ => None,
        })
        .expect("lock_timeout row after reset");
    assert_eq!(reset, "0");

    let _ = ctx.shutdown.send(());
}
