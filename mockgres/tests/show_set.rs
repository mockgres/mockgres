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
