mod common;

use common::{simple_first_cell, start};
use rand::Rng;

#[tokio::test(flavor = "multi_thread")]
async fn simple_select_one() {
    let ctx = start().await;
    let v = simple_first_cell(&ctx.client, "select 1").await;
    assert_eq!(v, "1");
    let _ = ctx.shutdown.send(());
}

#[tokio::test]
async fn extended_select_one() {
    let ctx = start().await;
    let rows = ctx.client.query("select 1", &[]).await.expect("query ok");
    assert_eq!(rows.len(), 1);
    let v: i32 = rows[0].get(0);
    assert_eq!(v, 1);
    let _ = ctx.shutdown.send(());
}

#[tokio::test]
async fn extended_select_random() {
    let ctx = start().await;
    let n: i32 = rand::thread_rng().gen_range(1..=1000);
    let rows = ctx
        .client
        .query(&*format!("select {n}"), &[])
        .await
        .expect("query ok");
    assert_eq!(rows.len(), 1);
    let v: i32 = rows[0].get(0);
    assert_eq!(v, n);
    let _ = ctx.shutdown.send(());
}

use tokio_postgres::SimpleQueryMessage;

#[tokio::test(flavor = "multi_thread")]
async fn simple_select_two_literals() {
    let ctx = common::start().await;

    let msgs = ctx.client.simple_query("SELECT 4, 5").await.expect("query");
    let row = msgs
        .into_iter()
        .find_map(|m| match m {
            SimpleQueryMessage::Row(r) => Some(r),
            _ => None,
        })
        .expect("one row");

    assert_eq!(row.get(0).unwrap(), "4");
    assert_eq!(row.get(1).unwrap(), "5");

    let _ = ctx.shutdown.send(());
}
