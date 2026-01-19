mod common;

#[tokio::test(flavor = "multi_thread")]
async fn empty_query_via_extended_protocol() {
    let ctx = common::start().await;

    let rows = ctx
        .client
        .execute(";", &[])
        .await
        .expect("execute empty query");

    assert_eq!(rows, 0);
}
