mod common;

#[tokio::test(flavor = "multi_thread")]
async fn create_unique_index_parses() {
    let ctx = common::start().await;
    ctx.client
        .batch_execute("create table u(i int primary key, v int)")
        .await
        .unwrap();
    ctx.client
        .batch_execute("create unique index ux on u(v)")
        .await
        .unwrap();
    let _ = ctx.shutdown.send(());
}
