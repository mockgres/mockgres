mod common;

#[tokio::test(flavor = "multi_thread")]
async fn update_returns_rowcount() {
    let ctx = common::start().await;

    ctx.client
        .execute("create table update_demo(id int primary key, val int)", &[])
        .await
        .expect("create table");
    ctx.client
        .execute("insert into update_demo values (1, 10)", &[])
        .await
        .expect("insert row");

    let updated = ctx
        .client
        .execute("update update_demo set val = 11 where id = 1", &[])
        .await
        .expect("update row");
    assert_eq!(updated, 1);

    let _ = ctx.shutdown.send(());
}
