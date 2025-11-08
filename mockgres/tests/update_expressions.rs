mod common;

#[tokio::test(flavor = "multi_thread")]
async fn update_with_scalar_expressions() {
    let ctx = common::start().await;

    ctx.client
        .execute(
            "create table widgets(
                id int primary key,
                qty int not null,
                note text
            )",
            &[],
        )
        .await
        .expect("create table");

    ctx.client
        .execute(
            "insert into widgets values (1, 5, 'alpha'), (2, 1, NULL)",
            &[],
        )
        .await
        .expect("insert rows");

    ctx.client
        .execute(
            "update widgets
             set qty = qty + 3,
                 note = upper(coalesce(note, 'pending'))",
            &[],
        )
        .await
        .expect("update expressions");

    let rows = ctx
        .client
        .query("select qty, note from widgets order by id", &[])
        .await
        .expect("select rows");
    assert_eq!(rows[0].get::<_, i32>(0), 8);
    assert_eq!(rows[0].get::<_, String>(1), "ALPHA");
    assert_eq!(rows[1].get::<_, i32>(0), 4);
    assert_eq!(rows[1].get::<_, String>(1), "PENDING");

    let _ = ctx.shutdown.send(());
}
