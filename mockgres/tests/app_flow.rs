mod common;

#[tokio::test(flavor = "multi_thread")]
async fn crud_flow_covers_transactions() {
    let ctx = common::start().await;

    ctx.client
        .execute(
            "create table app_accounts(
                id int primary key,
                owner text not null,
                balance int not null,
                active bool not null default true
            )",
            &[],
        )
        .await
        .expect("create table");

    let fixtures = vec![
        (1, "alpha", 10, true),
        (2, "bravo", 5, false),
        (3, "charlie", 20, true),
    ];
    for (id, owner, balance, active) in fixtures {
        ctx.client
            .execute(
                "insert into app_accounts values ($1, $2, $3, $4)",
                &[&id, &owner, &balance, &active],
            )
            .await
            .expect("insert fixture");
    }

    let page = ctx
        .client
        .query(
            "select owner, balance from app_accounts order by id limit 2 offset 1",
            &[],
        )
        .await
        .expect("page results");
    assert_eq!(page.len(), 2);
    assert_eq!(page[0].get::<_, String>(0), "bravo");
    assert_eq!(page[0].get::<_, i32>(1), 5);
    assert_eq!(page[1].get::<_, String>(0), "charlie");
    assert_eq!(page[1].get::<_, i32>(1), 20);

    ctx.client.batch_execute("begin").await.expect("begin");
    ctx.client
        .execute(
            "update app_accounts set balance = balance + 10 where id = 1",
            &[],
        )
        .await
        .expect("update inside txn");
    ctx.client
        .execute("delete from app_accounts where id = 2", &[])
        .await
        .expect("delete inside txn");
    ctx.client
        .execute(
            "insert into app_accounts values (4, 'delta', 15, true)",
            &[],
        )
        .await
        .expect("insert inside txn");
    ctx.client
        .batch_execute("rollback")
        .await
        .expect("rollback");

    let rows = ctx
        .client
        .query("select id, balance from app_accounts order by id", &[])
        .await
        .expect("select after rollback");
    assert_eq!(rows.len(), 3);
    assert_eq!(rows[0].get::<_, i32>(1), 10);
    assert_eq!(rows[1].get::<_, i32>(1), 5);
    assert_eq!(rows[2].get::<_, i32>(1), 20);

    ctx.client
        .batch_execute("begin")
        .await
        .expect("begin second");
    ctx.client
        .execute(
            "update app_accounts set balance = balance - 3 where id = 3",
            &[],
        )
        .await
        .expect("update second txn");
    ctx.client
        .execute("delete from app_accounts where id = 2", &[])
        .await
        .expect("delete second txn");
    ctx.client
        .execute(
            "insert into app_accounts values (4, 'delta', 15, true)",
            &[],
        )
        .await
        .expect("insert second txn");
    ctx.client.batch_execute("commit").await.expect("commit");

    let final_rows = ctx
        .client
        .query(
            "select id, owner, balance, active from app_accounts order by id",
            &[],
        )
        .await
        .expect("final select");
    assert_eq!(final_rows.len(), 3);
    assert_eq!(final_rows[0].get::<_, i32>(0), 1);
    assert_eq!(final_rows[0].get::<_, i32>(2), 10);
    assert_eq!(final_rows[1].get::<_, i32>(0), 3);
    assert_eq!(final_rows[1].get::<_, i32>(2), 17);
    assert_eq!(final_rows[2].get::<_, i32>(0), 4);
    assert!(final_rows[2].get::<_, bool>(3));

    let _ = ctx.shutdown.send(());
}
