mod common;

#[tokio::test(flavor = "multi_thread")]
async fn prepared_select_supports_multiple_portals() {
    let mut ctx = common::start().await;

    ctx.client
        .execute(
            "create table portal_demo(id int primary key, score int, active bool)",
            &[],
        )
        .await
        .expect("create table");
    ctx.client
        .execute(
            "insert into portal_demo values
                (1, 5, true),
                (2, 15, true),
                (3, 25, false),
                (4, 35, false)",
            &[],
        )
        .await
        .expect("insert rows");

    let stmt = ctx
        .client
        .prepare(
            "select id from portal_demo
             where score >= $1 and active = $2
             order by id",
        )
        .await
        .expect("prepare select");

    let txn = ctx.client.transaction().await.expect("start txn");
    let portal_active = txn.bind(&stmt, &[&10_i32, &true]).await.expect("bind 1");
    let portal_inactive = txn.bind(&stmt, &[&10_i32, &false]).await.expect("bind 2");

    let ids_active: Vec<i32> = txn
        .query_portal(&portal_active, 0)
        .await
        .expect("query portal 1")
        .into_iter()
        .map(|row| row.get(0))
        .collect();
    let ids_inactive: Vec<i32> = txn
        .query_portal(&portal_inactive, 0)
        .await
        .expect("query portal 2")
        .into_iter()
        .map(|row| row.get(0))
        .collect();

    assert_eq!(ids_active, vec![2]);
    assert_eq!(ids_inactive, vec![3, 4]);
    txn.commit().await.expect("commit txn");

    let _ = ctx.shutdown.send(());
}

#[tokio::test(flavor = "multi_thread")]
async fn prepared_update_behaves_inside_and_outside_transactions() {
    let ctx = common::start().await;

    ctx.client
        .execute(
            "create table prepared_stock(id int primary key, qty int)",
            &[],
        )
        .await
        .expect("create table");
    ctx.client
        .execute("insert into prepared_stock values (1, 10)", &[])
        .await
        .expect("seed row");

    let select_stmt = ctx
        .client
        .prepare("select qty from prepared_stock where id = $1")
        .await
        .expect("prepare select");
    let update_stmt = ctx
        .client
        .prepare("update prepared_stock set qty = qty + $1 where id = $2")
        .await
        .expect("prepare update");

    let original: i32 = ctx
        .client
        .query_one(&select_stmt, &[&1_i32])
        .await
        .expect("initial select")
        .get(0);
    assert_eq!(original, 10);

    ctx.client.execute("begin", &[]).await.expect("begin");
    ctx.client
        .execute(&update_stmt, &[&5_i32, &1_i32])
        .await
        .expect("update in txn");
    let mid: i32 = ctx
        .client
        .query_one(&select_stmt, &[&1_i32])
        .await
        .expect("select in txn")
        .get(0);
    assert_eq!(mid, original + 5);
    ctx.client.execute("rollback", &[]).await.expect("rollback");

    let after_rollback: i32 = ctx
        .client
        .query_one(&select_stmt, &[&1_i32])
        .await
        .expect("select after rollback")
        .get(0);
    assert_eq!(after_rollback, original);

    ctx.client
        .execute(&update_stmt, &[&3_i32, &1_i32])
        .await
        .expect("autocommit update");
    let final_qty: i32 = ctx
        .client
        .query_one(&select_stmt, &[&1_i32])
        .await
        .expect("select after commit")
        .get(0);
    assert_eq!(final_qty, original + 3);

    let _ = ctx.shutdown.send(());
}
