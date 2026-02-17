mod common;

#[tokio::test(flavor = "multi_thread")]
async fn cte_supports_multi_reference_self_join() {
    let ctx = common::start().await;
    ctx.client
        .batch_execute(
            "create table items(id int primary key); insert into items values (1),(2),(3)",
        )
        .await
        .unwrap();

    let rows = ctx
        .client
        .query(
            "with c as (
                select id from items where id <= 2
             )
             select a.id, b.id
             from c a
             join c b on a.id = b.id
             order by a.id",
            &[],
        )
        .await
        .unwrap();

    let got: Vec<(i32, i32)> = rows.into_iter().map(|r| (r.get(0), r.get(1))).collect();
    assert_eq!(got, vec![(1, 1), (2, 2)]);

    let _ = ctx.shutdown.send(());
}

#[tokio::test(flavor = "multi_thread")]
async fn cte_name_shadows_catalog_table_during_statement() {
    let ctx = common::start().await;
    ctx.client
        .batch_execute("create table dupe(id int primary key); insert into dupe values (99)")
        .await
        .unwrap();

    let row = ctx
        .client
        .query_one(
            "with dupe as (select 1 as id)
             select id from dupe",
            &[],
        )
        .await
        .unwrap();
    let id: i32 = row.get(0);
    assert_eq!(id, 1);

    let plain = ctx
        .client
        .query_one("select id from dupe", &[])
        .await
        .unwrap();
    let plain_id: i32 = plain.get(0);
    assert_eq!(plain_id, 99);

    let _ = ctx.shutdown.send(());
}

#[tokio::test(flavor = "multi_thread")]
async fn for_update_on_cte_reference_is_allowed() {
    let ctx = common::start().await;
    ctx.client
        .batch_execute(
            "create table jobs(id int primary key);
             insert into jobs values (1), (2)",
        )
        .await
        .unwrap();

    let rows = ctx
        .client
        .query(
            "with picked as (
                select id from jobs order by id
             )
             select id
               from picked
              for update skip locked",
            &[],
        )
        .await
        .unwrap();
    let ids: Vec<i32> = rows.into_iter().map(|r| r.get(0)).collect();
    assert_eq!(ids, vec![1, 2]);

    let _ = ctx.shutdown.send(());
}
