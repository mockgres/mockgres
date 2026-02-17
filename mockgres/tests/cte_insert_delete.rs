mod common;

#[tokio::test(flavor = "multi_thread")]
async fn cte_insert_select_inserts_projected_rows() {
    let ctx = common::start().await;
    ctx.client
        .batch_execute(
            "create table src(id int primary key, qty int);
             create table dst(id int primary key, qty int);
             insert into src values (1, 10), (2, 20), (3, 30)",
        )
        .await
        .unwrap();

    let inserted = ctx
        .client
        .execute(
            "with picked as (
                select id, qty + 1 as qty
                  from src
                 where id <= 2
             )
             insert into dst(id, qty)
             select id, qty from picked",
            &[],
        )
        .await
        .unwrap();
    assert_eq!(inserted, 2);

    let rows = ctx
        .client
        .query("select id, qty from dst order by id", &[])
        .await
        .unwrap();
    let got: Vec<(i32, i32)> = rows.into_iter().map(|r| (r.get(0), r.get(1))).collect();
    assert_eq!(got, vec![(1, 11), (2, 21)]);

    let _ = ctx.shutdown.send(());
}

#[tokio::test(flavor = "multi_thread")]
async fn cte_delete_removes_only_selected_rows() {
    let ctx = common::start().await;
    ctx.client
        .batch_execute(
            "create table items(id int primary key, status text not null);
             insert into items values
                (1, 'done'),
                (2, 'pending'),
                (3, 'done'),
                (4, 'pending')",
        )
        .await
        .unwrap();

    let deleted = ctx
        .client
        .execute(
            "with doomed as (
                select id from items where status = 'done'
             )
             delete from items
              where id in (select id from doomed)",
            &[],
        )
        .await
        .unwrap();
    assert_eq!(deleted, 2);

    let rows = ctx
        .client
        .query("select id from items order by id", &[])
        .await
        .unwrap();
    let ids: Vec<i32> = rows.into_iter().map(|r| r.get(0)).collect();
    assert_eq!(ids, vec![2, 4]);

    let _ = ctx.shutdown.send(());
}
