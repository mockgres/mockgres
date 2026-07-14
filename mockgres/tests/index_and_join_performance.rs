mod common;

fn insert_rows_sql(table: &str, count: usize, key_offset: usize) -> String {
    let values = (0..count)
        .map(|id| format!("({id},{})", id + key_offset))
        .collect::<Vec<_>>()
        .join(",");
    format!("insert into {table} values {values}")
}

#[tokio::test(flavor = "multi_thread")]
async fn equi_join_avoids_cartesian_candidate_limit() {
    let ctx = common::start().await;
    ctx.client
        .batch_execute(
            "create table join_left(id int primary key, join_key int);
             create table join_right(id int primary key, join_key int);",
        )
        .await
        .unwrap();
    ctx.client
        .batch_execute(&insert_rows_sql("join_left", 1_100, 0))
        .await
        .unwrap();
    ctx.client
        .batch_execute(&insert_rows_sql("join_right", 1_100, 0))
        .await
        .unwrap();

    let row = ctx
        .client
        .query_one(
            "select count(*)
               from join_left
               join join_right on join_left.join_key = join_right.join_key",
            &[],
        )
        .await
        .unwrap();
    assert_eq!(row.get::<_, i64>(0), 1_100);

    let implicit = ctx
        .client
        .query_one(
            "select count(*)
               from join_left, join_right
              where join_left.join_key = join_right.join_key",
            &[],
        )
        .await
        .unwrap();
    assert_eq!(implicit.get::<_, i64>(0), 1_100);

    let _ = ctx.shutdown.send(());
}

#[tokio::test(flavor = "multi_thread")]
async fn index_lookup_stays_correct_after_updates_deletes_and_rollback() {
    let ctx = common::start().await;
    ctx.client
        .batch_execute(
            "create table indexed_items(id int primary key, lookup_key int, payload text);
             insert into indexed_items values
                 (1, 10, 'first'),
                 (2, 10, 'second'),
                 (3, 20, 'third');
             create index indexed_items_lookup on indexed_items(lookup_key);",
        )
        .await
        .unwrap();

    let initial = ctx
        .client
        .query(
            "select id from indexed_items where lookup_key = 10 order by id",
            &[],
        )
        .await
        .unwrap();
    assert_eq!(
        initial
            .iter()
            .map(|row| row.get::<_, i32>(0))
            .collect::<Vec<_>>(),
        vec![1, 2]
    );

    ctx.client
        .batch_execute(
            "update indexed_items set lookup_key = 30 where id = 1;
             delete from indexed_items where id = 2;
             begin;
             update indexed_items set lookup_key = 40 where id = 3;
             rollback;",
        )
        .await
        .unwrap();

    let old_key = ctx
        .client
        .query("select id from indexed_items where lookup_key = 10", &[])
        .await
        .unwrap();
    assert!(old_key.is_empty());
    let new_key = ctx
        .client
        .query_one(
            "select payload from indexed_items where lookup_key = 30",
            &[],
        )
        .await
        .unwrap();
    assert_eq!(new_key.get::<_, String>(0), "first");
    let rolled_back = ctx
        .client
        .query_one(
            "select payload from indexed_items where lookup_key = 20",
            &[],
        )
        .await
        .unwrap();
    assert_eq!(rolled_back.get::<_, String>(0), "third");

    let _ = ctx.shutdown.send(());
}

#[tokio::test(flavor = "multi_thread")]
async fn composite_index_uses_leading_prefixes() {
    let ctx = common::start().await;
    ctx.client
        .batch_execute(
            "create table composite_items(id int primary key, tenant_id int, item_key int);
             insert into composite_items values (1, 7, 10), (2, 7, 20), (3, 8, 10);
             create index composite_items_tenant_key on composite_items(tenant_id, item_key);",
        )
        .await
        .unwrap();

    let prefix = ctx
        .client
        .query(
            "select id from composite_items where tenant_id = 7 order by id",
            &[],
        )
        .await
        .unwrap();
    assert_eq!(prefix.len(), 2);
    let exact = ctx
        .client
        .query_one(
            "select id from composite_items where item_key = 20 and tenant_id = 7",
            &[],
        )
        .await
        .unwrap();
    assert_eq!(exact.get::<_, i32>(0), 2);

    let _ = ctx.shutdown.send(());
}

#[tokio::test(flavor = "multi_thread")]
async fn sequential_scan_order_survives_rollback_and_truncate() {
    let ctx = common::start().await;
    ctx.client
        .batch_execute(
            "create table ordered_scan(id int);
             insert into ordered_scan values (1);
             begin;
             insert into ordered_scan values (2);
             rollback;
             insert into ordered_scan values (3);",
        )
        .await
        .unwrap();

    let rows = ctx
        .client
        .query("select id from ordered_scan", &[])
        .await
        .unwrap();
    assert_eq!(
        rows.iter()
            .map(|row| row.get::<_, i32>(0))
            .collect::<Vec<_>>(),
        vec![1, 3]
    );

    ctx.client
        .batch_execute("truncate ordered_scan; insert into ordered_scan values (4), (5);")
        .await
        .unwrap();
    let rows = ctx
        .client
        .query("select id from ordered_scan", &[])
        .await
        .unwrap();
    assert_eq!(
        rows.iter()
            .map(|row| row.get::<_, i32>(0))
            .collect::<Vec<_>>(),
        vec![4, 5]
    );

    let _ = ctx.shutdown.send(());
}

#[tokio::test(flavor = "multi_thread")]
async fn indexed_update_and_delete_accept_parameters_at_scale() {
    let ctx = common::start().await;
    ctx.client
        .batch_execute(
            "create table indexed_dml(id int primary key, lookup_key int);
             create index indexed_dml_lookup on indexed_dml(lookup_key);",
        )
        .await
        .unwrap();
    ctx.client
        .batch_execute(&insert_rows_sql("indexed_dml", 1_100, 1_000))
        .await
        .unwrap();

    assert_eq!(
        ctx.client
            .execute(
                "update indexed_dml set lookup_key = 3000 where lookup_key = $1",
                &[&1777_i32],
            )
            .await
            .unwrap(),
        1
    );
    assert_eq!(
        ctx.client
            .execute(
                "delete from indexed_dml where lookup_key = $1",
                &[&1888_i32]
            )
            .await
            .unwrap(),
        1
    );

    let moved = ctx
        .client
        .query_one("select id from indexed_dml where lookup_key = 3000", &[])
        .await
        .unwrap();
    assert_eq!(moved.get::<_, i32>(0), 777);
    assert!(
        ctx.client
            .query("select id from indexed_dml where lookup_key = 1888", &[])
            .await
            .unwrap()
            .is_empty()
    );
    let count = ctx
        .client
        .query_one("select count(*) from indexed_dml", &[])
        .await
        .unwrap();
    assert_eq!(count.get::<_, i64>(0), 1_099);

    let _ = ctx.shutdown.send(());
}

#[tokio::test(flavor = "multi_thread")]
async fn count_rows_fast_path_respects_transaction_visibility() {
    let ctx = common::start().await;
    ctx.client
        .batch_execute(
            "create table count_items(id int);
             insert into count_items values (1), (2);
             begin;
             insert into count_items values (3);",
        )
        .await
        .unwrap();

    let inside = ctx
        .client
        .query_one("select count(*) from count_items", &[])
        .await
        .unwrap();
    assert_eq!(inside.get::<_, i64>(0), 3);

    ctx.client.batch_execute("rollback").await.unwrap();
    let after = ctx
        .client
        .query_one("select count(*) from count_items", &[])
        .await
        .unwrap();
    assert_eq!(after.get::<_, i64>(0), 2);

    let _ = ctx.shutdown.send(());
}
