mod common;

use tokio_postgres::Row;

#[tokio::test(flavor = "multi_thread")]
async fn update_rows_with_filters() {
    let ctx = common::start().await;

    ctx.client
        .execute(
            "create table t(id int primary key, score int, note text, active bool not null)",
            &[],
        )
        .await
        .expect("create");

    ctx.client
        .execute(
            "insert into t values
                (1, 10, 'cold', TRUE),
                (2, 20, 'warm', FALSE),
                (3, 30, NULL, TRUE)",
            &[],
        )
        .await
        .expect("insert");

    let dup = ctx
        .client
        .execute("insert into t values (1, 99, 'dup', TRUE)", &[])
        .await;
    assert!(dup.is_err(), "duplicate primary key should fail");

    ctx.client
        .execute("update t set note = 'vip', active = TRUE where id = 2", &[])
        .await
        .expect("simple update");

    ctx.client
        .execute("update t set score = id where id >= 2", &[])
        .await
        .expect("column-to-column update");

    let rows: Vec<Row> = ctx
        .client
        .query("select id, score, note, active from t order by id", &[])
        .await
        .expect("select after update");
    let snapshot: Vec<(i32, i32, Option<String>, bool)> = rows
        .into_iter()
        .map(|r| (r.get(0), r.get(1), r.get(2), r.get(3)))
        .collect();
    assert_eq!(
        snapshot,
        vec![
            (1, 10, Some("cold".into()), true),
            (2, 2, Some("vip".into()), true),
            (3, 3, None, true)
        ]
    );

    // type mismatch
    let err = ctx
        .client
        .execute("update t set active = 5 where id = 1", &[])
        .await
        .expect_err("type mismatch");
    assert!(err.to_string().contains("type mismatch"));

    let existing = ctx
        .client
        .query("select id from t where id = 1", &[])
        .await
        .expect("fetch id=1");
    assert_eq!(existing.len(), 1);

    ctx.client
        .execute("update t set id = 10 where id = 1", &[])
        .await
        .expect("pk update succeeds when no inbound refs");
    let err = ctx
        .client
        .execute("update t set id = 2 where id = 10", &[])
        .await
        .expect_err("pk collision");
    assert!(err.to_string().contains("duplicate key"));

    let _ = ctx.shutdown.send(());
}

#[tokio::test(flavor = "multi_thread")]
async fn delete_rows_with_parameters() {
    let ctx = common::start().await;

    ctx.client
        .execute(
            "create table items(id int primary key, qty int, note text, active bool)",
            &[],
        )
        .await
        .expect("create");

    ctx.client
        .execute(
            "insert into items values
                (1, 5, 'alpha', TRUE),
                (2, 10, 'beta', FALSE),
                (3, 15, 'gamma', TRUE)",
            &[],
        )
        .await
        .expect("insert");

    ctx.client
        .execute("delete from items where active = FALSE", &[])
        .await
        .expect("delete inactive");

    let stmt = ctx
        .client
        .prepare("delete from items where qty > $1")
        .await
        .expect("prepare delete");
    ctx.client
        .execute(&stmt, &[&10_i32])
        .await
        .expect("delete by param");

    let rows = ctx
        .client
        .query("select id from items order by 1", &[])
        .await
        .expect("remaining rows");
    let ids: Vec<i32> = rows.into_iter().map(|r| r.get(0)).collect();
    assert_eq!(ids, vec![1]);

    // delete all
    ctx.client
        .execute("delete from items", &[])
        .await
        .expect("delete all");
    let remaining = ctx
        .client
        .query("select id from items", &[])
        .await
        .expect("remaining rows");
    assert!(remaining.is_empty());

    let _ = ctx.shutdown.send(());
}
