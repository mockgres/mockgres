mod common;
use tokio_postgres::SimpleQueryMessage;

#[tokio::test(flavor = "multi_thread")]
async fn where_order_limit_and_tags() {
    let ctx = common::start().await;

    ctx.client
        .execute("create table t(a int4 not null, b int4)", &[])
        .await
        .expect("create");

    ctx.client
        .execute("insert into t values (1,10),(2,20),(3,15),(4,null)", &[])
        .await
        .expect("insert");

    let msgs = ctx
        .client
        .simple_query("select b from t where a >= 2 order by 1 desc limit 2")
        .await
        .expect("query");
    let mut rows = Vec::new();
    let mut rows_affected: Option<u64> = None;
    for m in msgs {
        match m {
            SimpleQueryMessage::Row(r) => rows.push(r.get(0).map(|s| s.to_string())),
            SimpleQueryMessage::CommandComplete(n) => rows_affected = Some(n),
            _ => {}
        }
    }
    assert_eq!(rows.len(), 2);
    assert_eq!(rows_affected, Some(2));

    let rows = ctx
        .client
        .query("select b from t where a >= 2 order by 1 desc limit 2", &[])
        .await
        .expect("query");
    assert_eq!(rows.len(), 2);

    let _ = ctx.shutdown.send(());
}

#[tokio::test(flavor = "multi_thread")]
async fn limit_and_offset_clauses_page_results() {
    let ctx = common::start().await;

    ctx.client
        .execute("create table paged(i int primary key)", &[])
        .await
        .expect("create paged");
    ctx.client
        .execute("insert into paged values (1),(2),(3),(4),(5)", &[])
        .await
        .expect("seed paged");

    let page: Vec<i32> = ctx
        .client
        .query("select i from paged order by i limit 2 offset 1", &[])
        .await
        .expect("limit/offset query")
        .into_iter()
        .map(|row| row.get(0))
        .collect();
    assert_eq!(page, vec![2, 3]);

    let offset_only: Vec<i32> = ctx
        .client
        .query("select i from paged order by i offset 3", &[])
        .await
        .expect("offset only")
        .into_iter()
        .map(|row| row.get(0))
        .collect();
    assert_eq!(offset_only, vec![4, 5]);

    let empty: Vec<i32> = ctx
        .client
        .query("select i from paged order by i limit 0 offset 2", &[])
        .await
        .expect("limit zero")
        .into_iter()
        .map(|row| row.get(0))
        .collect();
    assert!(empty.is_empty());

    let _ = ctx.shutdown.send(());
}

#[tokio::test(flavor = "multi_thread")]
async fn limit_and_offset_accept_bind_parameters() {
    let ctx = common::start().await;

    ctx.client
        .execute("create table paged(i int primary key)", &[])
        .await
        .expect("create paged");
    ctx.client
        .execute("insert into paged values (1),(2),(3),(4),(5)", &[])
        .await
        .expect("seed paged");

    let page: Vec<i32> = ctx
        .client
        .query(
            "select i from paged order by i limit $1 offset $2",
            &[&2_i64, &1_i64],
        )
        .await
        .expect("limit/offset with binds")
        .into_iter()
        .map(|row| row.get(0))
        .collect();
    assert_eq!(page, vec![2, 3]);

    let offset_only: Vec<i32> = ctx
        .client
        .query("select i from paged order by i offset $1", &[&3_i64])
        .await
        .expect("offset with bind")
        .into_iter()
        .map(|row| row.get(0))
        .collect();
    assert_eq!(offset_only, vec![4, 5]);

    let err = ctx
        .client
        .query("select i from paged order by i limit $1", &[&(-1_i64)])
        .await
        .expect_err("negative limit should fail");
    let msg = err
        .as_db_error()
        .expect("expected db error")
        .message()
        .to_string();
    assert!(
        msg.contains("limit must be non-negative"),
        "unexpected error: {msg}"
    );

    let casted_page: Vec<i32> = ctx
        .client
        .query(
            "select i from paged order by i limit $1::integer offset $2::integer",
            &[&2_i32, &1_i32],
        )
        .await
        .expect("limit/offset with casted binds")
        .into_iter()
        .map(|row| row.get(0))
        .collect();
    assert_eq!(casted_page, vec![2, 3]);

    let _ = ctx.shutdown.send(());
}

#[tokio::test(flavor = "multi_thread")]
async fn where_boolean_and_null_ops() {
    let ctx = common::start().await;

    ctx.client
        .execute(
            "create table people(id int primary key, name text, active bool, note text)",
            &[],
        )
        .await
        .expect("create people");

    ctx.client
        .execute(
            "insert into people values
                (1, 'Ada', TRUE, NULL),
                (2, 'Beau', FALSE, 'prefers ctl'),
                (3, 'Cora', NULL, 'misc')",
            &[],
        )
        .await
        .expect("insert people");

    // (active AND name = 'Ada') OR active IS NULL  => Ada + Cora
    let names: Vec<String> = ctx
        .client
        .query(
            "select name from people
             where (active AND name = 'Ada') OR active IS NULL
             order by name",
            &[],
        )
        .await
        .expect("select AND/OR")
        .into_iter()
        .map(|row| row.get(0))
        .collect();
    assert_eq!(names, vec!["Ada", "Cora"]);

    // NOT active should only pick Beau (NULL treated as UNKNOWN -> filtered out)
    let inactive: Vec<String> = ctx
        .client
        .query(
            "select name from people where NOT active order by name",
            &[],
        )
        .await
        .expect("select NOT")
        .into_iter()
        .map(|row| row.get(0))
        .collect();
    assert_eq!(inactive, vec!["Beau"]);

    // bare bool expression `WHERE active` works and ignores NULL
    let active_only: Vec<String> = ctx
        .client
        .query("select name from people where active order by name", &[])
        .await
        .expect("select bool column")
        .into_iter()
        .map(|row| row.get(0))
        .collect();
    assert_eq!(active_only, vec!["Ada"]);

    // IS NULL / IS NOT NULL over text columns
    let null_notes: Vec<String> = ctx
        .client
        .query("select name from people where note IS NULL", &[])
        .await
        .expect("is null")
        .into_iter()
        .map(|row| row.get(0))
        .collect();
    assert_eq!(null_notes, vec!["Ada"]);

    let not_null_notes: Vec<String> = ctx
        .client
        .query(
            "select name from people where note IS NOT NULL order by name",
            &[],
        )
        .await
        .expect("is not null")
        .into_iter()
        .map(|row| row.get(0))
        .collect();
    assert_eq!(not_null_notes, vec!["Beau", "Cora"]);

    let _ = ctx.shutdown.send(());
}
