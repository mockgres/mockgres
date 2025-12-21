mod common;

use tokio_postgres::SimpleQueryMessage;

#[tokio::test(flavor = "multi_thread")]
async fn freeze_creates_base_and_isolates_sessions() {
    let ctx = common::start().await;

    // set up fixtures
    ctx.client
        .execute("create table fr_items(id int primary key)", &[])
        .await
        .expect("create table");
    ctx.client
        .execute("insert into fr_items values (1)", &[])
        .await
        .expect("seed");
    let freeze_msgs = ctx
        .client
        .simple_query("select mockgres_freeze()")
        .await
        .expect("freeze");
    let froze = freeze_msgs
        .iter()
        .find_map(|m| match m {
            SimpleQueryMessage::Row(r) => r.get(0),
            _ => None,
        })
        .expect("freeze row");
    assert_eq!(froze, "t");

    // new session
    let conn_b = ctx.new_client().await;
    let ids_b: Vec<i32> = conn_b
        .query("select id from fr_items order by id", &[])
        .await
        .expect("load in B")
        .into_iter()
        .map(|r| r.get(0))
        .collect();
    assert_eq!(ids_b, vec![1]);

    // session a mutates shared db
    // session b mutates its cloned db
    ctx.client
        .execute("insert into fr_items values (2)", &[])
        .await
        .expect("insert in A");
    conn_b
        .execute("insert into fr_items values (3)", &[])
        .await
        .expect("insert in B");

    // db views should be different
    let ids_a: Vec<i32> = ctx
        .client
        .query("select id from fr_items order by id", &[])
        .await
        .expect("load in A")
        .into_iter()
        .map(|r| r.get(0))
        .collect();
    assert_eq!(ids_a, vec![1, 2]);

    let ids_b_after: Vec<i32> = conn_b
        .query("select id from fr_items order by id", &[])
        .await
        .expect("load in B after")
        .into_iter()
        .map(|r| r.get(0))
        .collect();
    assert_eq!(ids_b_after, vec![1, 3]);

    let _ = ctx.shutdown.send(());
}

#[tokio::test(flavor = "multi_thread")]
async fn reset_discards_session_overlay() {
    let ctx = common::start().await;

    ctx.client
        .execute("create table fr_reset(id int primary key)", &[])
        .await
        .expect("create table");
    ctx.client
        .execute("insert into fr_reset values (1)", &[])
        .await
        .expect("seed");
    ctx.client
        .query_one("select mockgres_freeze()", &[])
        .await
        .expect("freeze");

    let conn = ctx.new_client().await;
    conn.execute("insert into fr_reset values (2)", &[])
        .await
        .expect("session insert");

    let pre_reset: Vec<i32> = conn
        .query("select id from fr_reset order by id", &[])
        .await
        .expect("pre-reset read")
        .into_iter()
        .map(|r| r.get(0))
        .collect();
    assert_eq!(pre_reset, vec![1, 2]);

    let reset_row = conn
        .query_one("select mockgres_reset()", &[])
        .await
        .expect("reset");
    assert!(reset_row.get::<_, bool>(0));

    let post_reset: Vec<i32> = conn
        .query("select id from fr_reset order by id", &[])
        .await
        .expect("post-reset read")
        .into_iter()
        .map(|r| r.get(0))
        .collect();
    assert_eq!(
        post_reset,
        vec![1],
        "reset should drop overlay changes and re-clone the frozen base"
    );

    let _ = ctx.shutdown.send(());
}
