mod common;
use tokio_postgres::SimpleQueryMessage;

#[tokio::test(flavor = "multi_thread")]
async fn where_order_limit_and_tags() {
    let ctx = common::start().await;

    ctx.client.execute("create table t(a int4 not null, b int4)", &[]).await.expect("create");

    ctx.client.execute("insert into t values (1,10),(2,20),(3,15),(4,null)", &[]).await.expect("insert");

    let msgs = ctx.client.simple_query("select b from t where a >= 2 order by 1 desc limit 2").await.expect("query");
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

    let rows = ctx.client.query("select b from t where a >= 2 order by 1 desc limit 2", &[]).await.expect("query");
    assert_eq!(rows.len(), 2);

    let _ = ctx.shutdown.send(());
}
