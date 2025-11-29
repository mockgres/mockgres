mod common;

#[tokio::test(flavor = "multi_thread")]
async fn update_uses_from_subquery_counts() {
    let ctx = common::start().await;

    ctx.client
        .batch_execute("create table posts(id int primary key, score bigint)")
        .await
        .unwrap();
    ctx.client
        .batch_execute("create table votes(post_id int)")
        .await
        .unwrap();
    ctx.client
        .batch_execute("insert into posts values (1,0),(2,0),(3,0)")
        .await
        .unwrap();
    ctx.client
        .batch_execute("insert into votes values (1),(1),(2)")
        .await
        .unwrap();

    ctx.client
        .batch_execute(
            "update posts
                set score = agg.score
                from (
                    select post_id, count(*) as score
                      from votes
                  group by post_id
                ) agg
               where posts.id = agg.post_id",
        )
        .await
        .unwrap();

    let rows = ctx
        .client
        .query("select id, score from posts order by id", &[])
        .await
        .unwrap();
    let got: Vec<(i32, i64)> = rows.into_iter().map(|r| (r.get(0), r.get(1))).collect();
    assert_eq!(got, vec![(1, 2), (2, 1), (3, 0)]);

    let _ = ctx.shutdown.send(());
}
