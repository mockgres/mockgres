mod common;

#[tokio::test(flavor = "multi_thread")]
async fn order_by_alias_and_expression() {
    let ctx = common::start().await;

    ctx.client
        .execute(
            "create table people(
                id int primary key,
                name text not null,
                score int not null
            )",
            &[],
        )
        .await
        .expect("create table");

    ctx.client
        .execute(
            "insert into people values
                (1, 'alice', 10),
                (2, 'bob', 5),
                (3, 'carol', 7)",
            &[],
        )
        .await
        .expect("insert rows");

    let alias_rows = ctx
        .client
        .query(
            "select name, upper(name) as uname
             from people
             order by uname desc",
            &[],
        )
        .await
        .expect("order by alias");
    let ordered_names: Vec<String> = alias_rows.iter().map(|r| r.get(0)).collect();
    assert_eq!(ordered_names, vec!["carol", "bob", "alice"]);

    let expr_rows = ctx
        .client
        .query(
            "select name from people order by score + 1 asc, name desc",
            &[],
        )
        .await
        .expect("order by expression");
    let expr_ordered: Vec<String> = expr_rows.iter().map(|r| r.get(0)).collect();
    assert_eq!(expr_ordered, vec!["bob", "carol", "alice"]);

    let ordinal_rows = ctx
        .client
        .query(
            "select upper(name) as uname, score from people order by 1 asc",
            &[],
        )
        .await
        .expect("order by ordinal");
    let ordinal_names: Vec<String> = ordinal_rows.iter().map(|r| r.get(0)).collect();
    assert_eq!(ordinal_names, vec!["ALICE", "BOB", "CAROL"]);

    let _ = ctx.shutdown.send(());
}
