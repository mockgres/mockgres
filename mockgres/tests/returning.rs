mod common;

#[tokio::test(flavor = "multi_thread")]
async fn returning_across_dml() {
    let ctx = common::start().await;

    ctx.client
        .execute(
            "create table widgets(
                id int primary key,
                qty int not null,
                note text
            )",
            &[],
        )
        .await
        .expect("create table");

    let inserted = ctx
        .client
        .query(
            "insert into widgets (id, qty, note)
             values (1, 10, 'alpha'), (2, 5, NULL)
             returning id, qty + 1 as qty_next, upper(coalesce(note, 'blank'))",
            &[],
        )
        .await
        .expect("insert returning");
    assert_eq!(inserted.len(), 2);
    assert_eq!(inserted[0].get::<_, i32>(0), 1);
    assert_eq!(inserted[0].get::<_, i32>(1), 11);
    assert_eq!(inserted[0].get::<_, String>(2), "ALPHA");
    assert_eq!(inserted[1].get::<_, i32>(0), 2);
    assert_eq!(inserted[1].get::<_, i32>(1), 6);
    assert_eq!(inserted[1].get::<_, String>(2), "BLANK");

    let star_rows = ctx
        .client
        .query(
            "insert into widgets values (3, 20, 'beta') returning *",
            &[],
        )
        .await
        .expect("insert returning star");
    assert_eq!(star_rows.len(), 1);
    assert_eq!(star_rows[0].get::<_, i32>(0), 3);
    assert_eq!(star_rows[0].get::<_, i32>(1), 20);
    assert_eq!(star_rows[0].get::<_, String>(2), "beta");

    let updated = ctx
        .client
        .query(
            "update widgets set qty = qty + 5 where id <= 2 returning id, qty",
            &[],
        )
        .await
        .expect("update returning");
    assert_eq!(updated.len(), 2);
    let mut updated_vals: Vec<(i32, i32)> =
        updated.into_iter().map(|r| (r.get(0), r.get(1))).collect();
    updated_vals.sort_by_key(|(id, _)| *id);
    assert_eq!(updated_vals[0], (1, 15));
    assert_eq!(updated_vals[1], (2, 10));

    let deleted = ctx
        .client
        .query("delete from widgets where id = 3 returning id, note", &[])
        .await
        .expect("delete returning");
    assert_eq!(deleted.len(), 1);
    assert_eq!(deleted[0].get::<_, i32>(0), 3);
    assert_eq!(deleted[0].get::<_, String>(1), "beta");

    let stmt = ctx
        .client
        .prepare("insert into widgets values ($1, $2, $3) returning $2 + 100")
        .await
        .expect("prepare insert returning param");
    let rows = ctx
        .client
        .query(&stmt, &[&4_i32, &7_i32, &"delta"])
        .await
        .expect("insert returning param");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].get::<_, i32>(0), 107);

    let upd_stmt = ctx
        .client
        .prepare("update widgets set note = $1 where id = $2 returning id, $1")
        .await
        .expect("prepare update returning param");
    let rows = ctx
        .client
        .query(&upd_stmt, &[&"patched", &1_i32])
        .await
        .expect("update returning param");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].get::<_, String>(1), "patched");

    let _ = ctx.shutdown.send(());
}
