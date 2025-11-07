mod common;

#[tokio::test(flavor = "multi_thread")]
async fn text_and_bool_roundtrip() {
    let ctx = common::start().await;

    ctx.client
        .execute(
            "CREATE TABLE contacts(id INT PRIMARY KEY, name TEXT, active BOOL);",
            &[],
        )
        .await
        .expect("create contacts");

    ctx.client
        .execute(
            "INSERT INTO contacts VALUES
                (1, 'alpha', TRUE),
                (2, 'beta', FALSE),
                (3, NULL, NULL);",
            &[],
        )
        .await
        .expect("insert rows");

    let rows = ctx
        .client
        .query("SELECT name, active FROM contacts ORDER BY name ASC;", &[])
        .await
        .expect("select text/bool");

    let ordered: Vec<(Option<String>, Option<bool>)> = rows
        .into_iter()
        .map(|row| (row.get(0), row.get(1)))
        .collect();
    assert_eq!(
        ordered,
        vec![
            (Some("alpha".into()), Some(true)),
            (Some("beta".into()), Some(false)),
            (None, None)
        ]
    );

    let active_ids: Vec<i32> = ctx
        .client
        .query(
            "SELECT id FROM contacts WHERE active = TRUE ORDER BY id;",
            &[],
        )
        .await
        .expect("select active")
        .into_iter()
        .map(|row| row.get(0))
        .collect();
    assert_eq!(active_ids, vec![1]);

    let bool_order: Vec<Option<bool>> = ctx
        .client
        .query("SELECT active FROM contacts ORDER BY active;", &[])
        .await
        .expect("order by bool")
        .into_iter()
        .map(|row| row.get(0))
        .collect();
    assert_eq!(bool_order, vec![Some(false), Some(true), None]);

    let _ = ctx.shutdown.send(());
}
