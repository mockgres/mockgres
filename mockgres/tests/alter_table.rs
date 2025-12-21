mod common;

use tokio_postgres::Row;

#[tokio::test(flavor = "multi_thread")]
async fn alter_table_add_and_drop_column() {
    let ctx = common::start().await;

    ctx.client
        .execute(
            "create table items(
                id int primary key,
                qty int not null
            )",
            &[],
        )
        .await
        .expect("create items");

    ctx.client
        .execute("insert into items values (1, 10)", &[])
        .await
        .expect("insert row");

    ctx.client
        .execute(
            "alter table items add column note text default 'pending'",
            &[],
        )
        .await
        .expect("add column");

    // second add with IF NOT EXISTS should no-op
    ctx.client
        .execute(
            "alter table items add column if not exists note text default 'ignored'",
            &[],
        )
        .await
        .expect("idempotent add column");

    let note_row = ctx
        .client
        .query_one("select note from items where id = 1", &[])
        .await
        .expect("select note");
    let note: Option<String> = note_row.get(0);
    assert_eq!(note.as_deref(), Some("pending"));

    ctx.client
        .execute("insert into items values (2, 20, 'done')", &[])
        .await
        .expect("insert row with note");

    ctx.client
        .execute("alter table items drop column note", &[])
        .await
        .expect("drop column");

    // dropping again with IF EXISTS should be ignored
    ctx.client
        .execute("alter table items drop column if exists note", &[])
        .await
        .expect("drop column if exists");

    ctx.client
        .execute("insert into items values (3, 30)", &[])
        .await
        .expect("insert row after drop");

    let rows: Vec<Row> = ctx
        .client
        .query("select * from items order by id", &[])
        .await
        .expect("select items");
    assert_eq!(rows.len(), 3);
    for (idx, row) in rows.iter().enumerate() {
        assert_eq!(row.columns().len(), 2);
        let expected_id = (idx + 1) as i32;
        let expected_qty = (idx + 1) as i32 * 10;
        assert_eq!(row.get::<_, i32>(0), expected_id);
        assert_eq!(row.get::<_, i32>(1), expected_qty);
    }

    let _ = ctx.shutdown.send(());
}

#[tokio::test(flavor = "multi_thread")]
async fn alter_table_drop_column_constraints() {
    let ctx = common::start().await;

    ctx.client
        .execute(
            "create table shaped(
                a int,
                b int,
                id int primary key
            )",
            &[],
        )
        .await
        .expect("create shaped");

    ctx.client
        .execute("alter table shaped drop column b", &[])
        .await
        .expect("drop non-last column");

    let err_pk = ctx
        .client
        .execute("alter table shaped drop column id", &[])
        .await
        .expect_err("dropping primary key column should fail");
    let db_err = err_pk.as_db_error().expect("expected db error");
    assert!(
        db_err.message().contains("primary key"),
        "unexpected pk drop error: {:?}",
        db_err.message()
    );

    ctx.client
        .execute("insert into shaped values (1, 10)", &[])
        .await
        .expect("insert after dropping column");

    let _ = ctx.shutdown.send(());
}
