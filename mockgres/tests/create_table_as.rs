mod common;

use geo_types::Point;
use tokio_postgres::error::SqlState;
use tokio_postgres::types::Type;

fn assert_sqlstate(error: &tokio_postgres::Error, expected: &SqlState) {
    let db_error = error.as_db_error().expect("expected database error");
    assert_eq!(db_error.code(), expected, "unexpected error: {db_error:?}");
}

#[tokio::test(flavor = "multi_thread")]
async fn create_table_as_copies_rows_types_and_nullable_columns() {
    let ctx = common::start().await;
    ctx.client
        .execute(
            "create table source_values (\
                id int4 primary key, \
                label name, \
                location point\
            )",
            &[],
        )
        .await
        .expect("create CTAS source");
    ctx.client
        .execute(
            "insert into source_values values \
             (1, 'first', '(1.5,-2)'), \
             (2, 'second', '(3,4)')",
            &[],
        )
        .await
        .expect("insert CTAS source rows");

    let created = ctx
        .client
        .execute(
            "create table copied_values as \
             select id as copied_id, label, location \
             from source_values",
            &[],
        )
        .await
        .expect("CREATE TABLE AS SELECT");
    assert_eq!(created, 2);

    let rows = ctx
        .client
        .query(
            "select copied_id, label, location from copied_values order by copied_id",
            &[],
        )
        .await
        .expect("select CTAS rows");
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0].columns()[0].name(), "copied_id");
    assert_eq!(rows[0].columns()[0].type_(), &Type::INT4);
    assert_eq!(rows[0].columns()[1].type_(), &Type::NAME);
    assert_eq!(rows[0].columns()[2].type_(), &Type::POINT);
    assert_eq!(rows[0].get::<_, i32>(0), 1);
    assert_eq!(rows[0].get::<_, String>(1), "first");
    assert_eq!(rows[0].get::<_, Point<f64>>(2), Point::new(1.5, -2.0));

    ctx.client
        .execute("insert into copied_values values (null, null, null)", &[])
        .await
        .expect("CTAS output columns should be nullable");
    let null_rows: i64 = ctx
        .client
        .query_one(
            "select count(*) from copied_values where copied_id is null",
            &[],
        )
        .await
        .expect("count nullable CTAS rows")
        .get(0);
    assert_eq!(null_rows, 1);

    let _ = ctx.shutdown.send(());
}

#[tokio::test(flavor = "multi_thread")]
async fn create_table_as_supports_aliases_no_data_and_atomic_errors() {
    let ctx = common::start().await;
    ctx.client
        .execute("create table source_values (id int4, label text)", &[])
        .await
        .expect("create CTAS source");
    ctx.client
        .execute(
            "insert into source_values values (1, 'first'), (2, 'second')",
            &[],
        )
        .await
        .expect("insert CTAS source rows");

    let created = ctx
        .client
        .execute(
            "create table empty_copy (renamed_id) as \
             select id, label from source_values with no data",
            &[],
        )
        .await
        .expect("CREATE TABLE AS WITH NO DATA");
    assert_eq!(created, 0);
    let row = ctx
        .client
        .query_one(
            "select count(renamed_id), count(label) from empty_copy",
            &[],
        )
        .await
        .expect("select aliased empty CTAS table");
    assert_eq!(row.get::<_, i64>(0), 0);
    assert_eq!(row.get::<_, i64>(1), 0);

    let skipped = ctx
        .client
        .execute(
            "create table if not exists empty_copy as select 1 / 0 as value",
            &[],
        )
        .await
        .expect("IF NOT EXISTS should skip source execution");
    assert_eq!(skipped, 0);

    let error = ctx
        .client
        .execute("create table failed_copy as select 1 / 0 as value", &[])
        .await
        .expect_err("failing CTAS source should error");
    assert_sqlstate(&error, &SqlState::DIVISION_BY_ZERO);
    let failed_tables: i64 = ctx
        .client
        .query_one(
            "select count(*) from pg_catalog.pg_tables \
             where schemaname = 'public' and tablename = 'failed_copy'",
            &[],
        )
        .await
        .expect("check failed CTAS cleanup")
        .get(0);
    assert_eq!(failed_tables, 0);

    let error = ctx
        .client
        .execute(
            "create table too_many (a, b, c) as select 1 as x, 2 as y",
            &[],
        )
        .await
        .expect_err("too many CTAS aliases should fail");
    assert_sqlstate(&error, &SqlState::SYNTAX_ERROR);

    let error = ctx
        .client
        .execute("create table duplicate_names as select 1 as x, 2 as x", &[])
        .await
        .expect_err("duplicate CTAS columns should fail");
    assert_sqlstate(&error, &SqlState::DUPLICATE_COLUMN);

    let _ = ctx.shutdown.send(());
}
