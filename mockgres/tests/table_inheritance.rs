mod common;

use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};

use geo_types::Point;
use tokio_postgres::error::SqlState;
use tokio_postgres::types::Type;

static NEXT_FILE_ID: AtomicU64 = AtomicU64::new(1);

struct CopyFile(PathBuf);

impl CopyFile {
    fn new(contents: &[u8]) -> Self {
        let id = NEXT_FILE_ID.fetch_add(1, Ordering::Relaxed);
        let path = std::env::temp_dir().join(format!(
            "mockgres-inheritance-{}-{id}.data",
            std::process::id()
        ));
        std::fs::write(&path, contents).expect("write inheritance COPY fixture");
        Self(path)
    }

    fn sql_path(&self) -> String {
        self.0
            .to_str()
            .expect("temporary path is UTF-8")
            .replace('\'', "''")
    }
}

impl Drop for CopyFile {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.0);
    }
}

fn assert_sqlstate(error: &tokio_postgres::Error, expected: &SqlState) {
    let db_error = error.as_db_error().expect("expected database error");
    assert_eq!(db_error.code(), expected, "unexpected error: {db_error:?}");
}

#[tokio::test(flavor = "multi_thread")]
async fn inherited_columns_precede_local_columns_and_accept_copy_rows() {
    let ctx = common::start().await;
    ctx.client
        .execute(
            "create table person (\
                name text not null, \
                age int4 default 40, \
                location point\
            )",
            &[],
        )
        .await
        .expect("create inheritance parent");
    ctx.client
        .execute(
            "create table employee (salary int4, manager name) inherits (person)",
            &[],
        )
        .await
        .expect("create inheritance child");

    let file = CopyFile::new(b"alice\t30\t(1.5,-2)\t1000\tboss\ncarol\t25\t(3,4)\t900\t\\N\n");
    let copied = ctx
        .client
        .execute(&format!("copy employee from '{}'", file.sql_path()), &[])
        .await
        .expect("COPY inherited row layout");
    assert_eq!(copied, 2);

    ctx.client
        .execute(
            "insert into employee (name, location, salary, manager) \
             values ('bob', '(0,0)', 800, 'alice')",
            &[],
        )
        .await
        .expect("insert child row with inherited default");
    let rows = ctx
        .client
        .query("select * from employee order by name", &[])
        .await
        .expect("select inheritance child");
    let columns: Vec<_> = rows[0]
        .columns()
        .iter()
        .map(|column| (column.name(), column.type_().clone()))
        .collect();
    assert_eq!(
        columns,
        [
            ("name", Type::TEXT),
            ("age", Type::INT4),
            ("location", Type::POINT),
            ("salary", Type::INT4),
            ("manager", Type::NAME),
        ]
    );
    assert_eq!(rows[0].get::<_, String>(0), "alice");
    assert_eq!(rows[0].get::<_, i32>(1), 30);
    assert_eq!(rows[0].get::<_, Point<f64>>(2), Point::new(1.5, -2.0));
    assert_eq!(rows[1].get::<_, String>(0), "bob");
    assert_eq!(rows[1].get::<_, i32>(1), 40);

    let error = ctx
        .client
        .execute(
            "insert into employee (name, salary) values (null, 100)",
            &[],
        )
        .await
        .expect_err("inherited NOT NULL should be enforced");
    assert_sqlstate(&error, &SqlState::NOT_NULL_VIOLATION);

    let _ = ctx.shutdown.send(());
}

#[tokio::test(flavor = "multi_thread")]
async fn multiple_inheritance_merges_columns_defaults_and_reports_conflicts() {
    let ctx = common::start().await;
    ctx.client
        .execute(
            "create table root_parent (common int4 default 7, required text not null)",
            &[],
        )
        .await
        .expect("create root parent");
    ctx.client
        .execute(
            "create table left_parent (left_value text) inherits (root_parent)",
            &[],
        )
        .await
        .expect("create left parent");
    ctx.client
        .execute(
            "create table right_parent (right_value point) inherits (root_parent)",
            &[],
        )
        .await
        .expect("create right parent");
    ctx.client
        .execute(
            "create table leaf (local_value name) inherits (left_parent, right_parent)",
            &[],
        )
        .await
        .expect("create multiply inherited child");
    ctx.client
        .execute(
            "insert into leaf (required, left_value, right_value, local_value) \
             values ('yes', 'left', '(8,9)', 'local')",
            &[],
        )
        .await
        .expect("insert multiply inherited row");

    let row = ctx
        .client
        .query_one(
            "select common, required, left_value, right_value, local_value from leaf",
            &[],
        )
        .await
        .expect("select multiply inherited row");
    assert_eq!(row.get::<_, i32>(0), 7);
    assert_eq!(row.get::<_, String>(1), "yes");
    assert_eq!(row.get::<_, String>(2), "left");
    assert_eq!(row.get::<_, Point<f64>>(3), Point::new(8.0, 9.0));
    assert_eq!(row.get::<_, String>(4), "local");

    ctx.client
        .execute(
            "create table conflicting_default (common int4 default 8)",
            &[],
        )
        .await
        .expect("create conflicting default parent");
    let error = ctx
        .client
        .execute(
            "create table bad_default () \
             inherits (root_parent, conflicting_default)",
            &[],
        )
        .await
        .expect_err("conflicting inherited defaults should fail");
    assert_sqlstate(&error, &SqlState::INVALID_COLUMN_DEFINITION);

    ctx.client
        .execute(
            "create table resolved_default (common int4 default 9) \
             inherits (root_parent, conflicting_default)",
            &[],
        )
        .await
        .expect("local default should resolve inherited conflict");

    ctx.client
        .execute("create table text_parent (common text)", &[])
        .await
        .expect("create conflicting type parent");
    let error = ctx
        .client
        .execute(
            "create table bad_type () inherits (root_parent, text_parent)",
            &[],
        )
        .await
        .expect_err("conflicting inherited types should fail");
    assert_sqlstate(&error, &SqlState::DATATYPE_MISMATCH);

    let error = ctx
        .client
        .execute(
            "create table duplicate_parent () inherits (root_parent, root_parent)",
            &[],
        )
        .await
        .expect_err("duplicate direct parent should fail");
    assert_sqlstate(&error, &SqlState::DUPLICATE_OBJECT);

    let _ = ctx.shutdown.send(());
}

#[tokio::test(flavor = "multi_thread")]
async fn inherited_identity_columns_are_regular_not_null_columns() {
    let ctx = common::start().await;
    ctx.client
        .execute(
            "create table identity_parent (id int4 generated always as identity)",
            &[],
        )
        .await
        .expect("create identity parent");
    ctx.client
        .execute(
            "create table identity_child () inherits (identity_parent)",
            &[],
        )
        .await
        .expect("create identity child");
    ctx.client
        .execute("insert into identity_child values (42)", &[])
        .await
        .expect("identity property should not be inherited");
    let id: i32 = ctx
        .client
        .query_one("select id from identity_child", &[])
        .await
        .expect("select inherited identity column")
        .get(0);
    assert_eq!(id, 42);

    let error = ctx
        .client
        .execute("insert into identity_child values (null)", &[])
        .await
        .expect_err("identity NOT NULL should be inherited");
    assert_sqlstate(&error, &SqlState::NOT_NULL_VIOLATION);

    let _ = ctx.shutdown.send(());
}
