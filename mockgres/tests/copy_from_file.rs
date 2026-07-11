mod common;

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

use geo_types::Point;
use tokio_postgres::error::SqlState;

const LONG_NAME: &str = "1234567890ABCDEFGHIJKLMNOPQRSTUVWXYZ1234567890ABCDEFGHIJKLMNOPQR";
const TRUNCATED_NAME: &str = "1234567890ABCDEFGHIJKLMNOPQRSTUVWXYZ1234567890ABCDEFGHIJKLMNOPQ";

static NEXT_FILE_ID: AtomicU64 = AtomicU64::new(1);

struct CopyFile(PathBuf);

impl CopyFile {
    fn new(contents: &[u8]) -> Self {
        let id = NEXT_FILE_ID.fetch_add(1, Ordering::Relaxed);
        let path =
            std::env::temp_dir().join(format!("mockgres-copy-{}-{id}.data", std::process::id()));
        std::fs::write(&path, contents).expect("write COPY fixture");
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
async fn copy_from_text_file_uses_postgres_defaults_and_insert_coercion() {
    let ctx = common::start().await;
    ctx.client
        .execute(
            "create table copy_values (\
                id int4 primary key, \
                label name, \
                location point, \
                active bool, \
                note text, \
                omitted text default 'fallback'\
            )",
            &[],
        )
        .await
        .expect("create COPY target");

    let data = format!(
        "1\t{LONG_NAME}\t(1.5,-2)\tt\tline\\nvalue\n\
         2\tshort\t(0,0)\tf\t\\N\n"
    );
    let file = CopyFile::new(data.as_bytes());
    let copied = ctx
        .client
        .execute(
            &format!(
                "copy copy_values (id, label, location, active, note) from '{}'",
                file.sql_path()
            ),
            &[],
        )
        .await
        .expect("COPY FROM text file");
    assert_eq!(copied, 2);

    let rows = ctx
        .client
        .query(
            "select id, label, location, active, note, omitted \
             from copy_values order by id",
            &[],
        )
        .await
        .expect("select copied rows");
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0].get::<_, i32>(0), 1);
    assert_eq!(rows[0].get::<_, String>(1), TRUNCATED_NAME);
    assert_eq!(rows[0].get::<_, Point<f64>>(2), Point::new(1.5, -2.0));
    assert!(rows[0].get::<_, bool>(3));
    assert_eq!(
        rows[0].get::<_, Option<String>>(4).as_deref(),
        Some("line\nvalue")
    );
    assert_eq!(rows[0].get::<_, String>(5), "fallback");
    assert_eq!(rows[1].get::<_, Option<String>>(4), None);
    assert_eq!(rows[1].get::<_, String>(5), "fallback");

    let _ = ctx.shutdown.send(());
}

#[tokio::test(flavor = "multi_thread")]
async fn copy_from_file_reports_file_and_shape_errors_without_partial_inserts() {
    let ctx = common::start().await;
    ctx.client
        .execute(
            "create table copy_values (id int4 primary key, value text)",
            &[],
        )
        .await
        .expect("create COPY target");

    let malformed = CopyFile::new(b"1\tok\n2\textra\tfield\n");
    let error = ctx
        .client
        .execute(
            &format!("copy copy_values from '{}'", malformed.sql_path()),
            &[],
        )
        .await
        .expect_err("extra COPY field should fail");
    assert_sqlstate(&error, &SqlState::BAD_COPY_FILE_FORMAT);
    let count: i64 = ctx
        .client
        .query_one("select count(*) from copy_values", &[])
        .await
        .expect("count rows after failed COPY")
        .get(0);
    assert_eq!(count, 0);

    let invalid_value = CopyFile::new(b"1\tok\nnot-an-integer\tbad\n");
    let error = ctx
        .client
        .execute(
            &format!("copy copy_values from '{}'", invalid_value.sql_path()),
            &[],
        )
        .await
        .expect_err("invalid COPY value should fail");
    assert_sqlstate(&error, &SqlState::INVALID_TEXT_REPRESENTATION);
    let count: i64 = ctx
        .client
        .query_one("select count(*) from copy_values", &[])
        .await
        .expect("count rows after failed coercion")
        .get(0);
    assert_eq!(count, 0);

    let missing = std::env::temp_dir().join(format!(
        "mockgres-copy-missing-{}-{}",
        std::process::id(),
        NEXT_FILE_ID.fetch_add(1, Ordering::Relaxed)
    ));
    let error = ctx
        .client
        .execute(
            &format!("copy copy_values from '{}'", sql_path(&missing)),
            &[],
        )
        .await
        .expect_err("missing COPY file should fail");
    assert_eq!(
        error.as_db_error().expect("database error").code().code(),
        "58P01"
    );

    let error = ctx
        .client
        .execute("copy copy_values from 'relative.data'", &[])
        .await
        .expect_err("relative COPY path should fail");
    assert_sqlstate(&error, &SqlState::INVALID_NAME);

    let error = ctx
        .client
        .execute("copy copy_values from stdin", &[])
        .await
        .expect_err("COPY FROM STDIN should be deferred");
    assert_sqlstate(&error, &SqlState::FEATURE_NOT_SUPPORTED);

    let _ = ctx.shutdown.send(());
}

fn sql_path(path: &Path) -> String {
    path.to_str()
        .expect("temporary path is UTF-8")
        .replace('\'', "''")
}
