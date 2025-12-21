mod common;

use common::start;
use tokio_postgres::types::Type;

#[tokio::test(flavor = "multi_thread")]
async fn select_version_functions() {
    let ctx = start().await;

    for sql in ["select version()", "select pg_catalog.version()"] {
        let rows = ctx.client.query(sql, &[]).await.expect("query ok");
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].columns()[0].type_(), &Type::TEXT);
        let v: String = rows[0].get(0);
        assert!(
            v.starts_with("PostgreSQL 15.0"),
            "unexpected version string: {v}"
        );
    }

    let _ = ctx.shutdown.send(());
}
