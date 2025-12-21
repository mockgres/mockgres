mod common;

use common::start;

#[tokio::test(flavor = "multi_thread")]
async fn pg_type_contains_supported_types() {
    let ctx = start().await;
    let rows = ctx
        .client
        .query(
            "select typname, oid, typarray \
             from pg_catalog.pg_type \
             where typname in ('bool','int4','text','timestamptz') \
             order by typname",
            &[],
        )
        .await
        .expect("pg_type query");
    assert_eq!(rows.len(), 4);
    let expected = [
        ("bool".to_string(), 16_i32, 1000_i32),
        ("int4".to_string(), 23_i32, 1007_i32),
        ("text".to_string(), 25_i32, 1009_i32),
        ("timestamptz".to_string(), 1184_i32, 1185_i32),
    ];
    for (row, expected) in rows.iter().zip(expected.iter()) {
        let typname: String = row.get(0);
        let oid: i32 = row.get(1);
        let typarray: i32 = row.get(2);
        assert_eq!((typname, oid, typarray), expected.clone());
    }
    let _ = ctx.shutdown.send(());
}

#[tokio::test(flavor = "multi_thread")]
async fn pg_type_resolves_with_search_path() {
    let ctx = start().await;
    ctx.client
        .execute("set search_path = pg_catalog", &[])
        .await
        .expect("set search_path");
    let rows = ctx
        .client
        .query("select oid from pg_type where typname = 'jsonb'", &[])
        .await
        .expect("select from pg_type");
    assert_eq!(rows.len(), 1);
    let oid: i32 = rows[0].get(0);
    assert_eq!(oid, 3802);
    let _ = ctx.shutdown.send(());
}
