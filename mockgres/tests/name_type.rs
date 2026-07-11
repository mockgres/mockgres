mod common;

use tokio_postgres::types::Type;

const LONG_ASCII_NAME: &str = "1234567890ABCDEFGHIJKLMNOPQRSTUVWXYZ1234567890ABCDEFGHIJKLMNOPQR";
const TRUNCATED_ASCII_NAME: &str =
    "1234567890ABCDEFGHIJKLMNOPQRSTUVWXYZ1234567890ABCDEFGHIJKLMNOPQ";

#[tokio::test(flavor = "multi_thread")]
async fn name_literals_truncate_and_compare_like_postgres() {
    assert_eq!(LONG_ASCII_NAME.len(), 64);
    assert_eq!(TRUNCATED_ASCII_NAME.len(), 63);

    let ctx = common::start().await;
    let row = ctx
        .client
        .query_one(
            "select name 'name string' = name 'name string', \
                    name 'name string' = name 'name string '",
            &[],
        )
        .await
        .expect("compare name literals");
    assert!(row.get::<_, bool>(0));
    assert!(!row.get::<_, bool>(1));

    ctx.client
        .execute(
            "create table name_values (id int4 primary key, value name)",
            &[],
        )
        .await
        .expect("create name table");
    ctx.client
        .execute(
            &format!("insert into name_values values (1, '{LONG_ASCII_NAME}')"),
            &[],
        )
        .await
        .expect("insert long name literal");

    let row = ctx
        .client
        .query_one("select value from name_values where id = 1", &[])
        .await
        .expect("select name value");
    assert_eq!(row.columns()[0].type_(), &Type::NAME);
    assert_eq!(row.get::<_, String>(0), TRUNCATED_ASCII_NAME);

    let matching_rows: i64 = ctx
        .client
        .query_one(
            &format!("select count(*) from name_values where value = '{LONG_ASCII_NAME}'"),
            &[],
        )
        .await
        .expect("compare name to overlong untyped literal")
        .get(0);
    assert_eq!(matching_rows, 1);

    let text: String = ctx
        .client
        .query_one(&format!("select '{LONG_ASCII_NAME}'::name::text"), &[])
        .await
        .expect("cast name to text")
        .get(0);
    assert_eq!(text, TRUNCATED_ASCII_NAME);

    let _ = ctx.shutdown.send(());
}

#[tokio::test(flavor = "multi_thread")]
async fn name_binary_parameters_preserve_utf8_boundaries_and_catalog_metadata() {
    let ctx = common::start().await;
    ctx.client
        .execute(
            "create table name_values (id int4 primary key, value name)",
            &[],
        )
        .await
        .expect("create name table");

    let two_byte_name = "é".repeat(40);
    let three_byte_name = "€".repeat(30);
    ctx.client
        .execute(
            "insert into name_values values ($1, $2), ($3, $4)",
            &[&1_i32, &two_byte_name, &2_i32, &three_byte_name],
        )
        .await
        .expect("insert binary name parameters");

    let rows = ctx
        .client
        .query("select value from name_values order by id", &[])
        .await
        .expect("select truncated names");
    let expected_two_byte = "é".repeat(31);
    let expected_three_byte = "€".repeat(21);
    assert_eq!(rows[0].get::<_, String>(0), expected_two_byte);
    assert_eq!(rows[1].get::<_, String>(0), expected_three_byte);
    assert_eq!(rows[0].get::<_, String>(0).len(), 62);
    assert_eq!(rows[1].get::<_, String>(0).len(), 63);

    let round_trip: String = ctx
        .client
        .query_one(
            "select value from name_values where id = 1 and value = $1",
            &[&two_byte_name],
        )
        .await
        .expect("compare binary name parameter")
        .get(0);
    assert_eq!(round_trip, expected_two_byte);

    let metadata = ctx
        .client
        .query_one(
            "select typname, oid, typlen, typbyval, typcategory, typalign, \
                    typstorage, typelem, typarray, typcollation \
             from pg_catalog.pg_type where oid = 19",
            &[],
        )
        .await
        .expect("select name metadata");
    assert_eq!(metadata.columns()[0].type_(), &Type::NAME);
    assert_eq!(metadata.get::<_, String>(0), "name");
    assert_eq!(metadata.get::<_, i32>(1), 19);
    assert_eq!(metadata.get::<_, i32>(2), 64);
    assert!(!metadata.get::<_, bool>(3));
    assert_eq!(metadata.get::<_, String>(4), "S");
    assert_eq!(metadata.get::<_, String>(5), "c");
    assert_eq!(metadata.get::<_, String>(6), "p");
    assert_eq!(metadata.get::<_, i32>(7), 18);
    assert_eq!(metadata.get::<_, i32>(8), 1003);
    assert_eq!(metadata.get::<_, i32>(9), 950);

    let _ = ctx.shutdown.send(());
}
