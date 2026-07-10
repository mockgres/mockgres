mod common;

use tokio_postgres::error::SqlState;
use tokio_postgres::types::Type;

fn assert_sqlstate(error: &tokio_postgres::Error, expected: &SqlState) {
    let db_error = error.as_db_error().expect("expected database error");
    assert_eq!(db_error.code(), expected, "unexpected error: {db_error:?}");
}

#[tokio::test(flavor = "multi_thread")]
async fn bpchar_assignment_padding_casts_and_comparisons_match_postgres() {
    let ctx = common::start().await;
    ctx.client
        .execute("create table chars (value char(4))", &[])
        .await
        .expect("create char table");
    ctx.client
        .execute(
            "insert into chars values ('a'), ('ab'), ('abcd'), ('abcd    ')",
            &[],
        )
        .await
        .expect("insert char values");

    let rows = ctx
        .client
        .query("select value from chars order by value", &[])
        .await
        .expect("select char values");
    assert_eq!(rows[0].columns()[0].type_(), &Type::BPCHAR);
    let values: Vec<String> = rows.iter().map(|row| row.get(0)).collect();
    assert_eq!(values, ["a   ", "ab  ", "abcd", "abcd"]);

    let matching = ctx
        .client
        .query("select value from chars where value = 'a'", &[])
        .await
        .expect("compare char with unknown literal");
    assert_eq!(matching.len(), 1);

    let cast: String = ctx
        .client
        .query_one("select 'abcde'::char(4)", &[])
        .await
        .expect("explicit char cast truncates")
        .get(0);
    assert_eq!(cast, "abcd");

    let error = ctx
        .client
        .execute("insert into chars values ('abcde')", &[])
        .await
        .expect_err("assignment of non-space overflow should fail");
    assert_sqlstate(&error, &SqlState::STRING_DATA_RIGHT_TRUNCATION);

    let error = ctx
        .client
        .execute("create table invalid_char (value char(0))", &[])
        .await
        .expect_err("zero-length char should fail");
    assert_sqlstate(&error, &SqlState::INVALID_PARAMETER_VALUE);

    let error = ctx
        .client
        .execute("create table oversized_char (value char(10485761))", &[])
        .await
        .expect_err("oversized char length should fail");
    assert_sqlstate(&error, &SqlState::INVALID_PARAMETER_VALUE);

    let _ = ctx.shutdown.send(());
}

#[tokio::test(flavor = "multi_thread")]
async fn smallint_and_integer_text_input_accept_whitespace_and_enforce_ranges() {
    let ctx = common::start().await;
    ctx.client
        .execute(
            "create table integers (small int2, regular int4, large int8)",
            &[],
        )
        .await
        .expect("create integer table");
    ctx.client
        .execute(
            "insert into integers values \
             ('0   ', '   0  ', '  123   '), \
             ('  1234 ', '123456     ', +4567890123456789), \
             ('-32768', '    -123456', '-9223372036854775808')",
            &[],
        )
        .await
        .expect("insert whitespace-padded integer values");

    let rows = ctx
        .client
        .query(
            "select small, regular, large from integers order by small",
            &[],
        )
        .await
        .expect("select integer values");
    assert_eq!(rows[0].columns()[0].type_(), &Type::INT2);
    assert_eq!(rows[0].columns()[1].type_(), &Type::INT4);
    assert_eq!(rows[0].columns()[2].type_(), &Type::INT8);
    assert_eq!(rows[0].get::<_, i16>(0), i16::MIN);
    assert_eq!(rows[0].get::<_, i64>(2), i64::MIN);
    assert_eq!(rows[1].get::<_, i16>(0), 0);
    assert_eq!(rows[2].get::<_, i16>(0), 1234);
    assert_eq!(rows[2].get::<_, i64>(2), 4_567_890_123_456_789);

    for input in ["32768", "-32769"] {
        let sql = format!("insert into integers(small) values ('{input}')");
        let error = ctx
            .client
            .execute(&sql, &[])
            .await
            .expect_err("out-of-range smallint should fail");
        assert_sqlstate(&error, &SqlState::NUMERIC_VALUE_OUT_OF_RANGE);
    }
    let error = ctx
        .client
        .execute("insert into integers(small) values ('12x')", &[])
        .await
        .expect_err("invalid smallint syntax should fail");
    assert_sqlstate(&error, &SqlState::INVALID_TEXT_REPRESENTATION);

    let _ = ctx.shutdown.send(());
}

#[tokio::test(flavor = "multi_thread")]
async fn prepared_binary_parameters_support_bpchar_and_int2() {
    let ctx = common::start().await;
    ctx.client
        .execute(
            "create table prepared_scalars (label char(4), quantity int2)",
            &[],
        )
        .await
        .expect("create prepared scalar table");
    ctx.client
        .execute(
            "insert into prepared_scalars values ($1, $2)",
            &[&"xy", &123_i16],
        )
        .await
        .expect("insert binary scalar parameters");

    let row = ctx
        .client
        .query_one("select label, quantity from prepared_scalars", &[])
        .await
        .expect("select prepared scalar values");
    assert_eq!(row.get::<_, String>(0), "xy  ");
    assert_eq!(row.get::<_, i16>(1), 123);

    let error = ctx
        .client
        .execute(
            "insert into prepared_scalars values ($1, $2)",
            &[&"abcde", &1_i16],
        )
        .await
        .expect_err("prepared bpchar assignment should enforce its length");
    assert_sqlstate(&error, &SqlState::STRING_DATA_RIGHT_TRUNCATION);

    let _ = ctx.shutdown.send(());
}

#[tokio::test(flavor = "multi_thread")]
async fn integer_arithmetic_reports_overflow_and_division_by_zero_without_crashing() {
    let ctx = common::start().await;

    let minimum: i64 = ctx
        .client
        .query_one("select (-9223372036854775808)::int8", &[])
        .await
        .expect("minimum unquoted int8 literal")
        .get(0);
    assert_eq!(minimum, i64::MIN);

    let quotient: i64 = ctx
        .client
        .query_one("select 7::int8 / 2::int8", &[])
        .await
        .expect("integer division")
        .get(0);
    assert_eq!(quotient, 3);

    for sql in [
        "select '9223372036854775807'::int8 + 1::int8",
        "select '9223372036854775807'::int8 * 2::int8",
        "select -('-9223372036854775808'::int8)",
        "select abs('-9223372036854775808'::int8)",
    ] {
        let error = ctx
            .client
            .query(sql, &[])
            .await
            .expect_err("integer overflow should fail");
        assert_sqlstate(&error, &SqlState::NUMERIC_VALUE_OUT_OF_RANGE);
    }

    let error = ctx
        .client
        .query("select 1::int8 / 0::int8", &[])
        .await
        .expect_err("integer division by zero should fail");
    assert_sqlstate(&error, &SqlState::DIVISION_BY_ZERO);

    let one: i32 = ctx
        .client
        .query_one("select 1", &[])
        .await
        .expect("server remains available after arithmetic errors")
        .get(0);
    assert_eq!(one, 1);

    let _ = ctx.shutdown.send(());
}
