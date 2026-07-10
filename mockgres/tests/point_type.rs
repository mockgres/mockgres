mod common;

use geo_types::Point;
use tokio_postgres::SimpleQueryMessage;
use tokio_postgres::error::SqlState;
use tokio_postgres::types::Type;

fn assert_sqlstate(error: &tokio_postgres::Error, expected: &SqlState) {
    let db_error = error.as_db_error().expect("expected database error");
    assert_eq!(db_error.code(), expected, "unexpected error: {db_error:?}");
}

#[tokio::test(flavor = "multi_thread")]
async fn point_input_output_and_binary_wire_format_match_postgres() {
    let ctx = common::start().await;
    ctx.client
        .execute(
            "create table points (id int4 primary key, value point)",
            &[],
        )
        .await
        .expect("create point table");
    ctx.client
        .execute(
            "insert into points values \
             (1, '(0.0,0.0)'), \
             (2, '(-3.0,4.0)'), \
             (3, '(1e-300,-1e-300)'), \
             (4, '(1e+300,Inf)'), \
             (5, ' ( Nan , NaN ) '), \
             (6, '10.0,10.0')",
            &[],
        )
        .await
        .expect("insert point values");

    let messages = ctx
        .client
        .simple_query("select value from points order by id")
        .await
        .expect("select point text output");
    let values: Vec<String> = messages
        .iter()
        .filter_map(|message| match message {
            SimpleQueryMessage::Row(row) => row.get(0).map(str::to_string),
            _ => None,
        })
        .collect();
    assert_eq!(
        values,
        [
            "(0,0)",
            "(-3,4)",
            "(1e-300,-1e-300)",
            "(1e+300,Infinity)",
            "(NaN,NaN)",
            "(10,10)",
        ]
    );

    let row = ctx
        .client
        .query_one("select '(1.25,-2.5)'::point", &[])
        .await
        .expect("select binary point");
    assert_eq!(row.columns()[0].type_(), &Type::POINT);
    let point: Point<f64> = row.get(0);
    assert_eq!(point.x(), 1.25);
    assert_eq!(point.y(), -2.5);

    let parameter = Point::new(7.5, -8.25);
    ctx.client
        .execute("insert into points values ($1, $2)", &[&7_i32, &parameter])
        .await
        .expect("insert binary point parameter");
    let round_trip: Point<f64> = ctx
        .client
        .query_one("select value from points where id = 7", &[])
        .await
        .expect("select binary point parameter")
        .get(0);
    assert_eq!(round_trip, parameter);

    let text: String = ctx
        .client
        .query_one("select '(1.25,-2.5)'::point::text", &[])
        .await
        .expect("cast point to text")
        .get(0);
    assert_eq!(text, "(1.25,-2.5)");

    let _ = ctx.shutdown.send(());
}

#[tokio::test(flavor = "multi_thread")]
async fn point_rejects_malformed_and_out_of_range_coordinates() {
    let ctx = common::start().await;
    ctx.client
        .execute("create table points (value point)", &[])
        .await
        .expect("create point table");

    for input in ["asdfasdf", "(10.0 10.0)", "(10.0, 10.0) x", "(10.0,10.0"] {
        let sql = format!("insert into points values ('{input}')");
        let error = ctx
            .client
            .execute(&sql, &[])
            .await
            .expect_err("malformed point should fail");
        assert_sqlstate(&error, &SqlState::INVALID_TEXT_REPRESENTATION);
    }

    let error = ctx
        .client
        .execute("insert into points values ('(10.0, 1e+500)')", &[])
        .await
        .expect_err("out-of-range point coordinate should fail");
    assert_sqlstate(&error, &SqlState::NUMERIC_VALUE_OUT_OF_RANGE);

    let _ = ctx.shutdown.send(());
}
