mod common;

#[tokio::test(flavor = "multi_thread")]
async fn cast_literals_between_core_types() {
    let ctx = common::start().await;

    let int_val: i32 = ctx
        .client
        .query_one("select '42'::int4", &[])
        .await
        .expect("cast to int4")
        .get(0);
    assert_eq!(int_val, 42);

    let float_val: f64 = ctx
        .client
        .query_one("select '3.5'::float8", &[])
        .await
        .expect("cast to float8")
        .get(0);
    assert!((float_val - 3.5).abs() < f64::EPSILON);

    let text_val: String = ctx
        .client
        .query_one("select cast(99 as text)", &[])
        .await
        .expect("cast to text")
        .get(0);
    assert_eq!(text_val, "99");

    let bool_val: bool = ctx
        .client
        .query_one("select 'false'::bool", &[])
        .await
        .expect("cast to bool")
        .get(0);
    assert!(!bool_val);

    let _ = ctx.shutdown.send(());
}

#[tokio::test(flavor = "multi_thread")]
async fn cast_applies_parameter_hints() {
    let ctx = common::start().await;

    let int_param: i64 = ctx
        .client
        .query_one("select cast($1 as int8)", &[&"7"])
        .await
        .expect("cast param to int8")
        .get(0);
    assert_eq!(int_param, 7);

    let bool_param: bool = ctx
        .client
        .query_one("select cast($1 as bool)", &[&"t"])
        .await
        .expect("cast param to bool")
        .get(0);
    assert!(bool_param);

    let _ = ctx.shutdown.send(());
}
