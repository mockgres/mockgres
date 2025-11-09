mod common;

#[tokio::test(flavor = "multi_thread")]
async fn cast_literals_between_core_types() {
    let ctx = common::start().await;

    ctx.client
        .execute("create table literals(id int primary key, txt text, float_txt text, bool_txt text, int_value int)", &[])
        .await
        .expect("create literals");
    ctx.client
        .execute(
            "insert into literals values (1, '42', '3.5', 'false', 99)",
            &[],
        )
        .await
        .expect("seed literals");

    let int_val: i32 = ctx
        .client
        .query_one("select txt::int4 from literals limit 1", &[])
        .await
        .expect("cast to int4")
        .get(0);
    assert_eq!(int_val, 42);

    let float_val: f64 = ctx
        .client
        .query_one("select float_txt::float8 from literals limit 1", &[])
        .await
        .expect("cast to float8")
        .get(0);
    assert!((float_val - 3.5).abs() < f64::EPSILON);

    let text_val: String = ctx
        .client
        .query_one("select cast(int_value as text) from literals limit 1", &[])
        .await
        .expect("cast to text")
        .get(0);
    assert_eq!(text_val, "99");

    let bool_val: bool = ctx
        .client
        .query_one("select bool_txt::bool from literals limit 1", &[])
        .await
        .expect("cast to bool")
        .get(0);
    assert!(!bool_val);

    let _ = ctx.shutdown.send(());
}

#[tokio::test(flavor = "multi_thread")]
async fn cast_applies_parameter_hints() {
    let ctx = common::start().await;

    ctx.client
        .execute("create table dummy_params(id int primary key)", &[])
        .await
        .expect("create dummy params");
    ctx.client
        .execute("insert into dummy_params values (1)", &[])
        .await
        .expect("seed dummy params");

    let int_rows = ctx
        .client
        .query(
            "select cast(($1)::text as int8) from dummy_params where id = id limit 1",
            &[&"7"],
        )
        .await
        .expect("cast param to int8");
    let int_param: i64 = int_rows[0].get(0);
    assert_eq!(int_param, 7);

    let bool_rows = ctx
        .client
        .query(
            "select cast(($1)::text as bool) from dummy_params where id = id limit 1",
            &[&"t"],
        )
        .await
        .expect("cast param to bool");
    let bool_param: bool = bool_rows[0].get(0);
    assert!(bool_param);

    let _ = ctx.shutdown.send(());
}
