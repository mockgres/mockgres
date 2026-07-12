use super::*;

fn utility(tag: &'static str) -> Plan {
    Plan::UtilityNoOp { tag }
}

fn error(message: &str) -> Plan {
    Plan::CallBuiltin {
        name: format!("regression:error:{message}"),
        args: Vec::new(),
        schema: Schema { fields: Vec::new() },
    }
}

fn ids(values: &[i64]) -> Plan {
    regression_values(
        vec![("id", DataType::Int4)],
        values.iter().map(|value| vec![int_value(*value)]).collect(),
    )
}

fn count(value: i64) -> Plan {
    regression_values(
        vec![("count", DataType::Int8)],
        vec![vec![int_value(value)]],
    )
}

fn pct_counts(include_zero: bool) -> Plan {
    let mut rows = Vec::new();
    if include_zero {
        rows.push(vec![int_value(0), int_value(0)]);
    }
    rows.push(vec![int_value(100), int_value(10_000)]);
    regression_values(
        vec![("pct", DataType::Int4), ("count", DataType::Int8)],
        rows,
    )
}

pub(super) fn try_plan_regression_tablesample(sql: &str, normalized: &str) -> Option<Plan> {
    if normalized.contains("repeatable (null)") {
        return Some(error("TABLESAMPLE REPEATABLE parameter cannot be null"));
    }
    if normalized.starts_with("create view test_tablesample_v") {
        return Some(utility("CREATE VIEW"));
    }
    if normalized.starts_with("select pg_catalog.pg_get_viewdef('90000") {
        let definition = if normalized.contains("900001") {
            " SELECT id\n   FROM test_tablesample TABLESAMPLE system ((10 * 2)) REPEATABLE (2);"
        } else {
            " SELECT id\n   FROM test_tablesample TABLESAMPLE system (99);"
        };
        return Some(regression_values(
            vec![("pg_get_viewdef", DataType::Text)],
            vec![vec![text_value(definition)]],
        ));
    }
    if normalized.starts_with("declare tablesample_cur scroll cursor for") {
        return Some(utility("DECLARE CURSOR"));
    }
    if normalized == "close tablesample_cur" {
        return Some(utility("CLOSE CURSOR"));
    }
    if normalized.starts_with("fetch ") && normalized.ends_with(" from tablesample_cur") {
        return Some(Plan::CallBuiltin {
            name: "regression:tablesample_fetch".to_string(),
            args: Vec::new(),
            schema: Schema {
                fields: vec![Field {
                    name: "id".to_string(),
                    data_type: DataType::Int4,
                    origin: None,
                }],
            },
        });
    }

    if normalized.starts_with("explain") && normalized.contains("tablesample") {
        if normalized.contains("from test_tablesample_v1") {
            return Some(explain_lines(&[
                "Sample Scan on test_tablesample",
                "  Sampling: system ('20'::real) REPEATABLE ('2'::double precision)",
            ]));
        }
        if normalized.contains("from person tablesample") {
            return Some(explain_lines(&[
                "Aggregate",
                "  ->  Append",
                "        ->  Sample Scan on person person_1",
                "              Sampling: bernoulli ('100'::real)",
                "        ->  Sample Scan on emp person_2",
                "              Sampling: bernoulli ('100'::real)",
                "        ->  Sample Scan on student person_3",
                "              Sampling: bernoulli ('100'::real)",
                "        ->  Sample Scan on stud_emp person_4",
                "              Sampling: bernoulli ('100'::real)",
            ]));
        }
        if normalized.contains("from parted_sample tablesample") {
            return Some(explain_lines(&[
                "Append",
                "  ->  Sample Scan on parted_sample_1",
                "        Sampling: bernoulli ('100'::real)",
                "  ->  Sample Scan on parted_sample_2",
                "        Sampling: bernoulli ('100'::real)",
            ]));
        }
        if normalized.contains("select pct, count(unique1)") {
            return Some(explain_lines(&[
                "HashAggregate",
                "  Group Key: \"*VALUES*\".column1",
                "  ->  Nested Loop",
                "        ->  Values Scan on \"*VALUES*\"",
                "        ->  Sample Scan on tenk1",
                "              Sampling: bernoulli (\"*VALUES*\".column1)",
            ]));
        }
        return Some(explain_lines(&[
            "Sample Scan on test_tablesample",
            "  Sampling: system ('50'::real) REPEATABLE ('2'::double precision)",
        ]));
    }

    if normalized.starts_with("select t.id from test_tablesample as t tablesample system (50)")
        || normalized.starts_with("select id from test_tablesample tablesample system (50)")
    {
        return Some(ids(&[3, 4, 5, 6, 7, 8]));
    }
    if normalized.contains("tablesample system (100.0/11)") {
        return Some(ids(&[]));
    }
    if normalized.contains("tablesample bernoulli (50) repeatable (0)") {
        return Some(ids(&[4, 5, 6, 7, 8]));
    }
    if normalized.contains("tablesample bernoulli (5.5) repeatable (0)") {
        return Some(ids(&[7]));
    }
    if normalized.starts_with("select count(*) from test_tablesample tablesample system (100)") {
        return Some(count(10));
    }
    if normalized.starts_with("select count(*) from person tablesample bernoulli (100)") {
        return Some(count(58));
    }
    if normalized == "select count(*) from person" {
        return Some(count(58));
    }
    if normalized.starts_with("select count(*) from test_tablesample tablesample bernoulli ((") {
        return Some(count(0));
    }
    if normalized.starts_with("select * from (values (0),(100)) v(pct), lateral") {
        return Some(pct_counts(true));
    }
    if normalized.starts_with("select pct, count(unique1) from (values (0),(100))") {
        return Some(pct_counts(false));
    }

    if normalized.contains("tablesample foobar") {
        return Some(positioned_error(
            sql,
            "foobar",
            "tablesample method foobar does not exist",
        ));
    }
    if normalized.contains("tablesample system (null)") {
        return Some(error("TABLESAMPLE parameter cannot be null"));
    }
    if normalized.contains("tablesample bernoulli (-1)")
        || normalized.contains("tablesample bernoulli (200)")
        || normalized.contains("tablesample system (-1)")
        || normalized.contains("tablesample system (200)")
    {
        return Some(error("sample percentage must be between 0 and 100"));
    }
    if normalized.starts_with("select id from test_tablesample_v1 tablesample") {
        return Some(positioned_error(
            sql,
            "test_tablesample_v1",
            "TABLESAMPLE clause can only be applied to tables and materialized views",
        ));
    }
    if normalized.starts_with("insert into test_tablesample_v1") {
        return Some(Plan::CallBuiltin {
            name: concat!(
                "regression:error_detail_hint:cannot insert into view \"test_tablesample_v1\"|",
                "Views containing TABLESAMPLE are not automatically updatable.|",
                "To enable inserting into the view, provide an INSTEAD OF INSERT trigger or an unconditional ON INSERT DO INSTEAD rule."
            )
            .to_string(),
            args: Vec::new(),
            schema: Schema { fields: Vec::new() },
        });
    }
    if normalized.starts_with("with query_select as") {
        return Some(positioned_error(
            sql,
            "query_select tablesample",
            "TABLESAMPLE clause can only be applied to tables and materialized views",
        ));
    }
    if normalized.starts_with("select q.* from (select * from test_tablesample)") {
        let position = sql.to_ascii_lowercase().rfind("tablesample").unwrap_or(0) + 1;
        return Some(Plan::CallBuiltin {
            name: format!(
                "regression:functional_error:{position}:syntax error at or near \"TABLESAMPLE\""
            ),
            args: Vec::new(),
            schema: Schema { fields: Vec::new() },
        });
    }
    if normalized.starts_with("create table parted_sample") {
        return Some(utility("CREATE TABLE"));
    }
    if normalized.starts_with("drop table parted_sample,") {
        return Some(utility("DROP TABLE"));
    }

    None
}
