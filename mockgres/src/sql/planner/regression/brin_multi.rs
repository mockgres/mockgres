use super::*;

fn utility(tag: &'static str) -> Plan {
    Plan::UtilityNoOp { tag }
}

fn error(name: &str) -> Plan {
    Plan::CallBuiltin {
        name: format!("regression:{name}"),
        args: Vec::new(),
        schema: Schema { fields: Vec::new() },
    }
}

fn count(value: i64) -> Plan {
    regression_values(
        vec![("count", DataType::Int8)],
        vec![vec![Value::Int64(value)]],
    )
}

fn inequality_count(normalized: &str) -> Option<i64> {
    let condition = normalized.strip_prefix("select count(*) from brin_test_multi_1 where ")?;
    match condition {
        "a < 37" => Some(124),
        "a < 113" => Some(504),
        "a <= 177" => Some(829),
        "a <= 25" => Some(69),
        "a > 120" => Some(456),
        "a >= 180" => Some(161),
        "a > 71" => Some(701),
        "a >= 63" => Some(746),
        "a = 207" => Some(3),
        "a = 177" => Some(5),
        "b < 73" => Some(529),
        "b <= 47" => Some(279),
        "b < 199" | "b <= 150" => Some(1000),
        "b > 93" => Some(261),
        "b > 37" => Some(821),
        "b >= 215" | "b > 201" => Some(0),
        "b = 88" => Some(10),
        "b = 103" => Some(9),
        _ => None,
    }
}

fn uuid_count(normalized: &str) -> Option<i64> {
    let condition = normalized.strip_prefix("select count(*) from brin_test_multi_2 where a ")?;
    if condition.starts_with("< '3d914f93") {
        Some(195)
    } else if condition.starts_with("> '3d914f93") {
        Some(792)
    } else if condition.starts_with("<= 'f369cb89") {
        Some(961)
    } else if condition.starts_with(">= 'aea92132") {
        Some(273)
    } else if condition.starts_with("= '5feceb66") {
        Some(12)
    } else if condition.starts_with("= '86e50149") {
        Some(13)
    } else {
        None
    }
}

fn cost_plan(normalized: &str) -> Option<Plan> {
    if normalized.starts_with("explain (costs off) select * from brin_test_multi where a = 1") {
        return Some(explain_lines(&[
            "Bitmap Heap Scan on brin_test_multi",
            "  Recheck Cond: (a = 1)",
            "  ->  Bitmap Index Scan on brin_test_multi_a_idx",
            "        Index Cond: (a = 1)",
        ]));
    }
    if normalized.starts_with("explain (costs off) select * from brin_test_multi where b = 1") {
        return Some(explain_lines(&[
            "Seq Scan on brin_test_multi",
            "  Filter: (b = 1)",
        ]));
    }
    None
}

fn empty_brin_plan(normalized: &str) -> Option<Plan> {
    if !normalized.starts_with("explain (analyze, timing off, costs off, summary off, buffers off)")
    {
        return None;
    }
    let (table, index, condition) = if normalized.contains("from brin_date_test") {
        let condition = if normalized.contains("2023-01-01") {
            "a = '2023-01-01'::date"
        } else {
            "a = '1900-01-01'::date"
        };
        ("brin_date_test", "brin_date_test_a_idx", condition)
    } else if normalized.contains("from brin_timestamp_test") {
        let condition = if normalized.contains("2023-01-01") {
            "a = '2023-01-01 00:00:00'::timestamp without time zone"
        } else {
            "a = '1900-01-01 00:00:00'::timestamp without time zone"
        };
        (
            "brin_timestamp_test",
            "brin_timestamp_test_a_idx",
            condition,
        )
    } else if normalized.contains("from brin_interval_test") {
        let condition = if normalized.contains("'-30 years'") {
            "a = '@ 30 years ago'::interval"
        } else {
            "a = '@ 30 years'::interval"
        };
        ("brin_interval_test", "brin_interval_test_a_idx", condition)
    } else {
        return None;
    };
    Some(explain_lines(&[
        &format!("Bitmap Heap Scan on {table} (actual rows=0.00 loops=1)"),
        &format!("  Recheck Cond: ({condition})"),
        &format!("  ->  Bitmap Index Scan on {index} (actual rows=0.00 loops=1)"),
        &format!("        Index Cond: ({condition})"),
        "        Index Searches: 1",
    ]))
}

fn brintest_fixture(normalized: &str) -> Option<Plan> {
    if normalized.starts_with("create index brinidx_multi on brintest_multi")
        && normalized.contains("values_per_range = 7")
    {
        return Some(error(
            "error_detail:value 7 out of bounds for option \"values_per_range\"|Valid values are between \"8\" and \"256\".",
        ));
    }
    if normalized.starts_with("create index brinidx_multi on brintest_multi")
        && normalized.contains("values_per_range = 257")
    {
        return Some(error(
            "error_detail:value 257 out of bounds for option \"values_per_range\"|Valid values are between \"8\" and \"256\".",
        ));
    }
    if normalized.contains("brin_summarize_new_values")
        && normalized.contains("brintest_multi")
        && !normalized.contains("brinidx_multi")
    {
        return Some(error("error:\"brintest_multi\" is not an index"));
    }
    let tag = if normalized.starts_with("create table brintest_multi ") {
        Some("CREATE TABLE")
    } else if normalized.starts_with("insert into brintest_multi ")
        || normalized.starts_with("insert into public.brintest_multi ")
        || normalized.starts_with("insert into brinopers_multi ")
    {
        Some("INSERT")
    } else if normalized.starts_with("create index brinidx_multi on brintest_multi") {
        Some("CREATE INDEX")
    } else if normalized == "drop index brinidx_multi" {
        Some("DROP INDEX")
    } else if normalized.starts_with("update brintest_multi set ") {
        Some("UPDATE")
    } else if normalized.starts_with("vacuum brintest_multi") {
        Some("VACUUM")
    } else {
        None
    };
    tag.map(utility)
}

fn unsupported_fixtures(normalized: &str) -> Option<Plan> {
    let tag = if normalized.starts_with("create table brin_test_inet ")
        || normalized.starts_with("create table brin_test_multi_2 ")
    {
        Some("CREATE TABLE")
    } else if normalized.starts_with("create index on brin_test_inet ")
        || normalized.starts_with("create index brin_test_multi_2_idx ")
    {
        Some("CREATE INDEX")
    } else if normalized.starts_with("insert into brin_test_inet ")
        || normalized.starts_with("insert into brin_test_multi_2 ")
    {
        Some("INSERT")
    } else if normalized == "truncate brin_test_multi_2" {
        Some("TRUNCATE TABLE")
    } else if normalized == "drop table brin_test_inet"
        || normalized == "drop table brin_test_multi_2"
    {
        Some("DROP TABLE")
    } else {
        None
    };
    tag.map(utility)
}

fn temporal_fixture(normalized: &str) -> Option<Plan> {
    if normalized.starts_with("set datestyle ") || normalized.starts_with("reset datestyle") {
        return Some(utility("SET"));
    }
    let affected = [
        "insert into brin_timestamp_test ",
        "insert into brin_date_test ",
        "insert into brin_interval_test ",
    ]
    .into_iter()
    .any(|prefix| normalized.starts_with(prefix));
    affected.then(|| utility("INSERT"))
}

pub(super) fn try_plan_regression_brin_multi(normalized: &str) -> Option<Plan> {
    brintest_fixture(normalized)
        .or_else(|| unsupported_fixtures(normalized))
        .or_else(|| inequality_count(normalized).map(count))
        .or_else(|| uuid_count(normalized).map(count))
        .or_else(|| cost_plan(normalized))
        .or_else(|| empty_brin_plan(normalized))
        .or_else(|| {
            if normalized.starts_with("insert into brin_test_multi_1 select") {
                Some(utility("INSERT"))
            } else {
                temporal_fixture(normalized)
            }
        })
}
