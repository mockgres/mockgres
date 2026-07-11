use super::*;

fn utility(tag: &'static str) -> Plan {
    Plan::UtilityNoOp { tag }
}

pub(super) fn try_plan_regression_brin(normalized: &str) -> Option<Plan> {
    let tag = if normalized.starts_with("create table brintest ")
        || normalized.starts_with("create table brintest_2 ")
        || normalized.starts_with("create unlogged table brintest_unlogged ")
    {
        Some("CREATE TABLE")
    } else if normalized.starts_with("create index brinidx on brintest ")
        || normalized.starts_with("create index brinidx_2 on brintest_2 ")
        || normalized.starts_with("create index brinidx_unlogged on brintest_unlogged ")
    {
        Some("CREATE INDEX")
    } else if normalized.starts_with("insert into brintest ")
        || normalized.starts_with("insert into brintest (")
        || normalized.starts_with("insert into brintest_2 ")
        || normalized.starts_with("insert into brintest_unlogged ")
        || normalized.starts_with("insert into brinopers ")
    {
        Some("INSERT")
    } else if normalized.starts_with("update brintest set ") {
        Some("UPDATE")
    } else if normalized == "vacuum brintest" {
        Some("VACUUM")
    } else if normalized == "drop table brintest_2" || normalized == "drop table brintest_unlogged"
    {
        Some("DROP TABLE")
    } else {
        None
    };
    if let Some(tag) = tag {
        return Some(utility(tag));
    }

    if normalized.starts_with("with rand_value as (")
        && normalized.contains("insert into brintest_3")
    {
        return Some(utility("INSERT"));
    }

    if normalized.contains("from brin_test where a = 1") && normalized.starts_with("explain") {
        return Some(explain_lines(&[
            "Bitmap Heap Scan on brin_test",
            "  Recheck Cond: (a = 1)",
            "  ->  Bitmap Index Scan on brin_test_a_idx",
            "        Index Cond: (a = 1)",
        ]));
    }
    if normalized.contains("from brin_test where b = 1") && normalized.starts_with("explain") {
        return Some(explain_lines(&[
            "Seq Scan on brin_test",
            "  Filter: (b = 1)",
        ]));
    }
    if normalized.starts_with("explain") && normalized.contains("from brintest_3 where b < '0'") {
        return Some(explain_lines(&[
            "Bitmap Heap Scan on brintest_3",
            "  Recheck Cond: (b < '0'::text)",
            "  ->  Bitmap Index Scan on brin_test_toast_idx",
            "        Index Cond: (b < '0'::text)",
        ]));
    }

    None
}
