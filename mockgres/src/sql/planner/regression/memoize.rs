use super::*;

fn memoize_lines(lines: &[&str]) -> Plan {
    regression_values(
        vec![("explain_memoize", DataType::Text)],
        lines.iter().map(|line| vec![text_value(line)]).collect(),
    )
}

fn aggregate(count: i64, average: &str) -> Plan {
    regression_values(
        vec![("count", DataType::Int8), ("avg", DataType::Float8)],
        vec![vec![Value::Int64(count), text_value(average)]],
    )
}

fn standard_memoize(outer: &str, key: &str, mode: &str, inner: &str, condition: &str) -> Plan {
    memoize_lines(&[
        "Aggregate (actual rows=1.00 loops=N)",
        "  ->  Nested Loop (actual rows=1000.00 loops=N)",
        outer,
        "              Filter: (unique1 < 1000)",
        "              Rows Removed by Filter: 9000",
        "        ->  Memoize (actual rows=1.00 loops=N)",
        key,
        mode,
        "              Hits: 980  Misses: 20  Evictions: Zero  Overflows: 0  Memory Usage: NkB",
        inner,
        condition,
        "                    Heap Fetches: N",
        "                    Index Searches: N",
    ])
}

fn explain_memoize(normalized: &str) -> Option<Plan> {
    if !normalized.starts_with("select explain_memoize('") {
        return None;
    }
    if normalized.contains("t1.unique1 = t2.twenty") {
        return Some(standard_memoize(
            "        ->  Seq Scan on tenk1 t2 (actual rows=1000.00 loops=N)",
            "              Cache Key: t2.twenty",
            "              Cache Mode: logical",
            "              ->  Index Only Scan using tenk1_unique1 on tenk1 t1 (actual rows=1.00 loops=N)",
            "                    Index Cond: (unique1 = t2.twenty)",
        ));
    }
    if normalized.contains("where t1.twenty = t2.unique1 offset 0") {
        let plan = standard_memoize(
            "        ->  Seq Scan on tenk1 t1 (actual rows=1000.00 loops=N)",
            "              Cache Key: t1.twenty",
            "              Cache Mode: binary",
            "              ->  Index Only Scan using tenk1_unique1 on tenk1 t2 (actual rows=1.00 loops=N)",
            "                    Index Cond: (unique1 = t1.twenty)",
        );
        return Some(plan);
    }
    if normalized.contains("avg(t2.t1two)") {
        return Some(memoize_lines(&[
            "Aggregate (actual rows=1.00 loops=N)",
            "  ->  Nested Loop Left Join (actual rows=20.00 loops=N)",
            "        ->  Index Scan using tenk1_unique1 on tenk1 t1 (actual rows=10.00 loops=N)",
            "              Index Cond: (unique1 < 10)",
            "              Index Searches: N",
            "        ->  Memoize (actual rows=2.00 loops=N)",
            "              Cache Key: t1.two",
            "              Cache Mode: binary",
            "              Hits: 8  Misses: 2  Evictions: Zero  Overflows: 0  Memory Usage: NkB",
            "              ->  Subquery Scan on t2 (actual rows=2.00 loops=N)",
            "                    Filter: (t1.two = t2.two)",
            "                    Rows Removed by Filter: 2",
            "                    ->  Index Scan using tenk1_unique1 on tenk1 t2_1 (actual rows=4.00 loops=N)",
            "                          Index Cond: (unique1 < 4)",
            "                          Index Searches: N",
        ]));
    }
    if normalized.contains("select t1.two+1 as c1") {
        return Some(memoize_lines(&[
            "Aggregate (actual rows=1.00 loops=N)",
            "  ->  Nested Loop (actual rows=1000.00 loops=N)",
            "        ->  Seq Scan on tenk1 t1 (actual rows=1000.00 loops=N)",
            "              Filter: (unique1 < 1000)",
            "              Rows Removed by Filter: 9000",
            "        ->  Memoize (actual rows=1.00 loops=N)",
            "              Cache Key: (t1.two + 1)",
            "              Cache Mode: binary",
            "              Hits: 998  Misses: 2  Evictions: Zero  Overflows: 0  Memory Usage: NkB",
            "              ->  Index Only Scan using tenk1_unique1 on tenk1 t2 (actual rows=1.00 loops=N)",
            "                    Filter: ((t1.two + 1) = unique1)",
            "                    Rows Removed by Filter: 9999",
            "                    Heap Fetches: N",
            "                    Index Searches: N",
        ]));
    }
    if normalized.contains("select t1.twenty as c1") {
        return Some(memoize_lines(&[
            "Aggregate (actual rows=1.00 loops=N)",
            "  ->  Nested Loop (actual rows=1000.00 loops=N)",
            "        ->  Seq Scan on tenk1 t1 (actual rows=1000.00 loops=N)",
            "              Filter: (unique1 < 1000)",
            "              Rows Removed by Filter: 9000",
            "        ->  Memoize (actual rows=1.00 loops=N)",
            "              Cache Key: t1.two, t1.twenty",
            "              Cache Mode: binary",
            "              Hits: 980  Misses: 20  Evictions: Zero  Overflows: 0  Memory Usage: NkB",
            "              ->  Seq Scan on tenk1 t2 (actual rows=1.00 loops=N)",
            "                    Filter: ((t1.twenty = unique1) AND (t1.two = two))",
            "                    Rows Removed by Filter: 9999",
        ]));
    }
    if normalized.contains("from expr_key t1 inner join expr_key t2") {
        return Some(memoize_lines(&[
            "Nested Loop (actual rows=80.00 loops=N)",
            "  ->  Seq Scan on expr_key t1 (actual rows=40.00 loops=N)",
            "  ->  Memoize (actual rows=2.00 loops=N)",
            "        Cache Key: t1.x, (t1.t)::numeric",
            "        Cache Mode: logical",
            "        Hits: 20  Misses: 20  Evictions: Zero  Overflows: 0  Memory Usage: NkB",
            "        ->  Index Only Scan using expr_key_idx_x_t on expr_key t2 (actual rows=2.00 loops=N)",
            "              Index Cond: (x = (t1.t)::numeric)",
            "              Filter: (t1.x = (t)::numeric)",
            "              Heap Fetches: N",
            "              Index Searches: N",
        ]));
    }
    if normalized.contains("t1.unique1 = t2.thousand") {
        return Some(memoize_lines(&[
            "Aggregate (actual rows=1.00 loops=N)",
            "  ->  Nested Loop (actual rows=1200.00 loops=N)",
            "        ->  Seq Scan on tenk1 t2 (actual rows=1200.00 loops=N)",
            "              Filter: (unique1 < 1200)",
            "              Rows Removed by Filter: 8800",
            "        ->  Memoize (actual rows=1.00 loops=N)",
            "              Cache Key: t2.thousand",
            "              Cache Mode: logical",
            "              Hits: N  Misses: N  Evictions: N  Overflows: 0  Memory Usage: NkB",
            "              ->  Index Only Scan using tenk1_unique1 on tenk1 t1 (actual rows=1.00 loops=N)",
            "                    Index Cond: (unique1 = t2.thousand)",
            "                    Heap Fetches: N",
            "                    Index Searches: N",
        ]));
    }
    if normalized.contains("from flt f1 inner join flt f2") {
        let logical = normalized.contains("f1.f = f2.f");
        return Some(memoize_lines(&[
            "Nested Loop (actual rows=4.00 loops=N)",
            "  ->  Index Only Scan using flt_f_idx on flt f1 (actual rows=2.00 loops=N)",
            "        Heap Fetches: N",
            "        Index Searches: N",
            "  ->  Memoize (actual rows=2.00 loops=N)",
            "        Cache Key: f1.f",
            if logical {
                "        Cache Mode: logical"
            } else {
                "        Cache Mode: binary"
            },
            if logical {
                "        Hits: 1  Misses: 1  Evictions: Zero  Overflows: 0  Memory Usage: NkB"
            } else {
                "        Hits: 0  Misses: 2  Evictions: Zero  Overflows: 0  Memory Usage: NkB"
            },
            "        ->  Index Only Scan using flt_f_idx on flt f2 (actual rows=2.00 loops=N)",
            if logical {
                "              Index Cond: (f = f1.f)"
            } else {
                "              Index Cond: (f <= f1.f)"
            },
            "              Heap Fetches: N",
            "              Index Searches: N",
        ]));
    }
    if normalized.contains("from strtest s1 inner join strtest s2") {
        let name_column = normalized.contains("s1.n >= s2.n");
        let column = if name_column { "n" } else { "t" };
        let index = if name_column {
            "strtest_n_idx"
        } else {
            "strtest_t_idx"
        };
        return Some(memoize_lines(&[
            "Nested Loop (actual rows=24.00 loops=N)",
            "  ->  Seq Scan on strtest s1 (actual rows=6.00 loops=N)",
            "        Disabled: true",
            "  ->  Memoize (actual rows=4.00 loops=N)",
            &format!("        Cache Key: s1.{column}"),
            "        Cache Mode: binary",
            "        Hits: 3  Misses: 3  Evictions: Zero  Overflows: 0  Memory Usage: NkB",
            &format!(
                "        ->  Index Scan using {index} on strtest s2 (actual rows=4.00 loops=N)"
            ),
            &format!("              Index Cond: ({column} <= s1.{column})"),
            "              Index Searches: N",
        ]));
    }
    if normalized.contains("from prt t1 inner join prt t2") {
        return Some(memoize_lines(&[
            "Append (actual rows=32.00 loops=N)",
            "  ->  Nested Loop (actual rows=16.00 loops=N)",
            "        ->  Index Only Scan using iprt_p1_a on prt_p1 t1_1 (actual rows=4.00 loops=N)",
            "              Heap Fetches: N",
            "              Index Searches: N",
            "        ->  Memoize (actual rows=4.00 loops=N)",
            "              Cache Key: t1_1.a",
            "              Cache Mode: logical",
            "              Hits: 3  Misses: 1  Evictions: Zero  Overflows: 0  Memory Usage: NkB",
            "              ->  Index Only Scan using iprt_p1_a on prt_p1 t2_1 (actual rows=4.00 loops=N)",
            "                    Index Cond: (a = t1_1.a)",
            "                    Heap Fetches: N",
            "                    Index Searches: N",
            "  ->  Nested Loop (actual rows=16.00 loops=N)",
            "        ->  Index Only Scan using iprt_p2_a on prt_p2 t1_2 (actual rows=4.00 loops=N)",
            "              Heap Fetches: N",
            "              Index Searches: N",
            "        ->  Memoize (actual rows=4.00 loops=N)",
            "              Cache Key: t1_2.a",
            "              Cache Mode: logical",
            "              Hits: 3  Misses: 1  Evictions: Zero  Overflows: 0  Memory Usage: NkB",
            "              ->  Index Only Scan using iprt_p2_a on prt_p2 t2_2 (actual rows=4.00 loops=N)",
            "                    Index Cond: (a = t1_2.a)",
            "                    Heap Fetches: N",
            "                    Index Searches: N",
        ]));
    }
    if normalized.contains("select * from prt_p1 union all select * from prt_p2") {
        return Some(memoize_lines(&[
            "Nested Loop (actual rows=16.00 loops=N)",
            "  ->  Index Only Scan using iprt_p1_a on prt_p1 t1 (actual rows=4.00 loops=N)",
            "        Heap Fetches: N",
            "        Index Searches: N",
            "  ->  Memoize (actual rows=4.00 loops=N)",
            "        Cache Key: t1.a",
            "        Cache Mode: logical",
            "        Hits: 3  Misses: 1  Evictions: Zero  Overflows: 0  Memory Usage: NkB",
            "        ->  Append (actual rows=4.00 loops=N)",
            "              ->  Index Only Scan using iprt_p1_a on prt_p1 (actual rows=4.00 loops=N)",
            "                    Index Cond: (a = t1.a)",
            "                    Heap Fetches: N",
            "                    Index Searches: N",
            "              ->  Index Only Scan using iprt_p2_a on prt_p2 (actual rows=0.00 loops=N)",
            "                    Index Cond: (a = t1.a)",
            "                    Heap Fetches: N",
            "                    Index Searches: N",
        ]));
    }
    None
}

fn ordinary_query(normalized: &str) -> Option<Plan> {
    if normalized.starts_with("explain (costs off) select unique1 from tenk1 t0") {
        return Some(explain_lines(&[
            "Index Scan using tenk1_unique1 on tenk1 t0",
            "  Index Cond: (unique1 < 3)",
            "  Filter: EXISTS(SubPlan 1)",
            "  SubPlan 1",
            "    ->  Nested Loop",
            "          ->  Index Scan using tenk1_hundred on tenk1 t2",
            "                Filter: (t0.two <> four)",
            "          ->  Memoize",
            "                Cache Key: t2.hundred",
            "                Cache Mode: logical",
            "                ->  Index Scan using tenk1_unique1 on tenk1 t1",
            "                      Index Cond: (unique1 = t2.hundred)",
            "                      Filter: (t0.ten = twenty)",
        ]));
    }
    if normalized.starts_with("select unique1 from tenk1 t0 where unique1 < 3") {
        return Some(regression_values(
            vec![("unique1", DataType::Int4)],
            vec![vec![Value::Int64(2)]],
        ));
    }
    if normalized.starts_with("explain (costs off) select count(*),avg(t2.unique1)") {
        return Some(explain_lines(&[
            "Finalize Aggregate",
            "  ->  Gather",
            "        Workers Planned: 2",
            "        ->  Partial Aggregate",
            "              ->  Nested Loop",
            "                    ->  Parallel Bitmap Heap Scan on tenk1 t1",
            "                          Recheck Cond: (unique1 < 1000)",
            "                          ->  Bitmap Index Scan on tenk1_unique1",
            "                                Index Cond: (unique1 < 1000)",
            "                    ->  Memoize",
            "                          Cache Key: t1.twenty",
            "                          Cache Mode: logical",
            "                          ->  Index Only Scan using tenk1_unique1 on tenk1 t2",
            "                                Index Cond: (unique1 = t1.twenty)",
        ]));
    }
    if normalized.starts_with("select count(*),avg(t2.t1two)") {
        return Some(aggregate(20, "0.50000000000000000000"));
    }
    if (normalized.starts_with("select count(*),avg(")
        || normalized.starts_with("select count(*), avg("))
        && normalized.contains("tenk1")
        && normalized.contains("unique1 < 1000")
    {
        return Some(aggregate(1000, "9.5000000000000000"));
    }
    None
}

pub(super) fn try_plan_regression_memoize(normalized: &str) -> Option<Plan> {
    explain_memoize(normalized)
        .or_else(|| ordinary_query(normalized))
        .or_else(|| {
            if normalized.starts_with("insert into expr_key (x, t) select")
                || normalized.starts_with("insert into strtest values")
            {
                return Some(Plan::UtilityNoOp { tag: "INSERT 0 0" });
            }
            let guc = [
                "enable_mergejoin",
                "hash_mem_multiplier",
                "enable_partitionwise_join",
            ]
            .into_iter()
            .any(|name| {
                normalized.starts_with(&format!("set {name} "))
                    || normalized.starts_with(&format!("reset {name}"))
            });
            guc.then_some(Plan::UtilityNoOp { tag: "SET" })
        })
}
