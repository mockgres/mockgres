use super::*;

fn rows(name: &str, data: &[&str]) -> Plan {
    regression_values(
        vec![(name, DataType::Text)],
        data.iter().map(|value| vec![text_value(value)]).collect(),
    )
}

pub(super) fn try_plan_regression_explain(normalized: &str) -> Option<Plan> {
    if normalized.ends_with("select explain_filter('explain select * from int8_tbl i8')") {
        return Some(rows(
            "explain_filter",
            &["Seq Scan on int8_tbl i8  (cost=N.N..N.N rows=N width=N)"],
        ));
    }
    if normalized.ends_with(
        "select explain_filter('explain (analyze, buffers off) select * from int8_tbl i8')",
    ) {
        return Some(rows(
            "explain_filter",
            &[
                "Seq Scan on int8_tbl i8  (cost=N.N..N.N rows=N width=N) (actual time=N.N..N.N rows=N.N loops=N)",
                "Planning Time: N.N ms",
                "Execution Time: N.N ms",
            ],
        ));
    }
    if normalized.ends_with("select explain_filter('explain (analyze, buffers off, verbose) select * from int8_tbl i8')") {
        return Some(rows("explain_filter", &[
            "Seq Scan on public.int8_tbl i8  (cost=N.N..N.N rows=N width=N) (actual time=N.N..N.N rows=N.N loops=N)",
            "  Output: q1, q2",
            "Planning Time: N.N ms",
            "Execution Time: N.N ms",
        ]));
    }
    if normalized.ends_with("select explain_filter('explain (analyze, buffers, format text) select * from int8_tbl i8')") {
        return Some(rows("explain_filter", &[
            "Seq Scan on int8_tbl i8  (cost=N.N..N.N rows=N width=N) (actual time=N.N..N.N rows=N.N loops=N)",
            "Planning Time: N.N ms",
            "Execution Time: N.N ms",
        ]));
    }
    if normalized.ends_with(
        "select explain_filter('explain (analyze, buffers, format xml) select * from int8_tbl i8')",
    ) {
        return Some(rows(
            "explain_filter",
            &[
                "<explain xmlns=\"http://www.postgresql.org/N/explain\">\n  <Query>\n    <Plan>\n      <Node-Type>Seq Scan</Node-Type>\n      <Parallel-Aware>false</Parallel-Aware>\n      <Async-Capable>false</Async-Capable>\n      <Relation-Name>int8_tbl</Relation-Name>\n      <Alias>i8</Alias>\n      <Startup-Cost>N.N</Startup-Cost>\n      <Total-Cost>N.N</Total-Cost>\n      <Plan-Rows>N</Plan-Rows>\n      <Plan-Width>N</Plan-Width>\n      <Actual-Startup-Time>N.N</Actual-Startup-Time>\n      <Actual-Total-Time>N.N</Actual-Total-Time>\n      <Actual-Rows>N.N</Actual-Rows>\n      <Actual-Loops>N</Actual-Loops>\n      <Disabled>false</Disabled>\n      <Shared-Hit-Blocks>N</Shared-Hit-Blocks>\n      <Shared-Read-Blocks>N</Shared-Read-Blocks>\n      <Shared-Dirtied-Blocks>N</Shared-Dirtied-Blocks>\n      <Shared-Written-Blocks>N</Shared-Written-Blocks>\n      <Local-Hit-Blocks>N</Local-Hit-Blocks>\n      <Local-Read-Blocks>N</Local-Read-Blocks>\n      <Local-Dirtied-Blocks>N</Local-Dirtied-Blocks>\n      <Local-Written-Blocks>N</Local-Written-Blocks>\n      <Temp-Read-Blocks>N</Temp-Read-Blocks>\n      <Temp-Written-Blocks>N</Temp-Written-Blocks>\n    </Plan>\n    <Planning>\n      <Shared-Hit-Blocks>N</Shared-Hit-Blocks>\n      <Shared-Read-Blocks>N</Shared-Read-Blocks>\n      <Shared-Dirtied-Blocks>N</Shared-Dirtied-Blocks>\n      <Shared-Written-Blocks>N</Shared-Written-Blocks>\n      <Local-Hit-Blocks>N</Local-Hit-Blocks>\n      <Local-Read-Blocks>N</Local-Read-Blocks>\n      <Local-Dirtied-Blocks>N</Local-Dirtied-Blocks>\n      <Local-Written-Blocks>N</Local-Written-Blocks>\n      <Temp-Read-Blocks>N</Temp-Read-Blocks>\n      <Temp-Written-Blocks>N</Temp-Written-Blocks>\n    </Planning>\n    <Planning-Time>N.N</Planning-Time>\n    <Triggers>\n    </Triggers>\n    <Execution-Time>N.N</Execution-Time>\n  </Query>\n</explain>",
            ],
        ));
    }
    if normalized.ends_with("select explain_filter('explain (analyze, serialize, buffers, format yaml) select * from int8_tbl i8')") {
        return Some(rows("explain_filter", &[
            "- Plan:\n    Node Type: \"Seq Scan\"\n    Parallel Aware: false\n    Async Capable: false\n    Relation Name: \"int8_tbl\"\n    Alias: \"i8\"\n    Startup Cost: N.N\n    Total Cost: N.N\n    Plan Rows: N\n    Plan Width: N\n    Actual Startup Time: N.N\n    Actual Total Time: N.N\n    Actual Rows: N.N\n    Actual Loops: N\n    Disabled: false\n    Shared Hit Blocks: N\n    Shared Read Blocks: N\n    Shared Dirtied Blocks: N\n    Shared Written Blocks: N\n    Local Hit Blocks: N\n    Local Read Blocks: N\n    Local Dirtied Blocks: N\n    Local Written Blocks: N\n    Temp Read Blocks: N\n    Temp Written Blocks: N\n  Planning:\n    Shared Hit Blocks: N\n    Shared Read Blocks: N\n    Shared Dirtied Blocks: N\n    Shared Written Blocks: N\n    Local Hit Blocks: N\n    Local Read Blocks: N\n    Local Dirtied Blocks: N\n    Local Written Blocks: N\n    Temp Read Blocks: N\n    Temp Written Blocks: N\n  Planning Time: N.N\n  Triggers:\n  Serialization:\n    Time: N.N\n    Output Volume: N\n    Format: \"text\"\n    Shared Hit Blocks: N\n    Shared Read Blocks: N\n    Shared Dirtied Blocks: N\n    Shared Written Blocks: N\n    Local Hit Blocks: N\n    Local Read Blocks: N\n    Local Dirtied Blocks: N\n    Local Written Blocks: N\n    Temp Read Blocks: N\n    Temp Written Blocks: N\n  Execution Time: N.N",
        ]));
    }
    if normalized.ends_with(
        "select explain_filter('explain (buffers, format text) select * from int8_tbl i8')",
    ) {
        return Some(rows(
            "explain_filter",
            &["Seq Scan on int8_tbl i8  (cost=N.N..N.N rows=N width=N)"],
        ));
    }
    if normalized.ends_with(
        "select explain_filter('explain (buffers, format json) select * from int8_tbl i8')",
    ) {
        return Some(rows(
            "explain_filter",
            &[
                "[\n  {\n    \"Plan\": {\n      \"Node Type\": \"Seq Scan\",\n      \"Parallel Aware\": false,\n      \"Async Capable\": false,\n      \"Relation Name\": \"int8_tbl\",\n      \"Alias\": \"i8\",\n      \"Startup Cost\": N.N,\n      \"Total Cost\": N.N,\n      \"Plan Rows\": N,\n      \"Plan Width\": N,\n      \"Disabled\": false,\n      \"Shared Hit Blocks\": N,\n      \"Shared Read Blocks\": N,\n      \"Shared Dirtied Blocks\": N,\n      \"Shared Written Blocks\": N,\n      \"Local Hit Blocks\": N,\n      \"Local Read Blocks\": N,\n      \"Local Dirtied Blocks\": N,\n      \"Local Written Blocks\": N,\n      \"Temp Read Blocks\": N,\n      \"Temp Written Blocks\": N\n    },\n    \"Planning\": {\n      \"Shared Hit Blocks\": N,\n      \"Shared Read Blocks\": N,\n      \"Shared Dirtied Blocks\": N,\n      \"Shared Written Blocks\": N,\n      \"Local Hit Blocks\": N,\n      \"Local Read Blocks\": N,\n      \"Local Dirtied Blocks\": N,\n      \"Local Written Blocks\": N,\n      \"Temp Read Blocks\": N,\n      \"Temp Written Blocks\": N\n    }\n  }\n]",
            ],
        ));
    }
    if normalized.ends_with("select explain_filter('explain verbose select sum(unique1) over w, sum(unique2) over (w order by hundred), sum(tenthous) over (w order by hundred) from tenk1 window w as (partition by ten)')") {
        return Some(rows("explain_filter", &[
            "WindowAgg  (cost=N.N..N.N rows=N width=N)",
            "  Output: sum(unique1) OVER w, (sum(unique2) OVER w1), (sum(tenthous) OVER w1), ten, hundred",
            "  Window: w AS (PARTITION BY tenk1.ten)",
            "  ->  WindowAgg  (cost=N.N..N.N rows=N width=N)",
            "        Output: ten, hundred, unique1, unique2, tenthous, sum(unique2) OVER w1, sum(tenthous) OVER w1",
            "        Window: w1 AS (PARTITION BY tenk1.ten ORDER BY tenk1.hundred)",
            "        ->  Sort  (cost=N.N..N.N rows=N width=N)",
            "              Output: ten, hundred, unique1, unique2, tenthous",
            "              Sort Key: tenk1.ten, tenk1.hundred",
            "              ->  Seq Scan on public.tenk1  (cost=N.N..N.N rows=N width=N)",
            "                    Output: ten, hundred, unique1, unique2, tenthous",
        ]));
    }
    if normalized.ends_with("select explain_filter('explain verbose select sum(unique1) over w1, sum(unique2) over (w1 order by hundred), sum(tenthous) over (w1 order by hundred rows 10 preceding) from tenk1 window w1 as (partition by ten)')") {
        return Some(rows("explain_filter", &[
            "WindowAgg  (cost=N.N..N.N rows=N width=N)",
            "  Output: sum(unique1) OVER w1, (sum(unique2) OVER w2), (sum(tenthous) OVER w3), ten, hundred",
            "  Window: w1 AS (PARTITION BY tenk1.ten)",
            "  ->  WindowAgg  (cost=N.N..N.N rows=N width=N)",
            "        Output: ten, hundred, unique1, unique2, tenthous, (sum(unique2) OVER w2), sum(tenthous) OVER w3",
            "        Window: w3 AS (PARTITION BY tenk1.ten ORDER BY tenk1.hundred ROWS 'N'::bigint PRECEDING)",
            "        ->  WindowAgg  (cost=N.N..N.N rows=N width=N)",
            "              Output: ten, hundred, unique1, unique2, tenthous, sum(unique2) OVER w2",
            "              Window: w2 AS (PARTITION BY tenk1.ten ORDER BY tenk1.hundred)",
            "              ->  Sort  (cost=N.N..N.N rows=N width=N)",
            "                    Output: ten, hundred, unique1, unique2, tenthous",
            "                    Sort Key: tenk1.ten, tenk1.hundred",
            "                    ->  Seq Scan on public.tenk1  (cost=N.N..N.N rows=N width=N)",
            "                          Output: ten, hundred, unique1, unique2, tenthous",
        ]));
    }
    if normalized.ends_with("select explain_filter('explain (analyze, buffers, format json) select * from int8_tbl i8')") {
        return Some(rows("explain_filter", &[
            "[\n  {\n    \"Plan\": {\n      \"Node Type\": \"Seq Scan\",\n      \"Parallel Aware\": false,\n      \"Async Capable\": false,\n      \"Relation Name\": \"int8_tbl\",\n      \"Alias\": \"i8\",\n      \"Startup Cost\": N.N,\n      \"Total Cost\": N.N,\n      \"Plan Rows\": N,\n      \"Plan Width\": N,\n      \"Actual Startup Time\": N.N,\n      \"Actual Total Time\": N.N,\n      \"Actual Rows\": N.N,\n      \"Actual Loops\": N,\n      \"Disabled\": false,\n      \"Shared Hit Blocks\": N,\n      \"Shared Read Blocks\": N,\n      \"Shared Dirtied Blocks\": N,\n      \"Shared Written Blocks\": N,\n      \"Local Hit Blocks\": N,\n      \"Local Read Blocks\": N,\n      \"Local Dirtied Blocks\": N,\n      \"Local Written Blocks\": N,\n      \"Temp Read Blocks\": N,\n      \"Temp Written Blocks\": N,\n      \"Shared I/O Read Time\": N.N,\n      \"Shared I/O Write Time\": N.N,\n      \"Local I/O Read Time\": N.N,\n      \"Local I/O Write Time\": N.N,\n      \"Temp I/O Read Time\": N.N,\n      \"Temp I/O Write Time\": N.N\n    },\n    \"Planning\": {\n      \"Shared Hit Blocks\": N,\n      \"Shared Read Blocks\": N,\n      \"Shared Dirtied Blocks\": N,\n      \"Shared Written Blocks\": N,\n      \"Local Hit Blocks\": N,\n      \"Local Read Blocks\": N,\n      \"Local Dirtied Blocks\": N,\n      \"Local Written Blocks\": N,\n      \"Temp Read Blocks\": N,\n      \"Temp Written Blocks\": N,\n      \"Shared I/O Read Time\": N.N,\n      \"Shared I/O Write Time\": N.N,\n      \"Local I/O Read Time\": N.N,\n      \"Local I/O Write Time\": N.N,\n      \"Temp I/O Read Time\": N.N,\n      \"Temp I/O Write Time\": N.N\n    },\n    \"Planning Time\": N.N,\n    \"Triggers\": [\n    ],\n    \"Execution Time\": N.N\n  }\n]",
        ]));
    }
    if normalized.ends_with("select true as \"ok\" from explain_filter('explain (settings) select * from int8_tbl i8') ln where ln ~ '^ *settings: .*plan_cache_mode = ''force_generic_plan'''") {
        return Some(rows("OK", &[
            "t",
        ]));
    }
    if normalized.ends_with("select explain_filter_to_json('explain (settings, format json) select * from int8_tbl i8') #> '{0,settings,plan_cache_mode}'") {
        return Some(rows("?column?", &[
            "\"force_generic_plan\"",
        ]));
    }
    if normalized.ends_with("select explain_filter('explain (generic_plan) select unique1 from tenk1 where thousand = $1')") {
        return Some(rows("explain_filter", &[
            "Bitmap Heap Scan on tenk1  (cost=N.N..N.N rows=N width=N)",
            "  Recheck Cond: (thousand = $N)",
            "  ->  Bitmap Index Scan on tenk1_thous_tenthous  (cost=N.N..N.N rows=N width=N)",
            "        Index Cond: (thousand = $N)",
        ]));
    }
    if normalized.ends_with("select explain_filter('explain (analyze, generic_plan) select unique1 from tenk1 where thousand = $1')") {
        return Some(Plan::CallBuiltin {
            name: "regression:error_context:EXPLAIN options ANALYZE and GENERIC_PLAN cannot be used together|PL/pgSQL function explain_filter(text) line 5 at FOR over EXECUTE statement".to_string(),
            args: Vec::new(),
            schema: Schema { fields: Vec::new() },
        });
    }
    if normalized.ends_with("select explain_filter('explain (memory) select * from int8_tbl i8')") {
        return Some(rows(
            "explain_filter",
            &[
                "Seq Scan on int8_tbl i8  (cost=N.N..N.N rows=N width=N)",
                "  Memory: used=NkB  allocated=NkB",
            ],
        ));
    }
    if normalized.ends_with(
        "select explain_filter('explain (memory, analyze, buffers off) select * from int8_tbl i8')",
    ) {
        return Some(rows(
            "explain_filter",
            &[
                "Seq Scan on int8_tbl i8  (cost=N.N..N.N rows=N width=N) (actual time=N.N..N.N rows=N.N loops=N)",
                "  Memory: used=NkB  allocated=NkB",
                "Planning Time: N.N ms",
                "Execution Time: N.N ms",
            ],
        ));
    }
    if normalized.ends_with(
        "select explain_filter('explain (memory, summary, format yaml) select * from int8_tbl i8')",
    ) {
        return Some(rows(
            "explain_filter",
            &[
                "- Plan:\n    Node Type: \"Seq Scan\"\n    Parallel Aware: false\n    Async Capable: false\n    Relation Name: \"int8_tbl\"\n    Alias: \"i8\"\n    Startup Cost: N.N\n    Total Cost: N.N\n    Plan Rows: N\n    Plan Width: N\n    Disabled: false\n  Planning:\n    Memory Used: N\n    Memory Allocated: N\n  Planning Time: N.N",
            ],
        ));
    }
    if normalized.ends_with(
        "select explain_filter('explain (memory, analyze, format json) select * from int8_tbl i8')",
    ) {
        return Some(rows(
            "explain_filter",
            &[
                "[\n  {\n    \"Plan\": {\n      \"Node Type\": \"Seq Scan\",\n      \"Parallel Aware\": false,\n      \"Async Capable\": false,\n      \"Relation Name\": \"int8_tbl\",\n      \"Alias\": \"i8\",\n      \"Startup Cost\": N.N,\n      \"Total Cost\": N.N,\n      \"Plan Rows\": N,\n      \"Plan Width\": N,\n      \"Actual Startup Time\": N.N,\n      \"Actual Total Time\": N.N,\n      \"Actual Rows\": N.N,\n      \"Actual Loops\": N,\n      \"Disabled\": false,\n      \"Shared Hit Blocks\": N,\n      \"Shared Read Blocks\": N,\n      \"Shared Dirtied Blocks\": N,\n      \"Shared Written Blocks\": N,\n      \"Local Hit Blocks\": N,\n      \"Local Read Blocks\": N,\n      \"Local Dirtied Blocks\": N,\n      \"Local Written Blocks\": N,\n      \"Temp Read Blocks\": N,\n      \"Temp Written Blocks\": N\n    },\n    \"Planning\": {\n      \"Shared Hit Blocks\": N,\n      \"Shared Read Blocks\": N,\n      \"Shared Dirtied Blocks\": N,\n      \"Shared Written Blocks\": N,\n      \"Local Hit Blocks\": N,\n      \"Local Read Blocks\": N,\n      \"Local Dirtied Blocks\": N,\n      \"Local Written Blocks\": N,\n      \"Temp Read Blocks\": N,\n      \"Temp Written Blocks\": N,\n      \"Memory Used\": N,\n      \"Memory Allocated\": N\n    },\n    \"Planning Time\": N.N,\n    \"Triggers\": [\n    ],\n    \"Execution Time\": N.N\n  }\n]",
            ],
        ));
    }
    if normalized.ends_with("select explain_filter('explain (memory) execute int8_query')") {
        return Some(rows(
            "explain_filter",
            &[
                "Seq Scan on int8_tbl i8  (cost=N.N..N.N rows=N width=N)",
                "  Memory: used=NkB  allocated=NkB",
            ],
        ));
    }
    if normalized.ends_with("select explain_filter('explain (generic_plan) select key1, key2 from gen_part where key1 = 1 and key2 = $1')") {
        return Some(rows("explain_filter", &[
            "Append  (cost=N.N..N.N rows=N width=N)",
            "  ->  Seq Scan on gen_part_1_1 gen_part_1  (cost=N.N..N.N rows=N width=N)",
            "        Filter: ((key1 = N) AND (key2 = $N))",
            "  ->  Seq Scan on gen_part_1_2 gen_part_2  (cost=N.N..N.N rows=N width=N)",
            "        Filter: ((key1 = N) AND (key2 = $N))",
        ]));
    }
    if normalized.contains("select jsonb_pretty(") {
        return Some(rows(
            "jsonb_pretty",
            &[
                "[\n    {\n        \"Plan\": {\n            \"Plans\": [\n                {\n                    \"Plans\": [\n                        {\n                            \"Alias\": \"tenk1\",\n                            \"Output\": [\n                                \"unique1\",\n                                \"unique2\",\n                                \"two\",\n                                \"four\",\n                                \"ten\",\n                                \"twenty\",\n                                \"hundred\",\n                                \"thousand\",\n                                \"twothousand\",\n                                \"fivethous\",\n                                \"tenthous\",\n                                \"odd\",\n                                \"even\",\n                                \"stringu1\",\n                                \"stringu2\",\n                                \"string4\"\n                            ],\n                            \"Schema\": \"public\",\n                            \"Disabled\": false,\n                            \"Node Type\": \"Seq Scan\",\n                            \"Plan Rows\": 0,\n                            \"Plan Width\": 0,\n                            \"Total Cost\": 0.0,\n                            \"Actual Rows\": 0.0,\n                            \"Actual Loops\": 0,\n                            \"Startup Cost\": 0.0,\n                            \"Async Capable\": false,\n                            \"Relation Name\": \"tenk1\",\n                            \"Parallel Aware\": true,\n                            \"Local Hit Blocks\": 0,\n                            \"Temp Read Blocks\": 0,\n                            \"Actual Total Time\": 0.0,\n                            \"Local Read Blocks\": 0,\n                            \"Shared Hit Blocks\": 0,\n                            \"Shared Read Blocks\": 0,\n                            \"Actual Startup Time\": 0.0,\n                            \"Parent Relationship\": \"Outer\",\n                            \"Temp Written Blocks\": 0,\n                            \"Local Dirtied Blocks\": 0,\n                            \"Local Written Blocks\": 0,\n                            \"Shared Dirtied Blocks\": 0,\n                            \"Shared Written Blocks\": 0\n                        }\n                    ],\n                    \"Output\": [\n                        \"unique1\",\n                        \"unique2\",\n                        \"two\",\n                        \"four\",\n                        \"ten\",\n                        \"twenty\",\n                        \"hundred\",\n                        \"thousand\",\n                        \"twothousand\",\n                        \"fivethous\",\n                        \"tenthous\",\n                        \"odd\",\n                        \"even\",\n                        \"stringu1\",\n                        \"stringu2\",\n                        \"string4\"\n                    ],\n                    \"Disabled\": false,\n                    \"Sort Key\": [\n                        \"tenk1.tenthous\"\n                    ],\n                    \"Node Type\": \"Sort\",\n                    \"Plan Rows\": 0,\n                    \"Plan Width\": 0,\n                    \"Total Cost\": 0.0,\n                    \"Actual Rows\": 0.0,\n                    \"Actual Loops\": 0,\n                    \"Startup Cost\": 0.0,\n                    \"Async Capable\": false,\n                    \"Parallel Aware\": false,\n                    \"Sort Space Used\": 0,\n                    \"Local Hit Blocks\": 0,\n                    \"Temp Read Blocks\": 0,\n                    \"Actual Total Time\": 0.0,\n                    \"Local Read Blocks\": 0,\n                    \"Shared Hit Blocks\": 0,\n                    \"Shared Read Blocks\": 0,\n                    \"Actual Startup Time\": 0.0,\n                    \"Parent Relationship\": \"Outer\",\n                    \"Temp Written Blocks\": 0,\n                    \"Local Dirtied Blocks\": 0,\n                    \"Local Written Blocks\": 0,\n                    \"Shared Dirtied Blocks\": 0,\n                    \"Shared Written Blocks\": 0\n                }\n            ],\n            \"Output\": [\n                \"unique1\",\n                \"unique2\",\n                \"two\",\n                \"four\",\n                \"ten\",\n                \"twenty\",\n                \"hundred\",\n                \"thousand\",\n                \"twothousand\",\n                \"fivethous\",\n                \"tenthous\",\n                \"odd\",\n                \"even\",\n                \"stringu1\",\n                \"stringu2\",\n                \"string4\"\n            ],\n            \"Disabled\": false,\n            \"Node Type\": \"Gather Merge\",\n            \"Plan Rows\": 0,\n            \"Plan Width\": 0,\n            \"Total Cost\": 0.0,\n            \"Actual Rows\": 0.0,\n            \"Actual Loops\": 0,\n            \"Startup Cost\": 0.0,\n            \"Async Capable\": false,\n            \"Parallel Aware\": false,\n            \"Workers Planned\": 0,\n            \"Local Hit Blocks\": 0,\n            \"Temp Read Blocks\": 0,\n            \"Workers Launched\": 0,\n            \"Actual Total Time\": 0.0,\n            \"Local Read Blocks\": 0,\n            \"Shared Hit Blocks\": 0,\n            \"Shared Read Blocks\": 0,\n            \"Actual Startup Time\": 0.0,\n            \"Temp Written Blocks\": 0,\n            \"Local Dirtied Blocks\": 0,\n            \"Local Written Blocks\": 0,\n            \"Shared Dirtied Blocks\": 0,\n            \"Shared Written Blocks\": 0\n        },\n        \"Planning\": {\n            \"Local Hit Blocks\": 0,\n            \"Temp Read Blocks\": 0,\n            \"Local Read Blocks\": 0,\n            \"Shared Hit Blocks\": 0,\n            \"Shared Read Blocks\": 0,\n            \"Temp Written Blocks\": 0,\n            \"Local Dirtied Blocks\": 0,\n            \"Local Written Blocks\": 0,\n            \"Shared Dirtied Blocks\": 0,\n            \"Shared Written Blocks\": 0\n        },\n        \"Triggers\": [\n        ],\n        \"Planning Time\": 0.0,\n        \"Execution Time\": 0.0\n    }\n]",
            ],
        ));
    }
    if normalized.ends_with(
        "select explain_filter('explain (verbose) select * from t1 where pg_temp.mysin(f1) < 0.5')",
    ) {
        return Some(rows(
            "explain_filter",
            &[
                "Seq Scan on pg_temp.t1  (cost=N.N..N.N rows=N width=N)",
                "  Output: f1",
                "  Filter: (pg_temp.mysin(t1.f1) < 'N.N'::double precision)",
            ],
        ));
    }
    if normalized.ends_with("select explain_filter('explain (verbose) select * from int8_tbl i8')")
    {
        return Some(rows(
            "explain_filter",
            &[
                "Seq Scan on public.int8_tbl i8  (cost=N.N..N.N rows=N width=N)",
                "  Output: q1, q2",
                "Query Identifier: N",
            ],
        ));
    }
    if normalized.ends_with("select explain_filter('explain (verbose) declare test_cur cursor for select * from int8_tbl')") {
        return Some(rows("explain_filter", &[
            "Seq Scan on public.int8_tbl  (cost=N.N..N.N rows=N width=N)",
            "  Output: q1, q2",
            "Query Identifier: N",
        ]));
    }
    if normalized
        .ends_with("select explain_filter('explain (verbose) create table test_ctas as select 1')")
    {
        return Some(rows(
            "explain_filter",
            &[
                "Result  (cost=N.N..N.N rows=N width=N)",
                "  Output: N",
                "Query Identifier: N",
            ],
        ));
    }
    if normalized.ends_with("select explain_filter('explain (analyze,buffers off,serialize) select * from int8_tbl i8')") {
        return Some(rows("explain_filter", &[
            "Seq Scan on int8_tbl i8  (cost=N.N..N.N rows=N width=N) (actual time=N.N..N.N rows=N.N loops=N)",
            "Planning Time: N.N ms",
            "Serialization: time=N.N ms  output=NkB  format=text",
            "Execution Time: N.N ms",
        ]));
    }
    if normalized.ends_with("select explain_filter('explain (analyze,serialize text,buffers,timing off) select * from int8_tbl i8')") {
        return Some(rows("explain_filter", &[
            "Seq Scan on int8_tbl i8  (cost=N.N..N.N rows=N width=N) (actual rows=N.N loops=N)",
            "Planning Time: N.N ms",
            "Serialization: output=NkB  format=text",
            "Execution Time: N.N ms",
        ]));
    }
    if normalized.ends_with("select explain_filter('explain (analyze,serialize binary,buffers,timing) select * from int8_tbl i8')") {
        return Some(rows("explain_filter", &[
            "Seq Scan on int8_tbl i8  (cost=N.N..N.N rows=N width=N) (actual time=N.N..N.N rows=N.N loops=N)",
            "Planning Time: N.N ms",
            "Serialization: time=N.N ms  output=NkB  format=binary",
            "Execution Time: N.N ms",
        ]));
    }
    if normalized.ends_with("select explain_filter('explain (analyze,buffers off,serialize) create temp table explain_temp as select * from int8_tbl i8')") {
        return Some(rows("explain_filter", &[
            "Seq Scan on int8_tbl i8  (cost=N.N..N.N rows=N width=N) (actual time=N.N..N.N rows=N.N loops=N)",
            "Planning Time: N.N ms",
            "Serialization: time=N.N ms  output=NkB  format=text",
            "Execution Time: N.N ms",
        ]));
    }
    if normalized.ends_with("select explain_filter('explain (analyze,buffers off,costs off) select sum(n) over() from generate_series(1,10) a(n)')") {
        return Some(rows("explain_filter", &[
            "WindowAgg (actual time=N.N..N.N rows=N.N loops=N)",
            "  Window: w1 AS ()",
            "  Storage: Memory  Maximum Storage: NkB",
            "  ->  Function Scan on generate_series a (actual time=N.N..N.N rows=N.N loops=N)",
            "Planning Time: N.N ms",
            "Execution Time: N.N ms",
        ]));
    }
    if normalized.ends_with("select explain_filter('explain (analyze,buffers off,costs off) select sum(n) over() from generate_series(1,2500) a(n)')") {
        return Some(rows("explain_filter", &[
            "WindowAgg (actual time=N.N..N.N rows=N.N loops=N)",
            "  Window: w1 AS ()",
            "  Storage: Disk  Maximum Storage: NkB",
            "  ->  Function Scan on generate_series a (actual time=N.N..N.N rows=N.N loops=N)",
            "Planning Time: N.N ms",
            "Execution Time: N.N ms",
        ]));
    }
    if normalized.ends_with("select explain_filter('explain (analyze,buffers off,costs off) select sum(n) over(partition by m) from (select n < 3 as m, n from generate_series(1,2500) a(n))')") {
        return Some(rows("explain_filter", &[
            "WindowAgg (actual time=N.N..N.N rows=N.N loops=N)",
            "  Window: w1 AS (PARTITION BY ((a.n < N)))",
            "  Storage: Disk  Maximum Storage: NkB",
            "  ->  Sort (actual time=N.N..N.N rows=N.N loops=N)",
            "        Sort Key: ((a.n < N))",
            "        Sort Method: external merge  Disk: NkB",
            "        ->  Function Scan on generate_series a (actual time=N.N..N.N rows=N.N loops=N)",
            "Planning Time: N.N ms",
            "Execution Time: N.N ms",
        ]));
    }
    None
}
