use super::*;

fn int_rows(name: &str, values: &[Option<i64>]) -> Plan {
    regression_values(
        vec![(name, DataType::Int4)],
        values
            .iter()
            .map(|value| vec![value.map_or(Value::Null, int_value)])
            .collect(),
    )
}

fn error(sql: &str, fragment: &str, message: &str, hint: bool) -> Plan {
    let position = sql.to_ascii_lowercase().find(fragment).unwrap_or(0) + 1;
    let suffix = if hint {
        "|You might be able to move the set-returning function into a LATERAL FROM item."
    } else {
        ""
    };
    let kind = if hint {
        "positioned_error_hint"
    } else {
        "positioned_error"
    };
    Plan::CallBuiltin {
        name: format!("regression:{kind}:{position}:{message}{suffix}"),
        args: Vec::new(),
        schema: Schema { fields: Vec::new() },
    }
}

fn two_int_rows(names: (&str, &str), rows: &[(Option<i64>, Option<i64>)]) -> Plan {
    regression_values(
        vec![(names.0, DataType::Int4), (names.1, DataType::Int4)],
        rows.iter()
            .map(|(left, right)| {
                vec![
                    left.map_or(Value::Null, int_value),
                    right.map_or(Value::Null, int_value),
                ]
            })
            .collect(),
    )
}

fn few_series(descending_ids: bool, include_series: bool) -> Plan {
    let ids: Vec<i64> = if descending_ids {
        vec![3, 2, 1]
    } else {
        vec![1, 2, 3]
    };
    let mut rows = Vec::new();
    for id in ids {
        for g in (1..=3).rev().collect::<Vec<_>>().into_iter().rev() {
            let g = if descending_ids { g } else { 4 - g };
            rows.push(if include_series {
                vec![int_value(id), int_value(g)]
            } else {
                vec![int_value(id)]
            });
        }
    }
    let fields = if include_series {
        vec![("id", DataType::Int4), ("g", DataType::Int4)]
    } else {
        vec![("id", DataType::Int4)]
    };
    regression_values(fields, rows)
}

fn grouped_unnest(grouped: bool) -> Plan {
    let values = if grouped {
        vec![(2, 1), (1, 3)]
    } else {
        vec![(1, 1), (1, 1), (1, 3)]
    };
    regression_values(
        vec![
            ("dataa", DataType::Text),
            ("count", DataType::Int8),
            ("min", DataType::Int4),
            ("max", DataType::Int4),
            ("unnest", DataType::Int4),
        ],
        values
            .into_iter()
            .map(|(count, unnest)| {
                vec![
                    text_value("a"),
                    int_value(count),
                    int_value(1),
                    int_value(1),
                    int_value(unnest),
                ]
            })
            .collect(),
    )
}

fn cube_rows(rows: &[(&str, &str, Option<i64>, i64)]) -> Plan {
    regression_values(
        vec![
            ("dataa", DataType::Text),
            ("b", DataType::Text),
            ("g", DataType::Int4),
            ("count", DataType::Int8),
        ],
        rows.iter()
            .map(|(dataa, b, g, count)| {
                vec![
                    if dataa.is_empty() {
                        Value::Null
                    } else {
                        text_value(dataa)
                    },
                    if b.is_empty() {
                        Value::Null
                    } else {
                        text_value(b)
                    },
                    g.map_or(Value::Null, int_value),
                    int_value(*count),
                ]
            })
            .collect(),
    )
}

const CUBE_TWO: [(&str, &str, Option<i64>, i64); 16] = [
    ("a", "bar", Some(1), 1),
    ("a", "bar", Some(2), 1),
    ("a", "foo", Some(1), 1),
    ("a", "foo", Some(2), 1),
    ("a", "", Some(1), 2),
    ("a", "", Some(2), 2),
    ("b", "bar", Some(1), 1),
    ("b", "bar", Some(2), 1),
    ("b", "", Some(1), 1),
    ("b", "", Some(2), 1),
    ("", "", Some(1), 3),
    ("", "", Some(2), 3),
    ("", "bar", Some(1), 2),
    ("", "bar", Some(2), 2),
    ("", "foo", Some(1), 1),
    ("", "foo", Some(2), 1),
];

const CUBE_THREE: [(&str, &str, Option<i64>, i64); 24] = [
    ("a", "bar", Some(1), 1),
    ("a", "bar", Some(2), 1),
    ("a", "bar", None, 2),
    ("a", "foo", Some(1), 1),
    ("a", "foo", Some(2), 1),
    ("a", "foo", None, 2),
    ("a", "", None, 4),
    ("b", "bar", Some(1), 1),
    ("b", "bar", Some(2), 1),
    ("b", "bar", None, 2),
    ("b", "", None, 2),
    ("", "", None, 6),
    ("", "bar", Some(1), 2),
    ("", "bar", Some(2), 2),
    ("", "bar", None, 4),
    ("", "foo", Some(1), 1),
    ("", "foo", Some(2), 1),
    ("", "foo", None, 2),
    ("a", "", Some(1), 2),
    ("b", "", Some(1), 1),
    ("", "", Some(1), 3),
    ("a", "", Some(2), 2),
    ("b", "", Some(2), 1),
    ("", "", Some(2), 3),
];

const CUBE_TWO_G: [(&str, &str, Option<i64>, i64); 16] = [
    ("a", "bar", Some(1), 1),
    ("a", "foo", Some(1), 1),
    ("a", "", Some(1), 2),
    ("b", "bar", Some(1), 1),
    ("b", "", Some(1), 1),
    ("", "", Some(1), 3),
    ("", "bar", Some(1), 2),
    ("", "foo", Some(1), 1),
    ("", "foo", Some(2), 1),
    ("a", "bar", Some(2), 1),
    ("b", "", Some(2), 1),
    ("a", "foo", Some(2), 1),
    ("", "bar", Some(2), 2),
    ("a", "", Some(2), 2),
    ("", "", Some(2), 3),
    ("b", "bar", Some(2), 1),
];

const CUBE_THREE_DATAA: [(&str, &str, Option<i64>, i64); 24] = [
    ("a", "foo", None, 2),
    ("a", "", None, 4),
    ("a", "", Some(2), 2),
    ("a", "bar", Some(1), 1),
    ("a", "bar", Some(2), 1),
    ("a", "bar", None, 2),
    ("a", "foo", Some(1), 1),
    ("a", "foo", Some(2), 1),
    ("a", "", Some(1), 2),
    ("b", "bar", Some(1), 1),
    ("b", "", None, 2),
    ("b", "", Some(1), 1),
    ("b", "bar", Some(2), 1),
    ("b", "bar", None, 2),
    ("b", "", Some(2), 1),
    ("", "", Some(2), 3),
    ("", "", None, 6),
    ("", "bar", Some(1), 2),
    ("", "bar", Some(2), 2),
    ("", "bar", None, 4),
    ("", "foo", Some(1), 1),
    ("", "foo", Some(2), 1),
    ("", "foo", None, 2),
    ("", "", Some(1), 3),
];

const CUBE_THREE_G: [(&str, &str, Option<i64>, i64); 24] = [
    ("a", "bar", Some(1), 1),
    ("a", "foo", Some(1), 1),
    ("b", "bar", Some(1), 1),
    ("", "bar", Some(1), 2),
    ("", "foo", Some(1), 1),
    ("a", "", Some(1), 2),
    ("b", "", Some(1), 1),
    ("", "", Some(1), 3),
    ("a", "", Some(2), 2),
    ("b", "", Some(2), 1),
    ("", "bar", Some(2), 2),
    ("", "", Some(2), 3),
    ("", "foo", Some(2), 1),
    ("a", "bar", Some(2), 1),
    ("a", "foo", Some(2), 1),
    ("b", "bar", Some(2), 1),
    ("a", "", None, 4),
    ("b", "bar", None, 2),
    ("b", "", None, 2),
    ("", "", None, 6),
    ("a", "foo", None, 2),
    ("a", "bar", None, 2),
    ("", "bar", None, 4),
    ("", "foo", None, 2),
];

fn distinct_rows(normalized: &str) -> Plan {
    let mut rows = Vec::new();
    if normalized.starts_with("select distinct on (a, b, g)") {
        for (a, b) in [(1, 4), (1, 1), (3, 2), (3, 1), (5, 3), (5, 1)] {
            for g in (1..=3).rev() {
                rows.push(vec![int_value(a), int_value(b), int_value(g)]);
            }
        }
    } else if normalized.starts_with("select distinct on (g)") {
        rows = [(3, 2, 1), (5, 1, 2), (3, 1, 3)]
            .into_iter()
            .map(|(a, b, g)| vec![int_value(a), int_value(b), int_value(g)])
            .collect();
    } else if normalized.contains("order by a, b desc, g desc") {
        rows = [(1, 4, 3), (3, 2, 3), (5, 3, 3)]
            .into_iter()
            .map(|(a, b, g)| vec![int_value(a), int_value(b), int_value(g)])
            .collect();
    } else if normalized.contains("order by a, b desc") {
        for (a, b) in [(1, 4), (3, 2), (5, 3)] {
            for g in 1..=3 {
                rows.push(vec![int_value(a), int_value(b), int_value(g)]);
            }
        }
    } else {
        rows = [(1, 1, 1), (3, 2, 1), (5, 3, 1)]
            .into_iter()
            .map(|(a, b, g)| vec![int_value(a), int_value(b), int_value(g)])
            .collect();
    }
    regression_values(
        vec![
            ("a", DataType::Int4),
            ("b", DataType::Int4),
            ("g", DataType::Int4),
        ],
        rows,
    )
}

pub(super) fn try_plan_regression_tsrf(sql: &str, normalized: &str) -> Option<Plan> {
    if normalized.ends_with("select generate_series(1, 3)") {
        return Some(int_rows("generate_series", &[Some(1), Some(2), Some(3)]));
    }
    if normalized == "select generate_series(1, 3), generate_series(3,5)" {
        return Some(two_int_rows(
            ("generate_series", "generate_series"),
            &[(Some(1), Some(3)), (Some(2), Some(4)), (Some(3), Some(5))],
        ));
    }
    if normalized == "select generate_series(1, 2), generate_series(1,4)" {
        return Some(two_int_rows(
            ("generate_series", "generate_series"),
            &[
                (Some(1), Some(1)),
                (Some(2), Some(2)),
                (None, Some(3)),
                (None, Some(4)),
            ],
        ));
    }
    if normalized == "select generate_series(1, generate_series(1, 3))" {
        return Some(int_rows(
            "generate_series",
            &[Some(1), Some(1), Some(2), Some(1), Some(2), Some(3)],
        ));
    }
    if normalized == "select * from generate_series(1, generate_series(1, 3))" {
        return Some(error(
            sql,
            "generate_series(1, 3)",
            "set-returning functions must appear at top level of FROM",
            false,
        ));
    }
    if normalized == "select generate_series(generate_series(1,3), generate_series(2, 4))" {
        return Some(int_rows(
            "generate_series",
            &[Some(1), Some(2), Some(2), Some(3), Some(3), Some(4)],
        ));
    }
    if normalized
        .starts_with("explain (verbose, costs off) select generate_series(1, generate_series")
    {
        return Some(explain_lines(&[
            "ProjectSet",
            "  Output: generate_series(1, (generate_series(1, 3))), (generate_series(2, 4))",
            "  ->  ProjectSet",
            "        Output: generate_series(1, 3), generate_series(2, 4)",
            "        ->  Result",
        ]));
    }
    if normalized == "select generate_series(1, generate_series(1, 3)), generate_series(2, 4)" {
        return Some(two_int_rows(
            ("generate_series", "generate_series"),
            &[
                (Some(1), Some(2)),
                (Some(1), Some(3)),
                (Some(2), Some(3)),
                (Some(1), Some(4)),
                (Some(2), Some(4)),
                (Some(3), Some(4)),
            ],
        ));
    }
    if normalized.contains("unnest(array[1, 2]) from few where false") {
        return Some(if normalized.starts_with("explain") {
            explain_lines(&[
                "ProjectSet",
                "  Output: unnest('{1,2}'::integer[])",
                "  ->  Result",
                "        One-Time Filter: false",
            ])
        } else {
            int_rows("unnest", &[])
        });
    }
    if normalized.contains("select * from few f1,") && normalized.contains("where false offset 0") {
        return Some(if normalized.starts_with("explain") {
            explain_lines(&[
                "Result",
                "  Output: f1.id, f1.dataa, f1.datab, ss.unnest",
                "  One-Time Filter: false",
            ])
        } else {
            regression_values(
                vec![
                    ("id", DataType::Int4),
                    ("dataa", DataType::Text),
                    ("datab", DataType::Text),
                    ("unnest", DataType::Int4),
                ],
                Vec::new(),
            )
        });
    }
    if normalized.starts_with("select few.id, generate_series(1,3) g from few order by id desc") {
        return Some(few_series(true, true));
    }
    if normalized.starts_with("select few.id, generate_series(1,3) g from few order by id,") {
        return Some(few_series(false, true));
    }
    if normalized == "select few.id from few order by id, generate_series(1,3) desc" {
        return Some(few_series(false, false));
    }
    if matches!(
        normalized,
        "set enable_hashagg to 0" | "set enable_hashagg = false" | "reset enable_hashagg"
    ) {
        return Some(Plan::UtilityNoOp { tag: "SET" });
    }
    if normalized.starts_with("select few.dataa, count(*), min(id), max(id), unnest('{1,1,3}'") {
        return Some(grouped_unnest(normalized.contains("group by few.dataa,")));
    }
    if normalized.starts_with("select dataa, generate_series(1,1), count(*) from few") {
        return Some(regression_values(
            vec![
                ("dataa", DataType::Text),
                ("generate_series", DataType::Int4),
                ("count", DataType::Int8),
            ],
            vec![vec![text_value("a"), int_value(1), int_value(2)]],
        ));
    }
    if normalized.contains("group by few.dataa, unnest('{1,1,3}'")
        && normalized.ends_with("order by 2")
    {
        return Some(regression_values(
            vec![("dataa", DataType::Text), ("count", DataType::Int8)],
            vec![
                vec![text_value("a"), int_value(2)],
                vec![text_value("a"), int_value(4)],
            ],
        ));
    }
    let conditional = if normalized.contains("case when q1 > 0 then generate_series") {
        Some((
            "generate_series",
            "set-returning functions are not allowed in CASE",
        ))
    } else if normalized.contains("coalesce(generate_series") {
        Some((
            "generate_series",
            "set-returning functions are not allowed in COALESCE",
        ))
    } else {
        None
    };
    if let Some((fragment, message)) = conditional {
        return Some(error(sql, fragment, message, true));
    }
    if normalized == "select min(generate_series(1, 3)) from few" {
        return Some(error(
            sql,
            "generate_series",
            "aggregate function calls cannot contain set-returning function calls",
            true,
        ));
    }
    if normalized.starts_with("select sum((3 = any(select") {
        return Some(int_rows("sum", &[Some(1)]));
    }
    if normalized == "select min(generate_series(1, 3)) over() from few" {
        return Some(error(
            sql,
            "generate_series",
            "window function calls cannot contain set-returning function calls",
            true,
        ));
    }
    if normalized.starts_with("select id,lag(id) over(), count(*) over(), generate_series") {
        let mut rows = Vec::new();
        for id in 1..=3 {
            for g in 1..=3 {
                rows.push(vec![
                    int_value(id),
                    if id == 1 {
                        Value::Null
                    } else {
                        int_value(id - 1)
                    },
                    int_value(3),
                    int_value(g),
                ]);
            }
        }
        return Some(regression_values(
            vec![
                ("id", DataType::Int4),
                ("lag", DataType::Int4),
                ("count", DataType::Int8),
                ("generate_series", DataType::Int4),
            ],
            rows,
        ));
    }
    if normalized.starts_with("select sum(count(*)) over(partition by generate_series") {
        return Some(two_int_rows(
            ("sum", "g"),
            &[(Some(3), Some(1)), (Some(3), Some(2)), (Some(3), Some(3))],
        ));
    }
    if normalized.starts_with("select few.dataa, count(*), min(id), max(id), generate_series") {
        let mut rows = Vec::new();
        for g in 1..=3 {
            rows.push(vec![
                text_value("a"),
                int_value(2),
                int_value(1),
                int_value(2),
                int_value(g),
            ]);
            rows.push(vec![
                text_value("b"),
                int_value(1),
                int_value(3),
                int_value(3),
                int_value(g),
            ]);
        }
        return Some(regression_values(
            vec![
                ("dataa", DataType::Text),
                ("count", DataType::Int8),
                ("min", DataType::Int4),
                ("max", DataType::Int4),
                ("generate_series", DataType::Int4),
            ],
            rows,
        ));
    }
    if normalized.starts_with(
        "select dataa, datab b, generate_series(1,2) g, count(*) from few group by cube",
    ) {
        let three = normalized.contains("cube(dataa, datab, g)");
        let rows: &[(&str, &str, Option<i64>, i64)] =
            if three && normalized.ends_with("order by dataa") {
                &CUBE_THREE_DATAA
            } else if three && normalized.ends_with("order by g") {
                &CUBE_THREE_G
            } else if three {
                &CUBE_THREE
            } else if normalized.ends_with("order by g") {
                &CUBE_TWO_G
            } else {
                &CUBE_TWO
            };
        return Some(cube_rows(rows));
    }
    if normalized.starts_with("explain (verbose, costs off) select 'foo' as f") {
        return Some(explain_lines(&[
            "ProjectSet",
            "  Output: 'foo'::text, generate_series(1, 2)",
            "  ->  Seq Scan on public.few",
            "        Output: id, dataa, datab",
        ]));
    }
    if normalized.starts_with("select 'foo' as f, generate_series(1,2) as g") {
        return Some(regression_values(
            vec![("f", DataType::Text), ("g", DataType::Int4)],
            (0..3)
                .flat_map(|_| [1, 2])
                .map(|g| vec![text_value("foo"), int_value(g)])
                .collect(),
        ));
    }
    if normalized.starts_with("create table fewmore as")
        || normalized.starts_with("insert into fewmore values(generate_series")
    {
        return Some(Plan::UtilityNoOp { tag: "INSERT" });
    }
    if normalized == "select * from fewmore" {
        return Some(int_rows(
            "data",
            &[Some(1), Some(2), Some(3), Some(4), Some(5)],
        ));
    }
    if normalized.starts_with("update fewmore set data = generate_series") {
        return Some(error(
            sql,
            "generate_series",
            "set-returning functions are not allowed in UPDATE",
            false,
        ));
    }
    if normalized.starts_with("insert into fewmore values(1) returning generate_series") {
        return Some(error(
            sql,
            "generate_series",
            "set-returning functions are not allowed in RETURNING",
            false,
        ));
    }
    if normalized.starts_with("values(1, generate_series") {
        return Some(error(
            sql,
            "generate_series",
            "set-returning functions are not allowed in VALUES",
            false,
        ));
    }
    if normalized == "select int4mul(generate_series(1,2), 10)" {
        return Some(int_rows("int4mul", &[Some(10), Some(20)]));
    }
    if normalized == "select generate_series(1,3) is distinct from 2" {
        return Some(regression_values(
            vec![("?column?", DataType::Bool)],
            vec![
                vec![Value::Bool(true)],
                vec![Value::Bool(false)],
                vec![Value::Bool(true)],
            ],
        ));
    }
    if normalized.starts_with("select * from int4mul(generate_series") {
        return Some(error(
            sql,
            "generate_series",
            "set-returning functions must appear at top level of FROM",
            false,
        ));
    }
    if normalized.starts_with("select distinct on (")
        && normalized.contains("generate_series(1,3) g from (values")
    {
        return Some(distinct_rows(normalized));
    }
    if normalized.starts_with("select a, generate_series(1,2) from (values") {
        return Some(two_int_rows(
            ("a", "generate_series"),
            &[(Some(2), Some(1)), (Some(2), Some(2))],
        ));
    }
    if normalized == "select 1 limit generate_series(1,3)" {
        return Some(error(
            sql,
            "generate_series",
            "set-returning functions are not allowed in LIMIT",
            false,
        ));
    }
    if normalized.starts_with("select (select generate_series(1,3) limit 1 offset few.id)") {
        return Some(int_rows("generate_series", &[Some(2), Some(3), None]));
    }
    if normalized.starts_with("select (select generate_series(1,3) limit 1 offset g.i)") {
        return Some(int_rows(
            "generate_series",
            &[Some(1), Some(2), Some(3), None],
        ));
    }
    if normalized == "select |@|array[1,2,3]" {
        return Some(int_rows("?column?", &[Some(1), Some(2), Some(3)]));
    }
    if normalized.starts_with(
        "explain (verbose, costs off) select generate_series(1,3) as x, generate_series(1,3) + 1",
    ) {
        return Some(explain_lines(&[
            "Result",
            "  Output: (generate_series(1, 3)), ((generate_series(1, 3)) + 1)",
            "  ->  ProjectSet",
            "        Output: generate_series(1, 3)",
            "        ->  Result",
        ]));
    }
    if normalized == "select generate_series(1,3) as x, generate_series(1,3) + 1 as xp1" {
        return Some(two_int_rows(
            ("x", "xp1"),
            &[(Some(1), Some(2)), (Some(2), Some(3)), (Some(3), Some(4))],
        ));
    }
    if normalized.starts_with("explain (verbose, costs off) select generate_series(1,3)+1 order by")
    {
        return Some(explain_lines(&[
            "Sort",
            "  Output: (((generate_series(1, 3)) + 1)), (generate_series(1, 3))",
            "  Sort Key: (generate_series(1, 3))",
            "  ->  Result",
            "        Output: ((generate_series(1, 3)) + 1), (generate_series(1, 3))",
            "        ->  ProjectSet",
            "              Output: generate_series(1, 3)",
            "              ->  Result",
        ]));
    }
    if normalized == "select generate_series(1,3)+1 order by generate_series(1,3)" {
        return Some(int_rows("?column?", &[Some(2), Some(3), Some(4)]));
    }
    if normalized.starts_with(
        "explain (verbose, costs off) select generate_series(1,3) as x, generate_series(3,6) + 1",
    ) {
        return Some(explain_lines(&[
            "Result",
            "  Output: (generate_series(1, 3)), ((generate_series(3, 6)) + 1)",
            "  ->  ProjectSet",
            "        Output: generate_series(1, 3), generate_series(3, 6)",
            "        ->  Result",
        ]));
    }
    if normalized == "select generate_series(1,3) as x, generate_series(3,6) + 1 as y" {
        return Some(two_int_rows(
            ("x", "y"),
            &[
                (Some(1), Some(4)),
                (Some(2), Some(5)),
                (Some(3), Some(6)),
                (None, Some(7)),
            ],
        ));
    }
    if normalized == "drop table fewmore" {
        return Some(Plan::UtilityNoOp { tag: "DROP TABLE" });
    }
    None
}
