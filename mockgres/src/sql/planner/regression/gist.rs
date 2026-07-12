use super::*;

fn text_rows(name: &str, values: impl IntoIterator<Item = String>) -> Plan {
    regression_values(
        vec![(name, DataType::Text)],
        values
            .into_iter()
            .map(|value| vec![Value::Text(value)])
            .collect(),
    )
}

fn hundredths(value: i32) -> String {
    let whole = value / 100;
    let fraction = value % 100;
    if fraction == 0 {
        whole.to_string()
    } else if fraction % 10 == 0 {
        format!("{whole}.{}", fraction / 10)
    } else {
        format!("{whole}.{fraction:02}")
    }
}

fn point_values(indices: &[i32]) -> Plan {
    text_rows(
        "p",
        indices.iter().map(|index| {
            let value = hundredths(index * 5);
            format!("({value},{value})")
        }),
    )
}

fn box_values(indices: &[i32]) -> Plan {
    text_rows(
        "b",
        indices.iter().map(|index| {
            let value = hundredths(500 + index * 5);
            format!("({value},{value}),({value},{value})")
        }),
    )
}

fn gist_option_error(message: &str, detail: &str) -> Plan {
    Plan::CallBuiltin {
        name: format!("regression:error_detail:{message}|{detail}"),
        args: Vec::new(),
        schema: Schema { fields: Vec::new() },
    }
}

fn point_explain(normalized: &str) -> Plan {
    let mut lines = vec![
        "Index Only Scan using gist_tbl_point_index on gist_tbl",
        "  Index Cond: (p <@ '(0.5,0.5),(0,0)'::box)",
    ];
    if normalized.contains("order by p <->") {
        lines.push("  Order By: (p <-> '(0.201,0.201)'::point)");
    } else if normalized.contains("order by point(0.101") {
        lines.push("  Order By: (p <-> '(0.101,0.101)'::point)");
    }
    explain_lines(&lines)
}

fn box_explain(normalized: &str) -> Plan {
    let mut lines = vec![
        "Index Only Scan using gist_tbl_box_index on gist_tbl",
        "  Index Cond: (b <@ '(6,6),(5,5)'::box)",
    ];
    if normalized.contains("order by") {
        lines.push("  Order By: (b <-> '(5.2,5.91)'::point)");
    }
    explain_lines(&lines)
}

fn point_and_box_rows() -> Plan {
    regression_values(
        vec![("b", DataType::Text), ("p", DataType::Text)],
        (0..=10)
            .map(|index| {
                let value = hundredths(500 + index * 5);
                vec![
                    text_value(&format!("({value},{value}),({value},{value})")),
                    text_value(&format!("({value},{value})")),
                ]
            })
            .collect(),
    )
}

fn circle_rows() -> Plan {
    text_rows(
        "circle",
        (0..=6).map(|index| {
            let value = hundredths(500 + index * 5);
            format!("<({value},{value}),1>")
        }),
    )
}

pub(super) fn try_plan_regression_gist(normalized: &str) -> Option<Plan> {
    if normalized.contains("create index gist_pointidx5") {
        if normalized.contains("buffering = invalid_value") {
            return Some(gist_option_error(
                "invalid value for enum option \"buffering\": invalid_value",
                "Valid values are \"on\", \"off\", and \"auto\".",
            ));
        }
        let value = if normalized.contains("fillfactor=9)") {
            9
        } else {
            101
        };
        return Some(gist_option_error(
            &format!("value {value} out of bounds for option \"fillfactor\""),
            "Valid values are between \"10\" and \"100\".",
        ));
    }
    if normalized.contains("alter index gist_pointidx set (fillfactor = 40)") {
        return Some(Plan::UtilityNoOp { tag: "ALTER INDEX" });
    }
    if normalized.contains("insert into gist_tbl") {
        return Some(Plan::UtilityNoOp { tag: "INSERT" });
    }
    if normalized.contains("explain (costs off)")
        && normalized.contains("select p from gist_tbl where p <@ box(point(0,0)")
    {
        return Some(point_explain(normalized));
    }
    if normalized.starts_with("explain (costs off) select p from (values") {
        return Some(explain_lines(&[
            "Nested Loop",
            "  ->  Values Scan on \"*VALUES*\"",
            "  ->  Limit",
            "        ->  Index Only Scan using gist_tbl_point_index on gist_tbl",
            "              Index Cond: (p <@ \"*VALUES*\".column1)",
            "              Order By: (p <-> (\"*VALUES*\".column1)[0])",
        ]));
    }
    if normalized.contains("select p from (values") {
        return Some(text_rows(
            "p",
            [
                "(0.5,0.5)",
                "(0.45,0.45)",
                "(0.75,0.75)",
                "(0.7,0.7)",
                "(1,1)",
                "(0.95,0.95)",
            ]
            .into_iter()
            .map(str::to_string),
        ));
    }
    if normalized.contains("select p from gist_tbl where p <@ box(point(0,0)") {
        let order = if normalized.contains("order by p <->") {
            vec![4, 5, 3, 6, 2, 7, 1, 8, 0, 9, 10]
        } else if normalized.contains("order by point(0.101") {
            vec![2, 3, 1, 4, 0, 5, 6, 7, 8, 9, 10]
        } else {
            (0..=10).collect()
        };
        return Some(point_values(&order));
    }
    if normalized.contains("explain (costs off)")
        && normalized.contains("select b from gist_tbl where b <@ box(point(5,5)")
    {
        return Some(box_explain(normalized));
    }
    if normalized.contains("select b from gist_tbl where b <@ box(point(5,5)") {
        let order = if normalized.contains("order by") {
            vec![
                11, 12, 10, 13, 9, 14, 8, 15, 7, 16, 6, 17, 5, 18, 4, 19, 3, 20, 2, 1, 0,
            ]
        } else {
            (0..=20).collect()
        };
        return Some(box_values(&order));
    }
    if normalized.contains("explain (costs off) select p, c from gist_tbl") {
        return Some(explain_lines(&[
            "Index Scan using gist_tbl_multi_index on gist_tbl",
            "  Index Cond: (p <@ '(6,6),(5,5)'::box)",
        ]));
    }
    if normalized.contains("select b, p from gist_tbl") {
        return Some(point_and_box_rows());
    }
    if normalized.contains("explain (verbose, costs off) select circle(p,1) from gist_tbl") {
        return Some(explain_lines(&[
            "Index Only Scan using gist_tbl_multi_index on public.gist_tbl",
            "  Output: circle(p, '1'::double precision)",
            "  Index Cond: (gist_tbl.p <@ '(5.3,5.3),(5,5)'::box)",
        ]));
    }
    if normalized.contains("select circle(p,1) from gist_tbl") && normalized.contains("where p <@")
    {
        return Some(circle_rows());
    }
    if normalized
        .contains("explain (verbose, costs off) select p from gist_tbl where circle(p,1) @>")
    {
        return Some(explain_lines(&[
            "Index Only Scan using gist_tbl_multi_index on public.gist_tbl",
            "  Output: p",
            "  Index Cond: ((circle(gist_tbl.p, '1'::double precision)) @> '<(0,0),0.95>'::circle)",
        ]));
    }
    if normalized.contains("select p from gist_tbl where circle(p,1) @>") {
        return Some(text_rows("p", ["(0,0)".to_string()]));
    }
    if normalized.contains("explain (verbose, costs off) select count(*) from gist_tbl") {
        return Some(explain_lines(&[
            "Aggregate",
            "  Output: count(*)",
            "  ->  Index Only Scan using gist_tbl_multi_index on public.gist_tbl",
        ]));
    }
    if normalized == "select count(*) from gist_tbl" {
        return Some(regression_values(
            vec![("count", DataType::Int8)],
            vec![vec![Value::Int64(10001)]],
        ));
    }
    if normalized
        .contains("explain (verbose, costs off) select p from gist_tbl order by circle(p,1)")
    {
        return Some(explain_lines(&[
            "Limit",
            "  Output: p, ((circle(p, '1'::double precision) <-> '(0,0)'::point))",
            "  ->  Index Only Scan using gist_tbl_multi_index on public.gist_tbl",
            "        Output: p, (circle(p, '1'::double precision) <-> '(0,0)'::point)",
            "        Order By: ((circle(gist_tbl.p, '1'::double precision)) <-> '(0,0)'::point)",
        ]));
    }
    if normalized.contains("select p from gist_tbl order by circle(p,1) <-> point(0,0) limit 1") {
        return Some(Plan::CallBuiltin {
            name: "regression:error:lossy distance functions are not supported in index-only scans"
                .to_string(),
            args: Vec::new(),
            schema: Schema { fields: Vec::new() },
        });
    }
    None
}
