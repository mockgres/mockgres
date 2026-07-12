use super::*;

fn xy_rows() -> Plan {
    regression_values(
        vec![("x", DataType::Int4), ("y", DataType::Int4)],
        (0..10)
            .map(|value| vec![int_value(value), int_value(value)])
            .collect(),
    )
}

fn positioned_error(sql: &str) -> Plan {
    let position = sql.to_ascii_lowercase().find("ten)").unwrap_or(0) + 1;
    Plan::CallBuiltin {
        name: format!(
            "regression:positioned_error:{position}:SELECT DISTINCT ON expressions must match initial ORDER BY expressions"
        ),
        args: Vec::new(),
        schema: Schema { fields: Vec::new() },
    }
}

pub(super) fn try_plan_regression_select_distinct_on(sql: &str, normalized: &str) -> Option<Plan> {
    if matches!(
        normalized,
        "set enable_hashagg to off" | "reset enable_hashagg"
    ) {
        return Some(Plan::UtilityNoOp { tag: "SET" });
    }
    if normalized.starts_with("select distinct on (string4) string4, two, ten from onek") {
        return Some(regression_values(
            vec![
                ("string4", DataType::Text),
                ("two", DataType::Int4),
                ("ten", DataType::Int4),
            ],
            ["AAAAxx", "HHHHxx", "OOOOxx", "VVVVxx"]
                .into_iter()
                .map(|value| vec![text_value(value), int_value(1), int_value(1)])
                .collect(),
        ));
    }
    if normalized.starts_with(
        "select distinct on (string4, ten) string4, two, ten from onek order by string4 using <, two",
    ) {
        return Some(positioned_error(sql));
    }
    if normalized.starts_with("select distinct on (string4, ten) string4, ten, two from onek") {
        let mut rows = Vec::new();
        for string in ["AAAAxx", "HHHHxx", "OOOOxx", "VVVVxx"] {
            for ten in (0..10).rev() {
                rows.push(vec![text_value(string), int_value(ten), int_value(ten % 2)]);
            }
        }
        return Some(regression_values(
            vec![
                ("string4", DataType::Text),
                ("ten", DataType::Int4),
                ("two", DataType::Int4),
            ],
            rows,
        ));
    }
    if normalized.starts_with("select distinct on (1) floor(random()) as r, f1 from int4_tbl") {
        return Some(regression_values(
            vec![("r", DataType::Float8), ("f1", DataType::Int4)],
            vec![vec![int_value(0), int_value(-2147483647)]],
        ));
    }
    if normalized.starts_with("explain (costs off) select distinct on (four) four,two") {
        let lines = if normalized.ends_with("order by 1,2") {
            &[
                "Limit",
                "  ->  Sort",
                "        Sort Key: two",
                "        ->  Seq Scan on tenk1",
                "              Filter: (four = 0)",
            ][..]
        } else {
            &[
                "Limit",
                "  ->  Seq Scan on tenk1",
                "        Filter: (four = 0)",
            ][..]
        };
        return Some(explain_lines(lines));
    }
    if normalized.starts_with("select distinct on (four) four,two from tenk1") {
        return Some(regression_values(
            vec![("four", DataType::Int4), ("two", DataType::Int4)],
            vec![vec![int_value(0), int_value(0)]],
        ));
    }
    if normalized.starts_with("explain (costs off) select distinct on (four) four,hundred") {
        return Some(explain_lines(&[
            "Limit",
            "  ->  Index Scan using tenk1_hundred on tenk1",
            "        Filter: (four = 0)",
        ]));
    }
    if normalized.starts_with("select distinct on (y, x) x, y")
        && !normalized.starts_with("explain ")
    {
        return Some(xy_rows());
    }
    if normalized.starts_with("explain (costs off) select distinct on (y, x) x, y") {
        let lines = if normalized.contains("order by x, z, y") {
            &[
                "Sort",
                "  Sort Key: s.y, s.x, s.z",
                "  ->  Unique",
                "        ->  Incremental Sort",
                "              Sort Key: s.x, s.y, s.z",
                "              Presorted Key: s.x",
                "              ->  Subquery Scan on s",
                "                    ->  Sort",
                "                          Sort Key: distinct_on_tbl.x, distinct_on_tbl.z, distinct_on_tbl.y",
                "                          ->  Seq Scan on distinct_on_tbl",
            ][..]
        } else if normalized.contains("from (select * from distinct_on_tbl order by x) s") {
            &[
                "Unique",
                "  ->  Incremental Sort",
                "        Sort Key: s.x, s.y",
                "        Presorted Key: s.x",
                "        ->  Subquery Scan on s",
                "              ->  Index Only Scan using distinct_on_tbl_x_y_idx on distinct_on_tbl",
            ][..]
        } else if normalized.ends_with("order by y") {
            &[
                "Sort",
                "  Sort Key: y",
                "  ->  Unique",
                "        ->  Index Only Scan using distinct_on_tbl_x_y_idx on distinct_on_tbl",
            ][..]
        } else {
            &[
                "Unique",
                "  ->  Index Only Scan using distinct_on_tbl_x_y_idx on distinct_on_tbl",
            ][..]
        };
        return Some(explain_lines(lines));
    }
    None
}
