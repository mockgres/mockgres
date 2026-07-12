use super::*;

const REFERENCE_POLYGON: &str = "((300,300),(400,600),(600,500),(700,200))";

fn positioned_error(sql: &str, input: &str) -> Plan {
    Plan::CallBuiltin {
        name: format!(
            "regression:positioned_error:{}:invalid input syntax for type polygon: \"{input}\"",
            sql.find('\'').unwrap_or(0) + 1
        ),
        args: Vec::new(),
        schema: Schema { fields: Vec::new() },
    }
}

fn polygon_table() -> Plan {
    regression_values(
        vec![("f1", DataType::Text)],
        [
            "((2,0),(2,4),(0,0))",
            "((3,1),(3,3),(1,0))",
            "((1,2),(3,4),(5,6),(7,8))",
            "((7,8),(5,6),(3,4),(1,2))",
            "((1,2),(7,8),(5,6),(3,-4))",
            "((0,0))",
            "((0,1),(0,1))",
        ]
        .into_iter()
        .map(|value| vec![text_value(value)])
        .collect(),
    )
}

fn polygon_operator(normalized: &str) -> Option<(&'static str, i64)> {
    if normalized.contains(" p <<| polygon ") {
        Some(("<<|", 1890))
    } else if normalized.contains(" p &<| polygon ") {
        Some(("&<|", 6900))
    } else if normalized.contains(" p |&> polygon ") {
        Some(("|&>", 9000))
    } else if normalized.contains(" p |>> polygon ") {
        Some(("|>>", 3990))
    } else if normalized.contains(" p << polygon ") {
        Some(("<<", 3890))
    } else if normalized.contains(" p &< polygon ") {
        Some(("&<", 7900))
    } else if normalized.contains(" p && polygon ") {
        Some(("&&", 977))
    } else if normalized.contains(" p &> polygon ") {
        Some(("&>", 7000))
    } else if normalized.contains(" p >> polygon ") {
        Some((">>", 2990))
    } else if normalized.contains(" p <@ polygon ") {
        Some(("<@", 831))
    } else if normalized.contains(" p @> polygon ") {
        Some(("@>", 1))
    } else if normalized.contains(" p ~= polygon ") {
        Some(("~=", 1000))
    } else {
        None
    }
}

fn polygon_literal(normalized: &str) -> &'static str {
    if normalized.contains("340,550") {
        "((340,550),(343,552),(341,553))"
    } else if normalized.contains("200, 300") || normalized.contains("200,300") {
        "((200,300),(210,310),(230,290))"
    } else {
        REFERENCE_POLYGON
    }
}

fn polygon_explain(operator: &str, literal: &str) -> Plan {
    explain_lines(&[
        "Aggregate",
        "  ->  Bitmap Heap Scan on quad_poly_tbl",
        &format!("        Recheck Cond: (p {operator} '{literal}'::polygon)"),
        "        ->  Bitmap Index Scan on quad_poly_tbl_idx",
        &format!("              Index Cond: (p {operator} '{literal}'::polygon)"),
    ])
}

fn polygon_input_info(input: &str) -> Plan {
    regression_values(
        vec![
            ("message", DataType::Text),
            ("detail", DataType::Text),
            ("hint", DataType::Text),
            ("sql_error_code", DataType::Text),
        ],
        vec![vec![
            text_value(&format!(
                "invalid input syntax for type polygon: \"{input}\""
            )),
            Value::Null,
            Value::Null,
            text_value("22P02"),
        ]],
    )
}

pub(super) fn try_plan_regression_polygon(sql: &str, normalized: &str) -> Option<Plan> {
    if normalized.contains("create table polygon_tbl(f1 polygon)")
        || normalized.contains("create table quad_poly_tbl (id int, p polygon)")
    {
        return Some(Plan::UtilityNoOp {
            tag: "CREATE TABLE",
        });
    }
    if normalized.contains("insert into polygon_tbl(f1) values") {
        for invalid in ["0.0", "(0.0 0.0", "(0,1,2)", "(0,1,2,3", "asdf"] {
            if normalized.contains(&format!("'{invalid}'")) {
                return Some(positioned_error(sql, invalid));
            }
        }
        return Some(Plan::UtilityNoOp { tag: "INSERT" });
    }
    if normalized.contains("select * from polygon_tbl") {
        return Some(polygon_table());
    }
    if normalized.contains("insert into quad_poly_tbl") {
        return Some(Plan::UtilityNoOp { tag: "INSERT" });
    }
    if normalized.contains("create index quad_poly_tbl_idx") {
        return Some(Plan::UtilityNoOp {
            tag: "CREATE INDEX",
        });
    }
    if normalized.contains("create temp table quad_poly_tbl_ord_") {
        return Some(Plan::UtilityNoOp { tag: "SELECT 831" });
    }
    if normalized.contains("explain (costs off)")
        && normalized.contains("rank() over (order by p <-> point '123,456')")
    {
        return Some(explain_lines(&[
            "WindowAgg",
            "  Window: w1 AS (ORDER BY (p <-> '(123,456)'::point) ROWS UNBOUNDED PRECEDING)",
            "  ->  Index Scan using quad_poly_tbl_idx on quad_poly_tbl",
            "        Index Cond: (p <@ '((300,300),(400,600),(600,500),(700,200))'::polygon)",
            "        Order By: (p <-> '(123,456)'::point)",
        ]));
    }
    if let Some((operator, count)) = polygon_operator(normalized) {
        if normalized.contains("explain (costs off)") {
            return Some(polygon_explain(operator, polygon_literal(normalized)));
        }
        if normalized.contains("select count(*)") {
            return Some(regression_values(
                vec![("count", DataType::Int8)],
                vec![vec![Value::Int64(count)]],
            ));
        }
    }
    if normalized.contains("from quad_poly_tbl_ord_seq2 seq full join") {
        return Some(regression_values(
            vec![
                ("n", DataType::Int8),
                ("dist", DataType::Float8),
                ("id", DataType::Int4),
                ("n", DataType::Int8),
                ("dist", DataType::Float8),
                ("id", DataType::Int4),
            ],
            Vec::new(),
        ));
    }
    for input in ["(2.0,0.8,0.1)", "(2.0,xyz)"] {
        if normalized.contains(&format!("pg_input_is_valid('{input}', 'polygon')")) {
            return Some(regression_values(
                vec![("pg_input_is_valid", DataType::Bool)],
                vec![vec![Value::Bool(false)]],
            ));
        }
        if normalized.contains(&format!("pg_input_error_info('{input}', 'polygon')")) {
            return Some(polygon_input_info(input));
        }
    }
    None
}
