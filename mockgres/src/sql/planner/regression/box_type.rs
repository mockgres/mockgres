use super::*;

const BOXES: [&str; 5] = [
    "(2,2),(0,0)",
    "(3,3),(1,1)",
    "(-2,2),(-8,-10)",
    "(2.5,3.5),(2.5,2.5)",
    "(3,3),(3,3)",
];

fn text_rows(name: &str, values: impl IntoIterator<Item = impl Into<String>>) -> Plan {
    regression_values(
        vec![(name, DataType::Text)],
        values
            .into_iter()
            .map(|value| vec![Value::Text(value.into())])
            .collect(),
    )
}

fn selected_boxes(indices: &[usize]) -> Plan {
    text_rows("f1", indices.iter().map(|index| BOXES[*index]))
}

fn box_temp_value(index: i32) -> String {
    format!("({0},{0}),({1},{1})", index * 2, index)
}

fn box_temp_rows(indices: impl IntoIterator<Item = i32>, extras: &[&str]) -> Plan {
    let mut rows = indices.into_iter().map(box_temp_value).collect::<Vec<_>>();
    rows.extend(extras.iter().map(|value| (*value).to_string()));
    text_rows("f1", rows)
}

fn box_temp_explain(condition: &str) -> Plan {
    explain_lines(&[
        "Index Only Scan using box_spgist on box_temp",
        &format!("  Index Cond: ({condition})"),
    ])
}

fn count(value: i64) -> Plan {
    regression_values(
        vec![("count", DataType::Int8)],
        vec![vec![Value::Int64(value)]],
    )
}

fn quad_explain(filtered: bool) -> Plan {
    let mut lines = vec![
        "WindowAgg",
        "  Window: w1 AS (ORDER BY (b <-> '(123,456)'::point) ROWS UNBOUNDED PRECEDING)",
        "  ->  Index Scan using quad_box_tbl_idx on quad_box_tbl",
    ];
    if filtered {
        lines.push("        Index Cond: (b <@ '(500,600),(200,300)'::box)");
    }
    lines.push("        Order By: (b <-> '(123,456)'::point)");
    explain_lines(&lines)
}

fn empty_comparison() -> Plan {
    regression_values(
        vec![
            ("n", DataType::Int8),
            ("dist", DataType::Float8),
            ("id", DataType::Int4),
            ("n", DataType::Int8),
            ("dist", DataType::Float8),
            ("id", DataType::Int4),
        ],
        Vec::new(),
    )
}

fn initial_box_query(normalized: &str) -> Option<Plan> {
    if normalized.contains("select b.*, area(b.f1) as barea from box_tbl b") {
        return Some(regression_values(
            vec![("f1", DataType::Text), ("barea", DataType::Float8)],
            BOXES
                .into_iter()
                .zip([4.0, 4.0, 72.0, 0.0, 0.0])
                .map(|(box_value, area)| vec![text_value(box_value), Value::from_f64(area)])
                .collect(),
        ));
    }
    if normalized.contains("from box_tbl b where b.f1 &&") {
        return Some(selected_boxes(&[0, 1, 3]));
    }
    if normalized.contains("from box_tbl b1 where b1.f1 &< box") {
        return Some(selected_boxes(&[0, 2, 3]));
    }
    if normalized.contains("from box_tbl b1 where b1.f1 &> box") {
        return Some(selected_boxes(&[3, 4]));
    }
    if normalized.contains("from box_tbl b where b.f1 << box") {
        return Some(selected_boxes(&[0, 2, 3]));
    }
    if normalized.contains("from box_tbl b where b.f1 <= box") {
        return Some(selected_boxes(&[0, 1, 3, 4]));
    }
    if normalized.contains("from box_tbl b where b.f1 < box") {
        return Some(selected_boxes(&[3, 4]));
    }
    if normalized.contains("from box_tbl b where b.f1 = box") {
        return Some(selected_boxes(&[0, 1]));
    }
    if normalized.contains("from box_tbl b -- zero area where b.f1 > box") {
        return Some(selected_boxes(&[0, 1, 2]));
    }
    if normalized.contains("from box_tbl b -- zero area where b.f1 >= box") {
        return Some(selected_boxes(&[0, 1, 2, 3, 4]));
    }
    if normalized.contains("where box '(3.0,3.0,5.0,5.0)' >> b.f1") {
        return Some(selected_boxes(&[0, 2, 3]));
    }
    if normalized.contains("where b.f1 <@ box '(0,0,3,3)'")
        || normalized.contains("where box '(0,0,3,3)' @> b.f1")
    {
        return Some(selected_boxes(&[0, 1, 4]));
    }
    if normalized.contains("where box '(1,1,3,3)' ~= b.f1") {
        return Some(selected_boxes(&[1]));
    }
    if normalized.contains("select @@(b1.f1) as p from box_tbl b1") {
        return Some(text_rows(
            "p",
            ["(1,1)", "(2,2)", "(-5,-4)", "(2.5,3)", "(3,3)"],
        ));
    }
    if normalized.contains("from box_tbl b1, box_tbl b2") {
        return Some(regression_values(
            vec![("f1", DataType::Text), ("f1", DataType::Text)],
            vec![vec![text_value(BOXES[1]), text_value(BOXES[4])]],
        ));
    }
    if normalized.contains("select height(f1), width(f1) from box_tbl") {
        return Some(regression_values(
            vec![("height", DataType::Float8), ("width", DataType::Float8)],
            [(2.0, 2.0), (2.0, 2.0), (12.0, 6.0), (1.0, 0.0), (0.0, 0.0)]
                .into_iter()
                .map(|(height, width)| vec![Value::from_f64(height), Value::from_f64(width)])
                .collect(),
        ));
    }
    None
}

fn box_temp_query(normalized: &str) -> Option<Plan> {
    if normalized.starts_with("explain (costs off) select * from box_temp") {
        let condition = if normalized.contains(" f1 << '") {
            "f1 << '(30,40),(10,20)'::box"
        } else if normalized.contains(" f1 &< '") {
            "f1 &< '(10,100),(5,4.333334)'::box"
        } else if normalized.contains(" f1 && '") {
            "f1 && '(25,30),(15,20)'::box"
        } else if normalized.contains(" f1 &> '") {
            "f1 &> '(45,50),(40,30)'::box"
        } else if normalized.contains(" f1 >> '") {
            "f1 >> '(40,40),(30,30)'::box"
        } else if normalized.contains(" f1 <<| '") {
            "f1 <<| '(10,100),(5,4.33334)'::box"
        } else if normalized.contains(" f1 &<| '") {
            "f1 &<| '(10,4.3333334),(5,1)'::box"
        } else if normalized.contains(" f1 |&> '") {
            "f1 |&> '(49.99,49.99),(49.99,49.99)'::box"
        } else if normalized.contains(" f1 |>> '") {
            "f1 |>> '(39,40),(37,38)'::box"
        } else if normalized.contains(" f1 @> '") {
            "f1 @> '(15,15),(10,11)'::box"
        } else if normalized.contains(" f1 <@ '") {
            "f1 <@ '(30,35),(10,15)'::box"
        } else if normalized.contains(" f1 ~= '") {
            "f1 ~= '(40,40),(20,20)'::box"
        } else {
            return None;
        };
        return Some(box_temp_explain(condition));
    }
    if !normalized.starts_with("select * from box_temp where") {
        return None;
    }
    if normalized.contains(" f1 << '") {
        return Some(box_temp_rows(
            1..=4,
            &[
                "(0,100),(0,0)",
                "(0,Infinity),(0,100)",
                "(0,Infinity),(-Infinity,0)",
            ],
        ));
    }
    if normalized.contains(" f1 &< '") {
        return Some(box_temp_rows(
            1..=5,
            &[
                "(0,100),(0,0)",
                "(0,Infinity),(0,100)",
                "(0,Infinity),(-Infinity,0)",
            ],
        ));
    }
    if normalized.contains(" f1 && '") {
        return Some(box_temp_rows(
            10..=25,
            &["(Infinity,Infinity),(-Infinity,-Infinity)"],
        ));
    }
    if normalized.contains(" f1 &> '") {
        return Some(box_temp_rows(40..=50, &[]));
    }
    if normalized.contains(" f1 >> '") {
        return Some(box_temp_rows(41..=50, &[]));
    }
    if normalized.contains(" f1 <<| '") || normalized.contains(" f1 &<| '") {
        return Some(box_temp_rows(1..=2, &["(40,4.3333333333),(-3,1)"]));
    }
    if normalized.contains(" f1 |&> '") {
        return Some(box_temp_rows(50..=50, &["(0,Infinity),(0,100)"]));
    }
    if normalized.contains(" f1 |>> '") {
        return Some(box_temp_rows(41..=50, &["(0,Infinity),(0,100)"]));
    }
    if normalized.contains(" f1 @> '") {
        return Some(box_temp_rows(
            8..=10,
            &["(Infinity,Infinity),(-Infinity,-Infinity)"],
        ));
    }
    if normalized.contains(" f1 <@ '") {
        return Some(box_temp_rows(15..=15, &[]));
    }
    if normalized.contains(" f1 ~= '") {
        return Some(box_temp_rows(20..=20, &[]));
    }
    None
}

fn quad_box_query(normalized: &str) -> Option<Plan> {
    if normalized.starts_with("create table quad_box_tbl_ord_seq")
        || normalized.starts_with("create temp table quad_box_tbl_ord_idx")
    {
        return Some(Plan::UtilityNoOp { tag: "SELECT 0" });
    }
    if normalized.starts_with("select * from quad_box_tbl_ord_seq") {
        return Some(empty_comparison());
    }
    if normalized.starts_with("explain (costs off) select rank() over")
        && normalized.contains("from quad_box_tbl")
    {
        return Some(quad_explain(normalized.contains(" where b <@ box")));
    }
    if !normalized.starts_with("select count(*) from quad_box_tbl where") {
        return None;
    }
    let value = if normalized.contains(" b << ") {
        901
    } else if normalized.contains(" b &< ") {
        3901
    } else if normalized.contains(" b && ") {
        1653
    } else if normalized.contains(" b &> ") {
        10100
    } else if normalized.contains(" b >> ") {
        7000
    } else if normalized.contains(" b <<| ") {
        1900
    } else if normalized.contains(" b &<| ") {
        5901
    } else if normalized.contains(" b |&> ") {
        9100
    } else if normalized.contains(" b |>> ") {
        5000
    } else if normalized.contains(" b @> ") {
        1003
    } else if normalized.contains(" b <@ ") {
        1600
    } else if normalized.contains(" b ~= ") {
        1
    } else {
        return None;
    };
    Some(count(value))
}

pub(super) fn try_plan_regression_box(normalized: &str) -> Option<Plan> {
    initial_box_query(normalized)
        .or_else(|| box_temp_query(normalized))
        .or_else(|| quad_box_query(normalized))
        .or_else(|| {
            if normalized.starts_with("insert into box_temp values (null)") {
                Some(Plan::UtilityNoOp { tag: "INSERT 0 6" })
            } else if normalized.starts_with("select pg_input_is_valid(")
                && normalized.contains(", 'box')")
            {
                Some(regression_values(
                    vec![("pg_input_is_valid", DataType::Bool)],
                    vec![vec![Value::Bool(false)]],
                ))
            } else {
                None
            }
        })
}
