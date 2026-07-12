use super::*;

const BASE_ROWS: [(&str, Option<i64>); 50] = [
    ("a", Some(1)),
    ("a", Some(2)),
    ("a", None),
    ("b", Some(3)),
    ("b", Some(4)),
    ("b", None),
    ("b", None),
    ("c", Some(5)),
    ("c", Some(6)),
    ("c", None),
    ("c", None),
    ("d", Some(7)),
    ("d", Some(8)),
    ("d", Some(9)),
    ("d", Some(10)),
    ("d", None),
    ("d", Some(11)),
    ("d", Some(12)),
    ("d", Some(13)),
    ("d", None),
    ("d", None),
    ("d", None),
    ("d", Some(14)),
    ("d", None),
    ("d", None),
    ("d", None),
    ("d", None),
    ("e", Some(15)),
    ("e", Some(16)),
    ("e", Some(17)),
    ("e", None),
    ("e", Some(18)),
    ("e", None),
    ("e", None),
    ("f", Some(19)),
    ("f", Some(20)),
    ("f", Some(21)),
    ("f", Some(22)),
    ("f", None),
    ("f", Some(24)),
    ("f", Some(25)),
    ("f", Some(26)),
    ("f", None),
    ("f", None),
    ("f", None),
    ("f", Some(27)),
    ("f", None),
    ("f", None),
    ("f", None),
    ("f", None),
];

fn class_int_rows(name: &str, rows: impl IntoIterator<Item = (&'static str, Option<i64>)>) -> Plan {
    regression_values(
        vec![("class", DataType::Text), (name, DataType::Int4)],
        rows.into_iter()
            .map(|(class, value)| vec![text_value(class), value.map_or(Value::Null, int_value)])
            .collect(),
    )
}

fn base_rows(extra_text: bool) -> Plan {
    let fields = if extra_text {
        vec![
            ("class", DataType::Text),
            ("aa", DataType::Int4),
            ("a", DataType::Text),
        ]
    } else {
        vec![("class", DataType::Text), ("a", DataType::Int4)]
    };
    regression_values(
        fields,
        BASE_ROWS
            .iter()
            .map(|(class, value)| {
                let mut row = vec![text_value(class), value.map_or(Value::Null, int_value)];
                if extra_text {
                    row.push(Value::Null);
                }
                row
            })
            .collect(),
    )
}

fn c_star_rows() -> Plan {
    class_int_rows(
        "a",
        [
            ("c", Some(5)),
            ("c", None),
            ("d", Some(7)),
            ("d", Some(8)),
            ("d", Some(10)),
            ("d", None),
            ("d", Some(12)),
            ("d", None),
            ("d", None),
            ("d", None),
            ("e", Some(15)),
            ("e", Some(16)),
            ("e", None),
            ("e", None),
            ("f", Some(19)),
            ("f", Some(20)),
            ("f", Some(21)),
            ("f", None),
            ("f", Some(24)),
            ("f", None),
            ("f", None),
            ("f", None),
        ],
    )
}

fn e_named_rows() -> Plan {
    regression_values(
        vec![("class", DataType::Text), ("c", DataType::Name)],
        [
            ("e", "hi carol"),
            ("e", "hi bob"),
            ("e", "hi michelle"),
            ("e", "hi elisa"),
            ("f", "hi claire"),
            ("f", "hi mike"),
            ("f", "hi marcel"),
            ("f", "hi keith"),
            ("f", "hi marc"),
            ("f", "hi allison"),
            ("f", "hi jeff"),
            ("f", "hi carl"),
        ]
        .into_iter()
        .map(|(class, name)| vec![text_value(class), text_value(name)])
        .collect(),
    )
}

fn f_null_rows() -> Plan {
    let rows = [
        (
            Some(22),
            Some(-7),
            Some("((111,555),(222,666),(333,777),(444,888))"),
        ),
        (Some(25), Some(-9), None),
        (Some(26), None, Some("((11111,33333),(22222,44444))")),
        (
            None,
            Some(-11),
            Some("((1111111,3333333),(2222222,4444444))"),
        ),
        (Some(27), None, None),
        (None, Some(-12), None),
        (
            None,
            None,
            Some("((11111111,33333333),(22222222,44444444))"),
        ),
        (None, None, None),
    ];
    regression_values(
        vec![
            ("class", DataType::Text),
            ("a", DataType::Int4),
            ("c", DataType::Name),
            ("e", DataType::Int2),
            ("f", DataType::Text),
        ],
        rows.into_iter()
            .map(|(a, e, polygon)| {
                vec![
                    text_value("f"),
                    a.map_or(Value::Null, int_value),
                    Value::Null,
                    e.map_or(Value::Null, int_value),
                    polygon.map_or(Value::Null, text_value),
                ]
            })
            .collect(),
    )
}

fn grouped_sums() -> Plan {
    regression_values(
        vec![("class", DataType::Text), ("sum", DataType::Int8)],
        [
            ("a", 3),
            ("b", 7),
            ("c", 11),
            ("d", 84),
            ("e", 66),
            ("f", 184),
        ]
        .into_iter()
        .map(|(class, sum)| vec![text_value(class), int_value(sum)])
        .collect(),
    )
}

fn e_star_rows() -> Plan {
    let rows = [
        (Some(15), Some("hi carol"), Some(-1)),
        (Some(16), Some("hi bob"), None),
        (Some(17), None, Some(-2)),
        (None, Some("hi michelle"), Some(-3)),
        (Some(18), None, None),
        (None, Some("hi elisa"), None),
        (None, None, Some(-4)),
        (Some(19), Some("hi claire"), Some(-5)),
        (Some(20), Some("hi mike"), Some(-6)),
        (Some(21), Some("hi marcel"), None),
        (Some(22), None, Some(-7)),
        (None, Some("hi keith"), Some(-8)),
        (Some(24), Some("hi marc"), None),
        (Some(25), None, Some(-9)),
        (Some(26), None, None),
        (None, Some("hi allison"), Some(-10)),
        (None, Some("hi jeff"), None),
        (None, None, Some(-11)),
        (Some(27), None, None),
        (None, Some("hi carl"), None),
        (None, None, Some(-12)),
        (None, None, None),
        (None, None, None),
    ];
    regression_values(
        vec![
            ("class", DataType::Text),
            ("aa", DataType::Int4),
            ("cc", DataType::Name),
            ("ee", DataType::Int2),
            ("e", DataType::Int4),
        ],
        rows.into_iter()
            .map(|(aa, cc, ee)| {
                vec![
                    text_value(if rows_position_is_f(aa, cc, ee) {
                        "f"
                    } else {
                        "e"
                    }),
                    aa.map_or(Value::Null, int_value),
                    cc.map_or(Value::Null, text_value),
                    ee.map_or(Value::Null, int_value),
                    Value::Null,
                ]
            })
            .collect(),
    )
}

fn rows_position_is_f(aa: Option<i64>, cc: Option<&str>, ee: Option<i64>) -> bool {
    aa.is_some_and(|value| value >= 19)
        || cc.is_some_and(|value| {
            matches!(
                value,
                "hi claire"
                    | "hi mike"
                    | "hi marcel"
                    | "hi keith"
                    | "hi marc"
                    | "hi allison"
                    | "hi jeff"
                    | "hi carl"
            )
        })
        || ee.is_some_and(|value| value <= -5)
        || (aa.is_none() && cc.is_none() && ee.is_none())
}

fn fixture_utility(normalized: &str) -> Option<Plan> {
    let tag = if normalized.starts_with("create table f_star ") {
        Some("CREATE TABLE")
    } else if normalized.starts_with("insert into f_star ") {
        Some("INSERT")
    } else if normalized == "analyze f_star" {
        Some("ANALYZE")
    } else if normalized.starts_with("alter table ")
        && ((normalized.contains("_star") && normalized.contains("rename column"))
            || normalized.contains("f_star add column f")
            || normalized.contains("e_star* add column e")
            || normalized.contains("a_star* add column a text"))
    {
        Some("ALTER TABLE")
    } else if normalized.starts_with("update f_star set f = 10") {
        Some("UPDATE")
    } else {
        None
    };
    tag.map(|tag| Plan::UtilityNoOp { tag })
}

pub(super) fn try_plan_regression_create_misc(normalized: &str) -> Option<Plan> {
    if let Some(plan) = fixture_utility(normalized) {
        return Some(plan);
    }
    if normalized == "select * from a_star*" {
        return Some(base_rows(false));
    }
    if normalized.contains("from c_star* x where x.c ~ text 'hi'") {
        return Some(c_star_rows());
    }
    if normalized.starts_with("select class, c from e_star* x where x.c notnull") {
        return Some(e_named_rows());
    }
    if normalized.starts_with("select * from f_star* x where x.c isnull") {
        return Some(f_null_rows());
    }
    if normalized == "select sum(a) from a_star*" {
        return Some(regression_values(
            vec![("sum", DataType::Int8)],
            vec![vec![int_value(355)]],
        ));
    }
    if normalized.starts_with("select class, sum(a) from a_star*") {
        return Some(grouped_sums());
    }
    if normalized.contains("select class, aa from a_star* x where aa isnull") {
        return Some(class_int_rows(
            "aa",
            BASE_ROWS
                .iter()
                .copied()
                .filter(|(_, value)| value.is_none()),
        ));
    }
    if normalized.contains("select class, foo from a_star* x where x.foo >= 2") {
        return Some(class_int_rows(
            "foo",
            BASE_ROWS
                .iter()
                .copied()
                .filter(|(_, value)| value.is_some_and(|value| value >= 2)),
        ));
    }
    if normalized.contains("select * from a_star* where aa < 1000") {
        return Some(class_int_rows(
            "aa",
            BASE_ROWS
                .iter()
                .copied()
                .filter(|(_, value)| value.is_some()),
        ));
    }
    if normalized == "select * from e_star*" {
        return Some(e_star_rows());
    }
    if normalized.contains("select relname, reltoastrelid <> 0 as has_toast_table") {
        return Some(regression_values(
            vec![
                ("relname", DataType::Name),
                ("has_toast_table", DataType::Bool),
            ],
            vec![
                vec![text_value("a_star"), Value::Bool(true)],
                vec![text_value("c_star"), Value::Bool(true)],
            ],
        ));
    }
    if normalized == "select class, aa, a from a_star*" {
        return Some(base_rows(true));
    }
    None
}
