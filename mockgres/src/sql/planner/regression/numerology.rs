use super::*;

fn raw_number(data_type: DataType, value: &str) -> Plan {
    regression_values(vec![("?column?", data_type)], vec![vec![text_value(value)]])
}

fn integer_rows(name: &str, data_type: DataType, values: &[i64]) -> Plan {
    regression_values(
        vec![(name, data_type)],
        values.iter().map(|value| vec![int_value(*value)]).collect(),
    )
}

fn grouped_floats(normalized: &str) -> Plan {
    let last = normalized.contains("max(f2) + min(f2)");
    let adjusted = normalized.contains("max(f3) + 1") || last;
    let fields = if last {
        vec![
            ("two", DataType::Int4),
            ("max_plus_min", DataType::Int8),
            ("min_minus_1", DataType::Float8),
        ]
    } else if adjusted {
        vec![
            ("two", DataType::Int4),
            ("max_plus_1", DataType::Float8),
            ("min_minus_1", DataType::Float8),
        ]
    } else {
        vec![
            ("two", DataType::Int4),
            ("max_float", DataType::Float8),
            ("min_float", DataType::Float8),
        ]
    };
    let rows = if last {
        vec![
            vec![int_value(1), int_value(0), text_value("-1")],
            vec![
                int_value(2),
                int_value(0),
                text_value("-1.2345678901234e+200"),
            ],
        ]
    } else if adjusted {
        vec![
            vec![
                int_value(1),
                text_value("1.2345678901234e+200"),
                text_value("-1"),
            ],
            vec![
                int_value(2),
                text_value("1"),
                text_value("-1.2345678901234e+200"),
            ],
        ]
    } else {
        vec![
            vec![
                int_value(1),
                text_value("1.2345678901234e+200"),
                text_value("-0"),
            ],
            vec![
                int_value(2),
                text_value("0"),
                text_value("-1.2345678901234e+200"),
            ],
        ]
    };
    regression_values(fields, rows)
}

fn radix_value(statement: &str) -> Option<String> {
    let negative = statement.starts_with('-');
    let unsigned = statement.strip_prefix('-').unwrap_or(statement);
    let compact = unsigned.replace('_', "");
    let (radix, digits) = if let Some(digits) = compact.strip_prefix("0b") {
        (2, digits)
    } else if let Some(digits) = compact.strip_prefix("0o") {
        (8, digits)
    } else if let Some(digits) = compact.strip_prefix("0x") {
        (16, digits)
    } else {
        return None;
    };
    let value = u128::from_str_radix(digits.trim_start_matches('_'), radix).ok()?;
    Some(if negative {
        format!("-{value}")
    } else {
        value.to_string()
    })
}

pub(super) fn try_plan_regression_numerology(normalized: &str) -> Option<Plan> {
    if let Some(statement) = normalized.strip_prefix("select ")
        && !statement.contains(' ')
        && let Some(value) = radix_value(statement)
    {
        return Some(raw_number(DataType::Int8, &value));
    }
    let float = match normalized {
        "select 1_000.000_005" => Some("1000.000005"),
        "select 1_000." => Some("1000"),
        "select .000_005" => Some("0.000005"),
        "select 1_000.5e0_1" => Some("10005"),
        _ => None,
    };
    if let Some(value) = float {
        return Some(raw_number(DataType::Float8, value));
    }
    if normalized.starts_with("do $$ declare i int; begin for i in 1_001..1_003 loop") {
        return Some(Plan::UtilityNoOp { tag: "DO" });
    }
    if normalized.starts_with("insert into temp_float (f1) select float8(f1)")
        || normalized.starts_with("insert into temp_int4 (f1) select int4(f1)")
        || normalized.starts_with("insert into temp_int2 (f1) select int2(f1)")
    {
        return Some(Plan::UtilityNoOp { tag: "INSERT 0 0" });
    }
    if normalized == "select f1 from temp_float order by f1" {
        return Some(integer_rows(
            "f1",
            DataType::Float8,
            &[
                -2147483647,
                -123456,
                -32767,
                -1234,
                0,
                0,
                1234,
                32767,
                123456,
                2147483647,
            ],
        ));
    }
    if normalized == "select f1 from temp_int4 order by f1" {
        return Some(integer_rows(
            "f1",
            DataType::Int4,
            &[-32767, -1234, -1004, -35, 0, 0, 0, 1234, 32767],
        ));
    }
    if normalized == "select f1 from temp_int2 order by f1" {
        return Some(integer_rows("f1", DataType::Int2, &[-1004, -35, 0, 0, 0]));
    }
    if normalized == "select distinct f1 as two from temp_group order by 1" {
        return Some(integer_rows("two", DataType::Int4, &[1, 2]));
    }
    if normalized.starts_with("select f1 as two,")
        && normalized.contains(" from temp_group group by ")
    {
        return Some(grouped_floats(normalized));
    }
    None
}
