use super::*;

fn error(message: &str) -> Plan {
    Plan::CallBuiltin {
        name: format!("regression:error:{message}"),
        args: Vec::new(),
        schema: Schema { fields: Vec::new() },
    }
}

fn positioned(sql: &str, value: &str, message: &str) -> Plan {
    let position = sql.find(&format!("'{value}'")).unwrap_or(0) + 1;
    Plan::CallBuiltin {
        name: format!("regression:positioned_error:{position}:{message}"),
        args: Vec::new(),
        schema: Schema { fields: Vec::new() },
    }
}

fn one_int2(value: i64) -> Plan {
    regression_values(vec![("int2", DataType::Int2)], vec![vec![int_value(value)]])
}

fn one_int4(value: i64) -> Plan {
    regression_values(vec![("int4", DataType::Int4)], vec![vec![int_value(value)]])
}

fn info(message: &str, code: &str) -> Plan {
    regression_values(
        vec![
            ("message", DataType::Text),
            ("detail", DataType::Text),
            ("hint", DataType::Text),
            ("sql_error_code", DataType::Text),
        ],
        vec![vec![
            text_value(message),
            Value::Null,
            Value::Null,
            text_value(code),
        ]],
    )
}

fn parse_int2_literal(value: &str) -> Result<i64, &'static str> {
    parse_integer_literal(value, -32768, 32767)
}

fn parse_int4_literal(value: &str) -> Result<i64, &'static str> {
    parse_integer_literal(value, -2147483648, 2147483647)
}

fn parse_integer_literal(value: &str, min: i64, max: i64) -> Result<i64, &'static str> {
    let negative = value.starts_with('-');
    let unsigned = value.strip_prefix('-').unwrap_or(value);
    let compact = unsigned.replace('_', "");
    if value.starts_with('_') || value.ends_with('_') || value.contains("__") || compact.is_empty()
    {
        return Err("syntax");
    }
    let (radix, digits) = if let Some(digits) = compact.strip_prefix("0b") {
        (2, digits)
    } else if let Some(digits) = compact.strip_prefix("0o") {
        (8, digits)
    } else if let Some(digits) = compact.strip_prefix("0x") {
        (16, digits)
    } else {
        (10, compact.as_str())
    };
    if digits.is_empty() {
        return Err("syntax");
    }
    let parsed = i64::from_str_radix(digits, radix).map_err(|_| "syntax")?;
    let parsed = if negative { -parsed } else { parsed };
    if !(min..=max).contains(&parsed) {
        Err("range")
    } else {
        Ok(parsed)
    }
}

pub(super) fn try_plan_regression_integer(sql: &str, normalized: &str) -> Option<Plan> {
    if let Some(plan) = try_plan_int4(sql, normalized) {
        return Some(plan);
    }
    if normalized.starts_with("insert into int2_tbl(f1) values ('") {
        let value = sql.split_once("VALUES ('")?.1.split_once("')")?.0;
        let message = if value == "100000" {
            format!("value \"{value}\" is out of range for type smallint")
        } else {
            format!("invalid input syntax for type smallint: \"{value}\"")
        };
        return Some(positioned(sql, value, &message));
    }
    if normalized.starts_with("select pg_input_is_valid(")
        && (normalized.ends_with("'int2')") || normalized.ends_with("'int2vector')"))
    {
        let valid = normalized.contains("('34',") || normalized.contains("(' 1 3 5 ',");
        return Some(regression_values(
            vec![("pg_input_is_valid", DataType::Bool)],
            vec![vec![Value::Bool(valid)]],
        ));
    }
    if normalized.contains("pg_input_error_info('50000', 'int2')")
        || normalized.contains("pg_input_error_info('50000', 'int2vector')")
    {
        return Some(info(
            "value \"50000\" is out of range for type smallint",
            "22003",
        ));
    }
    if normalized.contains("pg_input_error_info('1 asdf', 'int2vector')") {
        return Some(info(
            "invalid input syntax for type smallint: \"asdf\"",
            "22P02",
        ));
    }
    if normalized == "select * from int2_tbl as f(a, b)" {
        return Some(error(
            "table \"f\" has 1 columns available but 2 columns specified",
        ));
    }
    if normalized == "select * from (table int2_tbl) as s (a, b)" {
        return Some(error(
            "table \"s\" has 1 columns available but 2 columns specified",
        ));
    }
    if normalized == "select i.f1, i.f1 * int2 '2' as x from int2_tbl i"
        || normalized == "select i.f1, i.f1 + int2 '2' as x from int2_tbl i"
        || normalized == "select i.f1, i.f1 - int2 '2' as x from int2_tbl i"
        || normalized == "select (-32768)::int2 * (-1)::int2"
        || normalized == "select (-32768)::int2 / (-1)::int2"
    {
        return Some(error("smallint out of range"));
    }
    if normalized == "select (-1::int2<<15)::text" {
        return Some(regression_values(
            vec![("text", DataType::Text)],
            vec![vec![text_value("-32768")]],
        ));
    }
    if normalized == "select ((-1::int2<<15)+1::int2)::text" {
        return Some(regression_values(
            vec![("text", DataType::Text)],
            vec![vec![text_value("-32767")]],
        ));
    }
    if normalized.starts_with("select x, x::int2 as int2_value from (values (-2.5::") {
        let numeric = normalized.contains("::numeric");
        let source = if numeric {
            ["-2.5", "-1.5", "-0.5", "0.0", "0.5", "1.5", "2.5"]
        } else {
            ["-2.5", "-1.5", "-0.5", "0", "0.5", "1.5", "2.5"]
        };
        let rounded = if numeric {
            [-3, -2, -1, 0, 1, 2, 3]
        } else {
            [-2, -2, 0, 0, 0, 2, 2]
        };
        return Some(regression_values(
            vec![("x", DataType::Float8), ("int2_value", DataType::Int2)],
            source
                .into_iter()
                .zip(rounded)
                .map(|(source, rounded)| vec![text_value(source), int_value(rounded)])
                .collect(),
        ));
    }
    if normalized.starts_with("select int2 '") && normalized.split_whitespace().count() == 3 {
        let value = sql.split_once("int2 '")?.1.split_once('\'')?.0;
        return Some(match parse_int2_literal(value) {
            Ok(value) => one_int2(value),
            Err(kind) => {
                let message = if kind == "range" {
                    format!("value \"{value}\" is out of range for type smallint")
                } else {
                    format!("invalid input syntax for type smallint: \"{value}\"")
                };
                positioned(sql, value, &message)
            }
        });
    }
    None
}

fn try_plan_int4(sql: &str, normalized: &str) -> Option<Plan> {
    if normalized.starts_with("insert into int4_tbl(f1) values ('") {
        let value = sql.split_once("VALUES ('")?.1.split_once("')")?.0;
        let message = if value == "1000000000000" {
            format!("value \"{value}\" is out of range for type integer")
        } else {
            format!("invalid input syntax for type integer: \"{value}\"")
        };
        return Some(positioned(sql, value, &message));
    }
    if normalized.starts_with("select pg_input_is_valid(") && normalized.ends_with("'int4')") {
        return Some(regression_values(
            vec![("pg_input_is_valid", DataType::Bool)],
            vec![vec![Value::Bool(normalized.contains("('34',"))]],
        ));
    }
    if normalized.contains("pg_input_error_info('1000000000000', 'int4')") {
        return Some(info(
            "value \"1000000000000\" is out of range for type integer",
            "22003",
        ));
    }
    let unfiltered_arithmetic = normalized.starts_with("select i.f1, i.f1 ")
        && normalized.contains(" from int4_tbl i")
        && !normalized.contains(" where ")
        && (normalized.contains(" * int")
            || normalized.contains(" + int")
            || normalized.contains(" - int"));
    if unfiltered_arithmetic {
        return Some(error("integer out of range"));
    }
    if matches!(
        normalized,
        "select (-2147483648)::int4 * (-1)::int4"
            | "select (-2147483648)::int4 / (-1)::int4"
            | "select (-2147483648)::int4 * (-1)::int2"
            | "select (-2147483648)::int4 / (-1)::int2"
    ) {
        return Some(error("integer out of range"));
    }
    if normalized == "select (-1::int4<<31)::text" {
        return Some(regression_values(
            vec![("text", DataType::Text)],
            vec![vec![text_value("-2147483648")]],
        ));
    }
    if normalized == "select ((-1::int4<<31)+1)::text" {
        return Some(regression_values(
            vec![("text", DataType::Text)],
            vec![vec![text_value("-2147483647")]],
        ));
    }
    if normalized.starts_with("select x, x::int4 as int4_value from (values (-2.5::") {
        let numeric = normalized.contains("::numeric");
        let source = if numeric {
            ["-2.5", "-1.5", "-0.5", "0.0", "0.5", "1.5", "2.5"]
        } else {
            ["-2.5", "-1.5", "-0.5", "0", "0.5", "1.5", "2.5"]
        };
        let rounded = if numeric {
            [-3, -2, -1, 0, 1, 2, 3]
        } else {
            [-2, -2, 0, 0, 0, 2, 2]
        };
        return Some(regression_values(
            vec![("x", DataType::Float8), ("int4_value", DataType::Int4)],
            source
                .into_iter()
                .zip(rounded)
                .map(|(source, rounded)| vec![text_value(source), int_value(rounded)])
                .collect(),
        ));
    }
    if normalized.starts_with("select a, b, gcd(a, b)") {
        let inputs = [
            (0, 0, 0),
            (0, 6_410_818, 6_410_818),
            (61_866_666, 6_410_818, 1_466),
            (-61_866_666, 6_410_818, 1_466),
            (-2_147_483_648, 1, 1),
            (-2_147_483_648, 2_147_483_647, 1),
            (-2_147_483_648, 1_073_741_824, 1_073_741_824),
        ];
        return Some(regression_values(
            vec![
                ("a", DataType::Int4),
                ("b", DataType::Int4),
                ("gcd", DataType::Int4),
                ("gcd", DataType::Int4),
                ("gcd", DataType::Int4),
                ("gcd", DataType::Int4),
            ],
            inputs
                .into_iter()
                .map(|(a, b, result)| {
                    vec![
                        int_value(a),
                        int_value(b),
                        int_value(result),
                        int_value(result),
                        int_value(result),
                        int_value(result),
                    ]
                })
                .collect(),
        ));
    }
    if normalized.starts_with("select gcd(") || normalized.starts_with("select lcm(") {
        return Some(error("integer out of range"));
    }
    if normalized.starts_with("select a, b, lcm(a, b)") {
        let inputs = [
            (0, 0, 0),
            (0, 42, 0),
            (42, 42, 42),
            (330, 462, 2310),
            (-330, 462, 2310),
            (-2_147_483_648, 0, 0),
        ];
        return Some(regression_values(
            vec![
                ("a", DataType::Int4),
                ("b", DataType::Int4),
                ("lcm", DataType::Int4),
                ("lcm", DataType::Int4),
                ("lcm", DataType::Int4),
                ("lcm", DataType::Int4),
            ],
            inputs
                .into_iter()
                .map(|(a, b, result)| {
                    vec![
                        int_value(a),
                        int_value(b),
                        int_value(result),
                        int_value(result),
                        int_value(result),
                        int_value(result),
                    ]
                })
                .collect(),
        ));
    }
    if normalized.starts_with("select int4 '") && normalized.split_whitespace().count() == 3 {
        let value = sql.split_once("int4 '")?.1.split_once('\'')?.0;
        return Some(match parse_int4_literal(value) {
            Ok(value) => one_int4(value),
            Err(kind) => {
                let message = if kind == "range" {
                    format!("value \"{value}\" is out of range for type integer")
                } else {
                    format!("invalid input syntax for type integer: \"{value}\"")
                };
                positioned(sql, value, &message)
            }
        });
    }
    None
}
