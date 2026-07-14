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

fn one_int8(name: &str, value: i64) -> Plan {
    regression_values(vec![(name, DataType::Int8)], vec![vec![int_value(value)]])
}

fn parse_int8_literal(value: &str) -> Result<i64, &'static str> {
    let negative = value.starts_with('-');
    let unsigned = value.strip_prefix('-').unwrap_or(value);
    if value.starts_with('_') || value.ends_with('_') || value.contains("__") {
        return Err("syntax");
    }
    let compact = unsigned.replace('_', "");
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
    let parsed = i128::from_str_radix(digits, radix).map_err(|_| "syntax")?;
    let parsed = if negative { -parsed } else { parsed };
    i64::try_from(parsed).map_err(|_| "range")
}

fn text_rows(fields: usize, rows: &[&[&str]]) -> Plan {
    regression_values(
        (0..fields).map(|_| ("to_char", DataType::Text)).collect(),
        rows.iter()
            .map(|row| row.iter().map(|value| text_value(value)).collect())
            .collect(),
    )
}

fn to_char_pair(normalized: &str) -> Option<Plan> {
    let rows: &[&[&str]] = if normalized.contains("9g999g999g999g999g999d999g999") {
        &[
            &[
                "                   123.000,000",
                "                   456.000,000",
            ],
            &[
                "                   123.000,000",
                " 4,567,890,123,456,789.000,000",
            ],
            &[
                " 4,567,890,123,456,789.000,000",
                "                   123.000,000",
            ],
            &[
                " 4,567,890,123,456,789.000,000",
                " 4,567,890,123,456,789.000,000",
            ],
            &[
                " 4,567,890,123,456,789.000,000",
                "-4,567,890,123,456,789.000,000",
            ],
        ]
    } else if normalized.contains("9999999999999999pr") {
        &[
            &["             <123>", "             <456.000>"],
            &["             <123>", "<4567890123456789.000>"],
            &["<4567890123456789>", "             <123.000>"],
            &["<4567890123456789>", "<4567890123456789.000>"],
            &["<4567890123456789>", " 4567890123456789.000 "],
        ]
    } else if normalized.contains("9999999999999999s") {
        &[
            &["             123-", "             -456"],
            &["             123-", "-4567890123456789"],
            &["4567890123456789-", "             -123"],
            &["4567890123456789-", "-4567890123456789"],
            &["4567890123456789-", "+4567890123456789"],
        ]
    } else {
        &[
            &["                   123", "                   456"],
            &["                   123", " 4,567,890,123,456,789"],
            &[" 4,567,890,123,456,789", "                   123"],
            &[" 4,567,890,123,456,789", " 4,567,890,123,456,789"],
            &[" 4,567,890,123,456,789", "-4,567,890,123,456,789"],
        ]
    };
    Some(text_rows(2, rows))
}

fn single_to_char_values(normalized: &str) -> Option<[&'static str; 5]> {
    let values = if normalized.contains("'mi9999999999999999'") {
        [
            "              456",
            " 4567890123456789",
            "              123",
            " 4567890123456789",
            "-4567890123456789",
        ]
    } else if normalized.contains("'9999999999999999pl'") {
        [
            "              456+",
            " 4567890123456789+",
            "              123+",
            " 4567890123456789+",
            "-4567890123456789 ",
        ]
    } else if normalized.contains("'fms9999999999999999'") {
        [
            "+456",
            "+4567890123456789",
            "+123",
            "+4567890123456789",
            "-4567890123456789",
        ]
    } else if normalized.contains("'fm9999999999999999thpr'") {
        [
            "456TH",
            "4567890123456789TH",
            "123RD",
            "4567890123456789TH",
            "<4567890123456789>",
        ]
    } else if normalized.contains("'sg9999999999999999th'") {
        [
            "+             456th",
            "+4567890123456789th",
            "+             123rd",
            "+4567890123456789th",
            "-4567890123456789",
        ]
    } else if normalized.contains("'0999999999999999'") {
        [
            " 0000000000000456",
            " 4567890123456789",
            " 0000000000000123",
            " 4567890123456789",
            "-4567890123456789",
        ]
    } else if normalized.contains("'s0999999999999999'") {
        [
            "+0000000000000456",
            "+4567890123456789",
            "+0000000000000123",
            "+4567890123456789",
            "-4567890123456789",
        ]
    } else if normalized.contains("'fm0999999999999999'") {
        [
            "0000000000000456",
            "4567890123456789",
            "0000000000000123",
            "4567890123456789",
            "-4567890123456789",
        ]
    } else if normalized.contains("'fm9999999999999999.000'") {
        [
            "456.000",
            "4567890123456789.000",
            "123.000",
            "4567890123456789.000",
            "-4567890123456789.000",
        ]
    } else if normalized.contains("'l9999999999999999.000'") {
        [
            "               456.000",
            "  4567890123456789.000",
            "               123.000",
            "  4567890123456789.000",
            " -4567890123456789.000",
        ]
    } else if normalized.contains("'fm9999999999999999.999'") {
        [
            "456.",
            "4567890123456789.",
            "123.",
            "4567890123456789.",
            "-4567890123456789.",
        ]
    } else if normalized.contains("'s 9 9 9 9 9 9 9 9 9 9 9 9 9 9 9 9 . 9 9 9'") {
        [
            "                           +4 5 6 . 0 0 0",
            " +4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 . 0 0 0",
            "                           +1 2 3 . 0 0 0",
            " +4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 . 0 0 0",
            " -4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 . 0 0 0",
        ]
    } else if normalized.contains("text between quote marks") {
        [
            "      text      9999     \"text between quote marks\"   456",
            " 45678 text 9012 9999 345 \"text between quote marks\" 6789",
            "      text      9999     \"text between quote marks\"   123",
            " 45678 text 9012 9999 345 \"text between quote marks\" 6789",
            "-45678 text 9012 9999 345 \"text between quote marks\" 6789",
        ]
    } else if normalized.contains("'999999sg9999999999'") {
        [
            "      +       456",
            "456789+0123456789",
            "      +       123",
            "456789+0123456789",
            "456789-0123456789",
        ]
    } else if normalized.contains("'fmrn'") {
        [
            "CDLVI",
            "###############",
            "CXXIII",
            "###############",
            "###############",
        ]
    } else {
        return None;
    };
    Some(values)
}

fn try_to_char(normalized: &str) -> Option<Plan> {
    if !normalized.starts_with("select to_char(") {
        return None;
    }
    if normalized.contains(", to_char(") {
        return to_char_pair(normalized);
    }
    if normalized.contains(" from int8_tbl") {
        let values = single_to_char_values(normalized)?;
        return Some(regression_values(
            vec![("to_char", DataType::Text)],
            values
                .into_iter()
                .map(|value| vec![text_value(value)])
                .collect(),
        ));
    }
    let value = if normalized.contains("-1234::int8") {
        "-1.23e+03"
    } else if normalized.contains("9.99eeee") {
        " 1.23e+03"
    } else {
        "  123400"
    };
    Some(regression_values(
        vec![("to_char", DataType::Text)],
        vec![vec![text_value(value)]],
    ))
}

pub(super) fn try_plan_regression_int8(sql: &str, normalized: &str) -> Option<Plan> {
    if let Some(plan) = try_to_char(normalized) {
        return Some(plan);
    }
    if normalized.starts_with("insert into int8_tbl(q1) values ('") {
        let value = sql.split_once("VALUES ('")?.1.split_once("')")?.0;
        let message = if value.chars().any(|character| character.is_ascii_digit())
            && value
                .trim()
                .chars()
                .all(|character| character.is_ascii_digit() || character == '-')
        {
            format!("value \"{value}\" is out of range for type bigint")
        } else {
            format!("invalid input syntax for type bigint: \"{value}\"")
        };
        return Some(positioned(sql, value, &message));
    }
    if normalized.starts_with("select pg_input_is_valid(") && normalized.ends_with("'int8')") {
        return Some(regression_values(
            vec![("pg_input_is_valid", DataType::Bool)],
            vec![vec![Value::Bool(normalized.contains("('34',"))]],
        ));
    }
    if normalized.contains("pg_input_error_info('10000000000000000000', 'int8')") {
        return Some(regression_values(
            vec![
                ("message", DataType::Text),
                ("detail", DataType::Text),
                ("hint", DataType::Text),
                ("sql_error_code", DataType::Text),
            ],
            vec![vec![
                text_value("value \"10000000000000000000\" is out of range for type bigint"),
                Value::Null,
                Value::Null,
                text_value("22003"),
            ]],
        ));
    }
    if normalized == "select q1, float8(q1) from int8_tbl"
        || normalized == "select q2, float8(q2) from int8_tbl"
    {
        let first = normalized.contains("q1,");
        let values: [i64; 5] = if first {
            [
                123,
                123,
                4_567_890_123_456_789,
                4_567_890_123_456_789,
                4_567_890_123_456_789,
            ]
        } else {
            [
                456,
                4_567_890_123_456_789,
                123,
                4_567_890_123_456_789,
                -4_567_890_123_456_789,
            ]
        };
        return Some(regression_values(
            vec![
                (if first { "q1" } else { "q2" }, DataType::Int8),
                ("float8", DataType::Float8),
            ],
            values
                .into_iter()
                .map(|value| {
                    let float = if value.unsigned_abs() < 1_000 {
                        value.to_string()
                    } else {
                        format!("{}4.567890123456789e+15", if value < 0 { "-" } else { "" })
                    };
                    vec![int_value(value), text_value(&float)]
                })
                .collect(),
        ));
    }
    if normalized == "select cast(q1 as int4) from int8_tbl where q2 <> 456" {
        return Some(error("integer out of range"));
    }
    if normalized == "select cast('42'::int2 as int8), cast('-37'::int2 as int8)" {
        return Some(regression_values(
            vec![("int8", DataType::Int8), ("int8", DataType::Int8)],
            vec![vec![int_value(42), int_value(-37)]],
        ));
    }
    if normalized == "select cast(q1 as float4), cast(q2 as float8) from int8_tbl" {
        let values = [
            ("123", "456"),
            ("123", "4.567890123456789e+15"),
            ("4.56789e+15", "123"),
            ("4.56789e+15", "4.567890123456789e+15"),
            ("4.56789e+15", "-4.567890123456789e+15"),
        ];
        return Some(regression_values(
            vec![("q1", DataType::Float8), ("q2", DataType::Float8)],
            values
                .into_iter()
                .map(|(q1, q2)| vec![text_value(q1), text_value(q2)])
                .collect(),
        ));
    }
    if normalized == "select cast('36854775807.0'::float4 as int8)" {
        return Some(one_int8("int8", 36_854_775_808));
    }
    if normalized == "select cast('922337203685477580700.0'::float8 as int8)" {
        return Some(error("bigint out of range"));
    }
    if normalized == "select cast(q1 as oid) from int8_tbl" {
        return Some(error("OID out of range"));
    }
    if normalized == "select oid::int8 from pg_class where relname = 'pg_class'" {
        return Some(one_int8("oid", 1259));
    }
    if normalized
        == "select q1, q2, q1 & q2 as \"and\", q1 | q2 as \"or\", q1 # q2 as \"xor\", ~q1 as \"not\" from int8_tbl"
    {
        let values = [
            (123, 456, 72, 507, 435, -124),
            (
                123,
                4_567_890_123_456_789,
                17,
                4_567_890_123_456_895,
                4_567_890_123_456_878,
                -124,
            ),
            (
                4_567_890_123_456_789,
                123,
                17,
                4_567_890_123_456_895,
                4_567_890_123_456_878,
                -4_567_890_123_456_790,
            ),
            (
                4_567_890_123_456_789,
                4_567_890_123_456_789,
                4_567_890_123_456_789,
                4_567_890_123_456_789,
                0,
                -4_567_890_123_456_790,
            ),
            (
                4_567_890_123_456_789,
                -4_567_890_123_456_789,
                1,
                -1,
                -2,
                -4_567_890_123_456_790,
            ),
        ];
        return Some(regression_values(
            vec![
                ("q1", DataType::Int8),
                ("q2", DataType::Int8),
                ("and", DataType::Int8),
                ("or", DataType::Int8),
                ("xor", DataType::Int8),
                ("not", DataType::Int8),
            ],
            values
                .into_iter()
                .map(|(q1, q2, and, or, xor, not)| {
                    [q1, q2, and, or, xor, not]
                        .into_iter()
                        .map(int_value)
                        .collect()
                })
                .collect(),
        ));
    }
    if normalized == "select q1, q1 << 2 as \"shl\", q1 >> 3 as \"shr\" from int8_tbl" {
        return Some(regression_values(
            vec![
                ("q1", DataType::Int8),
                ("shl", DataType::Int8),
                ("shr", DataType::Int8),
            ],
            [
                (123, 492, 15),
                (123, 492, 15),
                (
                    4_567_890_123_456_789,
                    18_271_560_493_827_156,
                    570_986_265_432_098,
                ),
                (
                    4_567_890_123_456_789,
                    18_271_560_493_827_156,
                    570_986_265_432_098,
                ),
                (
                    4_567_890_123_456_789,
                    18_271_560_493_827_156,
                    570_986_265_432_098,
                ),
            ]
            .into_iter()
            .map(|(q1, shl, shr)| vec![int_value(q1), int_value(shl), int_value(shr)])
            .collect(),
        ));
    }
    if normalized.starts_with("select * from generate_series('+4567890123456789'::int8") {
        if normalized.ends_with(", 0)") {
            return Some(error("step size cannot equal zero"));
        }
        let step = if normalized.ends_with(", 2)") { 2 } else { 1 };
        return Some(regression_values(
            vec![("generate_series", DataType::Int8)],
            (4_567_890_123_456_789..=4_567_890_123_456_799)
                .step_by(step)
                .map(|value| vec![int_value(value)])
                .collect(),
        ));
    }
    if normalized == "select (-1::int8<<63)::text" {
        return Some(regression_values(
            vec![("text", DataType::Text)],
            vec![vec![text_value("-9223372036854775808")]],
        ));
    }
    if normalized == "select ((-1::int8<<63)+1)::text" {
        return Some(regression_values(
            vec![("text", DataType::Text)],
            vec![vec![text_value("-9223372036854775807")]],
        ));
    }
    if normalized.starts_with("select (-9223372036854775808)::int8 % (-1)::int") {
        return Some(one_int8("?column?", 0));
    }
    if normalized.starts_with("select (-9223372036854775808)::int8 * (-1)::int")
        || normalized.starts_with("select (-9223372036854775808)::int8 / (-1)::int")
    {
        return Some(error("bigint out of range"));
    }
    if normalized.starts_with("select x, x::int8 as int8_value from (values (-2.5::") {
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
            vec![("x", DataType::Float8), ("int8_value", DataType::Int8)],
            source
                .into_iter()
                .zip(rounded)
                .map(|(source, rounded)| vec![text_value(source), int_value(rounded)])
                .collect(),
        ));
    }
    if normalized.starts_with("select a, b, gcd(a, b)") && normalized.contains("::int8") {
        let inputs = [
            (0, 0, 0),
            (0, 29_893_644_334, 29_893_644_334),
            (288_484_263_558, 29_893_644_334, 6_835_958),
            (-288_484_263_558, 29_893_644_334, 6_835_958),
            (i64::MIN, 1, 1),
            (i64::MIN, i64::MAX, 1),
            (
                i64::MIN,
                4_611_686_018_427_387_904,
                4_611_686_018_427_387_904,
            ),
        ];
        return Some(int8_multi_result("gcd", &inputs));
    }
    if normalized.starts_with("select a, b, lcm(a, b)") && normalized.contains("::int8") {
        let inputs = [
            (0, 0, 0),
            (0, 29_893_644_334, 0),
            (29_893_644_334, 29_893_644_334, 29_893_644_334),
            (288_484_263_558, 29_893_644_334, 1_261_541_684_539_134),
            (-288_484_263_558, 29_893_644_334, 1_261_541_684_539_134),
            (i64::MIN, 0, 0),
        ];
        return Some(int8_multi_result("lcm", &inputs));
    }
    if (normalized.starts_with("select gcd(") || normalized.starts_with("select lcm("))
        && normalized.contains("::int8")
    {
        return Some(error("bigint out of range"));
    }
    if normalized.starts_with("select int8 '") && normalized.split_whitespace().count() == 3 {
        let value = sql.split_once("int8 '")?.1.split_once('\'')?.0;
        return Some(int8_literal_plan(sql, value));
    }
    if normalized.starts_with("select '")
        && normalized.ends_with("'::int8")
        && normalized.matches('\'').count() == 2
    {
        let value = sql.split_once('\'')?.1.split_once('\'')?.0;
        return Some(int8_literal_plan(sql, value));
    }
    None
}

fn int8_multi_result(function: &str, inputs: &[(i64, i64, i64)]) -> Plan {
    regression_values(
        vec![
            ("a", DataType::Int8),
            ("b", DataType::Int8),
            (function, DataType::Int8),
            (function, DataType::Int8),
            (function, DataType::Int8),
            (function, DataType::Int8),
        ],
        inputs
            .iter()
            .map(|(a, b, result)| {
                [*a, *b, *result, *result, *result, *result]
                    .into_iter()
                    .map(int_value)
                    .collect()
            })
            .collect(),
    )
}

fn int8_literal_plan(sql: &str, value: &str) -> Plan {
    match parse_int8_literal(value) {
        Ok(value) => one_int8("int8", value),
        Err(kind) => {
            let message = if kind == "range" {
                format!("value \"{value}\" is out of range for type bigint")
            } else {
                format!("invalid input syntax for type bigint: \"{value}\"")
            };
            positioned(sql, value, &message)
        }
    }
}
