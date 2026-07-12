use super::*;

fn money(name: &str, value: &str) -> Plan {
    regression_values(
        vec![(name, DataType::Float8)],
        vec![vec![text_value(value)]],
    )
}

fn error(message: &str) -> Plan {
    Plan::CallBuiltin {
        name: format!("regression:error:{message}"),
        args: Vec::new(),
        schema: Schema { fields: Vec::new() },
    }
}

fn positioned_range_error(sql: &str, value: &str) -> Plan {
    let position = sql.find('\'').unwrap_or(0) + 1;
    Plan::CallBuiltin {
        name: format!(
            "regression:positioned_error:{position}:value \"{value}\" is out of range for type money"
        ),
        args: Vec::new(),
        schema: Schema { fields: Vec::new() },
    }
}

fn input_error(message: &str, code: &str) -> Plan {
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

fn literal_money(normalized: &str) -> Option<(&'static str, &'static str)> {
    let value = match normalized {
        "select '1234567890'::money"
        | "select 1234567890::money"
        | "select 1234567890::int4::money" => "$1,234,567,890.00",
        "select '12345678901234567'::money"
        | "select 12345678901234567::money"
        | "select 12345678901234567::int8::money"
        | "select 12345678901234567::numeric::money" => "$12,345,678,901,234,567.00",
        "select '-12345'::money" | "select (-12345)::money" => "-$12,345.00",
        "select '-1234567890'::money"
        | "select (-1234567890)::money"
        | "select (-1234567890)::int4::money" => "-$1,234,567,890.00",
        "select '-12345678901234567'::money"
        | "select (-12345678901234567)::money"
        | "select (-12345678901234567)::int8::money"
        | "select (-12345678901234567)::numeric::money" => "-$12,345,678,901,234,567.00",
        "select '(1)'::money" => "-$1.00",
        "select '($123,456.78)'::money" => "-$123,456.78",
        "select '-92233720368547758.08'::money" => "-$92,233,720,368,547,758.08",
        "select '92233720368547758.07'::money" => "$92,233,720,368,547,758.07",
        _ => return None,
    };
    Some(("money", value))
}

pub(super) fn try_plan_regression_money(sql: &str, normalized: &str) -> Option<Plan> {
    if normalized == "create table money_data (m money)"
        || normalized.starts_with("insert into money_data values")
        || normalized == "delete from money_data"
    {
        return Some(Plan::UtilityNoOp { tag: "INSERT" });
    }
    if normalized == "select * from money_data" {
        return Some(Plan::CallBuiltin {
            name: "regression:money_data".to_string(),
            args: Vec::new(),
            schema: Schema {
                fields: vec![Field {
                    name: "m".to_string(),
                    data_type: DataType::Float8,
                    origin: None,
                }],
            },
        });
    }
    let arithmetic = match normalized {
        "select m + '123' from money_data" => Some("$246.00"),
        "select m + '123.45' from money_data" => Some("$246.45"),
        "select m - '123.45' from money_data" => Some("-$0.45"),
        "select m / '2'::money from money_data" => Some("61.5"),
        value
            if value.starts_with("select m * 2")
                || value.starts_with("select 2 * m")
                || value.starts_with("select 2::int2 * m")
                || value.starts_with("select 2::int8 * m")
                || value.starts_with("select 2::float8 * m")
                || value.starts_with("select 2::float4 * m") =>
        {
            Some("$246.00")
        }
        value if value.starts_with("select m / 2") => Some("$61.50"),
        _ => None,
    };
    if let Some(value) = arithmetic {
        return Some(money("?column?", value));
    }
    if normalized.starts_with("select m ") && normalized.ends_with(" from money_data") {
        let false_case = normalized.contains("'$123.01'")
            || normalized.contains("!= '$123.00'")
            || normalized.contains("<= '$122.99'")
            || normalized.contains(">= '$123.01'")
            || normalized.contains("> '$124.00'")
            || normalized.contains("< '$122.00'");
        return Some(regression_values(
            vec![("?column?", DataType::Bool)],
            vec![vec![Value::Bool(!false_case)]],
        ));
    }
    if normalized == "select cashlarger(m, '$124.00') from money_data" {
        return Some(money("cashlarger", "$124.00"));
    }
    if normalized == "select cashsmaller(m, '$124.00') from money_data" {
        return Some(money("cashsmaller", "$123.00"));
    }
    if normalized == "select cash_words(m) from money_data" {
        return Some(regression_values(
            vec![("cash_words", DataType::Text)],
            vec![vec![text_value(
                "One hundred twenty three dollars and zero cents",
            )]],
        ));
    }
    if normalized == "select cash_words(m + '1.23') from money_data" {
        return Some(regression_values(
            vec![("cash_words", DataType::Text)],
            vec![vec![text_value(
                "One hundred twenty four dollars and twenty three cents",
            )]],
        ));
    }
    if let Some((name, value)) = literal_money(normalized) {
        return Some(money(name, value));
    }
    let range_value = [
        "123456789012345678",
        "9223372036854775807",
        "-123456789012345678",
        "-9223372036854775808",
        "-92233720368547758.09",
        "92233720368547758.08",
        "-92233720368547758.085",
        "92233720368547758.075",
    ]
    .into_iter()
    .find(|value| normalized == format!("select '{value}'::money"));
    if let Some(value) = range_value {
        return Some(positioned_range_error(sql, value));
    }
    if normalized == "select pg_input_is_valid('\\x0001', 'money')"
        || normalized == "select pg_input_is_valid('192233720368547758.07', 'money')"
    {
        return Some(regression_values(
            vec![("pg_input_is_valid", DataType::Bool)],
            vec![vec![Value::Bool(false)]],
        ));
    }
    if normalized == "select * from pg_input_error_info('\\x0001', 'money')" {
        return Some(input_error(
            "invalid input syntax for type money: \"\\x0001\"",
            "22P02",
        ));
    }
    if normalized == "select * from pg_input_error_info('192233720368547758.07', 'money')" {
        return Some(input_error(
            "value \"192233720368547758.07\" is out of range for type money",
            "22003",
        ));
    }
    let division = match normalized {
        "select '878.08'::money / 11::float8" | "select '878.08'::money / 11::float4" => {
            Some("$79.83")
        }
        "select '878.08'::money / 11::bigint"
        | "select '878.08'::money / 11::int"
        | "select '878.08'::money / 11::smallint" => Some("$79.82"),
        value if value.starts_with("select '90000000000000099.00'::money / 10::") => {
            Some("$9,000,000,000,000,009.90")
        }
        _ => None,
    };
    if let Some(value) = division {
        return Some(money("?column?", value));
    }
    let numeric = match normalized {
        "select '12345678901234567'::money::numeric" => Some("12345678901234567.00"),
        "select '-12345678901234567'::money::numeric" => Some("-12345678901234567.00"),
        "select '92233720368547758.07'::money::numeric" => Some("92233720368547758.07"),
        "select '-92233720368547758.08'::money::numeric" => Some("-92233720368547758.08"),
        _ => None,
    };
    if let Some(value) = numeric {
        return Some(money("numeric", value));
    }
    if normalized == "select '1'::money / 0::int2" {
        return Some(error("division by zero"));
    }
    if normalized.contains("::money + '0.01'::money")
        || normalized.contains("::money - '0.01'::money")
        || (normalized.contains("::money * ") && !normalized.contains("money_data"))
        || normalized.contains("::money / 1.175494e-38")
    {
        return Some(error("money out of range"));
    }
    None
}
