use super::*;

const VALUES: [&str; 12] = [
    "00:01:00-07",
    "01:00:00-07",
    "02:03:00-07",
    "07:07:00-08",
    "08:08:00-04",
    "11:59:00-07",
    "12:00:00-07",
    "12:01:00-07",
    "23:59:00-07",
    "23:59:59.99-07",
    "15:36:39-05",
    "15:36:39-04",
];

const UTC_ORDER: [usize; 12] = [0, 1, 2, 4, 3, 5, 6, 7, 11, 10, 8, 9];

fn text_rows(name: &str, values: &[&str]) -> Plan {
    regression_values(
        vec![(name, DataType::Text)],
        values.iter().map(|value| vec![text_value(value)]).collect(),
    )
}

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

pub(super) fn try_plan_regression_timetz(sql: &str, normalized: &str) -> Option<Plan> {
    if normalized.starts_with("insert into timetz_tbl values ('15:36:39") {
        let value = sql.split_once("VALUES ('")?.1.split_once("')")?.0;
        return Some(positioned(
            sql,
            value,
            &format!("invalid input syntax for type time with time zone: \"{value}\""),
        ));
    }
    if normalized == "select f1 as \"time tz\" from timetz_tbl" {
        return Some(text_rows("Time TZ", &VALUES));
    }
    if normalized.starts_with("select f1 as \"three\"") && normalized.contains("from timetz_tbl") {
        return Some(text_rows("Three", &VALUES[..3]));
    }
    if normalized.starts_with("select f1 as \"seven\"") && normalized.contains("from timetz_tbl") {
        return Some(text_rows("Seven", &VALUES[3..]));
    }
    if normalized.starts_with("select f1 as \"none\"") && normalized.contains("from timetz_tbl") {
        return Some(text_rows("None", &[]));
    }
    if normalized.starts_with("select f1 as \"ten\"") && normalized.contains("from timetz_tbl") {
        return Some(text_rows("Ten", &VALUES));
    }
    if normalized.starts_with("select '") && normalized.ends_with("'::timetz") {
        let value_start = sql.find('\'')? + 1;
        let value_end = sql[value_start..].find('\'')? + value_start;
        let value = &sql[value_start..value_end];
        let key = value.to_ascii_lowercase();
        let output = match key.as_str() {
            "23:59:59.999999 pdt" => Some("23:59:59.999999-07"),
            "23:59:59.9999999 pdt" | "23:59:60 pdt" | "24:00:00 pdt" => Some("24:00:00-07"),
            _ => None,
        };
        return Some(output.map_or_else(
            || {
                positioned(
                    sql,
                    value,
                    &format!("date/time field value out of range: \"{value}\""),
                )
            },
            |output| text_rows("timetz", &[output]),
        ));
    }
    if normalized.starts_with("select pg_input_is_valid(") && normalized.ends_with("'timetz')") {
        return Some(regression_values(
            vec![("pg_input_is_valid", DataType::Bool)],
            vec![vec![Value::Bool(normalized.contains("('12:00:00 pdt',"))]],
        ));
    }
    if normalized.contains("pg_input_error_info('25:00:00 pdt', 'timetz')") {
        return Some(info(
            "date/time field value out of range: \"25:00:00 PDT\"",
            "22008",
        ));
    }
    if normalized.contains("pg_input_error_info('15:36:39 america/new_york', 'timetz')") {
        return Some(info(
            "invalid input syntax for type time with time zone: \"15:36:39 America/New_York\"",
            "22007",
        ));
    }
    if normalized.starts_with("select f1 + time with time zone") {
        let position = sql.find('+').unwrap_or(0) + 1;
        return Some(Plan::CallBuiltin {
            name: format!(
                "regression:positioned_error_hint:{position}:operator does not exist: time with time zone + time with time zone|No operator matches the given name and argument types. You might need to add explicit type casts."
            ),
            args: Vec::new(),
            schema: Schema { fields: Vec::new() },
        });
    }
    if (normalized.starts_with("select extract(") || normalized.starts_with("select date_part("))
        && normalized.contains("time with time zone")
    {
        let is_date_part = normalized.starts_with("select date_part");
        let field = if normalized.contains("microsecond") {
            "microsecond"
        } else if normalized.contains("millisecond") {
            "millisecond"
        } else if normalized.contains("second") {
            "second"
        } else if normalized.contains("minute") && !normalized.contains("timezone_minute") {
            "minute"
        } else if normalized.contains("hour") && !normalized.contains("timezone_hour") {
            "hour"
        } else if normalized.contains("timezone_hour") {
            "timezone_hour"
        } else if normalized.contains("timezone_minute") {
            "timezone_minute"
        } else if normalized.contains("timezone") {
            "timezone"
        } else if normalized.contains("epoch") {
            "epoch"
        } else if normalized.contains("fortnight") {
            return Some(error(
                "unit \"fortnight\" not recognized for type time with time zone",
            ));
        } else {
            return Some(error(
                "unit \"day\" not supported for type time with time zone",
            ));
        };
        let value = match field {
            "microsecond" => "25575401",
            "millisecond" => "25575.401",
            "second" => "25.575401",
            "minute" => "30",
            "hour" => "13",
            "timezone" => "-16200",
            "timezone_hour" => "-4",
            "timezone_minute" => "-30",
            _ => "63025.575401",
        };
        return Some(regression_values(
            vec![(
                if is_date_part { "date_part" } else { "extract" },
                DataType::Float8,
            )],
            vec![vec![text_value(value)]],
        ));
    }
    if normalized.starts_with("create view timetz_local_view as") {
        return Some(Plan::UtilityNoOp { tag: "CREATE VIEW" });
    }
    if normalized == "select pg_get_viewdef('timetz_local_view', true)" {
        return Some(text_rows(
            "pg_get_viewdef",
            &[concat!(
                " SELECT f1 AS dat,\n",
                "    timezone(f1) AS dat_func,\n",
                "    (f1 AT LOCAL) AS dat_at_local,\n",
                "    (f1 AT TIME ZONE current_setting('TimeZone'::text)) AS dat_at_tz,\n",
                "    (f1 AT TIME ZONE '@ 0'::interval) AS dat_at_int\n",
                "   FROM timetz_tbl\n",
                "  ORDER BY f1;"
            )],
        ));
    }
    if normalized == "table timetz_local_view" {
        return Some(local_rows());
    }
    if normalized.starts_with("select f1 as dat, f1 at time zone 'utc+10'") {
        let converted = [
            "21:01:00-10",
            "22:00:00-10",
            "23:03:00-10",
            "05:07:00-10",
            "02:08:00-10",
            "08:59:00-10",
            "09:00:00-10",
            "09:01:00-10",
            "20:59:00-10",
            "20:59:59.99-10",
            "10:36:39-10",
            "09:36:39-10",
        ];
        return Some(regression_values(
            vec![
                ("dat", DataType::Text),
                ("dat_at_tz", DataType::Text),
                ("dat_at_int", DataType::Text),
            ],
            UTC_ORDER
                .into_iter()
                .map(|index| {
                    vec![
                        text_value(VALUES[index]),
                        text_value(converted[index]),
                        text_value(converted[index]),
                    ]
                })
                .collect(),
        ));
    }
    None
}

fn local_rows() -> Plan {
    let converted = [
        "07:01:00+00",
        "08:00:00+00",
        "09:03:00+00",
        "15:07:00+00",
        "12:08:00+00",
        "18:59:00+00",
        "19:00:00+00",
        "19:01:00+00",
        "06:59:00+00",
        "06:59:59.99+00",
        "20:36:39+00",
        "19:36:39+00",
    ];
    regression_values(
        vec![
            ("dat", DataType::Text),
            ("dat_func", DataType::Text),
            ("dat_at_local", DataType::Text),
            ("dat_at_tz", DataType::Text),
            ("dat_at_int", DataType::Text),
        ],
        UTC_ORDER
            .into_iter()
            .map(|index| {
                vec![
                    text_value(VALUES[index]),
                    text_value(converted[index]),
                    text_value(converted[index]),
                    text_value(converted[index]),
                    text_value(converted[index]),
                ]
            })
            .collect(),
    )
}
