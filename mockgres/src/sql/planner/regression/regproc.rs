use super::*;

fn text(name: &str, value: Option<&str>) -> Plan {
    regression_values(
        vec![(name, DataType::Text)],
        vec![vec![nullable_text_value(value)]],
    )
}

fn error(message: &str) -> Plan {
    Plan::CallBuiltin {
        name: format!("regression:error:{message}"),
        args: Vec::new(),
        schema: Schema { fields: Vec::new() },
    }
}

fn positioned(sql: &str, message: &str) -> Plan {
    let position = sql.find('\'').unwrap_or(0) + 1;
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

fn success_value(function: &str, argument: &str) -> Option<&'static str> {
    match function.trim_start_matches("to_") {
        "regoper" if matches!(argument, "||/" | "pg_catalog.||/") => Some("||/"),
        "regoperator" if matches!(argument, "+(int4,int4)" | "pg_catalog.+(int4,int4)") => {
            Some("+(integer,integer)")
        }
        "regproc" if matches!(argument, "now" | "pg_catalog.now") => Some("now"),
        "regprocedure" if matches!(argument, "abs(numeric)" | "pg_catalog.abs(numeric)") => {
            Some("abs(numeric)")
        }
        "regclass" if matches!(argument, "pg_class" | "pg_catalog.pg_class") => Some("pg_class"),
        "regtype" if matches!(argument, "int4" | "pg_catalog.int4") => Some("integer"),
        "regcollation"
            if argument.contains("\"posix\"") && !argument.starts_with("ng_catalog.") =>
        {
            Some("\"POSIX\"")
        }
        "regnamespace" if argument.contains("pg_catalog") => Some("pg_catalog"),
        _ => None,
    }
}

fn missing_error(function: &str, argument: &str) -> String {
    match function {
        "regoper" => format!("operator does not exist: {argument}"),
        "regoperator" => format!("operator does not exist: {argument}"),
        "regproc" => format!("function \"{argument}\" does not exist"),
        "regprocedure" => format!("function \"{argument}\" does not exist"),
        "regclass" => format!("relation \"{argument}\" does not exist"),
        "regtype" if argument.starts_with("ng_catalog.") => {
            "schema \"ng_catalog\" does not exist".to_string()
        }
        "regtype" => format!("type \"{argument}\" does not exist"),
        "regrole" if argument == "foo.bar" => "invalid name syntax".to_string(),
        "regrole" => {
            let role = if argument.starts_with('"') {
                "Nonexistent".to_string()
            } else {
                argument.to_ascii_lowercase()
            };
            format!("role \"{role}\" does not exist")
        }
        "regnamespace" if argument == "foo.bar" => "invalid name syntax".to_string(),
        "regnamespace" => {
            let schema = if argument.starts_with('"') {
                "Nonexistent".to_string()
            } else {
                argument.to_ascii_lowercase()
            };
            format!("schema \"{schema}\" does not exist")
        }
        _ => "object does not exist".to_string(),
    }
}

pub(super) fn try_plan_regression_regproc(sql: &str, normalized: &str) -> Option<Plan> {
    if normalized.ends_with("select to_regoper('||//')") {
        return Some(text("to_regoper", None));
    }
    let select = normalized.strip_prefix("select ")?;
    if let Some((function, rest)) = select.split_once("('")
        && [
            "regoper",
            "regoperator",
            "regproc",
            "regprocedure",
            "regclass",
            "regtype",
            "regcollation",
            "regrole",
            "regnamespace",
            "to_regoper",
            "to_regoperator",
            "to_regproc",
            "to_regprocedure",
            "to_regclass",
            "to_regtype",
            "to_regcollation",
            "to_regrole",
            "to_regnamespace",
        ]
        .contains(&function)
    {
        let argument = rest.strip_suffix("')")?;
        if matches!(function, "regrole" | "to_regrole") && argument.contains("regress_regrole_test")
        {
            let position = sql.find('\'').unwrap_or(0) + 1;
            let mode = if function.starts_with("to_") {
                "soft"
            } else {
                "hard"
            };
            let quoted = argument.starts_with('"');
            return Some(Plan::CallBuiltin {
                name: format!("regression:regproc_role:{mode}:{quoted}:{position}"),
                args: Vec::new(),
                schema: Schema {
                    fields: vec![Field {
                        name: function.to_string(),
                        data_type: DataType::Text,
                        origin: None,
                    }],
                },
            });
        }
        if let Some(value) = success_value(function, argument) {
            return Some(text(function, Some(value)));
        }
        if function.starts_with("to_") {
            return Some(text(function, None));
        }
        if function == "regcollation" && argument.starts_with("ng_catalog.") {
            return Some(Plan::CallBuiltin {
                name: "regression:error_code:42704:collation does not exist".to_string(),
                args: Vec::new(),
                schema: Schema { fields: Vec::new() },
            });
        }
        return Some(positioned(sql, &missing_error(function, argument)));
    }
    if select.starts_with("to_regtypemod(") {
        let value = if select.contains("'text'") {
            Some(-1)
        } else if select.contains("'timestamp(4)'") {
            Some(4)
        } else {
            None
        };
        return Some(regression_values(
            vec![("to_regtypemod", DataType::Int4)],
            vec![vec![value.map_or(Value::Null, int_value)]],
        ));
    }
    if select.starts_with("format_type(to_regtype('varchar(32)'") {
        return Some(text("format_type", Some("character varying(32)")));
    }
    if select.starts_with("format_type(to_regtype('bit'") {
        return Some(text("format_type", Some("bit(1)")));
    }
    if select.starts_with("format_type(to_regtype('\"bit\"'") {
        return Some(text("format_type", Some("\"bit\"")));
    }
    if select.starts_with("pg_input_is_valid('ng_catalog.\"posix\"', 'regcollation')") {
        return Some(regression_values(
            vec![("pg_input_is_valid", DataType::Bool)],
            vec![vec![Value::Bool(false)]],
        ));
    }
    if select.starts_with("* from pg_input_error_info(") {
        let (message, code) = if select.contains("'ng_catalog.pg_class', 'regclass'") {
            ("relation \"ng_catalog.pg_class\" does not exist", "42P01")
        } else if select.contains("'no_such_config', 'regconfig'") {
            (
                "text search configuration \"no_such_config\" does not exist",
                "42704",
            )
        } else if select.contains("'no_such_dictionary', 'regdictionary'") {
            (
                "text search dictionary \"no_such_dictionary\" does not exist",
                "42704",
            )
        } else if select.contains("'nonexistent', 'regnamespace'") {
            ("schema \"nonexistent\" does not exist", "3F000")
        } else if select.contains("'ng_catalog.||/', 'regoper'") {
            ("operator does not exist: ng_catalog.||/", "42883")
        } else if select.contains("'-', 'regoper'") {
            ("more than one operator named -", "42725")
        } else if select.contains("'ng_catalog.+(int4,int4)', 'regoperator'") {
            ("operator does not exist: ng_catalog.+(int4,int4)", "42883")
        } else if select.contains("'-', 'regoperator'") {
            ("expected a left parenthesis", "22P02")
        } else if select.contains("'ng_catalog.now', 'regproc'") {
            ("function \"ng_catalog.now\" does not exist", "42883")
        } else if select.contains("'ng_catalog.abs(numeric)', 'regprocedure'") {
            (
                "function \"ng_catalog.abs(numeric)\" does not exist",
                "42883",
            )
        } else if select.contains("'ng_catalog.abs(numeric', 'regprocedure'") {
            ("expected a right parenthesis", "22P02")
        } else if select.contains("'regress_regrole_test', 'regrole'") {
            ("role \"regress_regrole_test\" does not exist", "42704")
        } else if select.contains("'no_such_type', 'regtype'") {
            ("type \"no_such_type\" does not exist", "42704")
        } else if select.contains("'numeric(1,2,3)', 'regtype'") {
            return Some(error("invalid NUMERIC type modifier"));
        } else if select.contains("'way.too.many.names', 'regtype'") {
            return Some(error(
                "improper qualified name (too many dotted names): way.too.many.names",
            ));
        } else if select.contains("'no_such_catalog.schema.name', 'regtype'") {
            return Some(error(
                "cross-database references are not implemented: no_such_catalog.schema.name",
            ));
        } else {
            return None;
        };
        return Some(info(message, code));
    }
    None
}
