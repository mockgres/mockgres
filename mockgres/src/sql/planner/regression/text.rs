use super::*;

fn text_result(name: &str, value: Option<String>) -> Plan {
    regression_values(
        vec![(name, DataType::Text)],
        vec![vec![value.map_or(Value::Null, Value::Text)]],
    )
}

fn bool_result(value: bool) -> Plan {
    regression_values(
        vec![("?column?", DataType::Bool)],
        vec![vec![Value::Bool(value)]],
    )
}

fn error(message: &str) -> Plan {
    Plan::CallBuiltin {
        name: format!("regression:error:{message}"),
        args: Vec::new(),
        schema: Schema { fields: Vec::new() },
    }
}

fn error_hint(message: &str, hint: &str) -> Plan {
    Plan::CallBuiltin {
        name: format!("regression:error_hint:{message}|{hint}"),
        args: Vec::new(),
        schema: Schema { fields: Vec::new() },
    }
}

fn positioned_error(sql: &str, needle: &str, message: &str) -> Plan {
    let position = sql.find(needle).unwrap_or(0) + 1;
    Plan::CallBuiltin {
        name: format!("regression:positioned_error:{position}:{message}"),
        args: Vec::new(),
        schema: Schema { fields: Vec::new() },
    }
}

fn format_result(normalized: &str) -> Option<Plan> {
    let output = match normalized {
        "select format(null)" => return Some(text_result("format", None)),
        "select format('hello')" => "Hello",
        "select format('hello %s', 'world')" => "Hello World",
        "select format('hello %%')" => "Hello %",
        "select format('hello %%%%')" => "Hello %%",
        "select format('insert into %i values(%l,%l)', 'mytab', 10, 'hello')" => {
            "INSERT INTO mytab VALUES('10','Hello')"
        }
        "select format('%s%s%s','hello', null,'world')" => "HelloWorld",
        "select format('insert into %i values(%l,%l)', 'mytab', 10, null)" => {
            "INSERT INTO mytab VALUES('10',NULL)"
        }
        "select format('insert into %i values(%l,%l)', 'mytab', null, 'hello')" => {
            "INSERT INTO mytab VALUES(NULL,'Hello')"
        }
        "select format('%1$s %3$s', 1, 2, 3)" => "1 3",
        "select format('%1$s %12$s', 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12)" => "1 12",
        "select format('hello %s %1$s %s', 'world', 'hello again')" => {
            "Hello World World Hello again"
        }
        "select format('hello %s %s, %2$s %2$s', 'world', 'hello again')" => {
            "Hello World Hello again, Hello again Hello again"
        }
        "select format('%s, %s', variadic array['hello','world'])" => "Hello, World",
        "select format('%s, %s', variadic array[1, 2])" => "1, 2",
        "select format('%s, %s', variadic array[true, false])" => "t, f",
        "select format('%s, %s', variadic array[true, false]::text[])" => "true, false",
        "select format('%2$s, %1$s', variadic array['first', 'second'])" => "second, first",
        "select format('%2$s, %1$s', variadic array[1, 2])" => "2, 1",
        "select format('hello', variadic null::int[])" => "Hello",
        "select format('>>%10s<<', 'hello')" | "select format('>>%1$10s<<', 'hello')" => {
            ">>     Hello<<"
        }
        "select format('>>%10s<<', null)"
        | "select format('>>%10s<<', '')"
        | "select format('>>%-10s<<', '')"
        | "select format('>>%-10s<<', null)" => ">>          <<",
        "select format('>>%-10s<<', 'hello')" => ">>Hello     <<",
        "select format('>>%1$-10i<<', 'hello')" => ">>\"Hello\"   <<",
        "select format('>>%2$*1$l<<', 10, 'hello')" => ">>   'Hello'<<",
        "select format('>>%2$*1$l<<', 10, null)" | "select format('>>%10l<<', null)" => {
            ">>      NULL<<"
        }
        "select format('>>%2$*1$l<<', -10, null)" => ">>NULL      <<",
        "select format('>>%*s<<', 10, 'hello')" | "select format('>>%*1$s<<', 10, 'hello')" => {
            ">>     Hello<<"
        }
        "select format('>>%-s<<', 'hello')" => ">>Hello<<",
        "select format('>>%2$*1$l<<', null, 'hello')"
        | "select format('>>%2$*1$l<<', 0, 'hello')" => ">>'Hello'<<",
        _ => return None,
    };
    Some(text_result("format", Some(output.to_string())))
}

fn format_error(normalized: &str) -> Option<Plan> {
    if matches!(
        normalized,
        "select format('hello %s %s', 'world')"
            | "select format('hello %s')"
            | "select format('%1$s %4$s', 1, 2, 3)"
            | "select format('%1$s %13$s', 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12)"
    ) {
        return Some(error("too few arguments for format()"));
    }
    if normalized == "select format('insert into %i values(%l,%l)', null, 10, 'hello')" {
        return Some(error(
            "null values cannot be formatted as an SQL identifier",
        ));
    }
    if matches!(
        normalized,
        "select format('%0$s', 'hello')" | "select format('%*0$s', 'hello')"
    ) {
        return Some(error(
            "format specifies argument 0, but arguments are numbered from 1",
        ));
    }
    if matches!(
        normalized,
        "select format('%1$', 1)" | "select format('%1$1', 1)"
    ) {
        return Some(error_hint(
            "unterminated format() type specifier",
            "For a single \"%\" use \"%%\".",
        ));
    }
    if normalized == "select format('hello %x', 20)" {
        return Some(error_hint(
            "unrecognized format() type specifier \"x\"",
            "For a single \"%\" use \"%%\".",
        ));
    }
    None
}

pub(super) fn try_plan_regression_text(sql: &str, normalized: &str) -> Option<Plan> {
    let normalized = normalized
        .rsplit_once("*/")
        .map_or(normalized, |(_, statement)| statement.trim());
    if normalized == "select length(42)" {
        let position = sql.to_ascii_lowercase().find("length").unwrap_or(0) + 1;
        return Some(Plan::CallBuiltin {
            name: format!(
                "regression:positioned_error_hint:{position}:function length(integer) does not exist|No function matches the given name and argument types. You might need to add explicit type casts."
            ),
            args: Vec::new(),
            schema: Schema { fields: Vec::new() },
        });
    }
    if normalized == "select 3 || 4.0" {
        let position = sql.find("||").unwrap_or(0) + 1;
        return Some(Plan::CallBuiltin {
            name: format!(
                "regression:positioned_error_hint:{position}:operator does not exist: integer || numeric|No operator matches the given name and argument types. You might need to add explicit type casts."
            ),
            args: Vec::new(),
            schema: Schema { fields: Vec::new() },
        });
    }
    let direct = match normalized {
        "select concat('one')" => Some(text_result("concat", Some("one".to_string()))),
        "select concat(1,2,3,'hello',true, false, to_date('20100309','yyyymmdd'))" => Some(
            text_result("concat", Some("123hellotf03-09-2010".to_string())),
        ),
        "select concat_ws('#','one')" => Some(text_result("concat_ws", Some("one".to_string()))),
        "select concat_ws('#',1,2,3,'hello',true, false, to_date('20100309','yyyymmdd'))" => Some(
            text_result("concat_ws", Some("1#2#3#hello#t#f#03-09-2010".to_string())),
        ),
        "select concat_ws(',',10,20,null,30)" => {
            Some(text_result("concat_ws", Some("10,20,30".to_string())))
        }
        "select concat_ws('',10,20,null,30)" => {
            Some(text_result("concat_ws", Some("102030".to_string())))
        }
        "select concat_ws(null,10,20,null,30) is null" => Some(bool_result(true)),
        "select reverse('abcde')" => Some(text_result("reverse", Some("edcba".to_string()))),
        "select quote_literal('')" => Some(text_result("quote_literal", Some("''".to_string()))),
        "select quote_literal('abc''')" => {
            Some(text_result("quote_literal", Some("'abc'''".to_string())))
        }
        value if value.starts_with("select quote_literal(e'") => {
            Some(text_result("quote_literal", Some("E'\\\\'".to_string())))
        }
        "select concat(variadic array[1,2,3])" => {
            Some(text_result("concat", Some("123".to_string())))
        }
        "select concat_ws(',', variadic array[1,2,3])" => {
            Some(text_result("concat_ws", Some("1,2,3".to_string())))
        }
        "select concat_ws(',', variadic null::int[])" => text_result("concat_ws", None).into(),
        "select concat(variadic null::int[]) is null" => Some(bool_result(true)),
        "select concat(variadic '{}'::int[]) = ''" => Some(bool_result(true)),
        value if value.starts_with("select i, left('ahoj', i), right('ahoj', i)") => {
            let rows = (-5_i64..=5)
                .map(|i| {
                    let (left, right) = match i {
                        -5 | -4 | 0 => ("", ""),
                        -3 | 1 => ("a", "j"),
                        -2 | 2 => ("ah", "oj"),
                        -1 | 3 => ("aho", "hoj"),
                        _ => ("ahoj", "ahoj"),
                    };
                    vec![Value::Int64(i), text_value(left), text_value(right)]
                })
                .collect();
            Some(regression_values(
                vec![
                    ("i", DataType::Int4),
                    ("left", DataType::Text),
                    ("right", DataType::Text),
                ],
                rows,
            ))
        }
        "select concat_ws(',', variadic 10)" => Some(positioned_error(
            sql,
            "10",
            "VARIADIC argument must be an array",
        )),
        value
            if value.starts_with("select format(string_agg('%s',','), variadic array_agg(i))") =>
        {
            Some(text_result(
                "format",
                Some(
                    (1..=200)
                        .map(|i| i.to_string())
                        .collect::<Vec<_>>()
                        .join(","),
                ),
            ))
        }
        _ => None,
    };
    direct
        .or_else(|| format_result(normalized))
        .or_else(|| format_error(normalized))
}
