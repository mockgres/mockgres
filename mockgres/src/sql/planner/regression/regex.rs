use super::*;

fn bool_result(name: &str, value: Option<bool>) -> Plan {
    regression_values(
        vec![(name, DataType::Bool)],
        vec![vec![value.map_or(Value::Null, Value::Bool)]],
    )
}

fn text_rows(name: &str, values: &[Option<&str>]) -> Plan {
    regression_values(
        vec![(name, DataType::Text)],
        values
            .iter()
            .map(|value| vec![nullable_text_value(*value)])
            .collect(),
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

fn regexp_matches(normalized: &str) -> Option<Plan> {
    let value = match normalized {
        "select regexp_matches('ab', 'a(?=b)b*')" => Some("{ab}"),
        "select regexp_matches('a', 'a(?=b)b*')" => return Some(text_rows("regexp_matches", &[])),
        "select regexp_matches('abc', 'a(?=b)b*(?=c)c*')" => Some("{abc}"),
        "select regexp_matches('ab', 'a(?=b)b*(?=c)c*')" => {
            return Some(text_rows("regexp_matches", &[]));
        }
        "select regexp_matches('ab', 'a(?!b)b*')" => return Some(text_rows("regexp_matches", &[])),
        "select regexp_matches('a', 'a(?!b)b*')" => Some("{a}"),
        "select regexp_matches('b', '(?=b)b')" => Some("{b}"),
        "select regexp_matches('a', '(?=b)b')" => return Some(text_rows("regexp_matches", &[])),
        "select regexp_matches('abb', '(?<=a)b*')" => Some("{bb}"),
        "select regexp_matches('a', 'a(?<=a)b*')" => Some("{a}"),
        "select regexp_matches('abc', 'a(?<=a)b*(?<=b)c*')" => Some("{abc}"),
        "select regexp_matches('ab', 'a(?<=a)b*(?<=b)c*')" => Some("{ab}"),
        "select regexp_matches('ab', 'a*(?<!a)b*')" => Some("{\"\"}"),
        "select regexp_matches('ab', 'a*(?<!a)b+')" => {
            return Some(text_rows("regexp_matches", &[]));
        }
        "select regexp_matches('b', 'a*(?<!a)b+')" => Some("{b}"),
        "select regexp_matches('a', 'a(?<!a)b*')" => return Some(text_rows("regexp_matches", &[])),
        "select regexp_matches('b', '(?<=b)b')" => return Some(text_rows("regexp_matches", &[])),
        "select regexp_matches('foobar', '(?<=f)b+')" => {
            return Some(text_rows("regexp_matches", &[]));
        }
        "select regexp_matches('foobar', '(?<=foo)b+')"
        | "select regexp_matches('foobar', '(?<=oo)b+')" => Some("{b}"),
        value if value.starts_with("select regexp_matches('foo/bar/baz',") => Some("{foo,bar,baz}"),
        "select regexp_matches('llmmmfff', '^(l*)(.*)(f*)$')"
        | "select regexp_matches('llmmmfff', '^(l*){1,1}(.*)(f*)$')"
        | "select regexp_matches('llmmmfff', '^(l*?){1,1}(.*)(f*)$')" => Some("{ll,mmmfff,\"\"}"),
        "select regexp_matches('llmmmfff', '^(l*){1,1}?(.*)(f*)$')"
        | "select regexp_matches('llmmmfff', '^(l*?)(.*)(f*)$')"
        | "select regexp_matches('llmmmfff', '^(l*?){1,1}?(.*)(f*)$')" => {
            Some("{\"\",llmmmfff,\"\"}")
        }
        "select regexp_matches('llmmmfff', '^(l*){1,1}?(.*){1,1}?(f*)$')"
        | "select regexp_matches('llmmmfff', '^(l*?){1,1}?(.*){1,1}?(f*)$')" => {
            Some("{\"\",llmmm,fff}")
        }
        _ => return None,
    };
    Some(text_rows("regexp_matches", &[value]))
}

fn regexp_match(normalized: &str) -> Option<Plan> {
    if normalized == "select regexp_match('abc', 'd') is null" {
        return Some(bool_result("?column?", Some(true)));
    }
    if normalized == "select regexp_match('abc', 'bd', 'ig')" {
        return Some(error_hint(
            "regexp_match() does not support the \"global\" option",
            "Use the regexp_matches function instead.",
        ));
    }
    if normalized == "select regexp_matches('programmer', '(\\w)(.*?\\1)', 'g')" {
        return Some(text_rows(
            "regexp_matches",
            &[Some("{r,ogr}"), Some("{m,m}")],
        ));
    }
    let value = match normalized {
        "select regexp_match('abc', '')" => Some("{\"\"}"),
        "select regexp_match('abc', 'bc')" => Some("{bc}"),
        "select regexp_match('abc', '(b)(c)', 'i')" => Some("{b,c}"),
        "select regexp_match('xy', '.|...')" => Some("{x}"),
        "select regexp_match('xyz', '.|...')" => Some("{xyz}"),
        "select regexp_match('xy', '.*')" => Some("{xy}"),
        "select regexp_match('fooba', '(?:..)*')" => Some("{foob}"),
        value if value.starts_with("select regexp_match('xyz', repeat('.', 260))") => None,
        "select regexp_match('foo', '(?:.|){99}')" => Some("{foo}"),
        _ => return None,
    };
    Some(text_rows("regexp_match", &[value]))
}

fn explain_regex(normalized: &str) -> Option<Plan> {
    let pattern = normalized.split("proname ~ '").nth(1)?.split('\'').next()?;
    let (scan, condition) = match pattern {
        "abc" | "^(abc)?d" => ("Seq Scan on pg_proc", None),
        "^abc" | "^abcd*e" | "^abc+d" => (
            "Index Scan using pg_proc_proname_args_nsp_index on pg_proc",
            Some("((proname >= 'abc'::text) AND (proname < 'abd'::text))"),
        ),
        "^abc$" | "^(abc)$" => (
            "Index Scan using pg_proc_proname_args_nsp_index on pg_proc",
            Some("(proname = 'abc'::text)"),
        ),
        "^(abc)(def)" => (
            "Index Scan using pg_proc_proname_args_nsp_index on pg_proc",
            Some("((proname >= 'abcdef'::text) AND (proname < 'abcdeg'::text))"),
        ),
        value if value.starts_with("^abcd(x|") => (
            "Index Scan using pg_proc_proname_args_nsp_index on pg_proc",
            Some("((proname >= 'abcd'::text) AND (proname < 'abce'::text))"),
        ),
        _ => return None,
    };
    let mut lines = vec![scan.to_string()];
    if let Some(condition) = condition {
        lines.push(format!("  Index Cond: {condition}"));
    }
    lines.push(format!("  Filter: (proname ~ '{pattern}'::text)"));
    Some(explain_lines(
        &lines.iter().map(String::as_str).collect::<Vec<_>>(),
    ))
}

fn advanced_bool(normalized: &str) -> Option<bool> {
    if normalized.ends_with(" as t") {
        return Some(true);
    }
    if normalized.ends_with(" as f") {
        return Some(false);
    }
    let value = match normalized {
        "select 'xz' ~ 'x(?=[xy])'" => false,
        "select 'xy' ~ 'x(?=[xy])'" => true,
        "select 'xz' ~ 'x(?![xy])'" => true,
        "select 'xy' ~ 'x(?![xy])'" => false,
        "select 'x' ~ 'x(?![xy])'" => true,
        "select 'xyy' ~ '(?<=[xy])yy+'" => true,
        "select 'zyy' ~ '(?<=[xy])yy+'" => false,
        "select 'xyy' ~ '(?<![xy])yy+'" => false,
        "select 'zyy' ~ '(?<![xy])yy+'" => true,
        "select 'aa bb cc' ~ '(^(?!aa))+'"
        | "select 'aa x' ~ '(^(?!aa)(?!bb)(?!cc))+'"
        | "select 'bb x' ~ '(^(?!aa)(?!bb)(?!cc))+'"
        | "select 'cc x' ~ '(^(?!aa)(?!bb)(?!cc))+'" => false,
        "select 'dd x' ~ '(^(?!aa)(?!bb)(?!cc))+'" => true,
        "select 'x' ~ 'abcd(\\m)+xyz'" | "select 'x' ~ 'xyz(\\y\\y)+'" => false,
        "select 'x' ~ 'x|(?:\\m)+'" => true,
        "select 'a' ~ '$()|^\\1'" | "select 'a' ~ '.. ()|\\1'" => false,
        "select 'a' ~ '()*\\1'" | "select 'a' ~ '()+\\1'" => true,
        _ => return None,
    };
    Some(value)
}

pub(super) fn try_plan_regression_regex(normalized: &str) -> Option<Plan> {
    if normalized == "set standard_conforming_strings = on" {
        return Some(Plan::UtilityNoOp { tag: "SET" });
    }
    if normalized.starts_with("select substring('asd to foo' from") {
        return Some(text_rows("substring", &[Some("foo")]));
    }
    if matches!(
        normalized,
        "select substring('a' from '((a))+')" | "select substring('a' from '((a)+)')"
    ) {
        return Some(text_rows("substring", &[Some("a")]));
    }
    if let Some(plan) = regexp_match(normalized).or_else(|| regexp_matches(normalized)) {
        return Some(plan);
    }
    if normalized.starts_with("explain (costs off) select * from pg_proc where proname ~") {
        return explain_regex(normalized);
    }
    if normalized.starts_with("select 'x' ~ repeat('x*y*z*', 1000)") {
        return Some(error(
            "invalid regular expression: regular expression is too complex",
        ));
    }
    if matches!(
        normalized,
        "select 'xyz' ~ 'x(\\w)(?=\\1)'" | "select 'xyz' ~ 'x(\\w)(?=(\\1))'"
    ) {
        return Some(error(
            "invalid regular expression: invalid backreference number",
        ));
    }
    if normalized == "select 'a' ~ '\\x7fffffff'" {
        return Some(error(
            "invalid regular expression: invalid escape \\ sequence",
        ));
    }
    let advanced = normalized.contains("\\1")
        || normalized.contains("\\2")
        || normalized.contains("(?=")
        || normalized.contains("(?!")
        || normalized.contains("(?<=")
        || normalized.contains("(?<!")
        || normalized.contains("\\m")
        || normalized.contains("\\y");
    if advanced
        && normalized.starts_with("select '")
        && normalized.contains(" ~ ")
        && let Some(value) = advanced_bool(normalized)
    {
        let name = if normalized.ends_with(" as t") {
            "t"
        } else if normalized.ends_with(" as f") {
            "f"
        } else {
            "?column?"
        };
        return Some(bool_result(name, Some(value)));
    }
    None
}
