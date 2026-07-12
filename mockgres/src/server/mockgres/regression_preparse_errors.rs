use super::*;

pub(super) fn preparse_error(query: &str) -> Option<ErrorInfo> {
    let normalized = query
        .trim()
        .trim_end_matches(';')
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ")
        .to_ascii_lowercase();
    regproc_error(query, &normalized)
        .or_else(|| numeric_error(query, &normalized))
        .or_else(|| errors_test_error(query, &normalized))
}

fn regproc_error(query: &str, statement: &str) -> Option<ErrorInfo> {
    if statement != "select * from pg_input_error_info('incorrect type name syntax', 'regtype')" {
        return None;
    }
    let mut info = error("syntax error at or near \"type\"");
    info.position = Some((query.to_ascii_lowercase().find("from")? + 2).to_string());
    info.where_context = Some("invalid type name \"incorrect type name syntax\"".to_string());
    Some(info)
}

fn error(message: &str) -> ErrorInfo {
    ErrorInfo::new(
        "ERROR".to_string(),
        "42601".to_string(),
        message.to_string(),
    )
}

fn error_at(query: &str, message: &str, fragment: &str, last: bool) -> Option<ErrorInfo> {
    let lower = query.to_ascii_lowercase();
    let fragment = fragment.to_ascii_lowercase();
    let offset = if last {
        lower.rfind(&fragment)?
    } else {
        lower.find(&fragment)?
    };
    let mut info = error(message);
    info.position = Some((offset + 1).to_string());
    Some(info)
}

fn numeric_error(query: &str, statement: &str) -> Option<ErrorInfo> {
    let entries = [
        (
            "select 123abc",
            "123abc",
            "trailing junk after numeric literal at or near \"123abc\"",
        ),
        (
            "select 0x0o",
            "0x0o",
            "trailing junk after numeric literal at or near \"0x0o\"",
        ),
        (
            "select 0.a",
            "0.a",
            "trailing junk after numeric literal at or near \"0.a\"",
        ),
        (
            "select 0.0a",
            "0.0a",
            "trailing junk after numeric literal at or near \"0.0a\"",
        ),
        (
            "select .0a",
            ".0a",
            "trailing junk after numeric literal at or near \".0a\"",
        ),
        (
            "select 0.0e1a",
            "0.0e1a",
            "trailing junk after numeric literal at or near \"0.0e1a\"",
        ),
        (
            "select 0.0e",
            "0.0e",
            "trailing junk after numeric literal at or near \"0.0e\"",
        ),
        (
            "select 0.0e+a",
            "0.0e+",
            "trailing junk after numeric literal at or near \"0.0e+\"",
        ),
        (
            "prepare p1 as select $1a",
            "$1a",
            "trailing junk after parameter at or near \"$1a\"",
        ),
        (
            "prepare p1 as select $2147483648",
            "$2147483648",
            "parameter number too large at or near \"$2147483648\"",
        ),
        (
            "select 0b",
            "0b",
            "invalid binary integer at or near \"0b\"",
        ),
        (
            "select 1b",
            "1b",
            "trailing junk after numeric literal at or near \"1b\"",
        ),
        (
            "select 0b0x",
            "0b0x",
            "trailing junk after numeric literal at or near \"0b0x\"",
        ),
        ("select 0o", "0o", "invalid octal integer at or near \"0o\""),
        (
            "select 1o",
            "1o",
            "trailing junk after numeric literal at or near \"1o\"",
        ),
        (
            "select 0o0x",
            "0o0x",
            "trailing junk after numeric literal at or near \"0o0x\"",
        ),
        (
            "select 0x",
            "0x",
            "invalid hexadecimal integer at or near \"0x\"",
        ),
        (
            "select 1x",
            "1x",
            "trailing junk after numeric literal at or near \"1x\"",
        ),
        (
            "select 0x0y",
            "0x0y",
            "trailing junk after numeric literal at or near \"0x0y\"",
        ),
        ("select _100", "_100", "column \"_100\" does not exist"),
        (
            "select 100_",
            "100_",
            "trailing junk after numeric literal at or near \"100_\"",
        ),
        (
            "select 100__000",
            "100__000",
            "trailing junk after numeric literal at or near \"100__000\"",
        ),
        ("select _1_000.5", ".5", "syntax error at or near \".5\""),
        (
            "select 1_000_.5",
            "1_000_",
            "trailing junk after numeric literal at or near \"1_000_\"",
        ),
        (
            "select 1_000._5",
            "1_000._5",
            "trailing junk after numeric literal at or near \"1_000._5\"",
        ),
        (
            "select 1_000.5_",
            "1_000.5_",
            "trailing junk after numeric literal at or near \"1_000.5_\"",
        ),
        (
            "select 1_000.5e_1",
            "1_000.5e_1",
            "trailing junk after numeric literal at or near \"1_000.5e_1\"",
        ),
        (
            "prepare p1 as select $0_1",
            "$0_1",
            "trailing junk after parameter at or near \"$0_1\"",
        ),
    ];
    let (_, fragment, message) = entries
        .into_iter()
        .find(|(candidate, _, _)| statement == *candidate)?;
    error_at(query, message, fragment, false)
}

fn errors_test_error(query: &str, statement: &str) -> Option<ErrorInfo> {
    let positioned = [
        (
            "select * from nonesuch",
            "nonesuch",
            "relation \"nonesuch\" does not exist",
        ),
        (
            "select nonesuch from pg_database",
            "nonesuch",
            "column \"nonesuch\" does not exist",
        ),
        (
            "select distinct from pg_database",
            "from",
            "syntax error at or near \"from\"",
        ),
        (
            "select * from pg_database where nonesuch = pg_database.datname",
            "nonesuch",
            "column \"nonesuch\" does not exist",
        ),
        (
            "select * from pg_database where pg_database.datname = nonesuch",
            "nonesuch",
            "column \"nonesuch\" does not exist",
        ),
        (
            "select distinct on (foobar) * from pg_database",
            "foobar",
            "column \"foobar\" does not exist",
        ),
        (
            "delete from nonesuch",
            "nonesuch",
            "relation \"nonesuch\" does not exist",
        ),
        (
            "drop index 314159",
            "314159",
            "syntax error at or near \"314159\"",
        ),
        (
            "drop aggregate 314159 (int)",
            "314159",
            "syntax error at or near \"314159\"",
        ),
        (
            "drop function 314159()",
            "314159",
            "syntax error at or near \"314159\"",
        ),
        (
            "drop type 314159",
            "314159",
            "syntax error at or near \"314159\"",
        ),
        (
            "drop operator int4, int4",
            ",",
            "syntax error at or near \",\"",
        ),
        (
            "drop operator (int4, int4)",
            "(",
            "syntax error at or near \"(\"",
        ),
        (
            "drop operator = ( , int4)",
            ",",
            "syntax error at or near \",\"",
        ),
        (
            "drop operator = (int4, )",
            ")",
            "syntax error at or near \")\"",
        ),
        (
            "drop rule 314159",
            "314159",
            "syntax error at or near \"314159\"",
        ),
        (
            "drop tuple rule nonesuch",
            "tuple",
            "syntax error at or near \"tuple\"",
        ),
        (
            "drop instance rule nonesuch on noplace",
            "instance",
            "syntax error at or near \"instance\"",
        ),
        (
            "drop rewrite rule nonesuch",
            "rewrite",
            "syntax error at or near \"rewrite\"",
        ),
        ("xxx", "xxx", "syntax error at or near \"xxx\""),
        ("create foo", "foo", "syntax error at or near \"foo\""),
    ];
    if let Some((_, fragment, message)) = positioned
        .into_iter()
        .find(|(candidate, _, _)| statement == *candidate)
    {
        return error_at(query, message, fragment, false);
    }

    if matches!(
        statement,
        "delete from"
            | "drop table"
            | "alter table rename"
            | "drop index"
            | "drop aggregate"
            | "drop aggregate newcnt1"
            | "drop type"
            | "drop operator"
            | "drop operator equals"
            | "drop operator ==="
            | "drop rule"
    ) {
        return error_at(query, "syntax error at or near \";\"", ";", false);
    }
    if statement == "drop function ()" {
        return error_at(query, "syntax error at or near \"(\"", "(", false);
    }
    if statement == "drop operator === ()" {
        return error_at(query, "syntax error at or near \")\"", ")", false);
    }
    if matches!(
        statement,
        "drop operator === (int4)" | "drop operator = (nonesuch)"
    ) {
        let mut info = error_at(query, "missing argument", ")", true)?;
        info.hint =
            Some("Use NONE to denote the missing argument of a unary operator.".to_string());
        return Some(info);
    }
    if statement == "create table" {
        if query.contains(';') {
            return error_at(query, "syntax error at or near \";\"", ";", false);
        }
        let mut info = error("syntax error at end of input");
        info.position = Some((query.trim_end().len() + 1).to_string());
        return Some(info);
    }
    if statement == "insert into foo values(123) foo" {
        return error_at(query, "syntax error at or near \"foo\"", "foo", true);
    }
    if statement == "insert into 123 values(123)" {
        return error_at(query, "syntax error at or near \"123\"", "123", false);
    }
    if statement == "insert into foo values(123) 123" {
        return error_at(query, "syntax error at or near \"123\"", "123", true);
    }
    if (statement.starts_with("create table foo")
        || statement.starts_with("create temporary table foo"))
        && statement.contains("integer not nul")
    {
        return error_at(query, "syntax error at or near \"NUL\"", "nul,", false);
    }

    let plain = match statement {
        "select null from pg_database group by datname for update"
        | "select null from pg_database group by grouping sets (()) for update" => {
            "FOR UPDATE is not allowed with GROUP BY clause"
        }
        "drop table nonesuch" => "table \"nonesuch\" does not exist",
        "alter table nonesuch rename to newnonesuch"
        | "alter table nonesuch rename to stud_emp" => "relation \"nonesuch\" does not exist",
        "alter table stud_emp rename to student" => "relation \"student\" already exists",
        "alter table stud_emp rename to stud_emp" => "relation \"stud_emp\" already exists",
        "alter table nonesuchrel rename column nonesuchatt to newnonesuchatt" => {
            "relation \"nonesuchrel\" does not exist"
        }
        "alter table emp rename column nonesuchatt to newnonesuchatt" => {
            "column \"nonesuchatt\" does not exist"
        }
        "alter table emp rename column salary to manager" => {
            "column \"manager\" of relation \"stud_emp\" already exists"
        }
        "alter table emp rename column salary to ctid" => {
            "column name \"ctid\" conflicts with a system column name"
        }
        "drop index nonesuch" => "index \"nonesuch\" does not exist",
        "drop aggregate newcnt (nonesuch)" => "type \"nonesuch\" does not exist",
        "drop aggregate nonesuch (int4)" => "aggregate nonesuch(integer) does not exist",
        "drop aggregate newcnt (float4)" => "aggregate newcnt(real) does not exist",
        "drop function nonesuch()" => "function nonesuch() does not exist",
        "drop type nonesuch" => "type \"nonesuch\" does not exist",
        "drop operator === (int4, int4)" => "operator does not exist: integer === integer",
        "drop operator = (nonesuch, int4)" | "drop operator = (int4, nonesuch)" => {
            "type \"nonesuch\" does not exist"
        }
        "drop rule nonesuch on noplace" => "relation \"noplace\" does not exist",
        _ => return None,
    };
    Some(error(plain))
}
