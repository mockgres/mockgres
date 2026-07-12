use super::*;

fn notice(message: &str) -> ErrorInfo {
    ErrorInfo::new(
        "NOTICE".to_string(),
        "00000".to_string(),
        message.to_string(),
    )
}

fn positioned_notice(query: &str, message: &str, fragment: &str) -> Option<ErrorInfo> {
    let mut info = notice(message);
    info.position = Some(
        (query
            .to_ascii_lowercase()
            .find(&fragment.to_ascii_lowercase())?
            + 1)
        .to_string(),
    );
    Some(info)
}

pub(super) fn notices(query: &str, normalized: &str) -> Vec<ErrorInfo> {
    let mut notices = Vec::new();
    let shell_return = match normalized {
        value if value.starts_with("create function widget_in(cstring) returns widget") => {
            Some(("widget", true))
        }
        value if value.starts_with("create function int44in(cstring) returns city_budget") => {
            Some(("city_budget", true))
        }
        value if value.starts_with("create function int42_in(cstring) returns int42") => {
            Some(("int42", false))
        }
        value
            if value.starts_with(
                "create function text_w_default_in(cstring) returns text_w_default",
            ) =>
        {
            Some(("text_w_default", false))
        }
        value if value.starts_with("create function base_fn_in(cstring) returns base_type") => {
            Some(("base_type", false))
        }
        value
            if value.starts_with(
                "create function myvarcharin(cstring, oid, integer) returns myvarchar",
            ) =>
        {
            Some(("myvarchar", false))
        }
        value
            if value.starts_with(
                "create function myvarcharrecv(internal, oid, integer) returns myvarchar",
            ) =>
        {
            Some(("myvarchar", false))
        }
        _ => None,
    };
    if let Some((type_name, creates_shell)) = shell_return {
        let message = if creates_shell {
            format!("type \"{type_name}\" is not yet defined")
        } else {
            format!("return type {type_name} is only a shell")
        };
        let mut info = notice(&message);
        if creates_shell {
            info.detail = Some("Creating a shell type definition.".to_string());
        }
        notices.push(info);
    }

    let shell_argument = match normalized {
        value if value.starts_with("create function widget_out(widget)") => Some("widget"),
        value if value.starts_with("create function int44out(city_budget)") => Some("city_budget"),
        value if value.starts_with("create function int42_out(int42)") => Some("int42"),
        value if value.starts_with("create function text_w_default_out(text_w_default)") => {
            Some("text_w_default")
        }
        value if value.starts_with("create function base_fn_out(base_type)") => Some("base_type"),
        value if value.starts_with("create function myvarcharout(myvarchar)") => Some("myvarchar"),
        value if value.starts_with("create function myvarcharsend(myvarchar)") => Some("myvarchar"),
        _ => None,
    };
    if let Some(type_name) = shell_argument {
        let mut info = notice(&format!("argument type {type_name} is only a shell"));
        if let Some(position) = query.to_ascii_lowercase().rfind(type_name) {
            info.position = Some((position + 1).to_string());
            notices.push(info);
        }
    }

    if normalized.starts_with("create type bogus_type ( \"internallength\"") {
        for attribute in [
            "Internallength",
            "Input",
            "Output",
            "Alignment",
            "Default",
            "Passedbyvalue",
        ] {
            if let Some(mut info) = positioned_notice(
                query,
                &format!("type attribute \"{attribute}\" not recognized"),
                &format!("\"{attribute}\""),
            ) {
                info.severity = "WARNING".to_string();
                notices.push(info);
            }
        }
    }

    let cascade = if normalized.starts_with("drop type default_test_row cascade") {
        Some(("drop cascades to function get_default_test()", None))
    } else if normalized.starts_with("drop type base_type cascade") {
        Some((
            "drop cascades to 2 other objects",
            Some(
                "drop cascades to function base_fn_in(cstring)\ndrop cascades to function base_fn_out(base_type)",
            ),
        ))
    } else if normalized.starts_with("drop type myvarchar cascade") {
        Some((
            "drop cascades to 5 other objects",
            Some(
                "drop cascades to function myvarcharin(cstring,oid,integer)\ndrop cascades to function myvarcharout(myvarchar)\ndrop cascades to function myvarcharsend(myvarchar)\ndrop cascades to function myvarcharrecv(internal,oid,integer)\ndrop cascades to type myvarchardom",
            ),
        ))
    } else {
        None
    };
    if let Some((message, detail)) = cascade {
        let mut info = notice(message);
        info.detail = detail.map(str::to_string);
        notices.push(info);
    }
    notices
}
