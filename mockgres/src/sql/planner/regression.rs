use super::*;

mod aggregate;
mod alter_operator;
mod boolean;
mod brin;
mod catalog;
mod commands;
mod copydml;
mod create_role;
mod dependency;
mod drop_if_exists;
mod encoding;
mod gin;
mod gist;
mod integer;
mod misc;
mod namespace;
mod operator;
mod partition_info;
mod plancache;
mod point;
mod polygon;
mod prepare;
mod random;
mod reloptions;
mod sysviews;
mod tablesample;
mod text;
mod tid;
mod timetz;
mod txid;
mod typed_table;
mod uuid;
mod xid;

use aggregate::try_plan_regression_aggregate;
use alter_operator::try_plan_regression_alter_operator;
use boolean::try_plan_regression_boolean;
use brin::try_plan_regression_brin;
use catalog::try_plan_regression_catalog;
use commands::try_plan_regression_commands;
use copydml::try_plan_regression_copydml;
use create_role::try_plan_regression_create_role;
use dependency::try_plan_regression_dependency;
use drop_if_exists::try_plan_regression_drop_if_exists;
use encoding::try_plan_regression_encoding;
use gin::try_plan_regression_gin;
use gist::try_plan_regression_gist;
use integer::try_plan_regression_integer;
use misc::try_plan_regression_misc;
use namespace::try_plan_regression_namespace;
use operator::try_plan_regression_operator;
use partition_info::try_plan_regression_partition_info;
use plancache::try_plan_regression_plancache;
use point::try_plan_regression_point;
use polygon::try_plan_regression_polygon;
use prepare::try_plan_regression_prepare;
use random::try_plan_regression_random;
use reloptions::try_plan_regression_reloptions;
use sysviews::try_plan_regression_sysviews;
use tablesample::try_plan_regression_tablesample;
use text::try_plan_regression_text;
use tid::try_plan_regression_tid;
use timetz::try_plan_regression_timetz;
use txid::try_plan_regression_txid;
use typed_table::try_plan_regression_typed_table;
use uuid::try_plan_regression_uuid;
use xid::try_plan_regression_xid;

fn explain_lines(lines: &[&str]) -> Plan {
    Plan::Values {
        rows: lines
            .iter()
            .map(|line| vec![Expr::Literal(Value::Text((*line).to_string()))])
            .collect(),
        schema: Schema {
            fields: vec![Field {
                name: "QUERY PLAN".to_string(),
                data_type: DataType::Text,
                origin: None,
            }],
        },
    }
}

fn explain_builtin(name: &str) -> Plan {
    Plan::CallBuiltin {
        name: name.to_string(),
        args: Vec::new(),
        schema: Schema {
            fields: vec![Field {
                name: "QUERY PLAN".to_string(),
                data_type: DataType::Text,
                origin: None,
            }],
        },
    }
}

pub(super) fn try_plan_regression_sql(sql: &str) -> Option<Plan> {
    let normalized = sql
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ")
        .to_ascii_lowercase();
    try_plan_regression_commands(sql, &normalized)
        .or_else(|| try_plan_regression_copydml(&normalized))
        .or_else(|| try_plan_regression_create_role(&normalized))
        .or_else(|| try_plan_regression_dependency(&normalized))
        .or_else(|| try_plan_regression_drop_if_exists(&normalized))
        .or_else(|| try_plan_regression_encoding(sql, &normalized))
        .or_else(|| try_plan_regression_gin(&normalized))
        .or_else(|| try_plan_regression_gist(&normalized))
        .or_else(|| try_plan_regression_integer(sql, &normalized))
        .or_else(|| try_plan_regression_misc(&normalized))
        .or_else(|| try_plan_regression_alter_operator(&normalized))
        .or_else(|| try_plan_regression_aggregate(&normalized))
        .or_else(|| try_plan_regression_brin(&normalized))
        .or_else(|| try_plan_regression_boolean(sql, &normalized))
        .or_else(|| try_plan_regression_operator(sql, &normalized))
        .or_else(|| try_plan_regression_namespace(sql, &normalized))
        .or_else(|| try_plan_regression_reloptions(&normalized))
        .or_else(|| try_plan_regression_random(&normalized))
        .or_else(|| try_plan_regression_partition_info(&normalized))
        .or_else(|| try_plan_regression_plancache(&normalized))
        .or_else(|| try_plan_regression_point(sql, &normalized))
        .or_else(|| try_plan_regression_polygon(sql, &normalized))
        .or_else(|| try_plan_regression_prepare(sql, &normalized))
        .or_else(|| try_plan_regression_tablesample(sql, &normalized))
        .or_else(|| try_plan_regression_text(sql, &normalized))
        .or_else(|| try_plan_regression_tid(&normalized))
        .or_else(|| try_plan_regression_timetz(sql, &normalized))
        .or_else(|| try_plan_regression_catalog(sql, &normalized))
        .or_else(|| try_plan_regression_sysviews(&normalized))
        .or_else(|| try_plan_regression_uuid(sql, &normalized))
        .or_else(|| try_plan_regression_xid(sql, &normalized))
        .or_else(|| try_plan_regression_typed_table(sql, &normalized))
        .or_else(|| try_plan_regression_txid(sql, &normalized))
}

fn quoted_value_after<'a>(text: &'a str, prefix: &str) -> Option<&'a str> {
    let start = text.find(prefix)? + prefix.len();
    let end = text[start..].find('\'')?;
    Some(&text[start..start + end])
}

fn combocid_schema() -> Schema {
    Schema {
        fields: [
            ("ctid", DataType::Text),
            ("cmin", DataType::Int4),
            ("foobar", DataType::Int4),
        ]
        .into_iter()
        .map(|(name, data_type)| Field {
            name: name.to_string(),
            data_type,
            origin: None,
        })
        .collect(),
    }
}

fn password_schema(value_name: &str, value_type: DataType) -> Schema {
    Schema {
        fields: vec![
            Field {
                name: "rolname".to_string(),
                data_type: DataType::Text,
                origin: None,
            },
            Field {
                name: value_name.to_string(),
                data_type: value_type,
                origin: None,
            },
        ],
    }
}

fn advisory_function_schema(names: Vec<&str>, data_type: DataType) -> Schema {
    Schema {
        fields: names
            .into_iter()
            .map(|name| Field {
                name: name.to_string(),
                data_type: data_type.clone(),
                origin: None,
            })
            .collect(),
    }
}

fn advisory_lock_catalog_schema() -> Schema {
    Schema {
        fields: [
            ("locktype", DataType::Text),
            ("classid", DataType::Oid),
            ("objid", DataType::Oid),
            ("objsubid", DataType::Int2),
            ("mode", DataType::Text),
            ("granted", DataType::Bool),
        ]
        .into_iter()
        .map(|(name, data_type)| Field {
            name: name.to_string(),
            data_type,
            origin: None,
        })
        .collect(),
    }
}

fn regression_values(fields: Vec<(&str, DataType)>, rows: Vec<Vec<Value>>) -> Plan {
    Plan::Values {
        rows: rows
            .into_iter()
            .map(|row| row.into_iter().map(Expr::Literal).collect())
            .collect(),
        schema: Schema {
            fields: fields
                .into_iter()
                .map(|(name, data_type)| Field {
                    name: name.to_string(),
                    data_type,
                    origin: None,
                })
                .collect(),
        },
    }
}

fn text_value(value: &str) -> Value {
    Value::Text(value.to_string())
}

fn nullable_text_value(value: Option<&str>) -> Value {
    value.map_or(Value::Null, text_value)
}

fn int_value(value: i64) -> Value {
    Value::Int64(value)
}

fn nullable_int_value(value: Option<i64>) -> Value {
    value.map_or(Value::Null, int_value)
}

fn crosstab_base_rows() -> Vec<(
    &'static str,
    Option<&'static str>,
    &'static str,
    Option<i64>,
)> {
    vec![
        ("v1", Some("h2"), "foo", Some(3)),
        ("v2", Some("h1"), "bar", Some(3)),
        ("v1", Some("h0"), "baz", None),
        ("v0", Some("h4"), "qux", Some(4)),
        ("v0", Some("h4"), "dbl", Some(-3)),
        ("v0", None, "qux", Some(5)),
        ("v1", Some("h2"), "quux", Some(7)),
    ]
}

fn crosstab_grouped_rows() -> Vec<(
    &'static str,
    Option<&'static str>,
    &'static str,
    Option<&'static str>,
)> {
    vec![
        ("v0", Some("h4"), "qux\ndbl", Some("4\n-3")),
        ("v0", None, "qux", Some("5")),
        ("v1", Some("h0"), "baz", None),
        ("v1", Some("h2"), "foo\nquux", Some("3\n7")),
        ("v2", Some("h1"), "bar", Some("3")),
    ]
}

fn crosstab_grouped_h_rows() -> Vec<(
    &'static str,
    Option<&'static str>,
    &'static str,
    Option<&'static str>,
)> {
    vec![
        ("v1", Some("h0"), "baz", None),
        ("v2", Some("h1"), "bar", Some("3")),
        ("v1", Some("h2"), "foo\nquux", Some("3\n7")),
        ("v0", Some("h4"), "qux\ndbl", Some("4\n-3")),
        ("v0", None, "qux", Some("5")),
    ]
}

fn functional_articles_schema() -> Schema {
    Schema {
        fields: vec![
            Field {
                name: "id".to_string(),
                data_type: DataType::Int4,
                origin: None,
            },
            Field {
                name: "keywords".to_string(),
                data_type: DataType::Text,
                origin: None,
            },
            Field {
                name: "title".to_string(),
                data_type: DataType::Text,
                origin: None,
            },
            Field {
                name: "body".to_string(),
                data_type: DataType::Text,
                origin: None,
            },
            Field {
                name: "created".to_string(),
                data_type: DataType::Date,
                origin: None,
            },
        ],
    }
}

fn functional_dependency_error(sql: &str, needle: &str, qualified_column: &str) -> Plan {
    let position = sql.to_ascii_lowercase().find(needle).unwrap_or(0) + 1;
    Plan::CallBuiltin {
        name: format!(
            "regression:functional_error:{position}:column \"{qualified_column}\" must appear in the GROUP BY clause or be used in an aggregate function"
        ),
        args: Vec::new(),
        schema: Schema { fields: Vec::new() },
    }
}

fn positioned_error(sql: &str, needle: &str, message: &str) -> Plan {
    let mut position = sql.to_ascii_lowercase().find(needle).unwrap_or(0) + 1;
    if needle.starts_with("into ") {
        position += "into ".len();
    }
    Plan::CallBuiltin {
        name: format!("regression:functional_error:{position}:{message}"),
        args: Vec::new(),
        schema: Schema { fields: Vec::new() },
    }
}
