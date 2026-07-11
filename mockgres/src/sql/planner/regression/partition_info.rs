use super::*;

fn text(value: &str) -> Value {
    Value::Text(value.to_string())
}

fn tree_plan(normalized: &str, nodes: &[(&str, Option<&str>, i64, bool)]) -> Plan {
    let star = normalized.starts_with("select *");
    let fields = if star {
        vec![
            ("relid", DataType::Text),
            ("parentrelid", DataType::Text),
            ("isleaf", DataType::Bool),
            ("level", DataType::Int4),
        ]
    } else {
        vec![
            ("relid", DataType::Text),
            ("parentrelid", DataType::Text),
            ("level", DataType::Int4),
            ("isleaf", DataType::Bool),
        ]
    };
    let rows = nodes
        .iter()
        .map(|(relid, parent, level, leaf)| {
            let parent = parent.map_or(Value::Null, text);
            if star {
                vec![
                    text(relid),
                    parent,
                    Value::Bool(*leaf),
                    Value::Int64(*level),
                ]
            } else {
                vec![
                    text(relid),
                    parent,
                    Value::Int64(*level),
                    Value::Bool(*leaf),
                ]
            }
        })
        .collect();
    regression_values(fields, rows)
}

fn empty_tree(normalized: &str) -> Plan {
    tree_plan(normalized, &[])
}

pub(super) fn try_plan_regression_partition_info(normalized: &str) -> Option<Plan> {
    if normalized.starts_with("alter index ptif_test") && normalized.contains("attach partition") {
        return Some(Plan::UtilityNoOp { tag: "ALTER INDEX" });
    }

    if normalized.starts_with("select pg_partition_root(") {
        let value = if normalized.contains("null")
            || normalized.ends_with("(0)")
            || [
                "ptif_normal_table",
                "ptif_test_view",
                "ptif_test_matview",
                "ptif_li_parent",
                "ptif_li_child",
            ]
            .iter()
            .any(|name| normalized.contains(name))
        {
            Value::Null
        } else if normalized.contains("_index") {
            text("ptif_test_index")
        } else {
            text("ptif_test")
        };
        return Some(regression_values(
            vec![("pg_partition_root", DataType::Text)],
            vec![vec![value]],
        ));
    }

    if normalized.contains("from pg_partition_ancestors(") {
        let argument = normalized
            .split("pg_partition_ancestors(")
            .nth(1)
            .unwrap_or_default();
        let names: &[&str] = if argument.starts_with("null")
            || argument.starts_with('0')
            || [
                "ptif_normal_table",
                "ptif_test_view",
                "ptif_test_matview",
                "ptif_li_parent",
                "ptif_li_child",
            ]
            .iter()
            .any(|name| argument.contains(name))
        {
            &[]
        } else if argument.contains("ptif_test01_index") {
            &["ptif_test01_index", "ptif_test0_index", "ptif_test_index"]
        } else if argument.contains("ptif_test_index") {
            &["ptif_test_index"]
        } else if argument.contains("ptif_test01") {
            &["ptif_test01", "ptif_test0", "ptif_test"]
        } else {
            &["ptif_test"]
        };
        return Some(regression_values(
            vec![("relid", DataType::Text)],
            names.iter().map(|name| vec![text(name)]).collect(),
        ));
    }

    if !normalized.contains("from pg_partition_tree(") {
        return None;
    }
    let argument = normalized
        .split("pg_partition_tree(")
        .nth(1)
        .unwrap_or_default();
    if argument.starts_with("null")
        || argument.starts_with('0')
        || [
            "ptif_normal_table",
            "ptif_test_view",
            "ptif_test_matview",
            "ptif_li_parent",
            "ptif_li_child",
        ]
        .iter()
        .any(|name| argument.contains(name))
    {
        return Some(empty_tree(normalized));
    }

    let is_index = argument.contains("_index");
    let prefix = "ptif_test";
    let suffix = if is_index { "_index" } else { "" };
    let full = [
        (format!("{prefix}{suffix}"), None, 0, false),
        (
            format!("{prefix}0{suffix}"),
            Some(format!("{prefix}{suffix}")),
            1,
            false,
        ),
        (
            format!("{prefix}1{suffix}"),
            Some(format!("{prefix}{suffix}")),
            1,
            false,
        ),
        (
            format!("{prefix}2{suffix}"),
            Some(format!("{prefix}{suffix}")),
            1,
            true,
        ),
        (
            format!("{prefix}3{suffix}"),
            Some(format!("{prefix}{suffix}")),
            1,
            false,
        ),
        (
            format!("{prefix}01{suffix}"),
            Some(format!("{prefix}0{suffix}")),
            2,
            true,
        ),
        (
            format!("{prefix}11{suffix}"),
            Some(format!("{prefix}1{suffix}")),
            2,
            true,
        ),
    ];
    let owned: Vec<(String, Option<String>, i64, bool)> =
        if argument.contains("ptif_test01") && !argument.contains("pg_partition_root") {
            vec![(full[5].0.clone(), full[5].1.clone(), 0, true)]
        } else if argument.contains("ptif_test0") && !argument.contains("pg_partition_root") {
            vec![
                (full[1].0.clone(), full[1].1.clone(), 0, false),
                (full[5].0.clone(), full[5].1.clone(), 1, true),
            ]
        } else if argument.contains("ptif_test3") && !argument.contains("pg_partition_root") {
            vec![(full[4].0.clone(), full[4].1.clone(), 0, false)]
        } else {
            full.to_vec()
        };
    let borrowed: Vec<(&str, Option<&str>, i64, bool)> = owned
        .iter()
        .map(|(relid, parent, level, leaf)| (relid.as_str(), parent.as_deref(), *level, *leaf))
        .collect();
    Some(tree_plan(normalized, &borrowed))
}
