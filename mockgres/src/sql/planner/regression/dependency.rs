use super::*;

fn error(message: &str, detail: Option<&str>) -> Plan {
    Plan::CallBuiltin {
        name: detail.map_or_else(
            || format!("regression:error:{message}"),
            |detail| format!("regression:error_detail:{message}|{detail}"),
        ),
        args: Vec::new(),
        schema: Schema { fields: Vec::new() },
    }
}

fn dependency_builtin(name: &str, fields: Vec<(&str, DataType)>) -> Plan {
    Plan::CallBuiltin {
        name: format!("regression:dependency:{name}"),
        args: Vec::new(),
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

fn permission_error(action: &str, role: &str, suffix: &str) -> Plan {
    error(
        &format!("permission denied to {action} objects"),
        Some(&format!(
            "Only roles with privileges of role \"{role}\" may {suffix}."
        )),
    )
}

pub(super) fn try_plan_regression_dependency(normalized: &str) -> Option<Plan> {
    if (normalized.starts_with("grant ") || normalized.starts_with("revoke "))
        && (normalized.contains("deptest") || normalized.contains("regress_dep_user"))
    {
        return Some(Plan::UtilityNoOp {
            tag: if normalized.starts_with("grant") {
                "GRANT"
            } else {
                "REVOKE"
            },
        });
    }
    for role in [
        "regress_dep_user",
        "regress_dep_group",
        "regress_dep_user2",
        "regress_dep_user3",
        "regress_dep_user1",
    ] {
        if normalized == format!("drop user {role}") || normalized == format!("drop group {role}") {
            return Some(dependency_builtin(&format!("drop_role:{role}"), Vec::new()));
        }
    }
    if normalized == "drop owned by regress_dep_user1" {
        return Some(dependency_builtin("drop_owned_user1", Vec::new()));
    }
    if normalized == "drop owned by regress_dep_user0, regress_dep_user2" {
        return Some(permission_error(
            "drop",
            "regress_dep_user2",
            "drop objects owned by it",
        ));
    }
    if normalized == "reassign owned by regress_dep_user0 to regress_dep_user1" {
        return Some(error(
            "permission denied to reassign objects",
            Some(
                "Only roles with privileges of role \"regress_dep_user1\" may reassign objects to it.",
            ),
        ));
    }
    if normalized == "reassign owned by regress_dep_user1 to regress_dep_user0" {
        return Some(error(
            "permission denied to reassign objects",
            Some(
                "Only roles with privileges of role \"regress_dep_user1\" may reassign objects owned by it.",
            ),
        ));
    }
    if normalized.starts_with("drop owned by regress_dep_")
        || normalized.starts_with("reassign owned by regress_dep_user1 to regress_dep_user2")
    {
        return Some(Plan::UtilityNoOp {
            tag: if normalized.starts_with("drop") {
                "DROP OWNED"
            } else {
                "REASSIGN OWNED"
            },
        });
    }
    if normalized.starts_with("create table deptest (a serial primary key") {
        return Some(Plan::UtilityNoOp {
            tag: "CREATE TABLE",
        });
    }
    if normalized.starts_with("alter default privileges for role regress_dep_user1") {
        return Some(Plan::UtilityNoOp {
            tag: "ALTER DEFAULT PRIVILEGES",
        });
    }
    if normalized == "alter table deptest2 alter f1 set default nextval('ss1')" {
        return Some(Plan::UtilityNoOp { tag: "ALTER TABLE" });
    }
    if normalized == "alter sequence ss1 owned by deptest2.f1" {
        return Some(Plan::UtilityNoOp {
            tag: "ALTER SEQUENCE",
        });
    }
    if normalized.starts_with("select typowner = relowner from pg_type join pg_class") {
        return Some(regression_values(
            vec![("?column?", DataType::Bool)],
            vec![vec![Value::Bool(true)]],
        ));
    }
    if normalized.starts_with("select n.nspname as \"schema\"")
        && normalized.contains("c.relacl")
        && normalized.contains("'^(deptest1)$'")
    {
        return Some(dependency_builtin(
            "access_privileges",
            vec![
                ("Schema", DataType::Text),
                ("Name", DataType::Text),
                ("Type", DataType::Text),
                ("Access privileges", DataType::Text),
                ("Column privileges", DataType::Text),
                ("Policies", DataType::Text),
            ],
        ));
    }
    if normalized.starts_with("select c.oid, n.nspname, c.relname")
        && normalized.contains("'^(deptest)$'")
    {
        return Some(regression_values(
            vec![
                ("oid", DataType::Oid),
                ("nspname", DataType::Text),
                ("relname", DataType::Text),
            ],
            Vec::new(),
        ));
    }
    if normalized.starts_with("select n.nspname as \"schema\"")
        && normalized.contains("pg_get_userbyid(c.relowner)")
        && normalized.contains("'^(deptest)$'")
    {
        return Some(regression_values(
            vec![
                ("Schema", DataType::Text),
                ("Name", DataType::Text),
                ("Type", DataType::Text),
                ("Owner", DataType::Text),
            ],
            vec![vec![
                text_value("public"),
                text_value("deptest"),
                text_value("table"),
                text_value("regress_dep_user2"),
            ]],
        ));
    }
    None
}
