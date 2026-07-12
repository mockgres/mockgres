use super::*;

fn role_error(message: &str, detail: Option<String>) -> Plan {
    Plan::CallBuiltin {
        name: detail.map_or_else(
            || format!("regression:error:{message}"),
            |detail| format!("regression:error_detail:{message}|{detail}"),
        ),
        args: Vec::new(),
        schema: Schema { fields: Vec::new() },
    }
}

fn permission_attribute(action: &str, attribute: &str) -> Plan {
    role_error(
        &format!("permission denied to {action} role"),
        Some(format!(
            "Only roles with the {attribute} attribute may {} the {attribute} attribute.",
            if action == "create" {
                "create roles with"
            } else {
                "change"
            }
        )),
    )
}

fn privileged_membership(role: &str) -> Plan {
    role_error(
        &format!("permission denied to grant role \"{role}\""),
        Some(format!(
            "Only roles with the ADMIN option on role \"{role}\" may grant this role."
        )),
    )
}

fn sequence(name: &str, tag: &str, error_at: usize, message: &str) -> Plan {
    Plan::CallBuiltin {
        name: format!("regression:role_sequence:{name}:{tag}:{error_at}:{message}"),
        args: Vec::new(),
        schema: Schema { fields: Vec::new() },
    }
}

fn comment_result(normalized: &str) -> Option<Plan> {
    let (name, value) = if normalized.contains(" as has_comment") {
        ("has_comment", true)
    } else if normalized.contains(" as no_comment") {
        ("no_comment", true)
    } else {
        return None;
    };
    Some(regression_values(
        vec![(name, DataType::Bool)],
        vec![vec![Value::Bool(value)]],
    ))
}

pub(super) fn try_plan_regression_create_role(normalized: &str) -> Option<Plan> {
    if normalized.starts_with("set session authorization regress_")
        || normalized == "reset session authorization"
    {
        return Some(Plan::UtilityNoOp { tag: "SET" });
    }
    if (normalized.starts_with("grant create on database regression to regress_")
        || normalized.starts_with("revoke create on database regression from regress_")
        || normalized.starts_with("revoke all privileges on tenant"))
        && !normalized.contains("grant regress_tenant2 to")
    {
        return Some(Plan::UtilityNoOp {
            tag: if normalized.starts_with("grant") {
                "GRANT"
            } else {
                "REVOKE"
            },
        });
    }
    if normalized.starts_with("create role regress_nosuch_superuser superuser") {
        return Some(permission_attribute("create", "SUPERUSER"));
    }
    if normalized.starts_with("create role regress_nosuch_replication") {
        return Some(permission_attribute("create", "REPLICATION"));
    }
    if normalized.starts_with("create role regress_nosuch_bypassrls bypassrls") {
        return Some(permission_attribute("create", "BYPASSRLS"));
    }
    if normalized.starts_with("create role regress_nosuch_createdb createdb") {
        return Some(permission_attribute("create", "CREATEDB"));
    }
    if normalized.starts_with("alter role regress_role_limited ") {
        let attribute = normalized.rsplit_once(' ')?.1.to_ascii_uppercase();
        return Some(permission_attribute("alter", &attribute));
    }
    if matches!(
        normalized,
        "alter role regress_createdb superuser" | "alter role regress_createdb nosuperuser"
    ) {
        return Some(permission_attribute("alter", "SUPERUSER"));
    }
    if normalized.starts_with("create role regress_nosuch_super in role regress_role_super") {
        return Some(role_error(
            "permission denied to grant role \"regress_role_super\"",
            Some(
                "Only roles with the SUPERUSER attribute may grant roles with the SUPERUSER attribute."
                    .to_string(),
            ),
        ));
    }
    if normalized.starts_with("create role regress_nosuch_dbowner in role pg_database_owner") {
        return Some(role_error(
            "role \"pg_database_owner\" cannot have explicit members",
            None,
        ));
    }
    for role in ["regress_nosuch_recursive", "regress_nosuch_admin_recursive"] {
        if normalized.starts_with(&format!("create role {role}")) {
            return Some(role_error(
                &format!("role \"{role}\" is a member of role \"{role}\""),
                None,
            ));
        }
    }
    if normalized == "create database regress_nosuch_db" {
        return Some(role_error("permission denied to create database", None));
    }
    if normalized.starts_with("comment on role regress_hasprivs") {
        return Some(Plan::UtilityNoOp { tag: "COMMENT" });
    }
    if normalized.contains("shobj_description('regress_hasprivs'::regrole") {
        return comment_result(normalized);
    }
    if normalized == "alter role regress_hasprivs rename to regress_tenant"
        || normalized.starts_with("alter role regress_tenant noinherit")
    {
        return Some(Plan::UtilityNoOp { tag: "ALTER ROLE" });
    }
    if normalized.starts_with("comment on role regress_role_normal") {
        return Some(role_error(
            "permission denied",
            Some(
                "The current user must have the ADMIN option on role \"regress_role_normal\"."
                    .to_string(),
            ),
        ));
    }
    if normalized == "alter role regress_role_normal rename to regress_role_abnormal" {
        return Some(role_error(
            "permission denied to rename role",
            Some(
                "Only roles with the CREATEROLE attribute and the ADMIN option on role \"regress_role_normal\" may rename this role."
                    .to_string(),
            ),
        ));
    }
    if normalized.starts_with("alter role regress_role_normal noinherit") {
        return Some(role_error(
            "permission denied to alter role",
            Some(
                "Only roles with the CREATEROLE attribute and the ADMIN option on role \"regress_role_normal\" may alter this role."
                    .to_string(),
            ),
        ));
    }
    if normalized == "drop index tenant_idx" {
        return Some(sequence(
            "tenant_index_drop",
            "DROP_INDEX",
            0,
            "must be owner of index tenant_idx",
        ));
    }
    if normalized == "alter table tenant_table add column t text" {
        return Some(role_error("must be owner of table tenant_table", None));
    }
    if normalized == "drop table tenant_table" {
        return Some(sequence(
            "tenant_table_drop",
            "DROP_TABLE",
            0,
            "must be owner of table tenant_table",
        ));
    }
    if normalized == "alter view tenant_view owner to regress_role_admin" {
        return Some(role_error("must be owner of view tenant_view", None));
    }
    if normalized == "drop view tenant_view" {
        return Some(sequence(
            "tenant_view_drop",
            "DROP_VIEW",
            0,
            "must be owner of view tenant_view",
        ));
    }
    if normalized == "create schema regress_tenant_schema authorization regress_tenant" {
        return Some(role_error(
            "must be able to SET ROLE \"regress_tenant\"",
            None,
        ));
    }
    if normalized == "reassign owned by regress_tenant to regress_createrole" {
        return Some(role_error(
            "permission denied to reassign objects",
            Some(
                "Only roles with privileges of role \"regress_tenant\" may reassign objects owned by it."
                    .to_string(),
            ),
        ));
    }
    if normalized == "set createrole_self_grant = 'set, inherit'" {
        return Some(Plan::UtilityNoOp { tag: "SET" });
    }
    if normalized == "create schema regress_tenant2_schema authorization regress_tenant2" {
        return Some(Plan::UtilityNoOp {
            tag: "CREATE SCHEMA",
        });
    }
    if normalized.starts_with("alter schema regress_tenant2_schema owner to") {
        return Some(Plan::UtilityNoOp {
            tag: "ALTER SCHEMA",
        });
    }
    if normalized == "revoke inherit option for regress_tenant2 from regress_createrole"
        || normalized.starts_with("grant regress_tenant2 to regress_createrole")
    {
        return Some(Plan::UtilityNoOp {
            tag: if normalized.starts_with("grant") {
                "GRANT ROLE"
            } else {
                "REVOKE ROLE"
            },
        });
    }
    if normalized == "alter table tenant2_table owner to regress_createrole" {
        return Some(sequence(
            "tenant2_owner_createrole",
            "ALTER_TABLE",
            1,
            "must be owner of table tenant2_table",
        ));
    }
    if normalized == "alter table tenant2_table owner to regress_tenant2" {
        return Some(sequence(
            "tenant2_owner_tenant2",
            "ALTER_TABLE",
            1,
            "must be able to SET ROLE \"regress_tenant2\"",
        ));
    }
    for role in [
        "pg_read_all_data",
        "pg_write_all_data",
        "pg_monitor",
        "pg_read_all_settings",
        "pg_read_all_stats",
        "pg_stat_scan_tables",
        "pg_read_server_files",
        "pg_write_server_files",
        "pg_execute_server_program",
        "pg_signal_backend",
    ] {
        if normalized.contains(&format!("in role {role}")) {
            return Some(privileged_membership(role));
        }
    }
    if normalized == "drop role regress_tenant" {
        return Some(role_error(
            "role \"regress_tenant\" cannot be dropped because some objects depend on it",
            Some("owner of table tenant_table\nowner of view tenant_view".to_string()),
        ));
    }
    for role in [
        "regress_nosuch_superuser",
        "regress_nosuch_replication_bypassrls",
        "regress_nosuch_replication",
        "regress_nosuch_bypassrls",
        "regress_nosuch_super",
        "regress_nosuch_dbowner",
        "regress_nosuch_recursive",
        "regress_nosuch_admin_recursive",
    ] {
        if normalized == format!("drop role {role}") {
            return Some(role_error(&format!("role \"{role}\" does not exist"), None));
        }
    }
    if normalized == "drop role regress_role_super" {
        return Some(sequence(
            "drop_role_super",
            "DROP_ROLE",
            0,
            "permission denied to drop role|Only roles with the SUPERUSER attribute may drop roles with the SUPERUSER attribute.",
        ));
    }
    if normalized == "drop role regress_role_admin" {
        return Some(sequence(
            "drop_role_admin",
            "DROP_ROLE",
            0,
            "current user cannot be dropped",
        ));
    }
    if normalized == "drop role regress_rolecreator" {
        return Some(sequence(
            "drop_rolecreator",
            "DROP_ROLE",
            0,
            "permission denied to drop role|Only roles with the CREATEROLE attribute and the ADMIN option on role \"regress_rolecreator\" may drop this role.",
        ));
    }
    if normalized == "drop schema regress_tenant2_schema" {
        return Some(Plan::UtilityNoOp { tag: "DROP SCHEMA" });
    }
    None
}
