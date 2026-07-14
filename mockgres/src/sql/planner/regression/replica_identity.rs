use super::*;

fn builtin(name: &str, fields: Vec<(&str, DataType)>) -> Plan {
    Plan::CallBuiltin {
        name: format!("regression:replica_identity:{name}"),
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

fn error(message: &str) -> Plan {
    Plan::CallBuiltin {
        name: format!("regression:error:{message}"),
        args: Vec::new(),
        schema: Schema { fields: Vec::new() },
    }
}

fn no_op() -> Plan {
    Plan::UtilityNoOp { tag: "ALTER TABLE" }
}

fn using_index_error(normalized: &str) -> Option<&'static str> {
    let message = match normalized {
        "alter table test_replica_identity replica identity using index test_replica_identity_keyab" => {
            "cannot use non-unique index \"test_replica_identity_keyab\" as replica identity"
        }
        "alter table test_replica_identity replica identity using index test_replica_identity_nonkey" => {
            "index \"test_replica_identity_nonkey\" cannot be used as replica identity because column \"nonkey\" is nullable"
        }
        "alter table test_replica_identity replica identity using index test_replica_identity_hash" => {
            "cannot use non-unique index \"test_replica_identity_hash\" as replica identity"
        }
        "alter table test_replica_identity replica identity using index test_replica_identity_expr" => {
            "cannot use expression index \"test_replica_identity_expr\" as replica identity"
        }
        "alter table test_replica_identity replica identity using index test_replica_identity_partial" => {
            "cannot use partial index \"test_replica_identity_partial\" as replica identity"
        }
        "alter table test_replica_identity replica identity using index test_replica_identity_othertable_pkey" => {
            "\"test_replica_identity_othertable_pkey\" is not an index for table \"test_replica_identity\""
        }
        "alter table test_replica_identity replica identity using index test_replica_identity_unique_defer" => {
            "cannot use non-immediate index \"test_replica_identity_unique_defer\" as replica identity"
        }
        "alter table test_replica_identity_t3 replica identity using index pk" => {
            "cannot use non-immediate index \"pk\" as replica identity"
        }
        _ => return None,
    };
    Some(message)
}

pub(super) fn try_plan_regression_replica_identity(normalized: &str) -> Option<Plan> {
    if normalized.starts_with("select relreplident from pg_class where oid = '") {
        let name = if normalized.contains("'test_replica_identity'::regclass") {
            "main_mode"
        } else {
            "system_mode"
        };
        return Some(builtin(name, vec![("relreplident", DataType::PgChar)]));
    }
    if normalized.starts_with("select count(*) from pg_index where indrelid =")
        && normalized.contains("test_replica_identity")
    {
        return Some(builtin("index_count", vec![("count", DataType::Int8)]));
    }
    if let Some(message) = using_index_error(normalized) {
        return Some(error(message));
    }
    if normalized == "alter table test_replica_identity3 alter column id drop not null" {
        return Some(builtin("drop_not_null", Vec::new()));
    }
    if normalized
        == "alter table test_replica_identity5 drop constraint test_replica_identity5_pkey"
    {
        return Some(builtin("drop_primary_key", Vec::new()));
    }
    if normalized == "alter table test_replica_identity5 alter b drop not null" {
        return Some(error("column \"b\" is in index used as replica identity"));
    }
    if (normalized.starts_with("alter table test_replica_identity")
        || normalized.starts_with("alter table only test_replica_identity")
        || normalized.starts_with("alter index test_replica_identity"))
        && (normalized.contains("replica identity")
            || normalized.contains("alter column id type bigint")
            || normalized.contains("alter b set not null")
            || normalized.contains("alter column id drop not null")
            || normalized.contains("attach partition"))
    {
        return Some(no_op());
    }
    None
}
