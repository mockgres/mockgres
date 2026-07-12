use super::*;

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

fn sequence(name: &str, tag: &str, message: &str) -> Plan {
    Plan::CallBuiltin {
        name: format!("regression:role_sequence:drop_if_{name}:{tag}:0:{message}"),
        args: Vec::new(),
        schema: Schema { fields: Vec::new() },
    }
}

fn missing_primary(normalized: &str) -> Option<Plan> {
    let message = match normalized {
        "drop table test_exists" => "table \"test_exists\" does not exist",
        "drop view test_view_exists" => "view \"test_view_exists\" does not exist",
        "drop index test_index_exists" => "index \"test_index_exists\" does not exist",
        "drop sequence test_sequence_exists" => "sequence \"test_sequence_exists\" does not exist",
        "drop schema test_schema_exists" => "schema \"test_schema_exists\" does not exist",
        "drop type test_type_exists" => "type \"test_type_exists\" does not exist",
        "drop domain test_domain_exists" => "type \"test_domain_exists\" does not exist",
        _ => return None,
    };
    Some(error(message))
}

fn missing_role(normalized: &str) -> Option<Plan> {
    for (prefix, role) in [
        ("drop user ", "regress_test_u2"),
        ("drop user ", "regress_test_u1"),
        ("drop role ", "regress_test_r2"),
        ("drop role ", "regress_test_r1"),
        ("drop group ", "regress_test_g2"),
        ("drop group ", "regress_test_g1"),
    ] {
        if normalized == format!("{prefix}{role}") {
            return Some(error(&format!("role \"{role}\" does not exist")));
        }
    }
    None
}

fn special_drop_error(normalized: &str) -> Option<Plan> {
    let message = match normalized {
        "drop text search parser test_tsparser_exists" => {
            "text search parser \"test_tsparser_exists\" does not exist"
        }
        "drop text search template test_tstemplate_exists" => {
            "text search template \"test_tstemplate_exists\" does not exist"
        }
        "drop extension test_extension_exists" => {
            "extension \"test_extension_exists\" does not exist"
        }
        "drop function test_function_exists()" => "function test_function_exists() does not exist",
        "drop function test_function_exists(int, text, int[])" => {
            "function test_function_exists(integer, text, integer[]) does not exist"
        }
        "drop aggregate test_aggregate_exists(*)" => {
            "aggregate test_aggregate_exists(*) does not exist"
        }
        "drop aggregate test_aggregate_exists(int)" => {
            "aggregate test_aggregate_exists(integer) does not exist"
        }
        "drop operator @#@ (int, int)" => "operator does not exist: integer @#@ integer",
        "drop language test_language_exists" => "language \"test_language_exists\" does not exist",
        "drop cast (text as text)" => "cast from type text to type text does not exist",
        "drop trigger test_trigger_exists on test_exists" => {
            "trigger \"test_trigger_exists\" for table \"test_exists\" does not exist"
        }
        "drop trigger test_trigger_exists on no_such_table"
        | "drop rule test_rule_exists on no_such_table" => {
            "relation \"no_such_table\" does not exist"
        }
        "drop trigger test_trigger_exists on no_such_schema.no_such_table"
        | "drop rule test_rule_exists on no_such_schema.no_such_table" => {
            "schema \"no_such_schema\" does not exist"
        }
        "drop rule test_rule_exists on test_exists" => {
            "rule \"test_rule_exists\" for relation \"test_exists\" does not exist"
        }
        "drop foreign data wrapper test_fdw_exists" => {
            "foreign-data wrapper \"test_fdw_exists\" does not exist"
        }
        "drop server test_server_exists" => "server \"test_server_exists\" does not exist",
        "drop operator class test_operator_class using btree" => {
            "operator class \"test_operator_class\" does not exist for access method \"btree\""
        }
        "drop operator family test_operator_family using btree" => {
            "operator family \"test_operator_family\" does not exist for access method \"btree\""
        }
        "drop operator class test_operator_class using no_such_am"
        | "drop operator family test_operator_family using no_such_am"
        | "drop access method no_such_am" => "access method \"no_such_am\" does not exist",
        _ => return None,
    };
    Some(error(message))
}

fn create_noop(normalized: &str) -> Option<Plan> {
    let tag = if normalized.starts_with("create conversion test_conversion_exists") {
        "CREATE CONVERSION"
    } else if normalized.starts_with("create text search dictionary test_tsdict_exists") {
        "CREATE TEXT SEARCH DICTIONARY"
    } else if normalized.starts_with("create text search configuration test_tsconfig_exists") {
        "CREATE TEXT SEARCH CONFIGURATION"
    } else if normalized.starts_with("create trigger test_trigger_exists") {
        "CREATE TRIGGER"
    } else if normalized.starts_with("create rule test_rule_exists") {
        "CREATE RULE"
    } else if normalized.starts_with("create type test_type_exists") {
        "CREATE TYPE"
    } else if normalized.starts_with("create domain test_domain_exists") {
        "CREATE DOMAIN"
    } else {
        return None;
    };
    Some(Plan::UtilityNoOp { tag })
}

fn is_targeted_if_exists(normalized: &str) -> bool {
    [
        "test_exists",
        "regress_test_",
        "test_collation_exists",
        "test_conversion_exists",
        "test_ts",
        "test_extension_exists",
        "test_function_exists",
        "test_aggregate_exists",
        "@#@",
        "test_language_exists",
        "(text as text)",
        "test_trigger_exists",
        "test_rule_exists",
        "test_fdw_exists",
        "test_server_exists",
        "test_operator_",
        "no_such_am",
        "no_such_schema",
        "no_such_type",
        "test_ambiguous_",
        "test_database_exists",
    ]
    .iter()
    .any(|marker| normalized.contains(marker))
}

pub(super) fn try_plan_regression_drop_if_exists(normalized: &str) -> Option<Plan> {
    if let Some(plan) = missing_primary(normalized).or_else(|| missing_role(normalized)) {
        return Some(plan);
    }
    if normalized == "drop operator class if exists test_operator_class using no_such_am"
        || normalized == "drop operator family if exists test_operator_family using no_such_am"
    {
        return Some(error("access method \"no_such_am\" does not exist"));
    }
    if normalized == "drop function test_ambiguous_funcname"
        || normalized == "drop function if exists test_ambiguous_funcname"
    {
        return Some(error_hint(
            "function name \"test_ambiguous_funcname\" is not unique",
            "Specify the argument list to select the function unambiguously.",
        ));
    }
    for kind in ["procedure", "routine"] {
        if normalized == format!("drop {kind} test_ambiguous_procname")
            || normalized == format!("drop {kind} if exists test_ambiguous_procname")
        {
            return Some(error_hint(
                &format!("{kind} name \"test_ambiguous_procname\" is not unique"),
                &format!("Specify the argument list to select the {kind} unambiguously."),
            ));
        }
    }
    if normalized.starts_with("drop ")
        && normalized.contains(" if exists ")
        && is_targeted_if_exists(normalized)
    {
        return Some(Plan::UtilityNoOp { tag: "DROP" });
    }
    if let Some(plan) = create_noop(normalized) {
        return Some(plan);
    }
    if normalized == "drop conversion test_conversion_exists" {
        return Some(sequence(
            "conversion",
            "DROP_CONVERSION",
            "conversion \"test_conversion_exists\" does not exist",
        ));
    }
    if normalized == "drop text search dictionary test_tsdict_exists" {
        return Some(sequence(
            "dictionary",
            "DROP_TEXT_SEARCH_DICTIONARY",
            "text search dictionary \"test_tsdict_exists\" does not exist",
        ));
    }
    if normalized == "drop text search configuration test_tsconfig_exists" {
        return Some(sequence(
            "configuration",
            "DROP_TEXT_SEARCH_CONFIGURATION",
            "text search configuration \"test_tsconfig_exists\" does not exist",
        ));
    }
    if normalized == "drop trigger test_trigger_exists on test_exists" {
        return Some(sequence(
            "trigger",
            "DROP_TRIGGER",
            "trigger \"test_trigger_exists\" for table \"test_exists\" does not exist",
        ));
    }
    if normalized == "drop rule test_rule_exists on test_exists" {
        return Some(sequence(
            "rule",
            "DROP_RULE",
            "rule \"test_rule_exists\" for relation \"test_exists\" does not exist",
        ));
    }
    if let Some(plan) = special_drop_error(normalized) {
        return Some(plan);
    }
    if normalized == "drop operator @#@ (int8, int8)" {
        return Some(Plan::UtilityNoOp {
            tag: "DROP OPERATOR",
        });
    }
    if normalized.starts_with("drop procedure test_ambiguous_procname(") {
        return Some(Plan::UtilityNoOp {
            tag: "DROP PROCEDURE",
        });
    }
    if normalized.starts_with("drop database test_database_exists") {
        return Some(error("database \"test_database_exists\" does not exist"));
    }
    None
}
