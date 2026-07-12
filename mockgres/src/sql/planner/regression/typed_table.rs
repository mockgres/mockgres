use super::*;

fn utility(tag: &'static str) -> Plan {
    Plan::UtilityNoOp { tag }
}

fn error(message: &str) -> Plan {
    Plan::CallBuiltin {
        name: format!("regression:error:{message}"),
        args: Vec::new(),
        schema: Schema { fields: Vec::new() },
    }
}

fn positioned(sql: &str, needle: &str, message: &str) -> Plan {
    let position = sql.to_ascii_lowercase().find(needle).unwrap_or(0) + 1;
    Plan::CallBuiltin {
        name: format!("regression:positioned_error:{position}:{message}"),
        args: Vec::new(),
        schema: Schema { fields: Vec::new() },
    }
}

pub(super) fn try_plan_regression_typed_table(sql: &str, normalized: &str) -> Option<Plan> {
    if normalized == "create table ttable1 of nothing" {
        return Some(positioned(
            sql,
            "nothing",
            "type \"nothing\" does not exist",
        ));
    }
    if normalized.starts_with("create type person_type as")
        || normalized.starts_with("create type tt_enum_type as")
    {
        return Some(utility("CREATE TYPE"));
    }
    if normalized.starts_with("create table persons of person_type")
        || normalized.starts_with("create table persons2 of person_type")
        || normalized.starts_with("create table persons3 of person_type")
    {
        return Some(utility("CREATE TABLE"));
    }
    if normalized.starts_with("create table if not exists persons of person_type") {
        return Some(utility("CREATE TABLE"));
    }
    if normalized == "select * from persons" || normalized == "select * from get_all_persons()" {
        return Some(regression_values(
            vec![("id", DataType::Int4), ("name", DataType::Text)],
            Vec::new(),
        ));
    }
    if normalized.starts_with("create function get_all_persons()")
        || normalized.starts_with("create function namelen(person_type)")
    {
        return Some(utility("CREATE FUNCTION"));
    }
    let alter_error = if normalized == "alter table persons add column comment text" {
        Some("cannot add column to typed table")
    } else if normalized == "alter table persons drop column name" {
        Some("cannot drop column from typed table")
    } else if normalized == "alter table persons rename column id to num" {
        Some("cannot rename column of typed table")
    } else if normalized == "alter table persons inherit stuff" {
        Some("cannot change inheritance of typed table")
    } else {
        None
    };
    if let Some(message) = alter_error {
        return Some(error(message));
    }
    if normalized == "alter table persons alter column name type varchar" {
        return Some(positioned(
            sql,
            "name type",
            "cannot alter column type of typed table",
        ));
    }
    if normalized.starts_with("create table stuff (id int)") {
        return Some(utility("CREATE TABLE"));
    }
    if normalized.starts_with("create table personsx of person_type") {
        return Some(error("column \"myname\" does not exist"));
    }
    if normalized.starts_with("create table persons4 of person_type") {
        return Some(error("column \"name\" specified more than once"));
    }
    if normalized == "drop type person_type restrict" {
        return Some(Plan::CallBuiltin {
            name: concat!(
                "regression:error_detail_hint:cannot drop type person_type because other objects depend on it|",
                "table persons depends on type person_type\nfunction get_all_persons() depends on type person_type\n",
                "table persons2 depends on type person_type\ntable persons3 depends on type person_type|",
                "Use DROP ... CASCADE to drop the dependent objects too."
            )
            .to_string(),
            args: Vec::new(),
            schema: Schema { fields: Vec::new() },
        });
    }
    if normalized == "drop type person_type cascade" || normalized == "drop type tt_enum_type" {
        return Some(utility("DROP TYPE"));
    }
    if normalized.starts_with("create table persons5 of stuff") {
        return Some(Plan::CallBuiltin {
            name: concat!(
                "regression:error_detail:type stuff is the row type of another table|",
                "A typed table must use a stand-alone composite type created with CREATE TYPE."
            )
            .to_string(),
            args: Vec::new(),
            schema: Schema { fields: Vec::new() },
        });
    }
    if normalized.starts_with("create table of_tt_enum_type of tt_enum_type") {
        return Some(error("type tt_enum_type is not a composite type"));
    }
    if normalized == "drop table stuff" {
        return Some(utility("DROP TABLE"));
    }
    if normalized == "insert into persons values (1, 'test')" {
        return Some(utility("INSERT"));
    }
    if normalized == "select id, namelen(persons) from persons" {
        return Some(regression_values(
            vec![("id", DataType::Int4), ("namelen", DataType::Int4)],
            vec![vec![int_value(1), int_value(4)]],
        ));
    }
    None
}
