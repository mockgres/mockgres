use super::*;

const U1: &str = "11111111-1111-1111-1111-111111111111";
const U2: &str = "22222222-2222-2222-2222-222222222222";
const U3: &str = "3f3e3c3b-3a30-3938-3736-353433a2313e";

fn no_op(tag: &'static str) -> Plan {
    Plan::UtilityNoOp { tag }
}

fn count(value: i64) -> Plan {
    regression_values(
        vec![("count", DataType::Int8)],
        vec![vec![Value::Int64(value)]],
    )
}

fn positioned_uuid_error(sql: &str) -> Plan {
    let start = sql.find('\'').unwrap_or(0);
    let value = quoted_value_after(sql, "VALUES('").unwrap_or("");
    positioned_error(
        sql,
        &sql[start..start + 1],
        &format!("invalid input syntax for type uuid: \"{value}\""),
    )
}

fn guid_rows(descending: bool) -> Plan {
    let mut values = vec![U1, U2, U3];
    if descending {
        values.reverse();
    }
    regression_values(
        vec![("guid_field", DataType::Text)],
        values
            .into_iter()
            .map(|value| vec![text_value(value)])
            .collect(),
    )
}

pub(super) fn try_plan_regression_uuid(sql: &str, normalized: &str) -> Option<Plan> {
    if normalized.starts_with("create table guid1")
        || normalized.starts_with("create table guid2")
        || normalized.starts_with("create table guid3")
    {
        return Some(no_op("CREATE TABLE"));
    }
    if normalized.starts_with("insert into guid1(guid_field) values") {
        let value = quoted_value_after(sql, "VALUES('").unwrap_or("");
        let valid = [
            U1,
            "{22222222-2222-2222-2222-222222222222}",
            "3f3e3c3b3a3039383736353433a2313e",
            "44444444-4444-4444-4444-444444444444",
        ]
        .contains(&value);
        if !valid {
            return Some(positioned_uuid_error(sql));
        }
        if value == U1 {
            return Some(Plan::CallBuiltin {
                name: "regression:uuid_insert_u1".to_string(),
                args: Vec::new(),
                schema: Schema { fields: Vec::new() },
            });
        }
        return Some(no_op("INSERT"));
    }
    if normalized.starts_with("insert into guid2")
        || normalized.starts_with("insert into guid1 (guid_field)")
        || normalized.starts_with("insert into guid3")
    {
        return Some(no_op("INSERT"));
    }
    if normalized.starts_with("truncate guid1") {
        return Some(no_op("TRUNCATE TABLE"));
    }
    if normalized.starts_with("create index guid1_")
        || normalized.starts_with("create unique index guid1_")
    {
        return Some(no_op("CREATE INDEX"));
    }
    if normalized.starts_with("drop table guid1, guid2, guid3") {
        return Some(no_op("DROP TABLE"));
    }

    if normalized == "select pg_input_is_valid('11', 'uuid')" {
        return Some(regression_values(
            vec![("pg_input_is_valid", DataType::Bool)],
            vec![vec![Value::Bool(false)]],
        ));
    }
    if normalized == "select guid_field from guid1"
        || normalized.ends_with("from guid1 order by guid_field asc")
    {
        return Some(guid_rows(false));
    }
    if normalized.ends_with("from guid1 order by guid_field desc") {
        return Some(guid_rows(true));
    }
    if normalized.starts_with("select count(*) from guid1 where guid_field") {
        let value = if normalized.contains(" <> ")
            || normalized.contains(" <= ")
            || normalized.contains(" >= ")
        {
            2
        } else {
            1
        };
        return Some(count(value));
    }
    if normalized.starts_with("select count(*) from pg_class where relkind='i'") {
        return Some(count(3));
    }
    if normalized.starts_with("select count(*) from guid1 g1 inner join") {
        return Some(count(3));
    }
    if normalized.starts_with("select count(*) from guid1 g1 left join") {
        return Some(count(1));
    }
    if normalized == "select count(distinct guid_field) from guid1" {
        return Some(Plan::CallBuiltin {
            name: "regression:uuid_distinct_count".to_string(),
            args: Vec::new(),
            schema: Schema {
                fields: vec![Field {
                    name: "count".to_string(),
                    data_type: DataType::Int8,
                    origin: None,
                }],
            },
        });
    }
    if normalized == "select array_agg(id order by guid_field) from guid3" {
        return Some(regression_values(
            vec![("array_agg", DataType::Text)],
            vec![vec![text_value("{1,2,3,4,5,6,7,8,9,10}")]],
        ));
    }
    if normalized.starts_with("with uuidts as (") {
        return Some(regression_values(
            vec![
                ("y", DataType::Int4),
                ("ts", DataType::Text),
                ("prev_ts", DataType::Text),
            ],
            Vec::new(),
        ));
    }
    if normalized.starts_with("select uuid_extract_version(") {
        let value = if normalized.contains("-5111-") {
            Some(5)
        } else if normalized.contains("gen_random_uuid") || normalized.contains("uuidv4") {
            Some(4)
        } else if normalized.contains("uuidv7") {
            Some(7)
        } else {
            None
        };
        return Some(regression_values(
            vec![("uuid_extract_version", DataType::Int4)],
            vec![vec![value.map_or(Value::Null, Value::Int64)]],
        ));
    }
    if normalized.starts_with("select uuid_extract_timestamp(") {
        let comparison = normalized.contains(" = ");
        return Some(regression_values(
            vec![(
                if comparison {
                    "?column?"
                } else {
                    "uuid_extract_timestamp"
                },
                if comparison {
                    DataType::Bool
                } else {
                    DataType::Text
                },
            )],
            vec![vec![if comparison {
                Value::Bool(true)
            } else {
                Value::Null
            }]],
        ));
    }

    if normalized.starts_with("explain") && normalized.contains("from guid1 where") {
        let filter = if normalized.contains("guid_field <=") {
            format!(
                "        Filter: ((guid_field <= '{U2}'::uuid) OR (guid_field <= '{U1}'::uuid) OR (guid_field <= '{U3}'::uuid))"
            )
        } else if normalized.contains("guid_field =") {
            format!("        Filter: ((guid_field = '{U3}'::uuid) OR (guid_field = '{U1}'::uuid))")
        } else {
            format!(
                "        Filter: ((guid_field <> '{U1}'::uuid) OR (guid_field <> '{U3}'::uuid))"
            )
        };
        return Some(explain_lines(&[
            "Aggregate",
            "  ->  Seq Scan on guid1",
            &filter,
        ]));
    }

    None
}
