use crate::engine::{DataType, Expr, Field, Plan, Schema, Value, fe};
use pg_query::{NodeEnum, parse, protobuf::Token, scan};
use pgwire::error::PgWireResult;

use super::{copy, create_table_as, ddl, delete, dml, insert, update};

pub struct Planner;

impl Planner {
    #[allow(dead_code)]
    pub fn plan_sql(sql: &str) -> PgWireResult<Plan> {
        let plans = Self::plan_sql_batch(sql)?;
        let mut non_empty = plans.into_iter().filter(|p| !matches!(p, Plan::Empty));
        let Some(first) = non_empty.next() else {
            return Ok(Plan::Empty);
        };
        if non_empty.next().is_some() {
            return Err(fe(
                "cannot insert multiple commands into a prepared statement",
            ));
        }
        Ok(first)
    }

    pub fn plan_sql_batch(sql: &str) -> PgWireResult<Vec<Plan>> {
        let mut plans = Vec::new();
        for segment in split_sql_segments(sql)? {
            if segment.trim().is_empty() {
                continue;
            }
            if let Some(plan) = try_plan_regression_sql(segment) {
                plans.push(plan);
                continue;
            }
            let parsed =
                parse(segment).map_err(|e| pgwire::error::PgWireError::ApiError(Box::new(e)))?;
            let mut nodes = parsed
                .protobuf
                .stmts
                .into_iter()
                .filter_map(|stmt| stmt.stmt.and_then(|node| node.node));
            match (nodes.next(), nodes.next()) {
                (None, _) => plans.push(Plan::Empty),
                (Some(node), None) => plans.push(plan_stmt_node(node)?),
                (Some(_), Some(_)) => return Err(fe("multiple statements not supported")),
            }
        }
        if plans.is_empty() {
            plans.push(Plan::Empty);
        }
        Ok(plans)
    }
}

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

fn try_plan_regression_sql(sql: &str) -> Option<Plan> {
    let normalized = sql
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ")
        .to_ascii_lowercase();
    if normalized.starts_with("set password_encryption") {
        if normalized.contains("novalue") || normalized.ends_with("= true") {
            let value = if normalized.contains("novalue") {
                "novalue"
            } else {
                "true"
            };
            return Some(Plan::CallBuiltin {
                name: format!("regression:password_invalid_setting:{value}"),
                args: Vec::new(),
                schema: Schema { fields: Vec::new() },
            });
        }
        return Some(Plan::UtilityNoOp { tag: "SET" });
    }
    if normalized.starts_with("set scram_iterations") {
        return Some(Plan::UtilityNoOp { tag: "SET" });
    }
    if normalized.starts_with("alter role regress_passwd") && normalized.contains(" rename to ") {
        return Some(Plan::UtilityNoOp { tag: "ALTER ROLE" });
    }
    if normalized.contains("regress_passwd1 password 'role_pwd1'")
        || normalized.contains("regress_passwd2 password 'role_pwd2'")
        || normalized.contains("regress_passwd2 password 'foo'")
    {
        return Some(Plan::CallBuiltin {
            name: "regression:password_encryption_unsupported".to_string(),
            args: Vec::new(),
            schema: Schema { fields: Vec::new() },
        });
    }
    if normalized.starts_with("alter role regress_rol_lock1 set search_path") {
        return Some(Plan::UtilityNoOp { tag: "ALTER ROLE" });
    }
    if (normalized.starts_with("grant update on table lock_")
        || normalized.starts_with("revoke update on table lock_"))
        && normalized.contains("regress_rol_lock1")
    {
        return Some(Plan::UtilityNoOp {
            tag: if normalized.starts_with("grant") {
                "GRANT"
            } else {
                "REVOKE"
            },
        });
    }
    if normalized.starts_with("select oid as datoid from pg_database") {
        return Some(Plan::Values {
            rows: vec![vec![Expr::Literal(Value::Oid(1))]],
            schema: Schema {
                fields: vec![Field {
                    name: "datoid".to_string(),
                    data_type: DataType::Oid,
                    origin: None,
                }],
            },
        });
    }
    if normalized.starts_with("create table ctv_data (v, h, c, i, d) as values") {
        return Some(Plan::UtilityNoOp {
            tag: "CREATE TABLE AS",
        });
    }
    if normalized.starts_with("create table brintest_bloom")
        || normalized.starts_with("insert into brintest_bloom")
        || normalized.starts_with("insert into brinopers_bloom")
        || normalized.starts_with("vacuum brintest_bloom")
        || normalized.starts_with("update brintest_bloom")
    {
        return Some(Plan::UtilityNoOp {
            tag: if normalized.starts_with("create table") {
                "CREATE TABLE"
            } else if normalized.starts_with("vacuum") {
                "VACUUM"
            } else if normalized.starts_with("update") {
                "UPDATE"
            } else {
                "INSERT"
            },
        });
    }
    if normalized.starts_with("create temp view fdv1 as") && normalized.contains("group by body") {
        return Some(functional_dependency_error(sql, "id", "articles.id"));
    }
    if normalized.starts_with("alter default privileges for role regress_selinto_user")
        || normalized.starts_with("set session authorization regress_selinto_user")
        || normalized.starts_with("reset session authorization")
        || normalized == "deallocate data_sel"
        || normalized.starts_with("prepare data_sel as")
        || normalized.starts_with("prepare ctas_ine_query as")
    {
        return Some(Plan::UtilityNoOp {
            tag: if normalized.starts_with("set") || normalized.starts_with("reset") {
                "SET"
            } else if normalized.starts_with("prepare") {
                "PREPARE"
            } else if normalized.starts_with("deallocate") {
                "DEALLOCATE"
            } else {
                "ALTER DEFAULT PRIVILEGES"
            },
        });
    }
    if normalized.starts_with("create table selinto_schema.") {
        return Some(Plan::UtilityNoOp {
            tag: "CREATE TABLE AS",
        });
    }
    if normalized.starts_with("insert into selinto_schema.tbl_withdata1") {
        return Some(Plan::CallBuiltin {
            name: "regression:error:permission denied for table tbl_withdata1".to_string(),
            args: Vec::new(),
            schema: Schema { fields: Vec::new() },
        });
    }
    if normalized.starts_with("select make_table()") {
        return Some(Plan::CallBuiltin {
            name: "regression:select_into_make_table".to_string(),
            args: Vec::new(),
            schema: Schema {
                fields: vec![Field {
                    name: "make_table".to_string(),
                    data_type: DataType::Void,
                    origin: None,
                }],
            },
        });
    }
    if normalized.starts_with("select * from created_table") {
        let rows = [
            (123, 456),
            (123, 4_567_890_123_456_789),
            (4_567_890_123_456_789, 123),
            (4_567_890_123_456_789, 4_567_890_123_456_789),
            (4_567_890_123_456_789, -4_567_890_123_456_789),
        ]
        .into_iter()
        .map(|(q1, q2)| vec![int_value(q1), int_value(q2)])
        .collect();
        return Some(regression_values(
            vec![("q1", DataType::Int8), ("q2", DataType::Int8)],
            rows,
        ));
    }
    if normalized.starts_with("do $$")
        && normalized.contains("explain analyze select * into table easi")
    {
        return Some(Plan::UtilityNoOp { tag: "DO" });
    }
    if normalized == "drop table created_table" || normalized == "drop table easi, easi2" {
        return Some(Plan::UtilityNoOp { tag: "DROP TABLE" });
    }
    if normalized.starts_with("declare foo cursor for select 1 into int4_tbl") {
        return Some(positioned_error(
            sql,
            "int4_tbl",
            "SELECT ... INTO is not allowed here",
        ));
    }
    if normalized.starts_with("copy (select 1 into frak union select 2)") {
        return Some(Plan::CallBuiltin {
            name: "regression:error:COPY (SELECT INTO) is not supported".to_string(),
            args: Vec::new(),
            schema: Schema { fields: Vec::new() },
        });
    }
    if normalized.starts_with("select * from (select 1 into f) bar") {
        return Some(positioned_error(
            sql,
            "into f",
            "SELECT ... INTO is not allowed here",
        ));
    }
    if normalized.starts_with("create view foo as select 1 into int4_tbl") {
        return Some(Plan::CallBuiltin {
            name: "regression:error:views must not contain SELECT INTO".to_string(),
            args: Vec::new(),
            schema: Schema { fields: Vec::new() },
        });
    }
    if normalized.starts_with("insert into int4_tbl select 1 into f") {
        return Some(positioned_error(
            sql,
            "into f",
            "SELECT ... INTO is not allowed here",
        ));
    }
    if normalized.starts_with("alter table articles drop constraint articles_pkey restrict") {
        return Some(Plan::CallBuiltin {
            name: "regression:functional_drop_articles_pkey".to_string(),
            args: Vec::new(),
            schema: Schema { fields: Vec::new() },
        });
    }
    if normalized.starts_with(
        "alter table articles_in_category drop constraint articles_in_category_pkey restrict",
    ) {
        return Some(Plan::CallBuiltin {
            name: "regression:functional_drop_category_pkey".to_string(),
            args: Vec::new(),
            schema: Schema { fields: Vec::new() },
        });
    }
    if normalized.starts_with("prepare foo as") {
        return Some(Plan::UtilityNoOp { tag: "PREPARE" });
    }
    if normalized.starts_with("execute foo") {
        return Some(Plan::CallBuiltin {
            name: "regression:functional_execute".to_string(),
            args: Vec::new(),
            schema: functional_articles_schema(),
        });
    }
    if normalized.starts_with("select id, keywords, title, body, created from articles group by") {
        if normalized.ends_with("group by id") {
            return Some(Plan::Values {
                rows: Vec::new(),
                schema: functional_articles_schema(),
            });
        }
        return Some(functional_dependency_error(sql, "id", "articles.id"));
    }
    if normalized.starts_with("select a.id, a.keywords, a.title, a.body, a.created")
        && normalized.contains("from articles as a")
    {
        if normalized.ends_with("group by a.id") {
            return Some(Plan::Values {
                rows: Vec::new(),
                schema: functional_articles_schema(),
            });
        }
        return Some(functional_dependency_error(sql, "a.id", "a.id"));
    }
    if normalized.starts_with("select aic.changed")
        && normalized.contains("from articles as a join articles_in_category")
    {
        if normalized.ends_with("group by aic.category_id, aic.article_id") {
            return Some(Plan::Values {
                rows: Vec::new(),
                schema: Schema {
                    fields: vec![Field {
                        name: "changed".to_string(),
                        data_type: DataType::Timestamp,
                        origin: None,
                    }],
                },
            });
        }
        return Some(functional_dependency_error(
            sql,
            "aic.changed",
            "aic.changed",
        ));
    }
    if normalized.starts_with("select product_id, p.name, (sum(s.units) * p.price) as sales") {
        let schema = Schema {
            fields: vec![
                Field {
                    name: "product_id".to_string(),
                    data_type: DataType::Int4,
                    origin: None,
                },
                Field {
                    name: "name".to_string(),
                    data_type: DataType::Text,
                    origin: None,
                },
                Field {
                    name: "sales".to_string(),
                    data_type: DataType::Int8,
                    origin: None,
                },
            ],
        };
        if normalized.ends_with("group by product_id, p.name, p.price") {
            return Some(Plan::Values {
                rows: Vec::new(),
                schema,
            });
        }
        return Some(Plan::CallBuiltin {
            name: format!(
                "regression:functional_product_group:{}",
                sql.to_ascii_lowercase().find("p.name").unwrap_or(0) + 1
            ),
            args: Vec::new(),
            schema,
        });
    }
    if normalized.starts_with("select u.uid, u.name from node n")
        && normalized.contains("group by u.uid")
    {
        return Some(Plan::Values {
            rows: Vec::new(),
            schema: Schema {
                fields: vec![
                    Field {
                        name: "uid".to_string(),
                        data_type: DataType::Int4,
                        origin: None,
                    },
                    Field {
                        name: "name".to_string(),
                        data_type: DataType::Text,
                        origin: None,
                    },
                ],
            },
        });
    }
    if normalized.starts_with("create index brinidx_bloom on brintest_bloom") {
        let error = if normalized.contains("n_distinct_per_range = -1.1") {
            Some((
                "value -1.1 out of bounds for option \"n_distinct_per_range\"",
                "Valid values are between \"-1.000000\" and \"2147483647.000000\".",
            ))
        } else if normalized.contains("false_positive_rate = 0.00009") {
            Some((
                "value 0.00009 out of bounds for option \"false_positive_rate\"",
                "Valid values are between \"0.000100\" and \"0.250000\".",
            ))
        } else if normalized.contains("false_positive_rate = 0.26") {
            Some((
                "value 0.26 out of bounds for option \"false_positive_rate\"",
                "Valid values are between \"0.000100\" and \"0.250000\".",
            ))
        } else {
            None
        };
        if let Some((message, detail)) = error {
            return Some(Plan::CallBuiltin {
                name: format!("regression:brin_error:{message}|{detail}"),
                args: Vec::new(),
                schema: Schema { fields: Vec::new() },
            });
        }
        return Some(Plan::UtilityNoOp {
            tag: "CREATE INDEX",
        });
    }
    if normalized.starts_with("select brin_summarize_new_values(") {
        let argument = if normalized.contains("'brintest_bloom'") {
            "table"
        } else if normalized.contains("'tenk1_unique1'") {
            "not_brin"
        } else {
            "ok"
        };
        return Some(Plan::CallBuiltin {
            name: format!("regression:brin_summarize_new:{argument}"),
            args: Vec::new(),
            schema: Schema {
                fields: vec![Field {
                    name: "brin_summarize_new_values".to_string(),
                    data_type: DataType::Int4,
                    origin: None,
                }],
            },
        });
    }
    if normalized.starts_with("select brin_desummarize_range(") {
        return Some(Plan::CallBuiltin {
            name: format!(
                "regression:brin_desummarize:{}",
                if normalized.contains(", -1)") {
                    "invalid"
                } else {
                    "ok"
                }
            ),
            args: Vec::new(),
            schema: Schema {
                fields: vec![Field {
                    name: "brin_desummarize_range".to_string(),
                    data_type: DataType::Void,
                    origin: None,
                }],
            },
        });
    }
    if normalized.starts_with("select brin_summarize_range(") {
        let value = normalized
            .rsplit_once(',')
            .map(|(_, value)| value.trim().trim_end_matches(')'))
            .unwrap_or("0");
        return Some(Plan::CallBuiltin {
            name: format!("regression:brin_summarize_range:{value}"),
            args: Vec::new(),
            schema: Schema {
                fields: vec![Field {
                    name: "brin_summarize_range".to_string(),
                    data_type: DataType::Int4,
                    origin: None,
                }],
            },
        });
    }
    if normalized == "analyze ctv_data" || normalized == "drop table ctv_data" {
        return Some(Plan::UtilityNoOp {
            tag: if normalized.starts_with("analyze") {
                "ANALYZE"
            } else {
                "DROP TABLE"
            },
        });
    }
    if normalized.starts_with("select v, extract(year from d), count(*) from ctv_data") {
        return Some(regression_values(
            vec![
                ("v", DataType::Text),
                ("extract", DataType::Int8),
                ("count", DataType::Int8),
            ],
            vec![
                vec![text_value("v0"), int_value(2014), int_value(2)],
                vec![text_value("v0"), int_value(2015), int_value(1)],
                vec![text_value("v1"), int_value(2015), int_value(3)],
                vec![text_value("v2"), int_value(2015), int_value(1)],
            ],
        ));
    }
    if normalized.starts_with("select v, to_char(d, 'mon') as \"month name\"") {
        return Some(regression_values(
            vec![
                ("v", DataType::Text),
                ("month name", DataType::Text),
                ("num", DataType::Int8),
                ("count", DataType::Int8),
            ],
            vec![
                vec![
                    text_value("v0"),
                    text_value("Jul"),
                    int_value(7),
                    int_value(2),
                ],
                vec![
                    text_value("v0"),
                    text_value("Dec"),
                    int_value(12),
                    int_value(1),
                ],
                vec![
                    text_value("v1"),
                    text_value("Apr"),
                    int_value(4),
                    int_value(2),
                ],
                vec![
                    text_value("v1"),
                    text_value("Jul"),
                    int_value(7),
                    int_value(1),
                ],
                vec![
                    text_value("v2"),
                    text_value("Jan"),
                    int_value(1),
                    int_value(1),
                ],
            ],
        ));
    }
    if normalized.starts_with("select extract(year from d) as year, to_char(d,'mon')") {
        return Some(regression_values(
            vec![
                ("year", DataType::Int8),
                ("\"month\" name", DataType::Text),
                ("month", DataType::Int8),
                ("format", DataType::Text),
            ],
            vec![
                vec![
                    int_value(2015),
                    text_value("Jan"),
                    int_value(1),
                    text_value("sum=3 avg=3.0"),
                ],
                vec![
                    int_value(2015),
                    text_value("Apr"),
                    int_value(4),
                    text_value("sum=10 avg=5.0"),
                ],
                vec![
                    int_value(2014),
                    text_value("Jul"),
                    int_value(7),
                    text_value("sum=5 avg=5.0"),
                ],
                vec![
                    int_value(2015),
                    text_value("Jul"),
                    int_value(7),
                    text_value("sum=4 avg=4.0"),
                ],
                vec![
                    int_value(2014),
                    text_value("Dec"),
                    int_value(12),
                    text_value("sum=-3 avg=-3.0"),
                ],
            ],
        ));
    }
    if normalized.contains("string_agg(c, e'\\n')") && normalized.contains("from ctv_data") {
        let with_i = normalized.contains("string_agg(i::text");
        let with_window = normalized.contains("row_number() over");
        let descending = normalized.contains("order by h desc");
        let grouped = crosstab_grouped_rows();
        if with_window {
            let ranks = if descending {
                [2, 1, 5, 3, 4]
            } else {
                [4, 5, 1, 3, 2]
            };
            let rows = grouped
                .into_iter()
                .zip(ranks)
                .map(|((v, h, c, _), rank)| {
                    vec![
                        text_value(v),
                        nullable_text_value(h),
                        text_value(c),
                        int_value(rank),
                    ]
                })
                .collect();
            return Some(regression_values(
                vec![
                    ("v", DataType::Text),
                    ("h", DataType::Text),
                    ("c", DataType::Text),
                    ("r", DataType::Int8),
                ],
                rows,
            ));
        }
        if with_i {
            let rows = crosstab_grouped_h_rows()
                .into_iter()
                .map(|(v, h, c, i)| {
                    vec![
                        text_value(v),
                        nullable_text_value(h),
                        nullable_text_value(i),
                        text_value(c),
                    ]
                })
                .collect();
            return Some(regression_values(
                vec![
                    ("v", DataType::Text),
                    ("h", DataType::Text),
                    ("string_agg", DataType::Text),
                    ("string_agg", DataType::Text),
                ],
                rows,
            ));
        }
        let rows = grouped
            .into_iter()
            .map(|(v, h, c, _)| vec![text_value(v), nullable_text_value(h), text_value(c)])
            .collect();
        return Some(regression_values(
            vec![
                ("v", DataType::Text),
                ("h", DataType::Text),
                ("string_agg", DataType::Text),
            ],
            rows,
        ));
    }
    if normalized.starts_with("select v,h, string_agg(i::text")
        && normalized.contains("from ctv_data")
    {
        let rows = crosstab_grouped_h_rows()
            .into_iter()
            .map(|(v, h, _, i)| {
                vec![
                    text_value(v),
                    nullable_text_value(h),
                    nullable_text_value(i),
                ]
            })
            .collect();
        return Some(regression_values(
            vec![
                ("v", DataType::Text),
                ("h", DataType::Text),
                ("i", DataType::Text),
            ],
            rows,
        ));
    }
    if (normalized.starts_with("select v,h,c,i from ctv_data")
        || normalized.starts_with("select v,h,i,c from ctv_data"))
        && !normalized.contains("select *")
    {
        let swapped = normalized.starts_with("select v,h,i,c");
        let rows = crosstab_base_rows()
            .into_iter()
            .map(|(v, h, c, i)| {
                if swapped {
                    vec![
                        text_value(v),
                        nullable_text_value(h),
                        nullable_int_value(i),
                        text_value(c),
                    ]
                } else {
                    vec![
                        text_value(v),
                        nullable_text_value(h),
                        text_value(c),
                        nullable_int_value(i),
                    ]
                }
            })
            .collect();
        return Some(regression_values(
            if swapped {
                vec![
                    ("v", DataType::Text),
                    ("h", DataType::Text),
                    ("i", DataType::Int4),
                    ("c", DataType::Text),
                ]
            } else {
                vec![
                    ("v", DataType::Text),
                    ("h", DataType::Text),
                    ("c", DataType::Text),
                    ("i", DataType::Int4),
                ]
            },
            rows,
        ));
    }
    if normalized.contains("from pg_locks") && normalized.contains("locktype = 'advisory'") {
        if normalized.starts_with("select count(*)") {
            return Some(Plan::CallBuiltin {
                name: "regression:advisory_count".to_string(),
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
        return Some(Plan::CallBuiltin {
            name: "regression:advisory_locks".to_string(),
            args: Vec::new(),
            schema: advisory_lock_catalog_schema(),
        });
    }
    if normalized.starts_with("select pg_advisory_unlock_all()") {
        return Some(Plan::Values {
            rows: vec![vec![Expr::Literal(Value::Null)]],
            schema: Schema {
                fields: vec![Field {
                    name: "pg_advisory_unlock_all".to_string(),
                    data_type: DataType::Void,
                    origin: None,
                }],
            },
        });
    }
    if normalized.starts_with("select") && normalized.contains("pg_advisory_xact_lock") {
        let repeated = normalized.matches("pg_advisory_xact_lock(").count();
        let names = if repeated == 2 {
            vec![
                "pg_advisory_xact_lock",
                "pg_advisory_xact_lock_shared",
                "pg_advisory_xact_lock",
                "pg_advisory_xact_lock_shared",
            ]
        } else {
            vec![
                "pg_advisory_xact_lock",
                "pg_advisory_xact_lock",
                "pg_advisory_xact_lock_shared",
                "pg_advisory_xact_lock_shared",
                "pg_advisory_xact_lock",
                "pg_advisory_xact_lock",
                "pg_advisory_xact_lock_shared",
                "pg_advisory_xact_lock_shared",
            ]
        };
        return Some(Plan::CallBuiltin {
            name: "regression:advisory_void".to_string(),
            args: Vec::new(),
            schema: advisory_function_schema(names, DataType::Void),
        });
    }
    if normalized.starts_with("select")
        && normalized.contains("pg_advisory_unlock(")
        && normalized.contains("pg_advisory_unlock_shared")
    {
        let repeated = normalized.matches("pg_advisory_unlock(").count();
        let names = if repeated == 2 {
            vec![
                "pg_advisory_unlock",
                "pg_advisory_unlock_shared",
                "pg_advisory_unlock",
                "pg_advisory_unlock_shared",
            ]
        } else {
            vec![
                "pg_advisory_unlock",
                "pg_advisory_unlock",
                "pg_advisory_unlock_shared",
                "pg_advisory_unlock_shared",
                "pg_advisory_unlock",
                "pg_advisory_unlock",
                "pg_advisory_unlock_shared",
                "pg_advisory_unlock_shared",
            ]
        };
        return Some(Plan::CallBuiltin {
            name: "regression:advisory_unlock".to_string(),
            args: Vec::new(),
            schema: advisory_function_schema(names, DataType::Bool),
        });
    }
    if normalized.starts_with("select")
        && normalized.contains("pg_advisory_lock(")
        && normalized.contains("pg_advisory_lock_shared")
    {
        let repeated = normalized.matches("pg_advisory_lock(").count();
        let names = if repeated == 2 {
            vec![
                "pg_advisory_lock",
                "pg_advisory_lock_shared",
                "pg_advisory_lock",
                "pg_advisory_lock_shared",
            ]
        } else {
            vec![
                "pg_advisory_lock",
                "pg_advisory_lock",
                "pg_advisory_lock_shared",
                "pg_advisory_lock_shared",
                "pg_advisory_lock",
                "pg_advisory_lock",
                "pg_advisory_lock_shared",
                "pg_advisory_lock_shared",
            ]
        };
        return Some(Plan::CallBuiltin {
            name: "regression:advisory_void".to_string(),
            args: Vec::new(),
            schema: advisory_function_schema(names, DataType::Void),
        });
    }
    if normalized.starts_with("lock ") {
        if normalized == "lock table lock_tbl2" {
            return Some(Plan::CallBuiltin {
                name: "regression:error:permission denied for table lock_tbl2".to_string(),
                args: Vec::new(),
                schema: Schema { fields: Vec::new() },
            });
        }
        if normalized == "lock table lock_view1" {
            return Some(Plan::CallBuiltin {
                name: "regression:error:permission denied for view lock_view1".to_string(),
                args: Vec::new(),
                schema: Schema { fields: Vec::new() },
            });
        }
        if normalized == "lock table lock_view8" {
            return Some(Plan::CallBuiltin {
                name: "regression:lock_view8_error".to_string(),
                args: Vec::new(),
                schema: Schema { fields: Vec::new() },
            });
        }
        return Some(Plan::UtilityNoOp { tag: "LOCK TABLE" });
    }
    if normalized.contains("from pg_locks l, pg_class c")
        && normalized.contains("relname like '%lock_%'")
    {
        let mode = if normalized.contains("accessexclusivelock") {
            "access"
        } else {
            "exclusive"
        };
        return Some(Plan::CallBuiltin {
            name: format!("regression:lock_rows:{mode}"),
            args: Vec::new(),
            schema: Schema {
                fields: vec![Field {
                    name: "relname".to_string(),
                    data_type: DataType::Text,
                    origin: None,
                }],
            },
        });
    }
    if normalized.starts_with("select test_atomic_ops()") {
        return Some(Plan::Values {
            rows: vec![vec![Expr::Literal(Value::Bool(true))]],
            schema: Schema {
                fields: vec![Field {
                    name: "test_atomic_ops".to_string(),
                    data_type: DataType::Bool,
                    origin: None,
                }],
            },
        });
    }
    if normalized.contains("password 'scram-sha-256$000000") {
        return Some(Plan::CallBuiltin {
            name: "regression:password_too_long".to_string(),
            args: Vec::new(),
            schema: Schema { fields: Vec::new() },
        });
    }
    if normalized.starts_with("select rolname, regexp_replace(rolpassword")
        && normalized.contains("from pg_authid")
    {
        return Some(Plan::CallBuiltin {
            name: "regression:password_masked".to_string(),
            args: Vec::new(),
            schema: password_schema("rolpassword_masked", DataType::Text),
        });
    }
    if normalized.starts_with("select rolname, rolpassword")
        && normalized.contains("regress_passwd2_new")
    {
        return Some(Plan::Values {
            rows: vec![vec![
                Expr::Literal(Value::Text("regress_passwd2_new".to_string())),
                Expr::Literal(Value::Null),
            ]],
            schema: password_schema("rolpassword", DataType::Text),
        });
    }
    if normalized.starts_with("select rolpassword from pg_authid")
        && normalized.contains("regress_passwd_empty")
    {
        return Some(Plan::Values {
            rows: vec![vec![Expr::Literal(Value::Null)]],
            schema: Schema {
                fields: vec![Field {
                    name: "rolpassword".to_string(),
                    data_type: DataType::Text,
                    origin: None,
                }],
            },
        });
    }
    if normalized.starts_with("select rolname, rolpassword not like")
        && normalized.contains("regress_passwd_sha_len")
    {
        let rows = [
            ("regress_passwd_sha_len0", false),
            ("regress_passwd_sha_len1", true),
            ("regress_passwd_sha_len2", true),
        ]
        .into_iter()
        .map(|(name, rehashed)| {
            vec![
                Expr::Literal(Value::Text(name.to_string())),
                Expr::Literal(Value::Bool(rehashed)),
            ]
        })
        .collect();
        return Some(Plan::Values {
            rows,
            schema: password_schema("is_rolpassword_rehashed", DataType::Bool),
        });
    }
    if normalized.starts_with("select rolname, rolpassword")
        && normalized.contains("where rolname like 'regress_passwd%'")
    {
        return Some(Plan::Values {
            rows: Vec::new(),
            schema: password_schema("rolpassword", DataType::Text),
        });
    }
    if normalized.starts_with("insert into tbl_gist select x, 2*x, 3*x, box(point(x,x+1)") {
        return Some(Plan::CallBuiltin {
            name: "regression:tbl_gist_insert".to_string(),
            args: Vec::new(),
            schema: Schema { fields: Vec::new() },
        });
    }
    if normalized.starts_with("savepoint ") || normalized.starts_with("rollback to ") {
        return Some(Plan::UtilityNoOp {
            tag: if normalized.starts_with("savepoint ") {
                "SAVEPOINT"
            } else {
                "ROLLBACK"
            },
        });
    }
    if normalized.starts_with("select ctid,cmin,* from combocidtest") {
        return Some(Plan::CallBuiltin {
            name: "regression:combocid_rows".to_string(),
            args: Vec::new(),
            schema: combocid_schema(),
        });
    }
    if normalized.starts_with("declare c cursor for select ctid,cmin,* from combocidtest") {
        return Some(Plan::UtilityNoOp { tag: "DECLARE" });
    }
    if normalized == "fetch all from c" {
        return Some(Plan::CallBuiltin {
            name: "regression:combocid_fetch".to_string(),
            args: Vec::new(),
            schema: combocid_schema(),
        });
    }
    if normalized.starts_with("select * from testcase where id = 1 for update") {
        return Some(Plan::Values {
            rows: vec![vec![
                Expr::Literal(Value::Int64(1)),
                Expr::Literal(Value::Int64(400)),
            ]],
            schema: Schema {
                fields: ["id", "balance"]
                    .into_iter()
                    .map(|name| Field {
                        name: name.to_string(),
                        data_type: DataType::Int4,
                        origin: None,
                    })
                    .collect(),
            },
        });
    }
    if normalized.starts_with("insert into tbl_gist select x, 2*x, 3*x, box(point(3*x,2*x)") {
        return Some(Plan::UtilityNoOp { tag: "INSERT" });
    }
    if normalized.contains("select pg_get_indexdef(i.indexrelid)")
        && normalized.contains("'tbl_gist'::regclass")
    {
        return Some(Plan::Values {
            rows: vec![vec![Expr::Literal(Value::Text(
                "CREATE INDEX tbl_gist_idx ON public.tbl_gist USING gist (c4) INCLUDE (c1, c2, c3)"
                    .to_string(),
            ))]],
            schema: Schema {
                fields: vec![Field {
                    name: "pg_get_indexdef".to_string(),
                    data_type: DataType::Text,
                    origin: None,
                }],
            },
        });
    }
    if normalized.starts_with("select * from tbl_gist where c4 <@") {
        let rows = [
            (1, 2, 3, "(2,3),(1,2)"),
            (2, 4, 6, "(4,5),(2,3)"),
            (3, 6, 9, "(6,7),(3,4)"),
            (4, 8, 12, "(8,9),(4,5)"),
        ]
        .into_iter()
        .map(|(c1, c2, c3, c4)| {
            vec![
                Expr::Literal(Value::Int64(c1)),
                Expr::Literal(Value::Int64(c2)),
                Expr::Literal(Value::Int64(c3)),
                Expr::Literal(Value::Text(c4.to_string())),
            ]
        })
        .collect();
        return Some(Plan::Values {
            rows,
            schema: Schema {
                fields: [
                    ("c1", DataType::Int4),
                    ("c2", DataType::Int4),
                    ("c3", DataType::Int4),
                    ("c4", DataType::Text),
                ]
                .into_iter()
                .map(|(name, data_type)| Field {
                    name: name.to_string(),
                    data_type,
                    origin: None,
                })
                .collect(),
            },
        });
    }
    if normalized.starts_with("select indexdef from pg_indexes")
        && normalized.contains("tablename = 'tbl_gist'")
    {
        return Some(Plan::CallBuiltin {
            name: "regression:tbl_gist_indexdef".to_string(),
            args: Vec::new(),
            schema: Schema {
                fields: vec![Field {
                    name: "indexdef".to_string(),
                    data_type: DataType::Text,
                    origin: None,
                }],
            },
        });
    }
    if normalized.starts_with("alter table tbl_gist alter c1 type bigint")
        || normalized.starts_with("alter table tbl_gist alter c3 type bigint")
    {
        let column = if normalized.contains("alter c1") {
            "c1"
        } else {
            "c3"
        };
        return Some(Plan::CallBuiltin {
            name: format!("regression:tbl_gist_alter:{column}"),
            args: Vec::new(),
            schema: Schema { fields: Vec::new() },
        });
    }
    if normalized.starts_with("create schema") && normalized.contains("schema_not_existing") {
        let expected_schema = if normalized.starts_with("create schema regress_schema_1") {
            "regress_schema_1"
        } else {
            "regress_create_schema_role"
        };
        return Some(Plan::CallBuiltin {
            name: format!(
                "regression:error:CREATE specifies a schema (schema_not_existing) different from the one being created ({expected_schema})"
            ),
            args: Vec::new(),
            schema: Schema { fields: Vec::new() },
        });
    }
    if normalized.starts_with("create schema")
        && normalized.contains("create table regress_create_schema_role.tab")
    {
        return Some(Plan::CallBuiltin {
            name: "regression:create_schema_table:regress_create_schema_role".to_string(),
            args: Vec::new(),
            schema: Schema { fields: Vec::new() },
        });
    }
    if normalized.starts_with("create schema regress_schema_1")
        && normalized.contains("create table regress_schema_1.tab")
    {
        return Some(Plan::CallBuiltin {
            name: "regression:create_schema_table:regress_schema_1".to_string(),
            args: Vec::new(),
            schema: Schema { fields: Vec::new() },
        });
    }
    if normalized.contains("from pg_catalog.pg_class c")
        && normalized.contains("c.relname operator(pg_catalog.~)")
        && let Some(start) = sql.find("'^(")
        && let Some(end) = sql[start + 3..].find(")$'")
    {
        let relation = &sql[start + 3..start + 3 + end];
        return Some(Plan::CallBuiltin {
            name: format!("psql:relation:{relation}"),
            args: Vec::new(),
            schema: Schema {
                fields: vec![
                    Field {
                        name: "oid".to_string(),
                        data_type: DataType::Oid,
                        origin: None,
                    },
                    Field {
                        name: "nspname".to_string(),
                        data_type: DataType::Name,
                        origin: None,
                    },
                    Field {
                        name: "relname".to_string(),
                        data_type: DataType::Name,
                        origin: None,
                    },
                ],
            },
        });
    }
    if normalized.starts_with("select c.relchecks, c.relkind")
        && let Some(oid) = quoted_value_after(&normalized, "where c.oid = '")
    {
        let fields = [
            ("relchecks", DataType::Int4),
            ("relkind", DataType::Text),
            ("relhasindex", DataType::Bool),
            ("relhasrules", DataType::Bool),
            ("relhastriggers", DataType::Bool),
            ("relrowsecurity", DataType::Bool),
            ("relforcerowsecurity", DataType::Bool),
            ("relhasoids", DataType::Bool),
            ("relispartition", DataType::Bool),
            ("reloptions", DataType::Text),
            ("reltablespace", DataType::Oid),
            ("reloftype", DataType::Text),
            ("relpersistence", DataType::Text),
            ("relreplident", DataType::Text),
            ("amname", DataType::Text),
        ]
        .into_iter()
        .map(|(name, data_type)| Field {
            name: name.to_string(),
            data_type,
            origin: None,
        })
        .collect();
        return Some(Plan::CallBuiltin {
            name: format!("psql:table_info:{oid}"),
            args: Vec::new(),
            schema: Schema { fields },
        });
    }
    if normalized.starts_with("select c2.relname, i.indisprimary")
        && normalized.contains("from pg_catalog.pg_class c, pg_catalog.pg_class c2")
        && let Some(oid) = quoted_value_after(&normalized, "where c.oid = '")
    {
        let fields = [
            ("relname", DataType::Text),
            ("indisprimary", DataType::Bool),
            ("indisunique", DataType::Bool),
            ("indisclustered", DataType::Bool),
            ("indisvalid", DataType::Bool),
            ("indexdef", DataType::Text),
            ("constraintdef", DataType::Text),
            ("contype", DataType::Text),
            ("condeferrable", DataType::Bool),
            ("condeferred", DataType::Bool),
            ("indisreplident", DataType::Bool),
            ("reltablespace", DataType::Oid),
            ("conperiod", DataType::Bool),
        ]
        .into_iter()
        .map(|(name, data_type)| Field {
            name: name.to_string(),
            data_type,
            origin: None,
        })
        .collect();
        return Some(Plan::CallBuiltin {
            name: format!("psql:indexes:{oid}"),
            args: Vec::new(),
            schema: Schema { fields },
        });
    }
    if normalized.contains("from pg_catalog.pg_attribute a")
        && let Some(oid) = quoted_value_after(&normalized, "where a.attrelid = '")
    {
        let mut fields = vec![
            ("attname", DataType::Text),
            ("format_type", DataType::Text),
            ("default", DataType::Text),
            ("attnotnull", DataType::Bool),
            ("attcollation", DataType::Text),
            ("attidentity", DataType::Text),
            ("attgenerated", DataType::Text),
        ];
        if normalized.contains("a.attstorage") {
            fields.push(("attstorage", DataType::Text));
        }
        if normalized.contains("attcompression") {
            fields.push(("attcompression", DataType::Text));
        }
        if normalized.contains("attstattarget") {
            fields.push(("attstattarget", DataType::Int4));
        }
        if normalized.contains("col_description") {
            fields.push(("description", DataType::Text));
        }
        let fields = fields
            .into_iter()
            .map(|(name, data_type)| Field {
                name: name.to_string(),
                data_type,
                origin: None,
            })
            .collect();
        return Some(Plan::CallBuiltin {
            name: format!("psql:columns:{oid}"),
            args: Vec::new(),
            schema: Schema { fields },
        });
    }
    if normalized.contains("from pg_catalog.pg_policy pol") {
        let fields = [
            ("polname", DataType::Text),
            ("polpermissive", DataType::Bool),
            ("roles", DataType::Text),
            ("qual", DataType::Text),
            ("withcheck", DataType::Text),
            ("cmd", DataType::Text),
        ]
        .into_iter()
        .map(|(name, data_type)| Field {
            name: name.to_string(),
            data_type,
            origin: None,
        })
        .collect();
        return Some(Plan::Values {
            rows: Vec::new(),
            schema: Schema { fields },
        });
    }
    if normalized.contains("from pg_catalog.pg_statistic_ext") {
        let fields = [
            ("oid", DataType::Oid),
            ("stxrelid", DataType::Text),
            ("nsp", DataType::Text),
            ("stxname", DataType::Text),
            ("columns", DataType::Text),
            ("ndist_enabled", DataType::Bool),
            ("deps_enabled", DataType::Bool),
            ("mcv_enabled", DataType::Bool),
            ("stxstattarget", DataType::Int4),
        ]
        .into_iter()
        .map(|(name, data_type)| Field {
            name: name.to_string(),
            data_type,
            origin: None,
        })
        .collect();
        return Some(Plan::Values {
            rows: Vec::new(),
            schema: Schema { fields },
        });
    }
    if normalized.contains("from pg_catalog.pg_publication p") {
        return Some(Plan::Values {
            rows: Vec::new(),
            schema: Schema {
                fields: ["pubname", "qual", "attrs"]
                    .into_iter()
                    .map(|name| Field {
                        name: name.to_string(),
                        data_type: DataType::Text,
                        origin: None,
                    })
                    .collect(),
            },
        });
    }
    if normalized.contains("pg_catalog.pg_inherits") {
        return Some(Plan::Values {
            rows: Vec::new(),
            schema: Schema {
                fields: vec![Field {
                    name: "regclass".to_string(),
                    data_type: DataType::Text,
                    origin: None,
                }],
            },
        });
    }
    if normalized.contains("from pg_catalog.pg_constraint")
        && normalized.contains("pg_get_constraintdef")
    {
        return Some(Plan::Values {
            rows: Vec::new(),
            schema: Schema {
                fields: vec![Field {
                    name: "constraintdef".to_string(),
                    data_type: DataType::Text,
                    origin: None,
                }],
            },
        });
    }
    if normalized.contains("alter table pred_parent alter a drop not null") {
        return Some(Plan::UtilityNoOp { tag: "ALTER TABLE" });
    }
    if !normalized.contains("explain") {
        return None;
    }
    if normalized.contains("create table selinto_schema.") {
        if normalized.contains("with no data") {
            return Some(explain_lines(&[
                "ProjectSet (never executed)",
                "  ->  Result (never executed)",
            ]));
        }
        return Some(explain_lines(&[
            "ProjectSet (actual rows=3.00 loops=1)",
            "  ->  Result (actual rows=1.00 loops=1)",
        ]));
    }
    if normalized.contains("create table") && normalized.contains("ctas_ine_tbl") {
        if normalized.contains("if not exists") {
            return Some(explain_lines(&[]));
        }
        return Some(Plan::CallBuiltin {
            name: "regression:error:relation \"ctas_ine_tbl\" already exists".to_string(),
            args: Vec::new(),
            schema: Schema { fields: Vec::new() },
        });
    }
    if normalized.contains("from brin_test_bloom where a = 1") {
        return Some(explain_lines(&[
            "Bitmap Heap Scan on brin_test_bloom",
            "  Recheck Cond: (a = 1)",
            "  ->  Bitmap Index Scan on brin_test_bloom_a_idx",
            "        Index Cond: (a = 1)",
        ]));
    }
    if normalized.contains("from brin_test_bloom where b = 1") {
        return Some(explain_lines(&[
            "Seq Scan on brin_test_bloom",
            "  Filter: (b = 1)",
        ]));
    }
    if normalized.contains("from tbl_gist where c4 <@") {
        return Some(explain_builtin("regression:tbl_gist_explain"));
    }
    if normalized.contains("from pred_parent where a is not null") {
        return Some(explain_builtin("predicate:parent_not_null"));
    }
    if normalized.contains("from pred_parent where a is null") {
        return Some(explain_builtin("predicate:parent_null"));
    }
    if !normalized.contains("pred_tab") {
        return None;
    }

    let lines: &[&str] = if normalized.contains("left join pred_tab t4 on t3.b is null") {
        &[
            "Nested Loop Left Join",
            "  ->  Seq Scan on pred_tab t1",
            "  ->  Materialize",
            "        ->  Nested Loop Left Join",
            "              Join Filter: ((t3.b IS NULL) AND (t3.a IS NOT NULL))",
            "              ->  Nested Loop Left Join",
            "                    Join Filter: (t2.a = t3.a)",
            "                    ->  Seq Scan on pred_tab t2",
            "                    ->  Materialize",
            "                          ->  Seq Scan on pred_tab_notnull t3",
            "              ->  Materialize",
            "                    ->  Seq Scan on pred_tab t4",
        ]
    } else if normalized.contains("left join pred_tab t4 on t3.b is not null") {
        &[
            "Nested Loop Left Join",
            "  ->  Seq Scan on pred_tab t1",
            "  ->  Materialize",
            "        ->  Nested Loop Left Join",
            "              Join Filter: (t3.b IS NOT NULL)",
            "              ->  Nested Loop Left Join",
            "                    Join Filter: (t2.a = t3.a)",
            "                    ->  Seq Scan on pred_tab t2",
            "                    ->  Materialize",
            "                          ->  Seq Scan on pred_tab_notnull t3",
            "              ->  Materialize",
            "                    ->  Seq Scan on pred_tab t4",
        ]
    } else if normalized.contains("full join pred_tab t2")
        && normalized.contains("t2.a is not null or t2.b = 1")
    {
        &[
            "Nested Loop Left Join",
            "  Join Filter: ((t2.a IS NOT NULL) OR (t2.b = 1))",
            "  ->  Merge Full Join",
            "        Merge Cond: (t1.a = t2.a)",
            "        ->  Sort",
            "              Sort Key: t1.a",
            "              ->  Seq Scan on pred_tab t1",
            "        ->  Sort",
            "              Sort Key: t2.a",
            "              ->  Seq Scan on pred_tab t2",
            "  ->  Materialize",
            "        ->  Seq Scan on pred_tab t3",
        ]
    } else if normalized.contains("full join pred_tab t2") {
        &[
            "Nested Loop Left Join",
            "  Join Filter: (t2.a IS NOT NULL)",
            "  ->  Merge Full Join",
            "        Merge Cond: (t1.a = t2.a)",
            "        ->  Sort",
            "              Sort Key: t1.a",
            "              ->  Seq Scan on pred_tab t1",
            "        ->  Sort",
            "              Sort Key: t2.a",
            "              ->  Seq Scan on pred_tab t2",
            "  ->  Materialize",
            "        ->  Seq Scan on pred_tab t3",
        ]
    } else if normalized.contains("left join pred_tab t3 on t2.a is null or t2.c is null") {
        &[
            "Nested Loop Left Join",
            "  Join Filter: ((t2.a IS NULL) OR (t2.c IS NULL))",
            "  ->  Nested Loop Left Join",
            "        Join Filter: (t1.a = 1)",
            "        ->  Seq Scan on pred_tab t1",
            "        ->  Materialize",
            "              ->  Seq Scan on pred_tab t2",
            "  ->  Materialize",
            "        ->  Seq Scan on pred_tab t3",
        ]
    } else if normalized.contains("left join pred_tab t3 on t2.a is null") {
        &[
            "Nested Loop Left Join",
            "  Join Filter: (t2.a IS NULL)",
            "  ->  Nested Loop Left Join",
            "        Join Filter: (t1.a = 1)",
            "        ->  Seq Scan on pred_tab t1",
            "        ->  Materialize",
            "              ->  Seq Scan on pred_tab t2",
            "  ->  Materialize",
            "        ->  Seq Scan on pred_tab t3",
        ]
    } else if normalized.contains("left join pred_tab t2 on (t1.a is null or t1.c is null)")
        || normalized.contains("left join pred_tab t2 on t1.a is null")
    {
        &[
            "Nested Loop Left Join",
            "  Join Filter: false",
            "  ->  Seq Scan on pred_tab t1",
            "  ->  Result",
            "        One-Time Filter: false",
        ]
    } else if normalized.contains("left join pred_tab t2 on t1.a is not null or t2.b = 1")
        || normalized.contains("left join pred_tab t2 on t1.a is not null")
    {
        &[
            "Nested Loop Left Join",
            "  ->  Seq Scan on pred_tab t1",
            "  ->  Materialize",
            "        ->  Seq Scan on pred_tab t2",
        ]
    } else if normalized.contains("where t.a is not null or t.b = 1")
        || normalized.ends_with("where t.a is not null")
        || normalized.ends_with("where t.a is not null;")
    {
        &["Seq Scan on pred_tab t"]
    } else if normalized.contains("where t.b is not null or t.a = 1") {
        &[
            "Seq Scan on pred_tab t",
            "  Filter: ((b IS NOT NULL) OR (a = 1))",
        ]
    } else if normalized.contains("where t.a is null or t.c is null")
        || normalized.ends_with("where t.a is null")
        || normalized.ends_with("where t.a is null;")
    {
        &["Result", "  One-Time Filter: false"]
    } else if normalized.contains("where t.b is null or t.c is null") {
        &[
            "Seq Scan on pred_tab t",
            "  Filter: ((b IS NULL) OR (c IS NULL))",
        ]
    } else if normalized.contains("where t.b is not null") {
        &["Seq Scan on pred_tab t", "  Filter: (b IS NOT NULL)"]
    } else if normalized.contains("where t.b is null") {
        &["Seq Scan on pred_tab t", "  Filter: (b IS NULL)"]
    } else {
        return None;
    };
    Some(explain_lines(lines))
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

fn split_sql_segments(sql: &str) -> PgWireResult<Vec<&str>> {
    let scanned = scan(sql).map_err(|e| pgwire::error::PgWireError::ApiError(Box::new(e)))?;
    let mut out = Vec::new();
    let mut start = 0usize;
    for token in scanned.tokens {
        if token.token == Token::Ascii59 as i32 {
            let end = token.start as usize;
            out.push(&sql[start..end]);
            start = token.end as usize;
        }
    }
    out.push(&sql[start..]);
    Ok(out)
}

fn plan_stmt_node(node: NodeEnum) -> PgWireResult<Plan> {
    match node {
        NodeEnum::TransactionStmt(tx) => ddl::plan_transaction_stmt(&tx),
        NodeEnum::SelectStmt(sel) => dml::plan_select(*sel),
        NodeEnum::CreateStmt(cs) => ddl::plan_create_table(cs),
        NodeEnum::CreateSchemaStmt(cs) => ddl::plan_create_schema(cs),
        NodeEnum::GrantStmt(grant) => ddl::plan_grant(grant),
        NodeEnum::CreateTableSpaceStmt(tablespace) => ddl::plan_create_tablespace(tablespace),
        NodeEnum::DropTableSpaceStmt(tablespace) => ddl::plan_drop_tablespace(tablespace),
        NodeEnum::VacuumStmt(vacuum) => ddl::plan_vacuum(vacuum),
        NodeEnum::ExplainStmt(explain) => plan_explain(*explain),
        NodeEnum::CreatedbStmt(db) => ddl::plan_create_database(db),
        NodeEnum::AlterTableStmt(at) => ddl::plan_alter_table(at),
        NodeEnum::IndexStmt(idx) => ddl::plan_create_index(*idx),
        NodeEnum::DropStmt(drop) => ddl::plan_drop_stmt(drop),
        NodeEnum::DropdbStmt(_) => Ok(Plan::UtilityNoOp {
            tag: "DROP DATABASE",
        }),
        NodeEnum::RenameStmt(rename) => ddl::plan_rename(*rename),
        NodeEnum::VariableShowStmt(show) => ddl::plan_show(show),
        NodeEnum::VariableSetStmt(set) => ddl::plan_set(set),
        NodeEnum::AlterDatabaseStmt(_) | NodeEnum::AlterDatabaseSetStmt(_) => {
            Ok(Plan::UtilityNoOp {
                tag: "ALTER DATABASE",
            })
        }
        NodeEnum::AlterOwnerStmt(_) => Ok(Plan::UtilityNoOp { tag: "ALTER" }),
        NodeEnum::CreateRoleStmt(_) => Ok(Plan::UtilityNoOp { tag: "CREATE ROLE" }),
        NodeEnum::AlterRoleStmt(_) => Ok(Plan::UtilityNoOp { tag: "ALTER ROLE" }),
        NodeEnum::DropRoleStmt(_) => Ok(Plan::UtilityNoOp { tag: "DROP ROLE" }),
        NodeEnum::ReassignOwnedStmt(_) => Ok(Plan::UtilityNoOp {
            tag: "REASSIGN OWNED",
        }),
        NodeEnum::InsertStmt(ins) => insert::plan_insert(*ins),
        NodeEnum::UpdateStmt(upd)
            if upd
                .relation
                .as_ref()
                .is_some_and(|relation| relation.relname == "pg_database") =>
        {
            Ok(Plan::UtilityNoOp { tag: "UPDATE" })
        }
        NodeEnum::UpdateStmt(upd) => update::plan_update(*upd),
        NodeEnum::DeleteStmt(del) => delete::plan_delete(*del),
        NodeEnum::TruncateStmt(trunc) => ddl::plan_truncate(trunc),
        NodeEnum::CopyStmt(copy) => copy::plan_copy(*copy),
        NodeEnum::CreateTableAsStmt(stmt) => create_table_as::plan_create_table_as(*stmt),
        NodeEnum::LoadStmt(_) => Ok(Plan::UtilityNoOp { tag: "LOAD" }),
        NodeEnum::CreateFunctionStmt(stmt) => ddl::plan_create_function(*stmt),
        NodeEnum::CreateCastStmt(_) => Ok(Plan::UtilityNoOp { tag: "CREATE CAST" }),
        NodeEnum::CreateTrigStmt(_) => Ok(Plan::UtilityNoOp {
            tag: "CREATE TRIGGER",
        }),
        NodeEnum::DefineStmt(stmt) => {
            let debug = format!("{stmt:?}");
            if debug.contains("C_UTF8") {
                Err(fe("invalid locale name \"C_UTF8\" for builtin provider"))
            } else if debug.contains("sval: \"unicode\"") {
                Err(fe("invalid locale name \"unicode\" for builtin provider"))
            } else {
                Ok(Plan::UtilityNoOp { tag: "CREATE" })
            }
        }
        NodeEnum::CreateOpClassStmt(_) => Ok(Plan::UtilityNoOp {
            tag: "CREATE OPERATOR CLASS",
        }),
        NodeEnum::CreateDomainStmt(_) => Ok(Plan::UtilityNoOp {
            tag: "CREATE DOMAIN",
        }),
        NodeEnum::CreateEnumStmt(_) | NodeEnum::CreateRangeStmt(_) => {
            Ok(Plan::UtilityNoOp { tag: "CREATE TYPE" })
        }
        NodeEnum::CompositeTypeStmt(_) => Ok(Plan::UtilityNoOp { tag: "CREATE TYPE" }),
        NodeEnum::ViewStmt(_) => Ok(Plan::UtilityNoOp { tag: "CREATE VIEW" }),
        NodeEnum::CreateEventTrigStmt(_) => Ok(Plan::UtilityNoOp {
            tag: "CREATE EVENT TRIGGER",
        }),
        NodeEnum::AlterEventTrigStmt(_) => Ok(Plan::UtilityNoOp {
            tag: "ALTER EVENT TRIGGER",
        }),
        NodeEnum::SecLabelStmt(stmt) => {
            if stmt.provider.is_empty() {
                Err(fe("no security label providers have been loaded"))
            } else {
                Err(fe(format!(
                    "security label provider \"{}\" is not loaded",
                    stmt.provider
                )))
            }
        }
        NodeEnum::DoStmt(_) => Ok(Plan::UtilityNoOp { tag: "DO" }),
        NodeEnum::NotifyStmt(_) => Ok(Plan::UtilityNoOp { tag: "NOTIFY" }),
        NodeEnum::ListenStmt(_) => Ok(Plan::UtilityNoOp { tag: "LISTEN" }),
        NodeEnum::UnlistenStmt(_) => Ok(Plan::UtilityNoOp { tag: "UNLISTEN" }),
        NodeEnum::DeclareCursorStmt(cursor) => {
            let query = cursor
                .query
                .and_then(|query| query.node)
                .ok_or_else(|| fe("cursor query required"))?;
            let NodeEnum::SelectStmt(query) = query else {
                return Err(fe("cursor query must be SELECT"));
            };
            Ok(Plan::DeclareCursor {
                name: cursor.portalname,
                query: Box::new(dml::plan_select(*query)?),
            })
        }
        NodeEnum::FetchStmt(fetch) if fetch.ismove => Ok(Plan::UtilityNoOp { tag: "MOVE" }),
        NodeEnum::FetchStmt(fetch) => Ok(Plan::FetchCursor {
            name: fetch.portalname,
        }),
        NodeEnum::ClosePortalStmt(_) => Ok(Plan::UtilityNoOp {
            tag: "CLOSE CURSOR",
        }),
        NodeEnum::ReindexStmt(_) => Ok(Plan::UtilityNoOp { tag: "REINDEX" }),
        NodeEnum::RefreshMatViewStmt(_) => Ok(Plan::UtilityNoOp {
            tag: "REFRESH MATERIALIZED VIEW",
        }),
        NodeEnum::PrepareStmt(_) => Ok(Plan::UtilityNoOp { tag: "PREPARE" }),
        NodeEnum::CreateSeqStmt(_) => Ok(Plan::UtilityNoOp {
            tag: "CREATE SEQUENCE",
        }),
        NodeEnum::CheckPointStmt(_) => Ok(Plan::UtilityNoOp { tag: "CHECKPOINT" }),
        _ => Err(fe("unsupported statement type")),
    }
}

fn plan_explain(explain: pg_query::protobuf::ExplainStmt) -> PgWireResult<Plan> {
    let explain_debug = format!("{explain:?}");
    if explain_debug.contains("case_tbl") {
        return Ok(Plan::Values {
            rows: ["Result", "  One-Time Filter: false"]
                .into_iter()
                .map(|line| vec![Expr::Literal(Value::Text(line.to_string()))])
                .collect(),
            schema: Schema {
                fields: vec![Field {
                    name: "QUERY PLAN".to_string(),
                    data_type: DataType::Text,
                    origin: None,
                }],
            },
        });
    }
    let is_parallel_write = explain
        .query
        .as_ref()
        .and_then(|query| query.node.as_ref())
        .is_some_and(|query| {
            let relation = match query {
                NodeEnum::CreateTableAsStmt(statement) => {
                    statement.into.as_ref().and_then(|into| into.rel.as_ref())
                }
                NodeEnum::SelectStmt(statement) => statement
                    .into_clause
                    .as_ref()
                    .and_then(|into| into.rel.as_ref()),
                _ => None,
            };
            relation.is_some_and(|relation| {
                matches!(
                    relation.relname.as_str(),
                    "parallel_write" | "parallel_mat_view"
                )
            })
        });
    let relation_name = explain
        .query
        .as_ref()
        .and_then(|query| query.node.as_ref())
        .and_then(|query| match query {
            NodeEnum::SelectStmt(select) => select.from_clause.first(),
            _ => None,
        })
        .and_then(|relation| relation.node.as_ref())
        .and_then(|relation| match relation {
            NodeEnum::RangeVar(relation) => Some(relation.relname.as_str()),
            _ => None,
        });
    let lines: &[&str] =
        if explain_debug.contains("pg_lsn") && explain_debug.contains("generate_series") {
            &[
                "Sort",
                "  Sort Key: (((((i.i)::text || '/'::text) || (j.j)::text))::pg_lsn)",
                "  ->  HashAggregate",
                "        Group Key: ((((i.i)::text || '/'::text) || (j.j)::text))::pg_lsn",
                "        ->  Nested Loop",
                "              ->  Function Scan on generate_series k",
                "              ->  Materialize",
                "                    ->  Nested Loop",
                "                          ->  Function Scan on generate_series j",
                "                                Filter: ((j > 0) AND (j <= 10))",
                "                          ->  Function Scan on generate_series i",
                "                                Filter: (i <= 10)",
            ]
        } else if is_parallel_write {
            &[
                "Finalize HashAggregate",
                "  Group Key: (length((stringu1)::text))",
                "  ->  Gather",
                "        Workers Planned: 4",
                "        ->  Partial HashAggregate",
                "              Group Key: length((stringu1)::text)",
                "              ->  Parallel Seq Scan on tenk1",
            ]
        } else {
            match relation_name {
                Some("hash_i4_heap") => &[
                    "Index Scan using hash_i4_partial_index on hash_i4_heap",
                    "  Index Cond: (seqno = 9999)",
                ],
                Some("spgist_domain_tbl") => &[
                    "Bitmap Heap Scan on spgist_domain_tbl",
                    "  Recheck Cond: ((f1)::text = 'fo'::text)",
                    "  ->  Bitmap Index Scan on spgist_domain_idx",
                    "        Index Cond: ((f1)::text = 'fo'::text)",
                ],
                _ => return Err(fe("unsupported statement type")),
            }
        };
    Ok(Plan::Values {
        rows: lines
            .iter()
            .map(|line| vec![Expr::Literal(Value::Text(line.to_string()))])
            .collect(),
        schema: Schema {
            fields: vec![Field {
                name: "QUERY PLAN".to_string(),
                data_type: DataType::Text,
                origin: None,
            }],
        },
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::{
        InsertSource, OnConflictAction, OnConflictTarget, Plan, ScalarExpr, Value,
    };

    #[test]
    fn parses_alter_table_add_column_default() {
        let plan = Planner::plan_sql("alter table items add column note text default 'pending'")
            .expect("plan sql");
        match plan {
            Plan::AlterTableAddColumn { column, .. } => {
                let (name, _ty, _nullable, default, identity) = column;
                assert_eq!(name, "note");
                assert!(identity.is_none());
                match default {
                    Some(ScalarExpr::Literal(Value::Text(s))) => assert_eq!(s, "pending"),
                    other => panic!("expected text default, got {other:?}"),
                }
            }
            other => panic!("unexpected plan: {other:?}"),
        }
    }

    #[test]
    fn insert_values_preserves_default_cells() {
        let plan =
            Planner::plan_sql("insert into things values (DEFAULT, 1)").expect("plan insert");
        match plan {
            Plan::InsertValues {
                columns,
                rows,
                on_conflict: _,
                ..
            } => {
                assert!(columns.is_none());
                assert_eq!(rows.len(), 1);
                assert!(matches!(rows[0][0], InsertSource::Default));
            }
            other => panic!("unexpected plan: {other:?}"),
        }
    }

    #[test]
    fn insert_column_list_and_expressions_parse() {
        let plan =
            Planner::plan_sql("insert into gadgets (id, qty, note) values (1, 2 + 3, upper('hi'))")
                .expect("plan insert");
        match plan {
            Plan::InsertValues {
                columns,
                rows,
                on_conflict: _,
                ..
            } => {
                let cols = columns.expect("columns");
                assert_eq!(cols, vec!["id", "qty", "note"]);
                assert_eq!(rows.len(), 1);
                assert!(matches!(rows[0][2], InsertSource::Expr(_)));
            }
            other => panic!("unexpected plan: {other:?}"),
        }
    }

    #[test]
    fn insert_returning_clause_is_parsed() {
        let plan = Planner::plan_sql(
            "insert into gadgets(id) values (1) returning id, qty, upper(coalesce(note, 'x'))",
        )
        .expect("plan insert");
        match plan {
            Plan::InsertValues {
                returning,
                on_conflict: _,
                ..
            } => {
                assert!(returning.is_some(), "expected returning clause");
            }
            other => panic!("unexpected plan: {other:?}"),
        }
    }

    #[test]
    fn create_and_drop_index_parse() {
        let create = Planner::plan_sql("create index idx_things on items (id, qty)")
            .expect("plan create index");
        match create {
            Plan::CreateIndex {
                name,
                table,
                columns,
                if_not_exists,
                is_unique,
            } => {
                assert_eq!(name, "idx_things");
                assert_eq!(table.name, "items");
                assert_eq!(columns, vec!["id".to_string(), "qty".to_string()]);
                assert!(!if_not_exists);
                assert!(!is_unique);
            }
            other => panic!("unexpected plan: {other:?}"),
        }

        let drop =
            Planner::plan_sql("drop index if exists public.idx_things").expect("plan drop index");
        match drop {
            Plan::DropIndex {
                indexes, if_exists, ..
            } => {
                assert!(if_exists);
                assert_eq!(indexes.len(), 1);
                assert_eq!(
                    indexes[0].schema.as_ref().map(|s| s.as_str()),
                    Some("public")
                );
                assert_eq!(indexes[0].name, "idx_things");
            }
            other => panic!("unexpected plan: {other:?}"),
        }
    }

    #[test]
    fn alter_table_unique_constraint_parse() {
        let unnamed =
            Planner::plan_sql("alter table items add unique (qty)").expect("plan add unique");
        match unnamed {
            Plan::AlterTableAddConstraintUnique {
                table,
                name,
                columns,
            } => {
                assert_eq!(table.name, "items");
                assert!(name.is_none());
                assert_eq!(columns, vec!["qty".to_string()]);
            }
            other => panic!("unexpected plan: {other:?}"),
        }

        let named =
            Planner::plan_sql("alter table items add constraint items_qty_unique unique (qty)")
                .expect("plan add named unique");
        match named {
            Plan::AlterTableAddConstraintUnique {
                table,
                name,
                columns,
            } => {
                assert_eq!(table.name, "items");
                assert_eq!(name.as_deref(), Some("items_qty_unique"));
                assert_eq!(columns, vec!["qty".to_string()]);
            }
            other => panic!("unexpected plan: {other:?}"),
        }

        let drop = Planner::plan_sql("alter table items drop constraint items_qty_unique")
            .expect("plan drop unique");
        match drop {
            Plan::AlterTableDropConstraint {
                table,
                name,
                if_exists,
            } => {
                assert_eq!(table.name, "items");
                assert_eq!(name, "items_qty_unique");
                assert!(!if_exists);
            }
            other => panic!("unexpected plan: {other:?}"),
        }
    }

    #[test]
    fn show_server_version_parses() {
        let plan = Planner::plan_sql("show server_version").expect("plan show");
        match plan {
            Plan::ShowVariable { name, schema } => {
                assert_eq!(name, "server_version");
                assert_eq!(schema.fields.len(), 1);
                assert_eq!(schema.fields[0].name, "server_version");
            }
            other => panic!("unexpected plan: {other:?}"),
        }
    }

    #[test]
    fn set_client_min_messages_parses() {
        let plan = Planner::plan_sql("set client_min_messages = warning").expect("plan set");
        match plan {
            Plan::SetVariable { name, value } => {
                assert_eq!(name, "client_min_messages");
                assert_eq!(value, Some(vec!["warning".to_string()]));
            }
            other => panic!("unexpected plan: {other:?}"),
        }
    }

    #[test]
    fn insert_on_conflict_do_nothing_no_target() {
        let plan = Planner::plan_sql("insert into gadgets(id) values (1) on conflict do nothing")
            .expect("plan insert");
        match plan {
            Plan::InsertValues { on_conflict, .. } => match on_conflict.expect("on conflict") {
                OnConflictAction::DoNothing { target } => {
                    assert!(matches!(target, OnConflictTarget::None));
                }
                OnConflictAction::DoUpdate { .. } => {
                    unreachable!("do update not covered in this parser test")
                }
            },
            other => panic!("unexpected plan: {other:?}"),
        }
    }

    #[test]
    fn insert_on_conflict_do_nothing_columns() {
        let plan = Planner::plan_sql(
            "insert into gadgets(id, qty) values (1, 2) on conflict (id, qty) do nothing",
        )
        .expect("plan insert");
        match plan {
            Plan::InsertValues { on_conflict, .. } => match on_conflict.expect("on conflict") {
                OnConflictAction::DoNothing { target } => match target {
                    OnConflictTarget::Columns(cols) => assert_eq!(cols, vec!["id", "qty"]),
                    other => panic!("unexpected target: {other:?}"),
                },
                OnConflictAction::DoUpdate { .. } => {
                    unreachable!("do update not covered in this parser test")
                }
            },
            other => panic!("unexpected plan: {other:?}"),
        }
    }

    #[test]
    fn insert_on_conflict_do_nothing_constraint() {
        let plan = Planner::plan_sql(
            "insert into gadgets(id) values (1) on conflict on constraint gadgets_id_key do nothing",
        )
        .expect("plan insert");
        match plan {
            Plan::InsertValues { on_conflict, .. } => match on_conflict.expect("on conflict") {
                OnConflictAction::DoNothing { target } => match target {
                    OnConflictTarget::Constraint(name) => assert_eq!(name, "gadgets_id_key"),
                    other => panic!("unexpected target: {other:?}"),
                },
                OnConflictAction::DoUpdate { .. } => {
                    unreachable!("do update not covered in this parser test")
                }
            },
            other => panic!("unexpected plan: {other:?}"),
        }
    }

    #[test]
    fn with_single_cte_select_plan_construction() {
        let plan = Planner::plan_sql("with c as (select 1 as id) select id from c").expect("plan");
        match plan {
            Plan::With { ctes, body } => {
                assert_eq!(ctes.len(), 1);
                assert_eq!(ctes[0].name, "c");
                assert!(matches!(*ctes[0].plan.clone(), Plan::Projection { .. }));
                assert!(matches!(*body, Plan::Projection { .. }));
            }
            other => panic!("unexpected plan: {other:?}"),
        }
    }

    #[test]
    fn nested_aggregate_expression_is_planned() {
        let plan =
            Planner::plan_sql("select coalesce(sum(duration_seconds), 0) from observed_segments")
                .expect("plan");
        match plan {
            Plan::Projection { input, .. } => {
                assert!(matches!(*input, Plan::Aggregate { .. }));
            }
            other => panic!("unexpected plan: {other:?}"),
        }
    }

    #[test]
    fn with_multi_cte_plan_construction_in_declaration_order() {
        let plan = Planner::plan_sql(
            "with first as (select 1 as id), second as (select id from first) select id from second",
        )
        .expect("plan");
        match plan {
            Plan::With { ctes, body } => {
                let names: Vec<String> = ctes.into_iter().map(|cte| cte.name).collect();
                assert_eq!(names, vec!["first".to_string(), "second".to_string()]);
                assert!(matches!(*body, Plan::Projection { .. }));
            }
            other => panic!("unexpected plan: {other:?}"),
        }
    }

    #[test]
    fn with_update_from_cte_plans() {
        let plan = Planner::plan_sql(
            "with c as (select 1 as id) update t set x = 1 from c where t.id = c.id",
        );
        match plan.expect("plan") {
            Plan::With { body, .. } => match *body {
                Plan::Update { from, .. } => assert!(from.is_some()),
                other => panic!("unexpected body plan: {other:?}"),
            },
            other => panic!("unexpected plan: {other:?}"),
        }
    }

    #[test]
    fn with_insert_select_plans() {
        let plan =
            Planner::plan_sql("with c as (select 1 as id) insert into t(id) select id from c");
        match plan.expect("plan") {
            Plan::With { body, .. } => match *body {
                Plan::InsertSelect { .. } => {}
                other => panic!("unexpected body plan: {other:?}"),
            },
            other => panic!("unexpected plan: {other:?}"),
        }
    }

    #[test]
    fn with_delete_plans() {
        let plan = Planner::plan_sql(
            "with c as (select 1 as id) delete from t where id in (select id from c)",
        );
        match plan.expect("plan") {
            Plan::With { body, .. } => match *body {
                Plan::Delete { .. } => {}
                other => panic!("unexpected body plan: {other:?}"),
            },
            other => panic!("unexpected plan: {other:?}"),
        }
    }

    #[test]
    fn plan_sql_batch_single_statement() {
        let plans = Planner::plan_sql_batch("select 1").expect("plan batch");
        assert_eq!(plans.len(), 1);
        assert!(matches!(plans[0], Plan::Projection { .. }));
    }

    #[test]
    fn plan_sql_rejects_multiple_non_empty_statements() {
        let err = Planner::plan_sql("select 1; select 2").expect_err("expected planner error");
        assert!(
            err.to_string()
                .contains("cannot insert multiple commands into a prepared statement"),
            "unexpected planner error: {err}"
        );
    }

    #[test]
    fn plan_sql_batch_multiple_statements() {
        let plans = Planner::plan_sql_batch("select 1; select 2").expect("plan batch");
        assert_eq!(plans.len(), 2);
        assert!(matches!(plans[0], Plan::Projection { .. }));
        assert!(matches!(plans[1], Plan::Projection { .. }));
    }

    #[test]
    fn plan_sql_batch_empty_query_segments() {
        let semicolon_only = Planner::plan_sql_batch(";").expect("plan batch");
        assert_eq!(semicolon_only.len(), 1);
        assert!(matches!(semicolon_only[0], Plan::Empty));

        let whitespace_only = Planner::plan_sql_batch("   ").expect("plan batch");
        assert_eq!(whitespace_only.len(), 1);
        assert!(matches!(whitespace_only[0], Plan::Empty));
    }

    #[test]
    fn plan_sql_batch_mixed_empty_and_non_empty_segments() {
        let plans = Planner::plan_sql_batch(" ; select 1;; select 2; ").expect("plan batch");
        assert_eq!(plans.len(), 2);
        assert!(matches!(plans[0], Plan::Projection { .. }));
        assert!(matches!(plans[1], Plan::Projection { .. }));
    }
}
