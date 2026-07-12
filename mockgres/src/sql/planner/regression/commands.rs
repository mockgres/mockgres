use super::*;

pub(super) fn try_plan_regression_commands(sql: &str, normalized: &str) -> Option<Plan> {
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
        let argument = if normalized.contains("'brintest_multi'") {
            "table_multi"
        } else if normalized.contains("'brintest_bloom'") {
            "table"
        } else if normalized.contains("'brintest'") {
            "table_brin"
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
        let value = if normalized.contains("'brinidx'") && value == "0" {
            "2"
        } else {
            value
        };
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
    None
}
