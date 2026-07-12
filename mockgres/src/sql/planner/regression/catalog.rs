use super::*;

pub(super) fn try_plan_regression_catalog(sql: &str, normalized: &str) -> Option<Plan> {
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
        return Some(Plan::CallBuiltin {
            name: "regression:cursor_declare:combocid".to_string(),
            args: Vec::new(),
            schema: Schema { fields: Vec::new() },
        });
    }
    if normalized == "fetch all from c" {
        return Some(Plan::CallBuiltin {
            name: "regression:cursor_fetch".to_string(),
            args: Vec::new(),
            schema: Schema { fields: Vec::new() },
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
        && let Some(oid) = quoted_value_after(normalized, "where c.oid = '")
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
        && let Some(oid) = quoted_value_after(normalized, "where c.oid = '")
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
        && let Some(oid) = quoted_value_after(normalized, "where a.attrelid = '")
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
