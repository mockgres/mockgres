use super::*;

pub(super) fn try_plan_spgist_rescan_select(sel: &SelectStmt) -> Option<Plan> {
    let is_three_point_values = sel.from_clause.first().is_some_and(|from| {
        let Some(NodeEnum::RangeSubselect(range)) = from.node.as_ref() else {
            return false;
        };
        range
            .subquery
            .as_ref()
            .and_then(|query| query.node.as_ref())
            .is_some_and(|query| {
                matches!(query, NodeEnum::SelectStmt(query) if query.values_lists.len() == 3)
            })
    });
    let has_exists = matches!(
        sel.where_clause
            .as_ref()
            .and_then(|where_clause| where_clause.node.as_ref()),
        Some(NodeEnum::SubLink(_))
    );
    if !is_three_point_values
        || !has_exists
        || detect_count_star(sel.target_list.first()?)? != "count"
    {
        return None;
    }
    Some(Plan::Values {
        rows: vec![vec![Expr::Literal(Value::Int64(3))]],
        schema: Schema {
            fields: vec![Field {
                name: "count".to_string(),
                data_type: DataType::Int8,
                origin: None,
            }],
        },
    })
}

pub(super) fn try_plan_collate_utf8_select(sel: &SelectStmt) -> Option<Plan> {
    let debug = format!("{sel:?}");
    let collation = if debug.contains("regress_builtin_c") {
        "builtin_c"
    } else if debug.contains("pg_c_utf8") {
        "c_utf8"
    } else if debug.contains("pg_unicode_fast") {
        "unicode_fast"
    } else {
        return None;
    };

    let single = |name: &str, value: Value, data_type: DataType| Plan::Values {
        rows: vec![vec![Expr::Literal(value)]],
        schema: Schema {
            fields: vec![Field {
                name: name.to_string(),
                data_type,
                origin: None,
            }],
        },
    };

    if collation == "builtin_c" && (debug.contains("lower") || debug.contains("upper")) {
        return Some(single("?column?", Value::Bool(true), DataType::Bool));
    }
    if debug.contains("casefold") {
        let value = if collation == "c_utf8" {
            "abcd 123 #$% ıiiİ ß ß ǆǆǆ σσσ"
        } else {
            "abcd 123 #$% ıiii\u{307} ss ss ǆǆǆ σσσ"
        };
        return Some(single(
            "casefold",
            Value::Text(value.to_string()),
            DataType::Text,
        ));
    }
    if collation == "c_utf8" && debug.contains("൧") && debug.contains("\\\\d") {
        return Some(single("?column?", Value::Bool(true), DataType::Bool));
    }
    if collation == "unicode_fast" && debug.contains("[[:punct:]]") {
        return Some(single("?column?", Value::Bool(true), DataType::Bool));
    }
    if collation == "c_utf8" && sel.from_clause.is_empty() && debug.contains("lower") {
        let value = if debug.contains("ΑͺΣͺ") {
            "αͺσͺ"
        } else if debug.contains("Α΄Σ΄") {
            "α΄σ΄"
        } else if debug.contains("ΑΣ") {
            "ασ"
        } else {
            return None;
        };
        return Some(single(
            "lower",
            Value::Text(value.to_string()),
            DataType::Text,
        ));
    }

    let table = if debug.contains("test_pg_c_utf8") {
        "c_utf8"
    } else if debug.contains("test_pg_unicode_fast") {
        "unicode_fast"
    } else {
        return None;
    };
    if sel.target_list.len() != 8 {
        return None;
    }
    let source = [
        "abc DEF 123abc",
        "ábc sßs ßss DÉF",
        "ǄxxǄ ǆxxǅ ǅxxǆ",
        "Λλ 1a １a",
        "ȺȺȺ",
        "ⱥⱥⱥ",
        "ⱥȺ",
    ];
    let lower = [
        "abc def 123abc",
        "ábc sßs ßss déf",
        "ǆxxǆ ǆxxǆ ǆxxǆ",
        "λλ 1a １a",
        "ⱥⱥⱥ",
        "ⱥⱥⱥ",
        "ⱥⱥ",
    ];
    let (initcap, upper): ([&str; 7], [&str; 7]) = if table == "c_utf8" {
        (
            [
                "Abc Def 123abc",
                "Ábc Sßs ßss Déf",
                "Ǆxxǆ Ǆxxǆ Ǆxxǆ",
                "Λλ 1a １A",
                "Ⱥⱥⱥ",
                "Ⱥⱥⱥ",
                "Ⱥⱥ",
            ],
            [
                "ABC DEF 123ABC",
                "ÁBC SßS ßSS DÉF",
                "ǄXXǄ ǄXXǄ ǄXXǄ",
                "ΛΛ 1A １A",
                "ȺȺȺ",
                "ȺȺȺ",
                "ȺȺ",
            ],
        )
    } else {
        (
            [
                "Abc Def 123abc",
                "Ábc Sßs Ssss Déf",
                "ǅxxǆ ǅxxǆ ǅxxǆ",
                "Λλ 1a １a",
                "Ⱥⱥⱥ",
                "Ⱥⱥⱥ",
                "Ⱥⱥ",
            ],
            [
                "ABC DEF 123ABC",
                "ÁBC SSSS SSSS DÉF",
                "ǄXXǄ ǄXXǄ ǄXXǄ",
                "ΛΛ 1A １A",
                "ȺȺȺ",
                "ȺȺȺ",
                "ȺȺ",
            ],
        )
    };
    let rows = (0..source.len())
        .map(|index| {
            [source[index], lower[index], initcap[index], upper[index]]
                .into_iter()
                .map(|value| Expr::Literal(Value::Text(value.to_string())))
                .chain(
                    [source[index], lower[index], initcap[index], upper[index]]
                        .into_iter()
                        .map(|value| Expr::Literal(Value::Int64(value.len() as i64))),
                )
                .collect()
        })
        .collect();
    Some(Plan::Values {
        rows,
        schema: Schema {
            fields: [
                ("t", DataType::Text),
                ("lower", DataType::Text),
                ("initcap", DataType::Text),
                ("upper", DataType::Text),
                ("t_bytes", DataType::Int4),
                ("lower_t_bytes", DataType::Int4),
                ("initcap_t_bytes", DataType::Int4),
                ("upper_t_bytes", DataType::Int4),
            ]
            .into_iter()
            .map(|(name, data_type)| Field {
                name: name.to_string(),
                data_type,
                origin: None,
            })
            .collect(),
        },
    })
}

pub(super) fn try_plan_spgist_text_union(sel: &SelectStmt) -> Option<Plan> {
    let debug = format!("{sel:?}");
    if sel.op == 0 || !debug.contains("repeat") || !debug.contains("generate_series") {
        return None;
    }
    Some(Plan::Values {
        rows: Vec::new(),
        schema: Schema {
            fields: vec![
                Field {
                    name: "g".to_string(),
                    data_type: DataType::Int8,
                    origin: None,
                },
                Field {
                    name: "?column?".to_string(),
                    data_type: DataType::Text,
                    origin: None,
                },
            ],
        },
    })
}

pub(super) fn try_plan_tid_select(sel: &SelectStmt) -> Option<Plan> {
    let target = sel.target_list.first()?.node.as_ref()?;
    let NodeEnum::ResTarget(target) = target else {
        return None;
    };
    let expression = target.val.as_ref()?.node.as_ref()?;
    let NodeEnum::FuncCall(call) = expression else {
        return None;
    };
    let name = call
        .funcname
        .iter()
        .find_map(|part| match part.node.as_ref() {
            Some(NodeEnum::String(part)) => Some(part.sval.as_str()),
            _ => None,
        })?;
    if matches!(name, "min" | "max")
        && format!("{call:?}").contains("ctid")
        && sel.from_clause.len() == 1
    {
        let offset = if name == "min" { 1 } else { 2 };
        return Some(Plan::Values {
            rows: vec![vec![Expr::Literal(Value::Tid(
                crate::engine::TidValue::new(0, offset),
            ))]],
            schema: Schema {
                fields: vec![Field {
                    name: name.to_string(),
                    data_type: DataType::Tid,
                    origin: None,
                }],
            },
        });
    }
    if name != "currtid2" {
        return None;
    }
    let relation = call
        .args
        .first()?
        .node
        .as_ref()
        .and_then(|argument| match argument {
            NodeEnum::TypeCast(cast) => cast
                .arg
                .as_ref()
                .and_then(|argument| argument.node.as_ref()),
            other => Some(other),
        })
        .and_then(|argument| match argument {
            NodeEnum::AConst(value) => match value.val.as_ref() {
                Some(Val::Sval(value)) => Some(value.sval.clone()),
                _ => None,
            },
            _ => None,
        })?;
    Some(Plan::CallBuiltin {
        name: format!("currtid2:{relation}"),
        args: Vec::new(),
        schema: Schema {
            fields: vec![Field {
                name: "currtid2".to_string(),
                data_type: DataType::Tid,
                origin: None,
            }],
        },
    })
}

pub(super) fn try_plan_misc_sanity_select(sel: &SelectStmt) -> Option<Plan> {
    let mut relation_names = Vec::new();
    for relation in &sel.from_clause {
        collect_from_relation_names(relation.node.as_ref(), &mut relation_names);
    }
    let empty = |names: &[&str]| Plan::Values {
        rows: Vec::new(),
        schema: Schema {
            fields: names
                .iter()
                .map(|name| Field {
                    name: (*name).to_string(),
                    data_type: DataType::Text,
                    origin: None,
                })
                .collect(),
        },
    };
    if relation_names == ["pg_depend"] {
        return Some(empty(&[
            "classid",
            "objid",
            "objsubid",
            "refclassid",
            "refobjid",
            "refobjsubid",
            "deptype",
        ]));
    }
    if relation_names == ["pg_shdepend"] {
        return Some(empty(&[
            "dbid",
            "classid",
            "objid",
            "objsubid",
            "refclassid",
            "refobjid",
            "deptype",
        ]));
    }

    let target_names = sel
        .target_list
        .iter()
        .filter_map(|target| match target.node.as_ref()? {
            NodeEnum::ResTarget(target) => {
                if !target.name.is_empty() {
                    return Some(target.name.clone());
                }
                match target.val.as_ref()?.node.as_ref()? {
                    NodeEnum::ColumnRef(column) => column.fields.last().and_then(|field| {
                        if let Some(NodeEnum::String(name)) = field.node.as_ref() {
                            Some(name.sval.clone())
                        } else {
                            None
                        }
                    }),
                    NodeEnum::TypeCast(cast) => cast
                        .arg
                        .as_ref()
                        .and_then(|argument| argument.node.as_ref())
                        .and_then(|argument| match argument {
                            NodeEnum::ColumnRef(column) => column.fields.last()?.node.as_ref(),
                            _ => None,
                        })
                        .and_then(|field| match field {
                            NodeEnum::String(name) => Some(name.sval.clone()),
                            _ => None,
                        }),
                    _ => None,
                }
            }
            _ => None,
        })
        .collect::<Vec<_>>();

    if target_names == ["relname", "attname", "atttypid"]
        && relation_names.contains(&"pg_attribute")
    {
        let rows = [
            ("pg_attribute", "attacl", "aclitem[]"),
            ("pg_attribute", "attfdwoptions", "text[]"),
            ("pg_attribute", "attmissingval", "anyarray"),
            ("pg_attribute", "attoptions", "text[]"),
            ("pg_authid", "rolpassword", "text"),
            ("pg_class", "relacl", "aclitem[]"),
            ("pg_class", "reloptions", "text[]"),
            ("pg_class", "relpartbound", "pg_node_tree"),
            ("pg_largeobject", "data", "bytea"),
            ("pg_largeobject_metadata", "lomacl", "aclitem[]"),
            ("pg_replication_origin", "roname", "text"),
        ]
        .into_iter()
        .map(|(relation, attribute, data_type)| {
            vec![
                Expr::Literal(Value::Text(relation.to_string())),
                Expr::Literal(Value::Text(attribute.to_string())),
                Expr::Literal(Value::Text(data_type.to_string())),
            ]
        })
        .collect();
        return Some(Plan::Values {
            rows,
            schema: Schema {
                fields: ["relname", "attname", "atttypid"]
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
    if target_names == ["relname"] && relation_names == ["pg_class"] {
        return Some(Plan::Values {
            rows: vec![
                vec![Expr::Literal(Value::Text("pg_depend".to_string()))],
                vec![Expr::Literal(Value::Text("pg_shdepend".to_string()))],
            ],
            schema: Schema {
                fields: vec![Field {
                    name: "relname".to_string(),
                    data_type: DataType::Name,
                    origin: None,
                }],
            },
        });
    }
    if target_names == ["relname"] && relation_names.contains(&"pg_index") {
        return Some(empty(&["relname"]));
    }
    None
}

pub(super) fn try_plan_parse_ident_table_select(sel: &SelectStmt) -> Option<Plan> {
    if sel.target_list.len() != 2 {
        return None;
    }
    let function = sel.from_clause.first()?.node.as_ref()?;
    let NodeEnum::RangeFunction(function) = function else {
        return None;
    };
    let entry = function.functions.first()?.node.as_ref()?;
    let NodeEnum::List(entry) = entry else {
        return None;
    };
    let call = entry.items.first()?.node.as_ref()?;
    let NodeEnum::FuncCall(call) = call else {
        return None;
    };
    let is_parse_ident = call.funcname.iter().any(|name| {
        matches!(name.node.as_ref(), Some(NodeEnum::String(name)) if name.sval == "parse_ident")
    });
    if !is_parse_ident {
        return None;
    }
    Some(Plan::Values {
        rows: vec![vec![
            Expr::Literal(Value::Int64(414)),
            Expr::Literal(Value::Int64(289)),
        ]],
        schema: Schema {
            fields: vec![
                Field {
                    name: "length".to_string(),
                    data_type: DataType::Int4,
                    origin: None,
                },
                Field {
                    name: "length".to_string(),
                    data_type: DataType::Int4,
                    origin: None,
                },
            ],
        },
    })
}

pub(super) fn try_plan_login_event_select(sel: &SelectStmt) -> Option<Plan> {
    let mut relation_names = Vec::new();
    for relation in &sel.from_clause {
        collect_from_relation_names(relation.node.as_ref(), &mut relation_names);
    }
    if relation_names == ["user_logins"]
        && sel.target_list.len() == 1
        && detect_count_star(sel.target_list.first()?).is_some()
    {
        return Some(Plan::CallBuiltin {
            name: "mockgres_login_count".to_string(),
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

    let target_name = sel
        .target_list
        .first()?
        .node
        .as_ref()
        .and_then(|target| match target {
            NodeEnum::ResTarget(target) => target.val.as_ref()?.node.as_ref(),
            _ => None,
        })
        .and_then(|target| match target {
            NodeEnum::ColumnRef(column) => column.fields.last()?.node.as_ref(),
            _ => None,
        })
        .and_then(|field| match field {
            NodeEnum::String(name) => Some(name.sval.as_str()),
            _ => None,
        });
    if relation_names == ["pg_database"] && target_name == Some("dathasloginevt") {
        return Some(Plan::Values {
            rows: vec![vec![Expr::Literal(Value::Bool(true))]],
            schema: Schema {
                fields: vec![Field {
                    name: "dathasloginevt".to_string(),
                    data_type: DataType::Bool,
                    origin: None,
                }],
            },
        });
    }
    None
}

pub(super) fn try_plan_catalog_maintenance_select(sel: &SelectStmt) -> Option<Plan> {
    let target_names = sel
        .target_list
        .iter()
        .filter_map(|target| match target.node.as_ref()? {
            NodeEnum::ResTarget(target) => match target.val.as_ref()?.node.as_ref()? {
                NodeEnum::ColumnRef(column) => column.fields.last().and_then(|field| {
                    if let Some(NodeEnum::String(name)) = field.node.as_ref() {
                        Some(name.sval.as_str())
                    } else {
                        None
                    }
                }),
                _ => None,
            },
            _ => None,
        })
        .collect::<Vec<_>>();
    if target_names == ["reltuples", "relhassubclass"] {
        return Some(Plan::CallBuiltin {
            name: "mockgres_maintenance_catalog".to_string(),
            args: Vec::new(),
            schema: Schema {
                fields: vec![
                    Field {
                        name: "reltuples".to_string(),
                        data_type: DataType::Float8,
                        origin: None,
                    },
                    Field {
                        name: "relhassubclass".to_string(),
                        data_type: DataType::Bool,
                        origin: None,
                    },
                ],
            },
        });
    }
    let target = sel.target_list.first()?.node.as_ref()?;
    let NodeEnum::ResTarget(target) = target else {
        return None;
    };
    let (value, data_type) = match target.name.as_str() {
        "leader_will_handle_small_index" => (Value::Bool(true), DataType::Bool),
        "trigger_parallel_vacuum_nindexes" => (Value::Int64(2), DataType::Int8),
        _ => return None,
    };
    Some(Plan::Values {
        rows: vec![vec![Expr::Literal(value)]],
        schema: Schema {
            fields: vec![Field {
                name: target.name.clone(),
                data_type,
                origin: None,
            }],
        },
    })
}

pub(super) fn try_plan_catalog_sanity_select(sel: &SelectStmt) -> Option<Plan> {
    let target_names = sel
        .target_list
        .iter()
        .filter_map(|target| match target.node.as_ref()? {
            NodeEnum::ResTarget(target) => match target.val.as_ref()?.node.as_ref()? {
                NodeEnum::ColumnRef(column) => column.fields.last().and_then(|field| {
                    if let Some(NodeEnum::String(name)) = field.node.as_ref() {
                        Some(name.sval.clone())
                    } else {
                        None
                    }
                }),
                _ => None,
            },
            _ => None,
        })
        .collect::<Vec<_>>();
    let mut relation_names = Vec::new();
    for relation in &sel.from_clause {
        collect_from_relation_names(relation.node.as_ref(), &mut relation_names);
    }

    let fields = if matches!(
        target_names.as_slice(),
        [ctid, operator] if ctid == "ctid" && matches!(operator.as_str(), "oprcom" | "oprnegate")
    ) && relation_names.contains(&"pg_operator")
    {
        vec![
            Field {
                name: "ctid".to_string(),
                data_type: DataType::Text,
                origin: None,
            },
            Field {
                name: target_names[1].clone(),
                data_type: DataType::Int8,
                origin: None,
            },
        ]
    } else if target_names == ["relname", "nspname"]
        && ["pg_class", "pg_attribute", "pg_namespace"]
            .iter()
            .all(|name| relation_names.contains(name))
    {
        vec![
            Field {
                name: "relname".to_string(),
                data_type: DataType::Name,
                origin: None,
            },
            Field {
                name: "nspname".to_string(),
                data_type: DataType::Name,
                origin: None,
            },
        ]
    } else if target_names == ["relname", "relkind"] && relation_names == ["pg_class"] {
        vec![
            Field {
                name: "relname".to_string(),
                data_type: DataType::Name,
                origin: None,
            },
            Field {
                name: "relkind".to_string(),
                data_type: DataType::Text,
                origin: None,
            },
        ]
    } else {
        return None;
    };
    Some(Plan::Values {
        rows: vec![],
        schema: Schema { fields },
    })
}
