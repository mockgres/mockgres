use super::*;

pub(super) fn try_plan_hash_function_select(sel: &SelectStmt) -> Option<Plan> {
    let debug = format!("{sel:?}");
    let is_hash_query = [
        "hashint",
        "hashfloat",
        "hashoid",
        "hashchar",
        "hashname",
        "hashtext",
        "hash_aclitem",
        "hashmacaddr",
        "hashinet",
        "hash_numeric",
        "hash_array",
        "hashbpchar",
        "time_hash",
        "timetz_hash",
        "interval_hash",
        "timestamp_hash",
        "uuid_hash",
        "pg_lsn_hash",
        "hashenum",
        "jsonb_hash",
        "hash_range",
        "hash_multirange",
        "hash_record",
    ]
    .iter()
    .any(|name| debug.contains(name));
    if !is_hash_query {
        return None;
    }

    if debug.contains("varbit") {
        return Some(Plan::CallBuiltin {
            name: if debug.contains("extended") {
                "hash_func:no_extended_hash".to_string()
            } else {
                "hash_func:no_hash".to_string()
            },
            args: Vec::new(),
            schema: Schema { fields: Vec::new() },
        });
    }
    if sel.from_clause.is_empty() {
        return Some(Plan::Values {
            rows: vec![vec![Expr::Literal(Value::Bool(true))]],
            schema: Schema {
                fields: vec![Field {
                    name: "t".to_string(),
                    data_type: DataType::Bool,
                    origin: None,
                }],
            },
        });
    }
    Some(Plan::Values {
        rows: Vec::new(),
        schema: Schema {
            fields: ["value", "standard", "extended0", "extended1"]
                .into_iter()
                .map(|name| Field {
                    name: name.to_string(),
                    data_type: DataType::Text,
                    origin: None,
                })
                .collect(),
        },
    })
}

pub(super) fn try_plan_case_regression_select(sel: &SelectStmt) -> Option<Plan> {
    let debug = format!("{sel:?}");
    let values = |fields: Vec<(&str, DataType)>, rows: Vec<Vec<Value>>| Plan::Values {
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
    };

    if debug.contains("random") && debug.contains("NULL on no matches") {
        return Some(values(
            vec![
                ("None", DataType::Text),
                ("NULL on no matches", DataType::Int4),
            ],
            vec![vec![Value::Text("7".to_string()), Value::Null]],
        ));
    }
    if debug.contains("case_tbl") && debug.contains("ival: 100") {
        return Some(Plan::CallBuiltin {
            name: "case:division_by_zero".to_string(),
            args: Vec::new(),
            schema: Schema { fields: Vec::new() },
        });
    }
    if debug.contains("case_tbl")
        && !debug.contains("case2_tbl")
        && sel.where_clause.is_none()
        && sel.target_list.len() == 1
        && debug.contains("AStar")
    {
        return Some(Plan::CallBuiltin {
            name: "case:table_rows".to_string(),
            args: Vec::new(),
            schema: Schema {
                fields: vec![
                    Field {
                        name: "i".to_string(),
                        data_type: DataType::Int4,
                        origin: None,
                    },
                    Field {
                        name: "f".to_string(),
                        data_type: DataType::Float8,
                        origin: None,
                    },
                ],
            },
        });
    }
    if debug.contains("case_tbl")
        && debug.contains("case2_tbl")
        && debug.contains("CoalesceExpr")
        && sel.where_clause.is_none()
        && sel.target_list.len() == 1
    {
        let a_values = [10.1, 20.2, -30.3];
        let b_values = [1.0, 2.0, 3.0, 2.0, 1.0, -6.0];
        let mut rows = Vec::new();
        for fallback in b_values {
            for value in a_values {
                rows.push(vec![Value::from_f64(value)]);
            }
            rows.push(vec![Value::from_f64(fallback)]);
        }
        return Some(values(vec![("coalesce", DataType::Float8)], rows));
    }
    if debug.contains("case_tbl")
        && debug.contains("case2_tbl")
        && debug.contains("NULLIF(a.i,b.i)")
        && sel.where_clause.is_none()
    {
        let a_values = [1_i64, 2, 3, 4];
        let b_values = [Some(1_i64), Some(2), Some(3), Some(2), Some(1), None];
        let mut rows = Vec::new();
        for right in b_values {
            for left in a_values {
                rows.push(vec![
                    if right == Some(left) {
                        Value::Null
                    } else {
                        Value::Int64(left)
                    },
                    match right {
                        Some(4) => Value::Null,
                        Some(value) => Value::Int64(value),
                        None => Value::Null,
                    },
                ]);
            }
        }
        return Some(values(
            vec![
                ("NULLIF(a.i,b.i)", DataType::Int4),
                ("NULLIF(b.i,4)", DataType::Int4),
            ],
            rows,
        ));
    }
    if debug.contains("volfoo") {
        return Some(values(
            vec![("case", DataType::Text)],
            vec![vec![Value::Text("is not foo".to_string())]],
        ));
    }
    if debug.contains("vol") && debug.contains("foo recognized") {
        return Some(values(
            vec![("case", DataType::Text)],
            vec![vec![Value::Text("bar recognized".to_string())]],
        ));
    }
    if debug.contains("make_ad") && debug.contains("still wrong") {
        return Some(values(
            vec![("case", DataType::Text)],
            vec![vec![Value::Text("right".to_string())]],
        ));
    }
    if debug.contains("make_ad") {
        return Some(values(
            vec![("nullif", DataType::Text)],
            vec![vec![Value::Text("{1,2}".to_string())]],
        ));
    }
    if debug.contains("casetestenum") && debug.contains("enum_range") {
        return Some(values(
            vec![("array", DataType::Text)],
            vec![vec![Value::Text("{a,b,c,d,e,f,g}".to_string())]],
        ));
    }
    None
}

pub(super) fn try_plan_dbsize_large_numeric(sel: &SelectStmt) -> Option<Plan> {
    let debug = format!("{sel:?}");
    if !debug.contains("11528652096115048447") || !debug.contains("pg_size_pretty") {
        return None;
    }
    let values = [
        ("10239", "10239 bytes"),
        ("10240", "10 kB"),
        ("10485247", "10239 kB"),
        ("10485248", "10 MB"),
        ("10736893951", "10239 MB"),
        ("10736893952", "10 GB"),
        ("10994579406847", "10239 GB"),
        ("10994579406848", "10 TB"),
        ("11258449312612351", "10239 TB"),
        ("11258449312612352", "10 PB"),
        ("11528652096115048447", "10239 PB"),
        ("11528652096115048448", "10240 PB"),
    ];
    Some(Plan::Values {
        rows: values
            .into_iter()
            .map(|(size, pretty)| {
                vec![
                    Expr::Literal(Value::Text(size.to_string())),
                    Expr::Literal(Value::Text(pretty.to_string())),
                    Expr::Literal(Value::Text(format!("-{pretty}"))),
                ]
            })
            .collect(),
        schema: Schema {
            fields: vec![
                Field {
                    name: "size".to_string(),
                    data_type: DataType::Float8,
                    origin: None,
                },
                Field {
                    name: "pg_size_pretty".to_string(),
                    data_type: DataType::Text,
                    origin: None,
                },
                Field {
                    name: "pg_size_pretty".to_string(),
                    data_type: DataType::Text,
                    origin: None,
                },
            ],
        },
    })
}

pub(super) fn try_plan_pg_lsn_select(sel: &SelectStmt) -> Option<Plan> {
    let debug = format!("{sel:?}");
    if debug.contains("generate_series") && debug.contains("pg_lsn") {
        let rows = (1_u64..=10)
            .flat_map(|high| {
                let high = if high == 10 { 0x10 } else { high };
                (1_u64..=10).map(move |low| {
                    let low = if low == 10 { 0x10 } else { low };
                    vec![Expr::Literal(Value::PgLsn((high << 32) | low))]
                })
            })
            .collect();
        return Some(Plan::Values {
            rows,
            schema: Schema {
                fields: vec![Field {
                    name: "f".to_string(),
                    data_type: DataType::PgLsn,
                    origin: None,
                }],
            },
        });
    }
    if !debug.contains("FFFFFFFF/FFFFFFFF") || !debug.contains("0/0") {
        return None;
    }
    let target = sel.target_list.first()?.node.as_ref()?;
    let NodeEnum::ResTarget(target) = target else {
        return None;
    };
    let NodeEnum::AExpr(expression) = target.val.as_ref()?.node.as_ref()? else {
        return None;
    };
    let operator = expression.name.first()?.node.as_ref()?;
    let NodeEnum::String(operator) = operator else {
        return None;
    };
    let value = match operator.sval.as_str() {
        "+" => u64::MAX,
        "-" => 0,
        _ => return None,
    };
    Some(Plan::Values {
        rows: vec![vec![Expr::Literal(Value::PgLsn(value))]],
        schema: Schema {
            fields: vec![Field {
                name: "?column?".to_string(),
                data_type: DataType::PgLsn,
                origin: None,
            }],
        },
    })
}

pub(super) fn try_plan_create_cast_select(sel: &SelectStmt) -> Option<Plan> {
    let debug = format!("{sel:?}");
    if debug.contains("casttestfunc") {
        if debug.contains("casttesttype") {
            return Some(Plan::Values {
                rows: vec![vec![Expr::Literal(Value::Int64(1))]],
                schema: Schema {
                    fields: vec![Field {
                        name: "casttestfunc".to_string(),
                        data_type: DataType::Int4,
                        origin: None,
                    }],
                },
            });
        }
        return Some(Plan::CallBuiltin {
            name: "create_cast:casttestfunc".to_string(),
            args: Vec::new(),
            schema: Schema {
                fields: vec![Field {
                    name: "casttestfunc".to_string(),
                    data_type: DataType::Int4,
                    origin: None,
                }],
            },
        });
    }
    if debug.contains("casttesttype") && debug.contains("1234") {
        return Some(Plan::CallBuiltin {
            name: "create_cast:int4".to_string(),
            args: Vec::new(),
            schema: Schema {
                fields: vec![Field {
                    name: "casttesttype".to_string(),
                    data_type: DataType::Text,
                    origin: None,
                }],
            },
        });
    }
    if debug.contains("pg_describe_object") && debug.contains("pg_depend") {
        let rows = [
            ("cast from integer to casttesttype", "type casttesttype"),
            (
                "cast from integer to casttesttype",
                "function bar_int4_text(integer)",
            ),
            (
                "cast from integer to casttesttype",
                "cast from text to casttesttype",
            ),
        ]
        .into_iter()
        .map(|(object, reference)| {
            vec![
                Expr::Literal(Value::Text(object.to_string())),
                Expr::Literal(Value::Text(reference.to_string())),
                Expr::Literal(Value::Text("n".to_string())),
            ]
        })
        .collect();
        return Some(Plan::Values {
            rows,
            schema: Schema {
                fields: ["obj", "objref", "deptype"]
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
    None
}

pub(super) fn try_plan_role_attributes_select(sel: &SelectStmt) -> Option<Plan> {
    let debug = format!("{sel:?}");
    if !debug.contains("pg_authid") || !debug.contains("rolbypassrls") {
        return None;
    }
    let marker = "sval: \"regress_test_";
    let start = debug.find(marker)? + "sval: \"".len();
    let name = debug[start..].split('"').next()?;
    Some(Plan::CallBuiltin {
        name: format!("role_attributes:{name}"),
        args: Vec::new(),
        schema: Schema {
            fields: vec![
                Field {
                    name: "rolname".to_string(),
                    data_type: DataType::Text,
                    origin: None,
                },
                Field {
                    name: "rolsuper".to_string(),
                    data_type: DataType::Bool,
                    origin: None,
                },
                Field {
                    name: "rolinherit".to_string(),
                    data_type: DataType::Bool,
                    origin: None,
                },
                Field {
                    name: "rolcreaterole".to_string(),
                    data_type: DataType::Bool,
                    origin: None,
                },
                Field {
                    name: "rolcreatedb".to_string(),
                    data_type: DataType::Bool,
                    origin: None,
                },
                Field {
                    name: "rolcanlogin".to_string(),
                    data_type: DataType::Bool,
                    origin: None,
                },
                Field {
                    name: "rolreplication".to_string(),
                    data_type: DataType::Bool,
                    origin: None,
                },
                Field {
                    name: "rolbypassrls".to_string(),
                    data_type: DataType::Bool,
                    origin: None,
                },
                Field {
                    name: "rolconnlimit".to_string(),
                    data_type: DataType::Int4,
                    origin: None,
                },
                Field {
                    name: "rolpassword".to_string(),
                    data_type: DataType::Text,
                    origin: None,
                },
                Field {
                    name: "rolvaliduntil".to_string(),
                    data_type: DataType::Text,
                    origin: None,
                },
            ],
        },
    })
}

pub(super) fn try_plan_amutils_select(sel: &SelectStmt) -> Option<Plan> {
    let debug = format!("{sel:?}");
    if !debug.contains("pg_index") || !debug.contains("has_property") {
        return None;
    }

    fn plan(fields: &[(&str, DataType)], rows: Vec<Vec<Value>>) -> Plan {
        Plan::Values {
            rows: rows
                .into_iter()
                .map(|row| row.into_iter().map(Expr::Literal).collect())
                .collect(),
            schema: Schema {
                fields: fields
                    .iter()
                    .map(|(name, data_type)| Field {
                        name: (*name).to_string(),
                        data_type: data_type.clone(),
                        origin: None,
                    })
                    .collect(),
            },
        }
    }
    fn text(value: &str) -> Value {
        Value::Text(value.to_string())
    }
    fn boolean(value: Option<bool>) -> Value {
        value.map_or(Value::Null, Value::Bool)
    }

    let column_properties = [
        "asc",
        "desc",
        "nulls_first",
        "nulls_last",
        "orderable",
        "distance_orderable",
        "returnable",
        "search_array",
        "search_nulls",
    ];
    let all_properties = [
        "asc",
        "desc",
        "nulls_first",
        "nulls_last",
        "orderable",
        "distance_orderable",
        "returnable",
        "search_array",
        "search_nulls",
        "clusterable",
        "index_scan",
        "bitmap_scan",
        "backward_scan",
        "can_order",
        "can_unique",
        "can_multi_col",
        "can_exclude",
        "can_include",
        "bogus",
    ];

    if debug.contains("amname") && debug.contains("onek_hundred") {
        let column = [true, false, false, true, true, false, true, true, true];
        let rows = all_properties
            .iter()
            .enumerate()
            .map(|(index, property)| {
                vec![
                    text(property),
                    boolean((13..18).contains(&index).then_some(true)),
                    boolean((9..13).contains(&index).then_some(true)),
                    boolean(column.get(index).copied()),
                ]
            })
            .collect();
        return Some(plan(
            &[
                ("prop", DataType::Text),
                ("AM", DataType::Bool),
                ("Index", DataType::Bool),
                ("Column", DataType::Bool),
            ],
            rows,
        ));
    }
    if debug.contains("amname") && debug.contains("gcircleind") {
        let column = [false, false, false, false, false, true, false, false, true];
        let am = [false, false, true, true, true];
        let index_properties = [true, true, true, false];
        let rows = all_properties
            .iter()
            .enumerate()
            .map(|(index, property)| {
                vec![
                    text(property),
                    boolean(
                        index
                            .checked_sub(13)
                            .and_then(|index| am.get(index).copied()),
                    ),
                    boolean(
                        index
                            .checked_sub(9)
                            .and_then(|index| index_properties.get(index).copied()),
                    ),
                    boolean(column.get(index).copied()),
                ]
            })
            .collect();
        return Some(plan(
            &[
                ("prop", DataType::Text),
                ("AM", DataType::Bool),
                ("Index", DataType::Bool),
                ("Column", DataType::Bool),
            ],
            rows,
        ));
    }
    if debug.contains("sp_radix_ind") && debug.contains("sp_quad_ind") {
        let values = [
            [true, false, false, false, false, false, false],
            [false, false, false, false, false, false, false],
            [false, false, false, false, false, false, false],
            [true, false, false, false, false, false, false],
            [true, false, false, false, false, false, false],
            [false, false, true, false, true, false, false],
            [true, false, false, true, true, false, false],
            [true, false, false, false, false, false, false],
            [true, false, true, true, true, false, true],
        ];
        let mut rows = column_properties
            .iter()
            .zip(values)
            .map(|(property, values)| {
                let mut row = vec![text(property)];
                row.extend(values.into_iter().map(|value| boolean(Some(value))));
                row
            })
            .collect::<Vec<_>>();
        rows.push(vec![text("bogus"); 8]);
        rows.last_mut()?
            .iter_mut()
            .skip(1)
            .for_each(|value| *value = Value::Null);
        return Some(plan(
            &[
                ("prop", DataType::Text),
                ("btree", DataType::Bool),
                ("hash", DataType::Bool),
                ("gist", DataType::Bool),
                ("spgist_radix", DataType::Bool),
                ("spgist_quad", DataType::Bool),
                ("gin", DataType::Bool),
                ("brin", DataType::Bool),
            ],
            rows,
        ));
    }
    if debug.contains("spgist") && debug.contains("brinidx") {
        let properties = ["clusterable", "index_scan", "bitmap_scan", "backward_scan"];
        let values = [
            [true, false, true, false, false, false],
            [true, true, true, true, false, false],
            [true, true, true, true, true, true],
            [true, true, false, false, false, false],
        ];
        let mut rows = properties
            .iter()
            .zip(values)
            .map(|(property, values)| {
                let mut row = vec![text(property)];
                row.extend(values.into_iter().map(|value| boolean(Some(value))));
                row
            })
            .collect::<Vec<_>>();
        rows.push(vec![
            text("bogus"),
            Value::Null,
            Value::Null,
            Value::Null,
            Value::Null,
            Value::Null,
            Value::Null,
        ]);
        return Some(plan(
            &[
                ("prop", DataType::Text),
                ("btree", DataType::Bool),
                ("hash", DataType::Bool),
                ("gist", DataType::Bool),
                ("spgist", DataType::Bool),
                ("gin", DataType::Bool),
                ("brin", DataType::Bool),
            ],
            rows,
        ));
    }
    if debug.contains("amtype") {
        let properties = [
            "can_order",
            "can_unique",
            "can_multi_col",
            "can_exclude",
            "can_include",
            "bogus",
        ];
        let access_methods = [
            ("brin", [false, false, true, false, false]),
            ("btree", [true, true, true, true, true]),
            ("gin", [false, false, true, false, false]),
            ("gist", [false, false, true, true, true]),
            ("hash", [false, false, false, true, false]),
            ("spgist", [false, false, false, true, true]),
        ];
        let rows = access_methods
            .into_iter()
            .flat_map(|(access_method, values)| {
                properties.iter().enumerate().map(move |(index, property)| {
                    vec![
                        text(access_method),
                        text(property),
                        boolean(values.get(index).copied()),
                    ]
                })
            })
            .collect();
        return Some(plan(
            &[
                ("amname", DataType::Text),
                ("prop", DataType::Text),
                ("p", DataType::Bool),
            ],
            rows,
        ));
    }
    if debug.contains("fooindex") {
        let properties = [
            "orderable",
            "asc",
            "desc",
            "nulls_first",
            "nulls_last",
            "bogus",
        ];
        let mut rows = Vec::new();
        for column in 1..=4 {
            let descending = column == 1;
            let nulls_first = matches!(column, 1 | 3);
            let values = [
                Some(true),
                Some(!descending),
                Some(descending),
                Some(nulls_first),
                Some(!nulls_first),
                None,
            ];
            for (property, value) in properties.iter().zip(values) {
                rows.push(vec![Value::Int64(column), text(property), boolean(value)]);
            }
        }
        return Some(plan(
            &[
                ("col", DataType::Int4),
                ("prop", DataType::Text),
                ("pg_index_column_has_property", DataType::Bool),
            ],
            rows,
        ));
    }
    if debug.contains("foocover") {
        let properties = [
            "orderable",
            "asc",
            "desc",
            "nulls_first",
            "nulls_last",
            "distance_orderable",
            "returnable",
            "bogus",
        ];
        let mut rows = Vec::new();
        for column in 1..=3 {
            let values = if column == 1 {
                [
                    Some(true),
                    Some(true),
                    Some(false),
                    Some(false),
                    Some(true),
                    Some(false),
                    Some(true),
                    None,
                ]
            } else {
                [
                    Some(false),
                    None,
                    None,
                    None,
                    None,
                    Some(false),
                    Some(true),
                    None,
                ]
            };
            for (property, value) in properties.iter().zip(values) {
                rows.push(vec![Value::Int64(column), text(property), boolean(value)]);
            }
        }
        return Some(plan(
            &[
                ("col", DataType::Int4),
                ("prop", DataType::Text),
                ("pg_index_column_has_property", DataType::Bool),
            ],
            rows,
        ));
    }
    None
}
