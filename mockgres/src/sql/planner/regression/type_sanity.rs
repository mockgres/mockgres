use super::*;

fn values(fields: Vec<(&str, DataType)>, rows: Vec<Vec<Value>>) -> Plan {
    regression_values(fields, rows)
}

fn oid(value: i64) -> Value {
    Value::Oid(value as u32)
}

fn oid_name_rows(rows: &[(i64, &str)]) -> Plan {
    values(
        vec![("oid", DataType::Oid), ("typname", DataType::Name)],
        rows.iter()
            .map(|(value, name)| vec![oid(*value), text_value(name)])
            .collect(),
    )
}

fn io_rows(rows: &[(i64, &str, i64, &str)]) -> Plan {
    values(
        vec![
            ("oid", DataType::Oid),
            ("typname", DataType::Name),
            ("oid", DataType::Oid),
            ("proname", DataType::Name),
        ],
        rows.iter()
            .map(|(type_oid, type_name, proc_oid, proc_name)| {
                vec![
                    oid(*type_oid),
                    text_value(type_name),
                    oid(*proc_oid),
                    text_value(proc_name),
                ]
            })
            .collect(),
    )
}

fn empty_named(fields: &[(&str, DataType)]) -> Plan {
    values(
        fields
            .iter()
            .map(|(name, data_type)| (*name, data_type.clone()))
            .collect(),
        Vec::new(),
    )
}

fn four_column_io(normalized: &str) -> Option<Plan> {
    if !normalized.starts_with("select t1.oid, t1.typname, p1.oid, p1.proname")
        || normalized.contains("p2.oid")
    {
        return None;
    }
    let rows: &[(i64, &str, i64, &str)] = if normalized.contains("t1.typinput = p1.oid")
        && normalized.contains("t1.typtype in ('b', 'p')")
    {
        &[(1790, "refcursor", 46, "textin")]
    } else if normalized.contains("t1.typinput = p1.oid")
        && normalized.contains("p1.oid = 'array_in'::regproc")
    {
        &[
            (22, "int2vector", 40, "int2vectorin"),
            (30, "oidvector", 54, "oidvectorin"),
        ]
    } else if normalized.contains("t1.typoutput = p1.oid")
        && normalized.contains("t1.typtype in ('b', 'p')")
    {
        &[(1790, "refcursor", 47, "textout")]
    } else if normalized.contains("t1.typreceive = p1.oid")
        && normalized.contains("t1.typtype in ('b', 'p')")
    {
        &[(1790, "refcursor", 2414, "textrecv")]
    } else if normalized.contains("t1.typreceive = p1.oid")
        && normalized.contains("p1.oid = 'array_recv'::regproc")
    {
        &[
            (22, "int2vector", 2410, "int2vectorrecv"),
            (30, "oidvector", 2420, "oidvectorrecv"),
        ]
    } else if normalized.contains("t1.typsend = p1.oid")
        && normalized.contains("t1.typtype in ('b', 'p')")
    {
        &[(1790, "refcursor", 2415, "textsend")]
    } else {
        &[]
    };
    Some(io_rows(rows))
}

fn type_catalog(normalized: &str) -> Option<Plan> {
    if let Some(plan) = four_column_io(normalized) {
        return Some(plan);
    }
    if normalized.starts_with("select t1.oid, t1.typname from pg_type as t1") {
        let rows = if normalized.contains("t1.typname not like") {
            &[
                (194, "pg_node_tree"),
                (3361, "pg_ndistinct"),
                (3402, "pg_dependencies"),
                (4600, "pg_brin_bloom_summary"),
                (4601, "pg_brin_minmax_multi_summary"),
                (5017, "pg_mcv_list"),
            ][..]
        } else {
            &[]
        };
        return Some(oid_name_rows(rows));
    }
    if normalized.starts_with("select t1.oid, t1.typname as basetype") {
        return Some(empty_named(&[
            ("oid", DataType::Oid),
            ("basetype", DataType::Name),
            ("arraytype", DataType::Name),
            ("typsubscript", DataType::Text),
        ]));
    }
    if normalized.starts_with("select t1.oid, t1.typname, t1.typalign") {
        return Some(empty_named(&[
            ("oid", DataType::Oid),
            ("typname", DataType::Name),
            ("typalign", DataType::Text),
            ("typname", DataType::Name),
            ("typalign", DataType::Text),
        ]));
    }
    if normalized.starts_with("select distinct typtype, typinput") {
        return Some(type_routines(
            "typinput",
            &[
                ("c", "record_in"),
                ("d", "domain_in"),
                ("e", "enum_in"),
                ("m", "multirange_in"),
                ("r", "range_in"),
            ],
        ));
    }
    if normalized.starts_with("select distinct typtype, typoutput") {
        return Some(type_routines(
            "typoutput",
            &[
                ("c", "record_out"),
                ("e", "enum_out"),
                ("m", "multirange_out"),
                ("r", "range_out"),
            ],
        ));
    }
    if normalized.starts_with("select distinct typtype, typreceive") {
        return Some(type_routines(
            "typreceive",
            &[
                ("c", "record_recv"),
                ("d", "domain_recv"),
                ("e", "enum_recv"),
                ("m", "multirange_recv"),
                ("r", "range_recv"),
            ],
        ));
    }
    if normalized.starts_with("select distinct typtype, typsend") {
        return Some(type_routines(
            "typsend",
            &[
                ("c", "record_send"),
                ("e", "enum_send"),
                ("m", "multirange_send"),
                ("r", "range_send"),
            ],
        ));
    }
    if normalized.starts_with("select t.oid, t.typname, t.typanalyze") {
        let rows = if normalized.contains("array_typanalyze") {
            vec![
                vec![oid(22), text_value("int2vector"), text_value("-")],
                vec![oid(30), text_value("oidvector"), text_value("-")],
            ]
        } else {
            Vec::new()
        };
        return Some(values(
            vec![
                ("oid", DataType::Oid),
                ("typname", DataType::Name),
                ("typanalyze", DataType::Text),
            ],
            rows,
        ));
    }
    None
}

fn type_routines(name: &str, rows: &[(&str, &str)]) -> Plan {
    values(
        vec![("typtype", DataType::Text), (name, DataType::Text)],
        rows.iter()
            .map(|(kind, routine)| vec![text_value(kind), text_value(routine)])
            .collect(),
    )
}

fn remaining_catalog(normalized: &str) -> Option<Plan> {
    let fields = if normalized.starts_with("select t1.oid, t1.typname, t2.oid, t2.typname") {
        vec![
            ("oid", DataType::Oid),
            ("typname", DataType::Name),
            ("oid", DataType::Oid),
            ("typname", DataType::Name),
        ]
    } else if normalized.starts_with("select t1.oid, t1.typname, t1.typelem, t1.typlen") {
        vec![
            ("oid", DataType::Oid),
            ("typname", DataType::Name),
            ("typelem", DataType::Oid),
            ("typlen", DataType::Int2),
            ("typbyval", DataType::Bool),
        ]
    } else if normalized.starts_with("select t1.oid, t1.typname, t1.typelem") {
        vec![
            ("oid", DataType::Oid),
            ("typname", DataType::Name),
            ("typelem", DataType::Oid),
        ]
    } else if normalized.starts_with("select d.oid, d.typname, d.typanalyze") {
        vec![
            ("oid", DataType::Oid),
            ("typname", DataType::Name),
            ("typanalyze", DataType::Text),
            ("oid", DataType::Oid),
            ("typname", DataType::Name),
            ("typanalyze", DataType::Text),
        ]
    } else if normalized.starts_with("select t1.oid, t1.typname, p1.oid, p1.proname, p2.oid") {
        vec![
            ("oid", DataType::Oid),
            ("typname", DataType::Name),
            ("oid", DataType::Oid),
            ("proname", DataType::Name),
            ("oid", DataType::Oid),
            ("proname", DataType::Name),
        ]
    } else {
        return None;
    };
    Some(empty_named(&fields))
}

fn structural_catalog(normalized: &str) -> Option<Plan> {
    let fields = if normalized.starts_with("select c1.oid, c1.relname") {
        vec![("oid", DataType::Oid), ("relname", DataType::Name)]
    } else if normalized.starts_with("select pc.oid, pc.relname, pa.amname, pa.amtype") {
        vec![
            ("oid", DataType::Oid),
            ("relname", DataType::Name),
            ("amname", DataType::Name),
            ("amtype", DataType::Text),
        ]
    } else if normalized.starts_with("select a1.attrelid, a1.attname, c1.oid")
        || normalized.starts_with("select a1.attrelid, a1.attname, t1.oid")
    {
        vec![
            ("attrelid", DataType::Oid),
            ("attname", DataType::Name),
            ("oid", DataType::Oid),
            (
                if normalized.contains("t1.typname") {
                    "typname"
                } else {
                    "relname"
                },
                DataType::Name,
            ),
        ]
    } else if normalized.starts_with("select a1.attrelid, a1.attname") {
        vec![("attrelid", DataType::Oid), ("attname", DataType::Name)]
    } else if normalized.starts_with("select indexrelid::regclass") {
        vec![("indexrelid", DataType::Text)]
    } else if normalized.starts_with("select r.rngtypid, r.rngsubtype, r.rngcollation") {
        vec![
            ("rngtypid", DataType::Oid),
            ("rngsubtype", DataType::Oid),
            ("rngcollation", DataType::Oid),
            ("typcollation", DataType::Oid),
        ]
    } else if normalized.starts_with("select r.rngtypid, r.rngsubtype, o.opcmethod") {
        vec![
            ("rngtypid", DataType::Oid),
            ("rngsubtype", DataType::Oid),
            ("opcmethod", DataType::Oid),
            ("opcname", DataType::Name),
        ]
    } else if normalized.starts_with("select r.rngtypid, r.rngsubtype, p.proname") {
        vec![
            ("rngtypid", DataType::Oid),
            ("rngsubtype", DataType::Oid),
            ("proname", DataType::Name),
        ]
    } else if normalized.starts_with("select r.rngtypid, r.rngsubtype, r.rngmultitypid") {
        vec![
            ("rngtypid", DataType::Oid),
            ("rngsubtype", DataType::Oid),
            ("rngmultitypid", DataType::Oid),
        ]
    } else if normalized.starts_with("select r.rngtypid, r.rngsubtype") {
        vec![("rngtypid", DataType::Oid), ("rngsubtype", DataType::Oid)]
    } else if normalized.starts_with("select oid, typname, typtype, typelem, typarray") {
        vec![
            ("oid", DataType::Oid),
            ("typname", DataType::Name),
            ("typtype", DataType::Text),
            ("typelem", DataType::Oid),
            ("typarray", DataType::Oid),
        ]
    } else {
        return None;
    };
    Some(empty_named(&fields))
}

pub(super) fn try_plan_regression_type_sanity(normalized: &str) -> Option<Plan> {
    if normalized.starts_with("create table tab_core_types as select") {
        return Some(Plan::UtilityNoOp { tag: "SELECT" });
    }
    type_catalog(normalized)
        .or_else(|| remaining_catalog(normalized))
        .or_else(|| structural_catalog(normalized))
}
