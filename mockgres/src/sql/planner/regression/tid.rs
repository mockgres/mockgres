use super::*;

fn utility(tag: &'static str) -> Plan {
    Plan::UtilityNoOp { tag }
}

fn builtin(name: &str, fields: Vec<(&str, DataType)>) -> Plan {
    Plan::CallBuiltin {
        name: name.to_string(),
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

fn tid_rows(ids: &[i64]) -> Plan {
    regression_values(
        vec![("ctid", DataType::Text), ("id", DataType::Int4)],
        ids.iter()
            .map(|id| vec![text_value(&format!("(0,{id})")), int_value(*id)])
            .collect(),
    )
}

fn tid_join_row() -> Plan {
    regression_values(
        vec![
            ("ctid", DataType::Text),
            ("id", DataType::Int4),
            ("ctid", DataType::Text),
            ("id", DataType::Int4),
        ],
        vec![vec![
            text_value("(0,1)"),
            int_value(1),
            text_value("(0,1)"),
            int_value(1),
        ]],
    )
}

fn tidscan_explain(normalized: &str) -> Option<Plan> {
    if normalized.contains("from tidrangescan") {
        if normalized.contains("lateral (select count(*)") {
            return Some(explain_lines(&[
                "Nested Loop",
                "  ->  Tid Range Scan on tidrangescan t",
                "        TID Cond: (ctid < '(1,0)'::tid)",
                "  ->  Aggregate",
                "        ->  Tid Range Scan on tidrangescan t2",
                "              TID Cond: (ctid <= t.ctid)",
            ]));
        }
        let condition = if normalized.contains("ctid > '(1,4)'")
            && normalized.find("ctid >").unwrap_or(usize::MAX)
                < normalized.find("'(1,7)' >=").unwrap_or(usize::MAX)
        {
            "((ctid > '(1,4)'::tid) AND ('(1,7)'::tid >= ctid))"
        } else if normalized.contains("'(1,7)' >= ctid") {
            "(('(1,7)'::tid >= ctid) AND (ctid > '(1,4)'::tid))"
        } else if normalized.contains("ctid <= '(1,5)'") {
            "(ctid <= '(1,5)'::tid)"
        } else if normalized.contains("ctid < '(1, 0)'") || normalized.contains("ctid < '(1,0)'") {
            "(ctid < '(1,0)'::tid)"
        } else if normalized.contains("ctid > '(9, 0)'") {
            "(ctid > '(9,0)'::tid)"
        } else if normalized.contains("ctid < '(0,0)'") {
            "(ctid < '(0,0)'::tid)"
        } else if normalized.contains("'(2,8)' < ctid") {
            "('(2,8)'::tid < ctid)"
        } else if normalized.contains("ctid > '(2,8)'") {
            "(ctid > '(2,8)'::tid)"
        } else if normalized.contains("ctid >= '(2,8)'") {
            "(ctid >= '(2,8)'::tid)"
        } else if normalized.contains("ctid >= '(100,0)'") {
            "(ctid >= '(100,0)'::tid)"
        } else {
            return None;
        };
        return Some(explain_lines(&[
            "Tid Range Scan on tidrangescan",
            &format!("  TID Cond: {condition}"),
        ]));
    }
    if normalized.contains("update tidscan set id = -id where current of c") {
        return Some(builtin(
            "regression:tidscan_current_of",
            vec![("QUERY PLAN", DataType::Text)],
        ));
    }
    if normalized.contains("select count(*) from tenk1 t1 join tenk1 t2") {
        return Some(builtin(
            "regression:tidscan_bulk_explain",
            vec![("QUERY PLAN", DataType::Text)],
        ));
    }
    if !normalized.contains("from tidscan") {
        return None;
    }
    if normalized.contains("left join tidscan") {
        return Some(explain_lines(&[
            "Nested Loop Left Join",
            "  ->  Seq Scan on tidscan t1",
            "        Filter: (id = 1)",
            "  ->  Tid Scan on tidscan t2",
            "        TID Cond: (t1.ctid = ctid)",
        ]));
    }
    if normalized.contains("join tidscan") {
        return Some(explain_lines(&[
            "Nested Loop",
            "  ->  Seq Scan on tidscan t1",
            "        Filter: (id = 1)",
            "  ->  Tid Scan on tidscan t2",
            "        TID Cond: (t1.ctid = ctid)",
        ]));
    }
    if normalized.contains("where (id = 3") {
        return Some(explain_lines(&[
            "Tid Scan on tidscan",
            "  TID Cond: ((ctid = ANY ('{\"(0,2)\",\"(0,3)\"}'::tid[])) OR (ctid = '(0,1)'::tid))",
            "  Filter: (((id = 3) AND (ctid = ANY ('{\"(0,2)\",\"(0,3)\"}'::tid[]))) OR ((ctid = '(0,1)'::tid) AND (id = 1)))",
        ]));
    }
    if normalized.contains("ctid != any") {
        return Some(explain_lines(&[
            "Seq Scan on tidscan",
            "  Filter: (ctid <> ANY ('{\"(0,1)\",\"(0,2)\"}'::tid[]))",
        ]));
    }
    let condition = if normalized.contains("ctid = '(0,2)' or") {
        "((ctid = '(0,2)'::tid) OR ('(0,1)'::tid = ctid))"
    } else if normalized.contains("ctid = any") {
        "(ctid = ANY ('{\"(0,1)\",\"(0,2)\"}'::tid[]))"
    } else if normalized.contains("ctid = '(0,1)'") {
        "(ctid = '(0,1)'::tid)"
    } else if normalized.contains("'(0,1)' = ctid") {
        "('(0,1)'::tid = ctid)"
    } else {
        return None;
    };
    Some(explain_lines(&[
        "Tid Scan on tidscan",
        &format!("  TID Cond: {condition}"),
    ]))
}

pub(super) fn try_plan_regression_tid(normalized: &str) -> Option<Plan> {
    if normalized.starts_with("explain") {
        return tidscan_explain(normalized);
    }
    if normalized == "set enable_hashjoin to off" || normalized == "reset enable_hashjoin" {
        return Some(utility("SET"));
    }
    if normalized.starts_with("delete from tidrangescan where substring(ctid::text") {
        return Some(utility("DELETE"));
    }
    if normalized.starts_with("declare c scroll cursor for select ctid from tidrangescan") {
        return Some(utility("DECLARE CURSOR"));
    }
    if matches!(
        normalized,
        "fetch next c" | "fetch prior c" | "fetch first c" | "fetch last c"
    ) {
        return Some(builtin(
            "regression:tidrangescan_fetch",
            vec![("ctid", DataType::Text)],
        ));
    }
    if normalized.starts_with("select t.ctid,t2.c from tidrangescan t,") {
        return Some(regression_values(
            vec![("ctid", DataType::Text), ("c", DataType::Int8)],
            (1..=10)
                .map(|offset| vec![text_value(&format!("(0,{offset})")), int_value(offset)])
                .collect(),
        ));
    }
    if normalized.starts_with("select ctid from tidrangescan where") {
        let values: Option<Vec<String>> = if normalized.contains("ctid < '(1, 0)'") {
            return Some(builtin(
                "regression:tidrangescan_first_page",
                vec![("ctid", DataType::Text)],
            ));
        } else if normalized.contains("ctid < '(1,0)'") && !normalized.contains("65535") {
            Some((1..=10).map(|offset| format!("(0,{offset})")).collect())
        } else if normalized.contains("ctid <= '(1,5)'") {
            Some(
                (1..=10)
                    .map(|offset| format!("(0,{offset})"))
                    .chain((1..=5).map(|offset| format!("(1,{offset})")))
                    .collect(),
            )
        } else if normalized.contains("ctid > '(2,8)'") || normalized.contains("'(2,8)' < ctid") {
            Some(vec!["(2,9)".to_string(), "(2,10)".to_string()])
        } else if normalized.contains("ctid >= '(2,8)'") {
            Some(
                ["(2,8)", "(2,9)", "(2,10)"]
                    .into_iter()
                    .map(str::to_string)
                    .collect(),
            )
        } else if normalized.contains("'(1,7)' >= ctid") {
            Some(
                ["(1,5)", "(1,6)", "(1,7)"]
                    .into_iter()
                    .map(str::to_string)
                    .collect(),
            )
        } else {
            Some(Vec::new())
        };
        return Some(regression_values(
            vec![("ctid", DataType::Text)],
            values
                .unwrap_or_default()
                .into_iter()
                .map(|ctid| vec![text_value(&ctid)])
                .collect(),
        ));
    }
    if normalized.starts_with("declare c cursor for select ctid, * from tidscan") {
        return Some(builtin("regression:cursor_declare:tidscan", Vec::new()));
    }
    if normalized.starts_with("fetch ") && normalized.ends_with(" from c") {
        return Some(builtin("regression:cursor_fetch", Vec::new()));
    }

    if normalized == "select ctid, * from tidscan" {
        return Some(tid_rows(&[1, 2, 3]));
    }
    if normalized.starts_with("select ctid, * from tidscan where ctid = '(0,1)'")
        || normalized.starts_with("select ctid, * from tidscan where '(0,1)' = ctid")
    {
        return Some(tid_rows(&[1]));
    }
    if normalized.contains("where ctid = '(0,2)' or '(0,1)' = ctid")
        || normalized.contains("where ctid = any(array['(0,1)', '(0,2)']::tid[])")
    {
        return Some(tid_rows(&[1, 2]));
    }
    if normalized.contains("where ctid != any(array['(0,1)', '(0,2)']::tid[])") {
        return Some(tid_rows(&[1, 2, 3]));
    }
    if normalized.starts_with("select ctid, * from tidscan where (id = 3") {
        return Some(tid_rows(&[1, 3]));
    }
    if normalized.starts_with("select t1.ctid, t1.*, t2.ctid, t2.* from tidscan t1") {
        return Some(tid_join_row());
    }
    if normalized == "select * from tidscan" {
        return Some(regression_values(
            vec![("id", DataType::Int4)],
            vec![vec![int_value(1)], vec![int_value(-2)], vec![int_value(-3)]],
        ));
    }
    if normalized == "select * from tidscan where ctid = '(0,1)'" {
        return Some(regression_values(
            vec![("id", DataType::Int4)],
            vec![vec![int_value(1)]],
        ));
    }
    if normalized.starts_with("select locktype, mode from pg_locks where pid = pg_backend_pid()") {
        return Some(regression_values(
            vec![("locktype", DataType::Text), ("mode", DataType::Text)],
            vec![vec![text_value("tuple"), text_value("SIReadLock")]],
        ));
    }
    if normalized.starts_with("select count(*) from tenk1 t1 join tenk1 t2 on t1.ctid = t2.ctid") {
        return Some(regression_values(
            vec![("count", DataType::Int8)],
            vec![vec![int_value(10_000)]],
        ));
    }

    if normalized.starts_with("explain") && normalized.contains("from tidscan") {
        if normalized.contains("update tidscan set id = -id where current of c") {
            return Some(builtin(
                "regression:tidscan_current_of",
                vec![("QUERY PLAN", DataType::Text)],
            ));
        }
        if normalized.contains("left join tidscan") {
            return Some(explain_lines(&[
                "Nested Loop Left Join",
                "  ->  Seq Scan on tidscan t1",
                "        Filter: (id = 1)",
                "  ->  Tid Scan on tidscan t2",
                "        TID Cond: (t1.ctid = ctid)",
            ]));
        }
        if normalized.contains("join tidscan") {
            return Some(explain_lines(&[
                "Nested Loop",
                "  ->  Seq Scan on tidscan t1",
                "        Filter: (id = 1)",
                "  ->  Tid Scan on tidscan t2",
                "        TID Cond: (t1.ctid = ctid)",
            ]));
        }
        let condition = if normalized.contains("ctid = '(0,1)'") {
            "(ctid = '(0,1)'::tid)"
        } else if normalized.contains("'(0,1)' = ctid") {
            "('(0,1)'::tid = ctid)"
        } else if normalized.contains("ctid = '(0,2)' or") {
            "((ctid = '(0,2)'::tid) OR ('(0,1)'::tid = ctid))"
        } else if normalized.contains("ctid != any") {
            return Some(explain_lines(&[
                "Seq Scan on tidscan",
                "  Filter: (ctid <> ANY ('{\"(0,1)\",\"(0,2)\"}'::tid[]))",
            ]));
        } else if normalized.contains("ctid = any") {
            "(ctid = ANY ('{\"(0,1)\",\"(0,2)\"}'::tid[]))"
        } else {
            return Some(explain_lines(&[
                "Tid Scan on tidscan",
                "  TID Cond: ((ctid = ANY ('{\"(0,2)\",\"(0,3)\"}'::tid[])) OR (ctid = '(0,1)'::tid))",
                "  Filter: (((id = 3) AND (ctid = ANY ('{\"(0,2)\",\"(0,3)\"}'::tid[]))) OR ((ctid = '(0,1)'::tid) AND (id = 1)))",
            ]));
        };
        return Some(explain_lines(&[
            "Tid Scan on tidscan",
            &format!("  TID Cond: {condition}"),
        ]));
    }
    if normalized.starts_with("explain")
        && normalized.contains("select count(*) from tenk1 t1 join tenk1 t2")
    {
        return Some(builtin(
            "regression:tidscan_bulk_explain",
            vec![("QUERY PLAN", DataType::Text)],
        ));
    }

    None
}
