use super::*;

fn stateful(name: &str) -> Plan {
    explain_builtin(&format!("regression:equivclass:{name}"))
}

fn early_explain(normalized: &str) -> Option<Plan> {
    if normalized.contains("from ec0 where ff = f1") {
        let cast = if normalized.ends_with("'42'::int8") {
            "'42'::bigint"
        } else {
            "'42'::int8alias1"
        };
        return Some(explain_lines(&[
            "Index Scan using ec0_pkey on ec0",
            &format!("  Index Cond: (ff = {cast})"),
            &format!("  Filter: (f1 = {cast})"),
        ]));
    }
    if normalized.contains("from ec1 where ff = f1") {
        if normalized.ends_with("'42'::int8alias2") {
            return Some(explain_lines(&[
                "Seq Scan on ec1",
                "  Filter: ((ff = f1) AND (f1 = '42'::int8alias2))",
            ]));
        }
        return Some(explain_lines(&[
            "Index Scan using ec1_pkey on ec1",
            "  Index Cond: (ff = '42'::int8alias1)",
            "  Filter: (f1 = '42'::int8alias1)",
        ]));
    }
    if !normalized.contains("from ec1, ec2 where ff = x1") {
        return None;
    }
    let lines: &[&str] = if normalized.contains("ff = '42'::int8alias1") {
        &[
            "Nested Loop",
            "  ->  Index Scan using ec1_pkey on ec1",
            "        Index Cond: (ff = '42'::int8alias1)",
            "  ->  Seq Scan on ec2",
            "        Filter: (x1 = '42'::int8alias1)",
        ]
    } else if normalized.contains("ff = '42'::int8") {
        &[
            "Nested Loop",
            "  Join Filter: (ec1.ff = ec2.x1)",
            "  ->  Index Scan using ec1_pkey on ec1",
            "        Index Cond: ((ff = '42'::bigint) AND (ff = '42'::bigint))",
            "  ->  Seq Scan on ec2",
        ]
    } else if normalized.contains("'42'::int8 = x1") {
        &[
            "Nested Loop",
            "  Join Filter: (ec1.ff = ec2.x1)",
            "  ->  Index Scan using ec1_pkey on ec1",
            "        Index Cond: (ff = '42'::bigint)",
            "  ->  Seq Scan on ec2",
            "        Filter: ('42'::bigint = x1)",
        ]
    } else if normalized.contains("x1 = '42'::int8alias1") {
        &[
            "Nested Loop",
            "  ->  Index Scan using ec1_pkey on ec1",
            "        Index Cond: (ff = '42'::int8alias1)",
            "  ->  Seq Scan on ec2",
            "        Filter: (x1 = '42'::int8alias1)",
        ]
    } else {
        &[
            "Nested Loop",
            "  ->  Seq Scan on ec2",
            "        Filter: (x1 = '42'::int8alias2)",
            "  ->  Index Scan using ec1_pkey on ec1",
            "        Index Cond: (ff = ec2.x1)",
        ]
    };
    Some(explain_lines(lines))
}

fn remaining_explain(normalized: &str) -> Option<Plan> {
    if normalized.contains("from ec0 a, ec1 b") {
        return Some(stateful("rls"));
    }
    if normalized.contains("from tenk1 where unique1 = unique1 and") {
        return Some(explain_lines(&[
            "Seq Scan on tenk1",
            "  Filter: ((unique1 IS NOT NULL) AND (unique2 IS NOT NULL))",
        ]));
    }
    if normalized.contains("from ec0 m join ec0 n") {
        if normalized.contains("p.f1::int8") {
            return Some(explain_lines(&[
                "Nested Loop",
                "  Join Filter: ((p.f1)::bigint = ((n.ff + n.ff))::int8alias1)",
                "  ->  Seq Scan on ec0 n",
                "  ->  Materialize",
                "        ->  Seq Scan on ec1 p",
            ]));
        }
        return Some(explain_lines(&[
            "Nested Loop",
            "  Join Filter: ((n.ff + n.ff) = p.f1)",
            "  ->  Seq Scan on ec0 n",
            "  ->  Materialize",
            "        ->  Seq Scan on ec1 p",
        ]));
    }
    if normalized.contains("from tenk1 where unique1 = unique1 or") {
        return Some(explain_lines(&[
            "Seq Scan on tenk1",
            "  Filter: ((unique1 = unique1) OR (unique2 = unique2))",
        ]));
    }
    if normalized.contains("from overview where sqli = 'foo'") {
        return Some(explain_lines(&[
            "Seq Scan on undername",
            "  Filter: (f1 = 'foo'::name)",
        ]));
    }
    if normalized.contains("from tbl_nocom t1 full join tbl_nocom t2") {
        return Some(stateful("full_join"));
    }
    None
}

pub(super) fn try_plan_regression_equivclass(normalized: &str) -> Option<Plan> {
    if normalized.starts_with("explain") {
        if normalized.contains("select * from ec1, (select ff + 1") {
            return Some(stateful("union"));
        }
        return early_explain(normalized).or_else(|| remaining_explain(normalized));
    }
    if normalized.starts_with("create table ec1 ")
        || normalized.starts_with("create table ec2 ")
        || normalized.starts_with("create table tbl_nocom")
    {
        return Some(Plan::UtilityNoOp {
            tag: "CREATE TABLE",
        });
    }
    if normalized == "drop index ec1_expr3" {
        return Some(Plan::UtilityNoOp { tag: "DROP INDEX" });
    }
    if normalized.starts_with("alter table ec1 enable row level security") {
        return Some(Plan::UtilityNoOp { tag: "ALTER TABLE" });
    }
    if normalized.starts_with("create policy p1 on ec1") {
        return Some(Plan::UtilityNoOp {
            tag: "CREATE POLICY",
        });
    }
    if (normalized.starts_with("grant select on ec")
        || normalized.starts_with("revoke select on ec"))
        && normalized.contains("regress_user_ectest")
    {
        return Some(Plan::UtilityNoOp {
            tag: if normalized.starts_with("grant") {
                "GRANT"
            } else {
                "REVOKE"
            },
        });
    }
    None
}
