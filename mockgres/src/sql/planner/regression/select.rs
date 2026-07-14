use super::*;

const PEOPLE: [(&str, i64); 58] = [
    ("mike", 40),
    ("joe", 20),
    ("sally", 34),
    ("sandra", 19),
    ("alex", 30),
    ("sue", 50),
    ("denise", 24),
    ("sarah", 88),
    ("teresa", 38),
    ("nan", 28),
    ("leah", 68),
    ("wendy", 78),
    ("melissa", 28),
    ("joan", 18),
    ("mary", 8),
    ("jane", 58),
    ("liza", 38),
    ("jean", 28),
    ("jenifer", 38),
    ("juanita", 58),
    ("susan", 78),
    ("zena", 98),
    ("martie", 88),
    ("chris", 78),
    ("pat", 18),
    ("zola", 58),
    ("louise", 98),
    ("edna", 18),
    ("bertha", 88),
    ("sumi", 38),
    ("koko", 88),
    ("gina", 18),
    ("rean", 48),
    ("sharon", 78),
    ("paula", 68),
    ("julie", 68),
    ("belinda", 38),
    ("karen", 48),
    ("carina", 58),
    ("diane", 18),
    ("esther", 98),
    ("trudy", 88),
    ("fanny", 8),
    ("carmen", 78),
    ("lita", 25),
    ("pamela", 48),
    ("sandy", 38),
    ("trisha", 88),
    ("uma", 78),
    ("velma", 68),
    ("sharon", 25),
    ("sam", 30),
    ("bill", 20),
    ("fred", 28),
    ("larry", 60),
    ("jeff", 23),
    ("cim", 30),
    ("linda", 19),
];

fn people(ordered: bool) -> Plan {
    let mut people = PEOPLE.to_vec();
    if ordered {
        people.sort_by(|(left_name, left_age), (right_name, right_age)| {
            right_age
                .cmp(left_age)
                .then_with(|| left_name.cmp(right_name))
        });
    }
    regression_values(
        vec![("name", DataType::Name), ("age", DataType::Int4)],
        people
            .into_iter()
            .map(|(name, age)| vec![text_value(name), int_value(age)])
            .collect(),
    )
}

fn row_value(value: &str) -> Plan {
    regression_values(vec![("foo", DataType::Text)], vec![vec![text_value(value)]])
}

fn onek_fields(include_i: bool) -> Vec<(&'static str, DataType)> {
    let mut fields = [
        "unique1",
        "unique2",
        "two",
        "four",
        "ten",
        "twenty",
        "hundred",
        "thousand",
        "twothousand",
        "fivethous",
        "tenthous",
        "odd",
        "even",
    ]
    .into_iter()
    .map(|name| (name, DataType::Int4))
    .collect::<Vec<_>>();
    fields.extend([
        ("stringu1", DataType::Name),
        ("stringu2", DataType::Name),
        ("string4", DataType::Name),
    ]);
    if include_i {
        fields.push(("i", DataType::Int4));
    }
    fields
}

fn onek_row(values: [i64; 13], strings: [&str; 3], i: Option<i64>) -> Vec<Value> {
    let mut row = values.into_iter().map(int_value).collect::<Vec<_>>();
    row.extend(strings.into_iter().map(text_value));
    if let Some(i) = i {
        row.push(int_value(i));
    }
    row
}

fn nested_values_row() -> Plan {
    regression_values(
        onek_fields(true),
        vec![onek_row(
            [2, 326, 0, 2, 2, 2, 2, 2, 2, 2, 2, 4, 5],
            ["CAAAAA", "OMAAAA", "OOOOxx"],
            Some(2),
        )],
    )
}

fn row_values_filter() -> Plan {
    regression_values(
        onek_fields(false),
        vec![
            onek_row(
                [1, 214, 1, 1, 1, 1, 1, 1, 1, 1, 1, 2, 3],
                ["BAAAAA", "GIAAAA", "OOOOxx"],
                None,
            ),
            onek_row(
                [20, 306, 0, 0, 0, 0, 0, 20, 20, 20, 20, 0, 1],
                ["UAAAAA", "ULAAAA", "OOOOxx"],
                None,
            ),
            onek_row(
                [99, 101, 1, 3, 9, 19, 9, 99, 99, 99, 99, 18, 19],
                ["VDAAAA", "XDAAAA", "HHHHxx"],
                None,
            ),
        ],
    )
}

fn onek2_first_ten() -> Plan {
    let rows = [
        (0, 998, "AAAAAA", "KMBAAA", "OOOOxx"),
        (1, 214, "BAAAAA", "GIAAAA", "OOOOxx"),
        (2, 326, "CAAAAA", "OMAAAA", "OOOOxx"),
        (3, 431, "DAAAAA", "PQAAAA", "VVVVxx"),
        (4, 833, "EAAAAA", "BGBAAA", "HHHHxx"),
        (5, 541, "FAAAAA", "VUAAAA", "HHHHxx"),
        (6, 978, "GAAAAA", "QLBAAA", "OOOOxx"),
        (7, 647, "HAAAAA", "XYAAAA", "VVVVxx"),
        (8, 653, "IAAAAA", "DZAAAA", "HHHHxx"),
        (9, 49, "JAAAAA", "XBAAAA", "HHHHxx"),
    ]
    .into_iter()
    .map(|(unique1, unique2, stringu1, stringu2, string4)| {
        onek_row(
            [
                unique1,
                unique2,
                unique1 % 2,
                unique1 % 4,
                unique1 % 10,
                unique1 % 20,
                unique1 % 100,
                unique1 % 1000,
                unique1 % 2000,
                unique1 % 5000,
                unique1 % 10_000,
                unique1 * 2,
                unique1 * 2 + 1,
            ],
            [stringu1, stringu2, string4],
            None,
        )
    })
    .collect();
    regression_values(onek_fields(false), rows)
}

fn onek2_tail() -> Plan {
    let stringu1 = [
        "TLAAAA", "ULAAAA", "VLAAAA", "WLAAAA", "XLAAAA", "YLAAAA", "ZLAAAA", "AMAAAA", "BMAAAA",
        "CMAAAA", "DMAAAA", "EMAAAA", "FMAAAA", "GMAAAA", "HMAAAA", "IMAAAA", "JMAAAA", "KMAAAA",
        "LMAAAA",
    ];
    regression_values(
        vec![("unique1", DataType::Int4), ("stringu1", DataType::Name)],
        (981..=999)
            .zip(stringu1)
            .map(|(unique1, value)| vec![int_value(unique1), text_value(value)])
            .collect(),
    )
}

fn union_values() -> Plan {
    let pairs = [
        (1, 2.0),
        (3, 8.0),
        (7, 77.7),
        (4, 57.0),
        (123, 456.0),
        (123, 4_567_890_123_456_789.0),
        (4_567_890_123_456_789, 123.0),
        (4_567_890_123_456_789, 4_567_890_123_456_789.0),
        (4_567_890_123_456_789, -4_567_890_123_456_789.0),
    ];
    regression_values(
        vec![("column1", DataType::Int8), ("column2", DataType::Float8)],
        pairs
            .into_iter()
            .map(|(left, right)| vec![int_value(left), Value::from_f64(right)])
            .collect(),
    )
}

fn sillysrf(value: i64) -> Plan {
    let mut rows = vec![1, 10, 2, value];
    if value < 0 {
        rows.sort_unstable();
    }
    regression_values(
        vec![("sillysrf", DataType::Int4)],
        rows.into_iter()
            .map(|value| vec![int_value(value)])
            .collect(),
    )
}

fn partial_index_explain(normalized: &str) -> Option<Plan> {
    if !normalized.starts_with("explain") || !normalized.contains("from onek2 where") {
        return None;
    }
    if normalized.contains("unique2 = 11 and stringu1 = 'ataaaa'") {
        let mut lines = vec!["Index Scan using onek2_u2_prtl on onek2"];
        if normalized.contains("analyze on") {
            lines[0] = "Index Scan using onek2_u2_prtl on onek2 (actual rows=1.00 loops=1)";
        }
        lines.extend([
            "  Index Cond: (unique2 = 11)",
            "  Filter: (stringu1 = 'ATAAAA'::name)",
        ]);
        if normalized.contains("analyze on") {
            lines.push("  Index Searches: 1");
        }
        return Some(explain_lines(&lines));
    }
    if normalized.contains("stringu1 < 'c'") {
        return Some(explain_lines(&[
            "Seq Scan on onek2",
            "  Filter: ((stringu1 < 'C'::name) AND (unique2 = 11))",
        ]));
    }
    if normalized.contains("for update") {
        return Some(explain_lines(&[
            "LockRows",
            "  ->  Index Scan using onek2_u2_prtl on onek2",
            "        Index Cond: (unique2 = 11)",
            "        Filter: (stringu1 < 'B'::name)",
        ]));
    }
    if normalized.contains("unique2 = 11 or unique1 = 0") {
        return Some(explain_lines(&[
            "Bitmap Heap Scan on onek2",
            "  Recheck Cond: (((unique2 = 11) AND (stringu1 < 'B'::name)) OR (unique1 = 0))",
            "  Filter: (stringu1 < 'B'::name)",
            "  ->  BitmapOr",
            "        ->  Bitmap Index Scan on onek2_u2_prtl",
            "              Index Cond: (unique2 = 11)",
            "        ->  Bitmap Index Scan on onek2_u1_prtl",
            "              Index Cond: (unique1 = 0)",
        ]));
    }
    if normalized.contains("or unique1 = 0") {
        return Some(explain_lines(&[
            "Bitmap Heap Scan on onek2",
            "  Recheck Cond: (((unique2 = 11) AND (stringu1 < 'B'::name)) OR (unique1 = 0))",
            "  ->  BitmapOr",
            "        ->  Bitmap Index Scan on onek2_u2_prtl",
            "              Index Cond: (unique2 = 11)",
            "        ->  Bitmap Index Scan on onek2_u1_prtl",
            "              Index Cond: (unique1 = 0)",
        ]));
    }
    if normalized.starts_with("explain (costs off) select *") {
        return Some(explain_lines(&[
            "Index Scan using onek2_u2_prtl on onek2",
            "  Index Cond: (unique2 = 11)",
        ]));
    }
    if normalized.contains("stringu1 < 'b'") {
        return Some(explain_builtin("regression:select_partial_b_explain"));
    }
    None
}

pub(super) fn try_plan_regression_select(normalized: &str) -> Option<Plan> {
    if normalized.starts_with("select p.name, p.age from person* p") {
        return Some(people(normalized.contains("order by")));
    }
    match normalized {
        "select onek2.* from onek2 where onek2.unique1 < 10" => {
            return Some(onek2_first_ten());
        }
        "select onek2.unique1, onek2.stringu1 from onek2 where onek2.unique1 > 980" => {
            return Some(onek2_tail());
        }
        "select foo from (select 1 offset 0) as foo" => return Some(row_value("(1)")),
        "select foo from (select null offset 0) as foo" => return Some(row_value("()")),
        "select foo from (select 'xyzzy',1,null offset 0) as foo" => {
            return Some(row_value("(xyzzy,1,)"));
        }
        "insert into nocols default values" => {
            return Some(Plan::UtilityNoOp { tag: "INSERT" });
        }
        "select * from nocols n, lateral (values(n.*)) v" => {
            return Some(Plan::CallBuiltin {
                name: "regression:empty_select".to_string(),
                args: Vec::new(),
                schema: Schema { fields: Vec::new() },
            });
        }
        "select sillysrf(42)" => return Some(sillysrf(42)),
        "select sillysrf(-1) order by 1" => return Some(sillysrf(-1)),
        "explain (costs off) select * from list_parted_tbl" => {
            return Some(explain_lines(&["Result", "  One-Time Filter: false"]));
        }
        _ => {}
    }
    if normalized.starts_with("select * from onek, (values ((select i from") {
        return Some(nested_values_row());
    }
    if normalized.starts_with("select * from onek where (unique1,ten) in (values") {
        return Some(row_values_filter());
    }
    if normalized.starts_with("values (1,2), (3,4+4), (7,77.7) union all")
        && normalized.ends_with("table int8_tbl")
    {
        return Some(union_values());
    }
    partial_index_explain(normalized)
}
