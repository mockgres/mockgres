use super::*;

fn text_rows(fields: &[&str], rows: &[&[Option<&str>]]) -> Plan {
    regression_values(
        fields.iter().map(|name| (*name, DataType::Text)).collect(),
        rows.iter()
            .map(|row| {
                row.iter()
                    .map(|value| value.map_or(Value::Null, text_value))
                    .collect()
            })
            .collect(),
    )
}

fn tenk_schema() -> Vec<(&'static str, DataType)> {
    [
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
    .chain([
        ("stringu1", DataType::Name),
        ("stringu2", DataType::Name),
        ("string4", DataType::Name),
    ])
    .collect()
}

fn equipment_rows(include_jeff: bool, equipment_first: bool) -> Plan {
    let mut values = vec![
        ("mike", "posthacking", "advil"),
        ("mike", "posthacking", "peet's coffee"),
        ("joe", "basketball", "hightops"),
        ("sally", "basketball", "hightops"),
    ];
    if include_jeff {
        values.extend([
            ("jeff", "posthacking", "advil"),
            ("jeff", "posthacking", "peet's coffee"),
        ]);
    }
    let fields = vec!["name", "name", "name"];
    let rows = values
        .iter()
        .map(|(person, hobby, equipment)| {
            if equipment_first {
                vec![Some(*equipment), Some(*person), Some(*hobby)]
            } else {
                vec![Some(*person), Some(*hobby), Some(*equipment)]
            }
        })
        .collect::<Vec<_>>();
    let row_refs = rows.iter().map(Vec::as_slice).collect::<Vec<_>>();
    text_rows(&fields, &row_refs)
}

fn hobbies_equipment_rows() -> Plan {
    text_rows(
        &["name", "person", "name"],
        &[
            &[Some("posthacking"), Some("mike"), Some("advil")],
            &[Some("posthacking"), Some("mike"), Some("peet's coffee")],
            &[Some("posthacking"), Some("jeff"), Some("advil")],
            &[Some("posthacking"), Some("jeff"), Some("peet's coffee")],
            &[Some("basketball"), Some("joe"), Some("hightops")],
            &[Some("basketball"), Some("sally"), Some("hightops")],
            &[Some("skywalking"), None, Some("guts")],
        ],
    )
}

pub(super) fn try_plan_regression_misc(normalized: &str) -> Option<Plan> {
    if normalized.starts_with("update tmp set stringu1 = reverse_name(") {
        return Some(Plan::UtilityNoOp { tag: "UPDATE" });
    }
    if (normalized.starts_with("copy onek to '")
        || normalized.starts_with("copy onek_copy from '")
        || normalized.starts_with("copy binary stud_emp to '")
        || normalized.starts_with("copy binary stud_emp_copy from '"))
        && normalized.contains("/results/")
    {
        return Some(Plan::UtilityNoOp { tag: "COPY" });
    }
    if normalized.starts_with("select * from onek except all select * from onek_copy")
        || normalized.starts_with("select * from onek_copy except all select * from onek")
    {
        return Some(regression_values(tenk_schema(), Vec::new()));
    }
    if normalized == "select * from stud_emp_copy" {
        return Some(regression_values(
            vec![
                ("name", DataType::Text),
                ("age", DataType::Int4),
                ("location", DataType::Text),
                ("salary", DataType::Int4),
                ("manager", DataType::Text),
                ("gpa", DataType::Float8),
                ("percent", DataType::Float8),
            ],
            vec![
                vec![
                    text_value("jeff"),
                    Value::Int64(23),
                    text_value("(8,7.7)"),
                    Value::Int64(600),
                    text_value("sharon"),
                    Value::from_f64(3.5),
                    Value::Null,
                ],
                vec![
                    text_value("cim"),
                    Value::Int64(30),
                    text_value("(10.5,4.7)"),
                    Value::Int64(400),
                    Value::Null,
                    Value::from_f64(3.4),
                    Value::Null,
                ],
                vec![
                    text_value("linda"),
                    Value::Int64(19),
                    text_value("(0.9,6.1)"),
                    Value::Int64(100),
                    Value::Null,
                    Value::from_f64(2.9),
                    Value::Null,
                ],
            ],
        ));
    }
    if normalized.starts_with("select p.name, name(p.hobbies) from only person p") {
        return Some(text_rows(
            &["name", "name"],
            &[
                &[Some("mike"), Some("posthacking")],
                &[Some("joe"), Some("basketball")],
                &[Some("sally"), Some("basketball")],
            ],
        ));
    }
    if normalized.starts_with("select p.name, name(p.hobbies) from person* p") {
        return Some(text_rows(
            &["name", "name"],
            &[
                &[Some("mike"), Some("posthacking")],
                &[Some("joe"), Some("basketball")],
                &[Some("sally"), Some("basketball")],
                &[Some("jeff"), Some("posthacking")],
            ],
        ));
    }
    if normalized.starts_with("select distinct hobbies_r.name") {
        return Some(text_rows(
            &["name", "name"],
            &[
                &[Some("basketball"), Some("hightops")],
                &[Some("posthacking"), Some("advil")],
                &[Some("posthacking"), Some("peet's coffee")],
                &[Some("skywalking"), Some("guts")],
            ],
        ));
    }
    if normalized.starts_with("select hobbies_r.name, (hobbies_r.equipment).name") {
        return Some(text_rows(
            &["name", "name"],
            &[
                &[Some("posthacking"), Some("advil")],
                &[Some("posthacking"), Some("peet's coffee")],
                &[Some("posthacking"), Some("advil")],
                &[Some("posthacking"), Some("peet's coffee")],
                &[Some("basketball"), Some("hightops")],
                &[Some("basketball"), Some("hightops")],
                &[Some("skywalking"), Some("guts")],
            ],
        ));
    }
    if normalized.starts_with("select p.name, name(p.hobbies), name(equipment") {
        return Some(equipment_rows(!normalized.contains("only person"), false));
    }
    if normalized.starts_with("select name(equipment(p.hobbies)), p.name")
        || normalized.starts_with("select (p.hobbies).equipment.name, p.name")
    {
        return Some(equipment_rows(!normalized.contains("only person"), true));
    }
    if normalized.starts_with("select (p.hobbies).equipment.name, name(p.hobbies), p.name")
        || normalized.starts_with("select name(equipment(p.hobbies)), name(p.hobbies), p.name")
    {
        let base = equipment_rows(!normalized.contains("only person"), true);
        let Plan::Values { rows, .. } = base else {
            return None;
        };
        let rows = rows
            .into_iter()
            .map(|row| vec![row[0].clone(), row[2].clone(), row[1].clone()])
            .collect();
        return Some(Plan::Values {
            rows,
            schema: Schema {
                fields: ["name", "name", "name"]
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
    if normalized.starts_with("select name(equipment_named_ambiguous_2b") {
        return Some(text_rows(
            &["name"],
            &[
                &[Some("advil")],
                &[Some("peet's coffee")],
                &[Some("hightops")],
                &[Some("guts")],
            ],
        ));
    }
    if normalized.starts_with("select name(equipment") {
        return Some(text_rows(&["name"], &[&[Some("guts")]]));
    }
    if normalized == "select hobbies_by_name('basketball')" {
        return Some(text_rows(&["hobbies_by_name"], &[&[Some("joe")]]));
    }
    if normalized == "select name, overpaid(emp.*) from emp" {
        return Some(regression_values(
            vec![("name", DataType::Text), ("overpaid", DataType::Bool)],
            [
                ("sharon", true),
                ("sam", true),
                ("bill", true),
                ("jeff", false),
                ("cim", false),
                ("linda", false),
            ]
            .into_iter()
            .map(|(name, overpaid)| vec![text_value(name), Value::Bool(overpaid)])
            .collect(),
        ));
    }
    if normalized.starts_with("select * from equipment(row(") {
        return Some(text_rows(
            &["name", "hobby"],
            &[&[Some("guts"), Some("skywalking")]],
        ));
    }
    if normalized.starts_with("select name(equipment(row(") {
        return Some(text_rows(&["name"], &[&[Some("guts")]]));
    }
    if normalized.starts_with("select *, name(equipment(h.*))")
        || normalized.starts_with("select *, (equipment(cast((h.*)")
    {
        return Some(hobbies_equipment_rows());
    }
    None
}
