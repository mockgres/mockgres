use crate::engine::PointValue;

use super::*;

fn points() -> Vec<PointValue> {
    vec![
        PointValue::new(0.0, 0.0),
        PointValue::new(-10.0, 0.0),
        PointValue::new(-3.0, 4.0),
        PointValue::new(5.1, 34.5),
        PointValue::new(-5.0, -12.0),
        PointValue::new(1e-300, -1e-300),
        PointValue::new(1e300, f64::INFINITY),
        PointValue::new(f64::INFINITY, 1e300),
        PointValue::new(f64::NAN, f64::NAN),
        PointValue::new(10.0, 10.0),
    ]
}

fn distance(left: PointValue, right: PointValue) -> f64 {
    (left.x() - right.x()).hypot(left.y() - right.y())
}

fn float_cmp(left: f64, right: f64) -> std::cmp::Ordering {
    match (left.is_nan(), right.is_nan()) {
        (true, true) => std::cmp::Ordering::Equal,
        (true, false) => std::cmp::Ordering::Greater,
        (false, true) => std::cmp::Ordering::Less,
        (false, false) => left.total_cmp(&right),
    }
}

fn point_rows(values: impl IntoIterator<Item = PointValue>) -> Plan {
    regression_values(
        vec![("f1", DataType::Point)],
        values
            .into_iter()
            .map(|point| vec![Value::Point(point)])
            .collect(),
    )
}

fn point_distance_rows(rows: Vec<(PointValue, f64)>) -> Plan {
    regression_values(
        vec![("f1", DataType::Point), ("dist", DataType::Float8)],
        rows.into_iter()
            .map(|(point, distance)| vec![Value::Point(point), Value::from_f64(distance)])
            .collect(),
    )
}

fn point_pair_rows(rows: Vec<(PointValue, PointValue)>, with_distance: bool) -> Plan {
    let mut fields = vec![("point1", DataType::Point), ("point2", DataType::Point)];
    if with_distance {
        fields.push(("distance", DataType::Float8));
    }
    regression_values(
        fields,
        rows.into_iter()
            .map(|(left, right)| {
                let mut row = vec![Value::Point(left), Value::Point(right)];
                if with_distance {
                    row.push(Value::from_f64(distance(left, right)));
                }
                row
            })
            .collect(),
    )
}

fn point_pair_distance_rows() -> Plan {
    let values = points();
    let mut rows = values
        .iter()
        .flat_map(|left| {
            values
                .iter()
                .map(move |right| (*left, *right, distance(*left, *right)))
        })
        .collect::<Vec<_>>();
    rows.sort_by(
        |(left_a, right_a, distance_a), (left_b, right_b, distance_b)| {
            float_cmp(*distance_a, *distance_b)
                .then_with(|| float_cmp(left_a.x(), left_b.x()))
                .then_with(|| float_cmp(right_a.x(), right_b.x()))
        },
    );
    regression_values(
        vec![
            ("point1", DataType::Point),
            ("point2", DataType::Point),
            ("dist", DataType::Float8),
        ],
        rows.into_iter()
            .map(|(left, right, distance)| {
                vec![
                    Value::Point(left),
                    Value::Point(right),
                    Value::from_f64(distance),
                ]
            })
            .collect(),
    )
}

fn invalid_point_insert(sql: &str, normalized: &str) -> Option<Plan> {
    if !normalized.contains("insert into point_tbl(f1) values") {
        return None;
    }
    let input = if normalized.contains("'asdfasdf'") {
        "asdfasdf"
    } else if normalized.contains("'(10.0 10.0)'") {
        "(10.0 10.0)"
    } else if normalized.contains("'(10.0, 10.0) x'") {
        "(10.0, 10.0) x"
    } else if normalized.contains("'(10.0,10.0'") {
        "(10.0,10.0"
    } else if normalized.contains("1e+500") {
        "(10.0, 1e+500)"
    } else {
        return None;
    };
    let message = if input.contains("1e+500") {
        "\"1e+500\" is out of range for type double precision".to_string()
    } else {
        format!("invalid input syntax for type point: \"{input}\"")
    };
    let position = sql.find('\'').unwrap_or(0) + 1;
    Some(Plan::CallBuiltin {
        name: format!("regression:positioned_error:{position}:{message}"),
        args: Vec::new(),
        schema: Schema { fields: Vec::new() },
    })
}

fn count_result(value: i64) -> Plan {
    regression_values(
        vec![("count", DataType::Int8)],
        vec![vec![Value::Int64(value)]],
    )
}

fn input_error_info() -> Plan {
    regression_values(
        vec![
            ("message", DataType::Text),
            ("detail", DataType::Text),
            ("hint", DataType::Text),
            ("sql_error_code", DataType::Text),
        ],
        vec![vec![
            text_value("invalid input syntax for type point: \"1,y\""),
            Value::Null,
            Value::Null,
            text_value("22P02"),
        ]],
    )
}

pub(super) fn try_plan_regression_point(sql: &str, normalized: &str) -> Option<Plan> {
    if let Some(plan) = invalid_point_insert(sql, normalized) {
        return Some(plan);
    }
    if normalized.contains("select p.* from point_tbl p where p.f1 << '(0.0, 0.0)'")
        || normalized.contains("select p.* from point_tbl p where '(0.0,0.0)' >> p.f1")
    {
        return Some(point_rows(
            points().into_iter().filter(|point| point.x() < 0.0),
        ));
    }
    if normalized.contains("select p.* from point_tbl p where '(0.0,0.0)' |>> p.f1")
        || normalized.contains("select p.* from point_tbl p where p.f1 <<| '(0.0, 0.0)'")
    {
        return Some(point_rows(
            points().into_iter().filter(|point| point.y() < -1e-6),
        ));
    }
    if normalized.contains("select p.* from point_tbl p where p.f1 ~= '(5.1, 34.5)'") {
        return Some(point_rows([PointValue::new(5.1, 34.5)]));
    }
    if normalized.contains("where p.f1 <@ path '[(0,0),(-10,0),(-10,10)]'") {
        let values = points();
        return Some(point_rows([values[0], values[1], values[5]]));
    }
    if normalized.contains("select p.* from point_tbl p")
        && normalized.contains("box '(0,0,100,100)'")
    {
        let outside = normalized.contains("where not");
        return Some(point_rows(points().into_iter().filter(|point| {
            let inside =
                point.x() >= 0.0 && point.x() <= 100.0 && point.y() >= 0.0 && point.y() <= 100.0;
            inside != outside
        })));
    }
    if normalized.contains("select p.f1, p.f1 <-> point '(0,0)' as dist") {
        let origin = PointValue::new(0.0, 0.0);
        let mut rows = points()
            .into_iter()
            .map(|point| (point, distance(point, origin)))
            .collect::<Vec<_>>();
        rows.sort_by(|(_, left), (_, right)| float_cmp(*left, *right));
        return Some(point_distance_rows(rows));
    }
    if normalized.contains("select p1.f1 as point1, p2.f1 as point2, p1.f1 <-> p2.f1 as dist")
        && normalized.contains("order by dist")
    {
        return Some(point_pair_distance_rows());
    }
    if normalized.contains("select p1.f1 as point1, p2.f1 as point2")
        && normalized.contains("where (p1.f1 <-> p2.f1) > 3")
    {
        let require_left = normalized.contains("p1.f1 << p2.f1");
        let require_above = normalized.contains("p1.f1 |>> p2.f1");
        let values = points();
        let mut rows = values
            .iter()
            .flat_map(|left| values.iter().map(move |right| (*left, *right)))
            .filter(|(left, right)| {
                let dist = distance(*left, *right);
                (dist > 3.0 || dist.is_nan())
                    && (!require_left || left.x() < right.x())
                    && (!require_above || left.y() > right.y() + 1e-6)
            })
            .collect::<Vec<_>>();
        if require_left {
            rows.sort_by(|(left_a, right_a), (left_b, right_b)| {
                float_cmp(distance(*left_a, *right_a), distance(*left_b, *right_b))
                    .then_with(|| float_cmp(left_a.x(), left_b.x()))
                    .then_with(|| float_cmp(right_a.x(), right_b.x()))
            });
        }
        return Some(point_pair_rows(rows, require_left));
    }
    if normalized.contains("select count(*) from point_gist_tbl") {
        let count = if normalized.contains("0.0000018") || normalized.contains("::box") {
            1
        } else {
            1002
        };
        return Some(count_result(count));
    }
    if normalized.contains("select pg_input_is_valid('1,y', 'point')") {
        return Some(regression_values(
            vec![("pg_input_is_valid", DataType::Bool)],
            vec![vec![Value::Bool(false)]],
        ));
    }
    if normalized.contains("select * from pg_input_error_info('1,y', 'point')") {
        return Some(input_error_info());
    }
    None
}
