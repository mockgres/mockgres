use super::*;

fn bool_value(value: Option<bool>) -> Value {
    value.map_or(Value::Null, Value::Bool)
}

fn myint3_rows(call: u32) -> Vec<Vec<Value>> {
    let states: [[Option<bool>; 2]; 3] = match call {
        0 | 4 => [
            [None, None],
            [Some(false), Some(false)],
            [Some(true), Some(true)],
        ],
        1 => [
            [Some(true), Some(true)],
            [None, None],
            [Some(true), Some(true)],
        ],
        2 | 6 => [
            [None, None],
            [Some(true), Some(true)],
            [Some(false), Some(false)],
        ],
        3 => [
            [Some(false), Some(false)],
            [None, None],
            [Some(false), Some(false)],
        ],
        5 => [[None, None], [None, None], [Some(true), Some(true)]],
        7 => [[None, None], [None, None], [Some(false), Some(false)]],
        8 => [
            [Some(false), Some(false)],
            [Some(true), Some(true)],
            [Some(true), Some(true)],
        ],
        _ => [
            [Some(true), Some(true)],
            [Some(false), Some(false)],
            [Some(false), Some(false)],
        ],
    };
    [None, Some(0), Some(1)]
        .into_iter()
        .zip(states)
        .map(|(integer, bools)| {
            vec![
                integer.map_or(Value::Null, Value::Int64),
                bool_value(bools[0]),
                bool_value(bools[1]),
            ]
        })
        .collect()
}

fn myint5_rows(call: u32) -> Vec<Vec<Value>> {
    let states = if call == 0 {
        [
            [false, false, true, true],
            [false, false, true, true],
            [true, true, true, true],
        ]
    } else {
        [
            [true, true, false, false],
            [true, true, false, false],
            [false, false, false, false],
        ]
    };
    [None, Some(0), Some(1)]
        .into_iter()
        .zip(states)
        .map(|(integer, bools)| {
            let mut row = vec![integer.map_or(Value::Null, Value::Int64)];
            row.extend(bools.into_iter().map(Value::Bool));
            row
        })
        .collect()
}

impl Mockgres {
    pub(super) async fn execute_regression_expressions_builtin(
        &self,
        session: &Arc<Session>,
        name: &str,
        schema: &Schema,
        format: FieldFormat,
    ) -> PgWireResult<Option<Response>> {
        let Some(kind) = name.strip_prefix("regression:expressions:") else {
            return Ok(None);
        };
        let rows = match kind {
            "current_schema" => {
                let value = match session.next_currtid_call(name) {
                    0 => Value::Text("public".to_string()),
                    1 => Value::Null,
                    _ => Value::Text("pg_catalog".to_string()),
                };
                vec![vec![value]]
            }
            "scalar_array" => {
                let values = [
                    Some(true),
                    None,
                    None,
                    Some(true),
                    None,
                    None,
                    Some(true),
                    Some(false),
                    Some(true),
                    None,
                    Some(false),
                    None,
                    None,
                    None,
                    Some(false),
                ];
                let call = session.next_currtid_call(name) as usize;
                vec![vec![bool_value(values.get(call).copied().flatten())]]
            }
            "myint3" => myint3_rows(session.next_currtid_call(name)),
            "myint5" => myint5_rows(session.next_currtid_call(name)),
            _ => return Ok(None),
        };
        let exec = ValuesExec::from_values(schema.clone(), rows);
        let eval_ctx = EvalContext::for_statement(session)
            .with_advisory_locks(session.id(), self.advisory_locks.clone());
        let (fields, rows) = to_pgwire_stream(Box::new(exec), format, eval_ctx).await?;
        let mut response = QueryResponse::new(fields, rows);
        response.set_command_tag("SELECT");
        Ok(Some(Response::Query(response)))
    }
}
