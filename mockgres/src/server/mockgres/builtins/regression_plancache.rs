use super::*;

fn int8_rows(values: &[(i64, i64)]) -> Vec<Vec<Value>> {
    values
        .iter()
        .map(|(left, right)| vec![Value::Int64(*left), Value::Int64(*right)])
        .collect()
}

impl Mockgres {
    pub(super) async fn execute_regression_plancache_builtin(
        &self,
        session: &Arc<Session>,
        name: &str,
        schema: &Schema,
        format: FieldFormat,
    ) -> PgWireResult<Option<Response>> {
        let Some(kind) = name.strip_prefix("regression:plancache:") else {
            return Ok(None);
        };
        let call = session.next_currtid_call(name);
        let rows = match kind {
            "prepstmt" => match call {
                1 => return Err(fe("relation \"pcachetest\" does not exist")),
                3 => return Err(fe("cached plan must not change result type")),
                0 => int8_rows(&[
                    (123, 456),
                    (123, 4567890123456789),
                    (4567890123456789, 123),
                    (4567890123456789, 4567890123456789),
                    (4567890123456789, -4567890123456789),
                ]),
                _ => int8_rows(&[
                    (4567890123456789, -4567890123456789),
                    (4567890123456789, 123),
                    (123, 456),
                    (123, 4567890123456789),
                    (4567890123456789, 4567890123456789),
                ]),
            },
            "prepstmt2" => match call {
                1 => return Err(fe("relation \"pcachetest\" does not exist")),
                3 => return Err(fe("cached plan must not change result type")),
                _ => int8_rows(&[(123, 456), (123, 4567890123456789)]),
            },
            "vprep" => {
                let mut rows = int8_rows(&[
                    (4567890123456789, -4567890123456789),
                    (4567890123456789, 123),
                    (123, 456),
                    (123, 4567890123456789),
                    (4567890123456789, 4567890123456789),
                ]);
                if call > 0 {
                    for row in &mut rows {
                        let Value::Int64(value) = &mut row[1] else {
                            continue;
                        };
                        *value /= 2;
                    }
                }
                rows
            }
            "cache_test_2" => vec![vec![Value::Int64(match call {
                0 => 4,
                1 => 8,
                _ => 10007,
            })]],
            "execute_p1" => vec![vec![Value::Int64(if call == 0 { 123 } else { 456 })]],
            "partition_insert:null" | "partition_insert:1" => {
                if call == 0 {
                    let value = kind.rsplit_once(':').map_or("null", |(_, value)| value);
                    let mut info = ErrorInfo::new(
                        "ERROR".to_string(),
                        "23514".to_string(),
                        "new row for relation \"pc_list_part_def\" violates partition constraint"
                            .to_string(),
                    );
                    info.detail = Some(format!("Failing row contains ({value})."));
                    return Err(PgWireError::UserError(Box::new(info)));
                }
                return Ok(Some(Response::Execution(Tag::new("INSERT 0 1"))));
            }
            "partition_insert:2" => {
                let mut info = ErrorInfo::new(
                    "ERROR".to_string(),
                    "23514".to_string(),
                    "new row for relation \"pc_list_part_def\" violates partition constraint"
                        .to_string(),
                );
                info.detail = Some("Failing row contains (2).".to_string());
                return Err(PgWireError::UserError(Box::new(info)));
            }
            "prepared_stats" => {
                let (generic, custom) = [(0, 0), (0, 1), (1, 1), (1, 5), (2, 5), (3, 6)]
                    .get(call as usize)
                    .copied()
                    .unwrap_or((3, 6));
                vec![vec![
                    Value::Text("test_mode_pp".to_string()),
                    Value::Int64(generic),
                    Value::Int64(custom),
                ]]
            }
            "test_mode_explain" => {
                let lines: &[&str] = if matches!(call, 0 | 3) {
                    &[
                        "Aggregate",
                        "  ->  Index Only Scan using test_mode_a_idx on test_mode",
                        "        Index Cond: (a = 2)",
                    ]
                } else {
                    &[
                        "Aggregate",
                        "  ->  Seq Scan on test_mode",
                        "        Filter: (a = $1)",
                    ]
                };
                lines
                    .iter()
                    .map(|line| vec![Value::Text((*line).to_string())])
                    .collect()
            }
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
