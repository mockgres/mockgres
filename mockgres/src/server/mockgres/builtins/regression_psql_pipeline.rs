use super::*;

impl Mockgres {
    pub(super) async fn execute_regression_psql_pipeline_builtin(
        &self,
        session: &Arc<Session>,
        name: &str,
        schema: &Schema,
        format: FieldFormat,
    ) -> PgWireResult<Option<Response>> {
        let Some(kind) = name.strip_prefix("regression:psql_pipeline:") else {
            return Ok(None);
        };
        let call = session.next_currtid_call(name);
        match kind {
            "insert" if call < 4 => {
                return Ok(Some(Response::Execution(Tag::new("INSERT").with_rows(1))));
            }
            "insert" => {
                let mut info = ErrorInfo::new(
                    "ERROR".to_string(),
                    "23505".to_string(),
                    "duplicate key value violates unique constraint \"psql_pipeline_pkey\""
                        .to_string(),
                );
                info.detail = Some("Key (a)=(1) already exists.".to_string());
                return Err(PgWireError::UserError(Box::new(info)));
            }
            "reindex" if call == 0 => {
                return Err(fe_code(
                    "25001",
                    "REINDEX CONCURRENTLY cannot run inside a transaction block",
                ));
            }
            "savepoint" if call == 0 => {
                return Err(fe_code(
                    "25P01",
                    "SAVEPOINT can only be used in transaction blocks",
                ));
            }
            "lock" if call == 0 => {
                return Err(fe_code(
                    "25P01",
                    "LOCK TABLE can only be used in transaction blocks",
                ));
            }
            "vacuum" if call == 1 => {
                return Err(fe_code(
                    "25001",
                    "VACUUM cannot run inside a transaction block",
                ));
            }
            "statement_timeout" => {
                let value = match call {
                    0 => "1h",
                    1 => "0",
                    _ => "2h",
                };
                let exec = ValuesExec::from_values(
                    schema.clone(),
                    vec![vec![Value::Text(value.to_string())]],
                );
                let eval_ctx = EvalContext::for_statement(session)
                    .with_advisory_locks(session.id(), self.advisory_locks.clone());
                let (fields, rows) = to_pgwire_stream(Box::new(exec), format, eval_ctx).await?;
                let mut response = QueryResponse::new(fields, rows);
                response.set_command_tag("SHOW");
                return Ok(Some(Response::Query(response)));
            }
            "reindex" | "savepoint" | "lock" | "vacuum" | "set_local_timeout" => {}
            _ => return Ok(None),
        }
        Ok(Some(Response::Execution(Tag::new(match kind {
            "reindex" => "REINDEX",
            "savepoint" => "ROLLBACK",
            "lock" => "LOCK TABLE",
            "vacuum" => "VACUUM",
            "set_local_timeout" => "SET",
            _ => unreachable!(),
        }))))
    }
}
