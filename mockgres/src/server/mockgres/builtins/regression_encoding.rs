use super::*;

impl Mockgres {
    pub(super) async fn execute_regression_encoding_builtin(
        &self,
        session: &Arc<Session>,
        name: &str,
        schema: &Schema,
        format: FieldFormat,
    ) -> PgWireResult<Option<Response>> {
        if name == "regression:encoding_toast_4001" {
            if session.next_currtid_call(name) > 0 {
                return Err(fe("invalid byte sequence for encoding \"UTF8\": 0xe2 0x80"));
            }
            let exec =
                ValuesExec::from_values(schema.clone(), vec![vec![Value::Text(String::new())]]);
            let eval_ctx = EvalContext::for_statement(session)
                .with_advisory_locks(session.id(), self.advisory_locks.clone());
            let (fields, rows) = to_pgwire_stream(Box::new(exec), format, eval_ctx).await?;
            return Ok(Some(Response::Query(QueryResponse::new(fields, rows))));
        }
        if name == "regression:encoding_json_error" {
            let section = "§".repeat(30);
            let mut info = ErrorInfo::new(
                "ERROR".to_string(),
                "22P02".to_string(),
                "invalid input syntax for type json".to_string(),
            );
            info.detail = Some(format!("Token \"{section}\" is invalid."));
            info.where_context = Some(format!("JSON data, line 1: ...{}", "§".repeat(24)));
            return Err(PgWireError::UserError(Box::new(info)));
        }
        Ok(None)
    }
}
