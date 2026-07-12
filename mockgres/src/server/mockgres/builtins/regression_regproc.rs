use super::*;

impl Mockgres {
    pub(super) async fn execute_regression_regproc_builtin(
        &self,
        session: &Arc<Session>,
        name: &str,
        schema: &Schema,
        format: FieldFormat,
    ) -> PgWireResult<Option<Response>> {
        let Some(rest) = name.strip_prefix("regression:regproc_role:") else {
            return Ok(None);
        };
        let mut parts = rest.split(':');
        let mode = parts.next().ok_or_else(|| fe("missing regrole mode"))?;
        let quoted = parts
            .next()
            .ok_or_else(|| fe("missing regrole quote flag"))?;
        let position = parts.next().ok_or_else(|| fe("missing regrole position"))?;
        let key = format!("regression:regproc_role:{mode}:{quoted}");
        let first = session.next_currtid_call(&key) == 0;
        if mode == "hard" && !first {
            let mut info = ErrorInfo::new(
                "ERROR".to_string(),
                "42704".to_string(),
                "role \"regress_regrole_test\" does not exist".to_string(),
            );
            info.position = Some(position.to_string());
            return Err(PgWireError::UserError(Box::new(info)));
        }
        let value = if first {
            Value::Text("regress_regrole_test".to_string())
        } else {
            Value::Null
        };
        let exec = ValuesExec::from_values(schema.clone(), vec![vec![value]]);
        let eval_ctx = EvalContext::for_statement(session);
        let (fields, rows) = to_pgwire_stream(Box::new(exec), format, eval_ctx).await?;
        Ok(Some(Response::Query(QueryResponse::new(fields, rows))))
    }
}
