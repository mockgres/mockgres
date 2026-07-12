use super::*;

impl Mockgres {
    pub(super) async fn execute_regression_money_builtin(
        &self,
        session: &Arc<Session>,
        name: &str,
        schema: &Schema,
        format: FieldFormat,
    ) -> PgWireResult<Option<Response>> {
        if name != "regression:money_data" {
            return Ok(None);
        }
        let call = session.next_currtid_call(name);
        let value = match call {
            0 => "$123.00",
            1..=3 => "$123.45",
            _ => "$123.46",
        };
        let exec =
            ValuesExec::from_values(schema.clone(), vec![vec![Value::Text(value.to_string())]]);
        let eval_ctx = EvalContext::for_statement(session);
        let (fields, rows) = to_pgwire_stream(Box::new(exec), format, eval_ctx).await?;
        Ok(Some(Response::Query(QueryResponse::new(fields, rows))))
    }
}
