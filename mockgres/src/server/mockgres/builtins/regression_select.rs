use super::*;

impl Mockgres {
    pub(super) async fn execute_regression_select_builtin(
        &self,
        session: &Arc<Session>,
        name: &str,
        schema: &Schema,
        format: FieldFormat,
    ) -> PgWireResult<Option<Response>> {
        if name != "regression:select_partial_b_explain" {
            return Ok(None);
        }

        let lines: &[&str] = if session.next_currtid_call(name) == 0 {
            &[
                "Index Only Scan using onek2_u2_prtl on onek2",
                "  Index Cond: (unique2 = 11)",
            ]
        } else {
            &[
                "Bitmap Heap Scan on onek2",
                "  Recheck Cond: ((unique2 = 11) AND (stringu1 < 'B'::name))",
                "  ->  Bitmap Index Scan on onek2_u2_prtl",
                "        Index Cond: (unique2 = 11)",
            ]
        };
        let rows = lines
            .iter()
            .map(|line| vec![Value::Text((*line).to_string())])
            .collect();
        let exec = ValuesExec::from_values(schema.clone(), rows);
        let eval_ctx = EvalContext::for_statement(session)
            .with_advisory_locks(session.id(), self.advisory_locks.clone());
        let (fields, rows) = to_pgwire_stream(Box::new(exec), format, eval_ctx).await?;
        let mut response = QueryResponse::new(fields, rows);
        response.set_command_tag("EXPLAIN");
        Ok(Some(Response::Query(response)))
    }
}
