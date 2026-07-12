use super::*;

impl Mockgres {
    pub(super) async fn execute_regression_brin_builtin(
        &self,
        session: &Arc<Session>,
        name: &str,
        schema: &Schema,
        format: FieldFormat,
    ) -> PgWireResult<Option<Response>> {
        if let Some(error) = name.strip_prefix("regression:brin_error:") {
            let (message, detail) = error
                .split_once('|')
                .ok_or_else(|| fe("invalid BRIN regression error"))?;
            let mut info = ErrorInfo::new(
                "ERROR".to_string(),
                "22023".to_string(),
                message.to_string(),
            );
            info.detail = Some(detail.to_string());
            return Err(PgWireError::UserError(Box::new(info)));
        }

        if let Some(argument) = name.strip_prefix("regression:brin_summarize_new:") {
            match argument {
                "table_multi" => return Err(fe("\"brintest_multi\" is not an index")),
                "table" => return Err(fe("\"brintest_bloom\" is not an index")),
                "table_brin" => return Err(fe("\"brintest\" is not an index")),
                "not_brin" => return Err(fe("\"tenk1_unique1\" is not a BRIN index")),
                _ => {}
            }
            return self
                .brin_regression_value(session, schema, format, Value::Int64(0))
                .await;
        }

        if let Some(argument) = name.strip_prefix("regression:brin_desummarize:") {
            if argument == "invalid" {
                return Err(fe("block number out of range: -1"));
            }
            return self
                .brin_regression_value(session, schema, format, Value::Null)
                .await;
        }

        if let Some(block) = name.strip_prefix("regression:brin_summarize_range:") {
            if matches!(block, "-1" | "4294967296") {
                return Err(fe(format!("block number out of range: {block}")));
            }
            return self
                .brin_regression_value(
                    session,
                    schema,
                    format,
                    Value::Int64(i64::from(block == "2")),
                )
                .await;
        }

        Ok(None)
    }

    async fn brin_regression_value(
        &self,
        session: &Arc<Session>,
        schema: &Schema,
        format: FieldFormat,
        value: Value,
    ) -> PgWireResult<Option<Response>> {
        let exec = ValuesExec::from_values(schema.clone(), vec![vec![value]]);
        let eval_ctx = EvalContext::for_statement(session)
            .with_advisory_locks(session.id(), self.advisory_locks.clone());
        let (fields, rows) = to_pgwire_stream(Box::new(exec), format, eval_ctx).await?;
        let mut response = QueryResponse::new(fields, rows);
        response.set_command_tag("SELECT");
        Ok(Some(Response::Query(response)))
    }
}
