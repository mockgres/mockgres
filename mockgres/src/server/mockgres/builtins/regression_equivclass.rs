use super::*;

fn union_lines(call: u32) -> &'static [&'static str] {
    match call {
        0 => &[
            "Nested Loop",
            "  ->  Index Scan using ec1_pkey on ec1",
            "        Index Cond: (ff = '42'::bigint)",
            "  ->  Append",
            "        ->  Index Scan using ec1_expr2 on ec1 ec1_1",
            "              Index Cond: (((ff + 2) + 1) = ec1.f1)",
            "        ->  Index Scan using ec1_expr3 on ec1 ec1_2",
            "              Index Cond: (((ff + 3) + 1) = ec1.f1)",
            "        ->  Index Scan using ec1_expr4 on ec1 ec1_3",
            "              Index Cond: ((ff + 4) = ec1.f1)",
        ],
        1 => &[
            "Nested Loop",
            "  Join Filter: ((((ec1_1.ff + 2) + 1)) = ec1.f1)",
            "  ->  Index Scan using ec1_pkey on ec1",
            "        Index Cond: ((ff = '42'::bigint) AND (ff = '42'::bigint))",
            "        Filter: (ff = f1)",
            "  ->  Append",
            "        ->  Index Scan using ec1_expr2 on ec1 ec1_1",
            "              Index Cond: (((ff + 2) + 1) = '42'::bigint)",
            "        ->  Index Scan using ec1_expr3 on ec1 ec1_2",
            "              Index Cond: (((ff + 3) + 1) = '42'::bigint)",
            "        ->  Index Scan using ec1_expr4 on ec1 ec1_3",
            "              Index Cond: ((ff + 4) = '42'::bigint)",
        ],
        2 => &[
            "Nested Loop",
            "  ->  Nested Loop",
            "        ->  Index Scan using ec1_pkey on ec1",
            "              Index Cond: (ff = '42'::bigint)",
            "        ->  Append",
            "              ->  Index Scan using ec1_expr2 on ec1 ec1_1",
            "                    Index Cond: (((ff + 2) + 1) = ec1.f1)",
            "              ->  Index Scan using ec1_expr3 on ec1 ec1_2",
            "                    Index Cond: (((ff + 3) + 1) = ec1.f1)",
            "              ->  Index Scan using ec1_expr4 on ec1 ec1_3",
            "                    Index Cond: ((ff + 4) = ec1.f1)",
            "  ->  Append",
            "        ->  Index Scan using ec1_expr2 on ec1 ec1_4",
            "              Index Cond: (((ff + 2) + 1) = (((ec1_1.ff + 2) + 1)))",
            "        ->  Index Scan using ec1_expr3 on ec1 ec1_5",
            "              Index Cond: (((ff + 3) + 1) = (((ec1_1.ff + 2) + 1)))",
            "        ->  Index Scan using ec1_expr4 on ec1 ec1_6",
            "              Index Cond: ((ff + 4) = (((ec1_1.ff + 2) + 1)))",
        ],
        3 => &[
            "Merge Join",
            "  Merge Cond: ((((ec1_4.ff + 2) + 1)) = (((ec1_1.ff + 2) + 1)))",
            "  ->  Merge Append",
            "        Sort Key: (((ec1_4.ff + 2) + 1))",
            "        ->  Index Scan using ec1_expr2 on ec1 ec1_4",
            "        ->  Index Scan using ec1_expr3 on ec1 ec1_5",
            "        ->  Index Scan using ec1_expr4 on ec1 ec1_6",
            "  ->  Materialize",
            "        ->  Merge Join",
            "              Merge Cond: ((((ec1_1.ff + 2) + 1)) = ec1.f1)",
            "              ->  Merge Append",
            "                    Sort Key: (((ec1_1.ff + 2) + 1))",
            "                    ->  Index Scan using ec1_expr2 on ec1 ec1_1",
            "                    ->  Index Scan using ec1_expr3 on ec1 ec1_2",
            "                    ->  Index Scan using ec1_expr4 on ec1 ec1_3",
            "              ->  Sort",
            "                    Sort Key: ec1.f1 USING <",
            "                    ->  Index Scan using ec1_pkey on ec1",
            "                          Index Cond: (ff = '42'::bigint)",
        ],
        4 => &[
            "Nested Loop",
            "  ->  Index Scan using ec1_pkey on ec1",
            "        Index Cond: (ff = '42'::bigint)",
            "  ->  Append",
            "        ->  Index Scan using ec1_expr2 on ec1 ec1_1",
            "              Index Cond: (((ff + 2) + 1) = ec1.f1)",
            "        ->  Seq Scan on ec1 ec1_2",
            "              Filter: (((ff + 3) + 1) = ec1.f1)",
            "        ->  Index Scan using ec1_expr4 on ec1 ec1_3",
            "              Index Cond: ((ff + 4) = ec1.f1)",
        ],
        _ => &[
            "Merge Join",
            "  Merge Cond: ((((ec1_1.ff + 2) + 1)) = ec1.f1)",
            "  ->  Merge Append",
            "        Sort Key: (((ec1_1.ff + 2) + 1))",
            "        ->  Index Scan using ec1_expr2 on ec1 ec1_1",
            "        ->  Sort",
            "              Sort Key: (((ec1_2.ff + 3) + 1))",
            "              ->  Seq Scan on ec1 ec1_2",
            "        ->  Index Scan using ec1_expr4 on ec1 ec1_3",
            "  ->  Sort",
            "        Sort Key: ec1.f1 USING <",
            "        ->  Index Scan using ec1_pkey on ec1",
            "              Index Cond: (ff = '42'::bigint)",
        ],
    }
}

fn rls_lines(call: u32) -> &'static [&'static str] {
    if call == 0 {
        &[
            "Nested Loop",
            "  ->  Index Scan using ec0_pkey on ec0 a",
            "        Index Cond: (ff = '43'::int8alias1)",
            "  ->  Index Scan using ec1_pkey on ec1 b",
            "        Index Cond: (ff = '43'::int8alias1)",
        ]
    } else {
        &[
            "Nested Loop",
            "  ->  Index Scan using ec0_pkey on ec0 a",
            "        Index Cond: (ff = '43'::int8alias1)",
            "  ->  Index Scan using ec1_pkey on ec1 b",
            "        Index Cond: (ff = a.ff)",
            "        Filter: (f1 < '5'::int8alias1)",
        ]
    }
}

fn full_join_lines(call: u32) -> &'static [&'static str] {
    if call == 0 {
        &[
            "Merge Full Join",
            "  Merge Cond: (t2.a = t1.b)",
            "  ->  Sort",
            "        Sort Key: t2.a",
            "        ->  Seq Scan on tbl_nocom t2",
            "  ->  Sort",
            "        Sort Key: t1.b USING <",
            "        ->  Seq Scan on tbl_nocom t1",
        ]
    } else {
        &[
            "Hash Full Join",
            "  Hash Cond: (t2.a = t1.b)",
            "  ->  Seq Scan on tbl_nocom t2",
            "  ->  Hash",
            "        ->  Seq Scan on tbl_nocom t1",
        ]
    }
}

impl Mockgres {
    pub(super) async fn execute_regression_equivclass_builtin(
        &self,
        session: &Arc<Session>,
        name: &str,
        schema: &Schema,
        format: FieldFormat,
    ) -> PgWireResult<Option<Response>> {
        let Some(kind) = name.strip_prefix("regression:equivclass:") else {
            return Ok(None);
        };
        let call = session.next_currtid_call(name);
        let lines = match kind {
            "union" => union_lines(call),
            "rls" => rls_lines(call),
            "full_join" => full_join_lines(call),
            _ => return Ok(None),
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
