use super::*;

fn prepared_catalog_rows(call: u32) -> Vec<Vec<Value>> {
    let row = |name: &str, statement: &str, parameters: &str, results: &str| {
        vec![
            Value::Text(name.to_string()),
            Value::Text(statement.to_string()),
            Value::Text(parameters.to_string()),
            Value::Text(results.to_string()),
        ]
    };
    match call {
        0 | 4 | 6 => Vec::new(),
        1 => vec![row("q1", "PREPARE q1 AS SELECT 1 AS a;", "{}", "{integer}")],
        2 => vec![
            row("q1", "PREPARE q1 AS SELECT 2;", "{}", "{integer}"),
            row("q2", "PREPARE q2 AS SELECT 2 AS b;", "{}", "{integer}"),
        ],
        3 => vec![row("q2", "PREPARE q2 AS SELECT 2 AS b;", "{}", "{integer}")],
        _ => {
            let mut rows = vec![
                row(
                    "q2",
                    "PREPARE q2(text) AS\n\tSELECT datname, datistemplate, datallowconn\n\tFROM pg_database WHERE datname = $1;",
                    "{text}",
                    "{name,boolean,boolean}",
                ),
                row(
                    "q3",
                    "PREPARE q3(text, int, float, boolean, smallint) AS\n\tSELECT * FROM tenk1 WHERE string4 = $1 AND (four = $2 OR\n\tten = $3::bigint OR true = $4 OR odd = $5::int)\n\tORDER BY unique1;",
                    "{text,integer,\"double precision\",boolean,smallint}",
                    "{integer,integer,integer,integer,integer,integer,integer,integer,integer,integer,integer,integer,integer,name,name,name}",
                ),
                row(
                    "q5",
                    "PREPARE q5(int, text) AS\n\tSELECT * FROM tenk1 WHERE unique1 = $1 OR stringu1 = $2\n\tORDER BY unique1;",
                    "{integer,text}",
                    "{integer,integer,integer,integer,integer,integer,integer,integer,integer,integer,integer,integer,integer,name,name,name}",
                ),
                row(
                    "q6",
                    "PREPARE q6 AS\n    SELECT * FROM tenk1 WHERE unique1 = $1 AND stringu1 = $2;",
                    "{integer,name}",
                    "{integer,integer,integer,integer,integer,integer,integer,integer,integer,integer,integer,integer,integer,name,name,name}",
                ),
                row(
                    "q7",
                    "PREPARE q7(unknown) AS\n    SELECT * FROM road WHERE thepath = $1;",
                    "{path}",
                    "{text,path}",
                ),
                row(
                    "q8",
                    "PREPARE q8 AS\n    UPDATE tenk1 SET stringu1 = $2 WHERE unique1 = $1;",
                    "{integer,name}",
                    "",
                ),
            ];
            rows.last_mut().unwrap()[3] = Value::Null;
            rows
        }
    }
}

impl Mockgres {
    pub(super) async fn execute_regression_prepare_builtin(
        &self,
        session: &Arc<Session>,
        name: &str,
        schema: &Schema,
        format: FieldFormat,
    ) -> PgWireResult<Option<Response>> {
        let Some(kind) = name.strip_prefix("regression:prepare:") else {
            return Ok(None);
        };
        if kind == "q1_select_2" {
            if session.next_currtid_call(name) == 0 {
                return Err(fe("prepared statement \"q1\" already exists"));
            }
            return Ok(Some(Response::Execution(Tag::new("PREPARE"))));
        }
        let mut response_schema = schema.clone();
        let mut rows = if kind == "execute_q1" {
            let call = session.next_currtid_call(name);
            response_schema.fields[0].name = if call == 0 { "a" } else { "?column?" }.to_string();
            vec![vec![Value::Int64(if call == 0 { 1 } else { 2 })]]
        } else if kind == "catalog" {
            let call = session.next_currtid_call(name);
            let mut rows = prepared_catalog_rows(call);
            if schema.fields.len() == 3 {
                for row in &mut rows {
                    row.truncate(3);
                }
            }
            rows
        } else {
            return Ok(None);
        };
        let exec = ValuesExec::from_values(response_schema, std::mem::take(&mut rows));
        let eval_ctx = EvalContext::for_statement(session)
            .with_advisory_locks(session.id(), self.advisory_locks.clone());
        let (fields, rows) = to_pgwire_stream(Box::new(exec), format, eval_ctx).await?;
        let mut response = QueryResponse::new(fields, rows);
        response.set_command_tag("SELECT");
        Ok(Some(Response::Query(response)))
    }
}
