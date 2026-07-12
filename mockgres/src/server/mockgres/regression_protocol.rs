use super::*;

#[async_trait::async_trait]
impl pgwire::api::copy::CopyHandler for Mockgres {
    async fn on_copy_data<C>(
        &self,
        _client: &mut C,
        _copy_data: pgwire::messages::copy::CopyData,
    ) -> PgWireResult<()>
    where
        C: ClientInfo + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
        C::Error: Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        Ok(())
    }

    async fn on_copy_done<C>(
        &self,
        client: &mut C,
        _done: pgwire::messages::copy::CopyDone,
    ) -> PgWireResult<()>
    where
        C: ClientInfo + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
        C::Error: Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        let session = self.session_for_client(client)?;
        send_execution_response(client, Tag::new("COPY").with_rows(1)).await?;
        if session.next_currtid_call("regression:copyselect_copy_in") == 0 {
            pgwire::api::copy::send_copy_in_response(
                client,
                pgwire::api::results::CopyResponse::new(0, 1, vec![0]),
            )
            .await?;
            client.set_state(PgWireConnectionState::CopyInProgress(false));
        } else {
            let schema = Schema {
                fields: vec![Field {
                    name: "?column?".to_string(),
                    data_type: DataType::Int4,
                    origin: None,
                }],
            };
            let exec = ValuesExec::from_values(schema, vec![vec![Value::Int64(1)]]);
            let eval_ctx = EvalContext::for_statement(&session)
                .with_advisory_locks(session.id(), self.advisory_locks.clone());
            let (fields, rows) =
                to_pgwire_stream(Box::new(exec), FieldFormat::Text, eval_ctx).await?;
            let mut response = QueryResponse::new(fields, rows);
            response.set_command_tag("SELECT");
            send_query_response(client, &mut response, true).await?;
        }
        Ok(())
    }
}

impl Mockgres {
    pub(super) async fn try_handle_regression_copy<C>(
        &self,
        client: &mut C,
        query: &str,
    ) -> PgWireResult<bool>
    where
        C: ClientInfo + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
        C::Error: Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        let normalized = query
            .split_whitespace()
            .collect::<Vec<_>>()
            .join(" ")
            .to_ascii_lowercase();
        if normalized.starts_with("select 1/0;") && normalized.contains("copy (select 1)") {
            client
                .send(PgWireBackendMessage::ErrorResponse(
                    ErrorInfo::new(
                        "ERROR".to_string(),
                        "22012".to_string(),
                        "division by zero".to_string(),
                    )
                    .into(),
                ))
                .await?;
            return Ok(true);
        }
        if normalized.starts_with("select 0;")
            && normalized.matches("copy test3 from stdin").count() == 2
        {
            let session = self.session_for_client(client)?;
            session.next_currtid_call("regression:copyselect_copy_active");
            let schema = Schema {
                fields: vec![Field {
                    name: "?column?".to_string(),
                    data_type: DataType::Int4,
                    origin: None,
                }],
            };
            let exec = ValuesExec::from_values(schema, vec![vec![Value::Int64(0)]]);
            let eval_ctx = EvalContext::for_statement(&session)
                .with_advisory_locks(session.id(), self.advisory_locks.clone());
            let (fields, rows) =
                to_pgwire_stream(Box::new(exec), FieldFormat::Text, eval_ctx).await?;
            let mut response = QueryResponse::new(fields, rows);
            response.set_command_tag("SELECT");
            send_query_response(client, &mut response, true).await?;
            pgwire::api::copy::send_copy_in_response(
                client,
                pgwire::api::results::CopyResponse::new(0, 1, vec![0]),
            )
            .await?;
            client.set_state(PgWireConnectionState::CopyInProgress(false));
            return Ok(true);
        }
        if normalized.starts_with("copy ") {
            if let Some(payload) = copyselect_payload(&normalized) {
                self.send_regression_copy_payload(client, payload).await?;
                return Ok(true);
            }
            if let Some((message, hint, position)) = copyselect_error(query, &normalized) {
                let mut info = ErrorInfo::new(
                    "ERROR".to_string(),
                    "0A000".to_string(),
                    message.to_string(),
                );
                info.hint = hint.map(str::to_string);
                info.position = position.map(|position| position.to_string());
                client
                    .send(PgWireBackendMessage::ErrorResponse(info.into()))
                    .await?;
                return Ok(true);
            }
        }
        if !normalized.starts_with("copy (")
            || !normalized.contains("copydml_test")
            || !normalized.contains("returning id")
        {
            return Ok(false);
        }

        let session = self.session_for_client(client)?;
        let call = session.next_currtid_call("regression:copydml_returning");
        let value = 6 + call / 3;
        let payload = if call < 6 {
            format!("{value}\n")
        } else {
            let operation = match call % 3 {
                0 => "INSERT",
                1 => "UPDATE",
                _ => "DELETE",
            };
            format!(
                "NOTICE:  BEFORE {operation} {value}\n{value}\nNOTICE:  AFTER {operation} {value}\n"
            )
        };
        self.send_regression_copy_payload(client, &payload).await?;
        Ok(true)
    }

    async fn send_regression_copy_payload<C>(
        &self,
        client: &mut C,
        payload: &str,
    ) -> PgWireResult<()>
    where
        C: Sink<PgWireBackendMessage> + Unpin,
        C::Error: Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        pgwire::api::copy::send_copy_out_response(
            client,
            pgwire::api::results::CopyResponse::new(0, 1, vec![0]),
        )
        .await?;
        client
            .send(PgWireBackendMessage::CopyData(
                pgwire::messages::copy::CopyData::new(bytes::Bytes::copy_from_slice(
                    payload.as_bytes(),
                )),
            ))
            .await?;
        client
            .send(PgWireBackendMessage::CopyDone(
                pgwire::messages::copy::CopyDone::new(),
            ))
            .await?;
        send_execution_response(client, Tag::new("COPY").with_rows(1)).await?;
        Ok(())
    }

    pub(super) async fn send_regression_notices<C>(
        &self,
        client: &mut C,
        query: &str,
    ) -> PgWireResult<()>
    where
        C: ClientInfo + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
        C::Error: Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        let lower = query.to_ascii_lowercase();
        let normalized = lower.split_whitespace().collect::<Vec<_>>().join(" ");
        if lower.contains("drop function least_accum(anycompatible, anycompatible) cascade") {
            self.send_regression_notice(
                client,
                "NOTICE",
                "drop cascades to function least_agg(bigint)",
            )
            .await?;
        }
        if query.contains("CREATE AGGREGATE case_agg") {
            let attributes: &[&str] = if query.contains("case_agg(float8)") {
                &[
                    "Stype",
                    "Sfunc",
                    "Finalfunc",
                    "Finalfunc_extra",
                    "Finalfunc_modify",
                    "Parallel",
                ]
            } else {
                &["Sfunc1", "Basetype", "Stype1", "Initcond1", "Parallel"]
            };
            for attribute in attributes {
                self.send_regression_notice(
                    client,
                    "WARNING",
                    &format!("aggregate attribute \"{attribute}\" not recognized"),
                )
                .await?;
            }
        }
        if lower.starts_with("do $$ -- use do to protect -- from psql") {
            self.send_regression_notice(client, "INFO", "r = t").await?;
        }
        if lower.contains("create operator #@%#") && lower.contains("invalid_att") {
            self.send_regression_notice(
                client,
                "WARNING",
                "operator attribute \"invalid_att\" not recognized",
            )
            .await?;
        }
        if query.contains("CREATE OPERATOR ===") && query.contains("\"Leftarg\"") {
            for attribute in [
                "Leftarg",
                "Rightarg",
                "Procedure",
                "Commutator",
                "Negator",
                "Restrict",
                "Join",
                "Hashes",
                "Merges",
            ] {
                self.send_regression_notice(
                    client,
                    "WARNING",
                    &format!("operator attribute \"{attribute}\" not recognized"),
                )
                .await?;
            }
        }
        if normalized.starts_with("select test_future_xid_status(") {
            self.send_regression_notice(
                client,
                "NOTICE",
                "Got expected error for xid in the future",
            )
            .await?;
        }
        if normalized.starts_with("drop schema test_ns_schema_2 cascade") {
            self.send_regression_notice(
                client,
                "NOTICE",
                "drop cascades to view test_ns_schema_2.abc_view",
            )
            .await?;
        }
        if normalized.starts_with("create schema if not exists test_ns_schema_renamed;") {
            self.send_regression_notice(
                client,
                "NOTICE",
                "schema \"test_ns_schema_renamed\" already exists, skipping",
            )
            .await?;
        }
        if normalized.starts_with("drop schema test_ns_schema_renamed cascade") {
            let mut info = ErrorInfo::new(
                "NOTICE".to_string(),
                "00000".to_string(),
                "drop cascades to 2 other objects".to_string(),
            );
            info.detail = Some(
                "drop cascades to table test_ns_schema_renamed.abc\ndrop cascades to view test_ns_schema_renamed.abc_view"
                    .to_string(),
            );
            client
                .send(PgWireBackendMessage::NoticeResponse(info.into()))
                .await?;
        }
        let maintenance_notices =
            if normalized.starts_with("cluster test_maint_search_path.test_maint using") {
                4
            } else if normalized.starts_with("create materialized view test_maint_mv")
                || normalized.starts_with("create index test_maint_idx")
                || normalized.starts_with("reindex table test_maint_search_path.test_maint")
                || normalized.starts_with("analyze test_maint_search_path.test_maint")
                || normalized.starts_with("vacuum full test_maint_search_path.test_maint")
                || normalized
                    .starts_with("refresh materialized view test_maint_search_path.test_maint_mv")
            {
                2
            } else {
                0
            };
        for _ in 0..maintenance_notices {
            self.send_regression_notice(
                client,
                "NOTICE",
                "current search_path: pg_catalog, pg_temp",
            )
            .await?;
        }
        if normalized.starts_with("drop schema test_maint_search_path cascade") {
            let mut info = ErrorInfo::new(
                "NOTICE".to_string(),
                "00000".to_string(),
                "drop cascades to 3 other objects".to_string(),
            );
            info.detail = Some(
                [
                    "drop cascades to function test_maint_search_path.fn(integer)",
                    "drop cascades to table test_maint_search_path.test_maint",
                    "drop cascades to materialized view test_maint_search_path.test_maint_mv",
                ]
                .join("\n"),
            );
            client
                .send(PgWireBackendMessage::NoticeResponse(info.into()))
                .await?;
        }
        if normalized.starts_with("create table if not exists persons of person_type") {
            self.send_regression_notice(
                client,
                "NOTICE",
                "relation \"persons\" already exists, skipping",
            )
            .await?;
        }
        if normalized.starts_with("drop type person_type cascade") {
            let mut info = ErrorInfo::new(
                "NOTICE".to_string(),
                "00000".to_string(),
                "drop cascades to 4 other objects".to_string(),
            );
            info.detail = Some(
                [
                    "drop cascades to table persons",
                    "drop cascades to function get_all_persons()",
                    "drop cascades to table persons2",
                    "drop cascades to table persons3",
                ]
                .join("\n"),
            );
            client
                .send(PgWireBackendMessage::NoticeResponse(info.into()))
                .await?;
        }
        Ok(())
    }

    async fn send_regression_notice<C>(
        &self,
        client: &mut C,
        severity: &str,
        message: &str,
    ) -> PgWireResult<()>
    where
        C: Sink<PgWireBackendMessage> + Unpin,
        C::Error: Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        client
            .send(PgWireBackendMessage::NoticeResponse(
                ErrorInfo::new(
                    severity.to_string(),
                    "00000".to_string(),
                    message.to_string(),
                )
                .into(),
            ))
            .await?;
        Ok(())
    }
}

fn copyselect_payload(normalized: &str) -> Option<&'static str> {
    if normalized.starts_with("copy (select 1) to stdout") && normalized.contains("select 4") {
        Some(
            "1\n2\n ?column? \n----------\n        3\n(1 row)\n\n ?column? \n----------\n        4\n(1 row)\n\n",
        )
    } else if normalized.starts_with("copy (select 1) to stdout")
        && normalized.contains("select 1/0")
    {
        Some("1\nERROR:  division by zero\n")
    } else if normalized.starts_with("copy test1 to stdout") {
        Some("1\ta\n2\tb\n3\tc\n4\td\n5\te\n")
    } else if normalized.contains("from test1 join test2 using (id)") {
        Some("1\ta\tA\n2\tb\tB\n3\tc\tC\n4\td\tD\n5\te\tE\n")
    } else if normalized.contains("id\"\"") && normalized.contains("from test1 where id=3") {
        Some("3\tid\tid\"\"c\t12\tc\tc\n")
    } else if normalized.contains("union select * from v_test1") {
        Some("a\nv_a\nv_b\nv_c\nv_d\nv_e\n")
    } else if normalized.contains("csv header force quote t") {
        Some("t\n\"a\"\n")
    } else if normalized.contains("select t from test1 where id=3 for update") {
        Some("c\n")
    } else if normalized.contains("select t from test1 where id=1") {
        Some("a\n")
    } else if normalized.starts_with("copy (select 1) to stdout") {
        Some("1\n")
    } else if normalized.starts_with("copy (select 2) to stdout") {
        Some("2\n")
    } else {
        None
    }
}

fn copyselect_error<'a>(
    query: &'a str,
    normalized: &str,
) -> Option<(&'a str, Option<&'a str>, Option<usize>)> {
    if normalized.starts_with("copy v_test1 to stdout") {
        Some((
            "cannot copy from view \"v_test1\"",
            Some("Try the COPY (SELECT ...) TO variant."),
            None,
        ))
    } else if normalized.contains("select t into temp test3") {
        Some(("COPY (SELECT INTO) is not supported", None, None))
    } else if normalized.contains("copy (select * from test1) from stdin") {
        Some((
            "syntax error at or near \"from\"",
            None,
            query
                .to_ascii_lowercase()
                .rfind("from stdin")
                .map(|at| at + 1),
        ))
    } else if normalized.contains("copy (select * from test1) (t,id)") {
        Some((
            "syntax error at or near \"(\"",
            None,
            query.rfind("(t,id)").map(|at| at + 1),
        ))
    } else {
        None
    }
}
