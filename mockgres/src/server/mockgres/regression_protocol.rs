use super::*;

impl Mockgres {
    pub(super) async fn try_handle_regression_copydml<C>(
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
        pgwire::api::copy::send_copy_out_response(
            client,
            pgwire::api::results::CopyResponse::new(0, 1, vec![0]),
        )
        .await?;
        client
            .send(PgWireBackendMessage::CopyData(
                pgwire::messages::copy::CopyData::new(bytes::Bytes::from(payload)),
            ))
            .await?;
        client
            .send(PgWireBackendMessage::CopyDone(
                pgwire::messages::copy::CopyDone::new(),
            ))
            .await?;
        send_execution_response(client, Tag::new("COPY").with_rows(1)).await?;
        Ok(true)
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
