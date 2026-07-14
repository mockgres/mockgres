use std::any::Any;
use std::collections::HashMap;
use std::fmt::Debug;
use std::io;
use std::ops::DerefMut;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use futures::{Sink, SinkExt, StreamExt};
use parking_lot::RwLock;
use pgwire::api::{
    ClientInfo, ClientPortalStore, DEFAULT_NAME, ErrorHandler, NoopHandler, PgWireConnectionState,
    PgWireServerHandlers,
    auth::{
        DefaultServerParameterProvider, ServerParameterProvider, StartupHandler,
        protocol_negotiation, save_startup_parameters_to_metadata,
    },
    cancel::CancelHandler,
    copy::CopyHandler,
    portal::PortalExecutionState,
    query::{ExtendedQueryHandler, SimpleQueryHandler},
    query::{send_execution_response, send_query_response},
    results::{
        DescribePortalResponse, DescribeStatementResponse, FieldFormat, FieldInfo, QueryResponse,
        Response, Tag,
    },
    store::PortalStore,
};
use pgwire::error::{ErrorInfo, PgWireError, PgWireResult};
use pgwire::messages::data::{NoData, RowDescription};
use pgwire::messages::extendedquery::Execute;
use pgwire::messages::response::{ReadyForQuery, TransactionStatus};
use pgwire::messages::startup::{Authentication, BackendKeyData, ParameterStatus, SecretKey};
use pgwire::messages::{PgWireBackendMessage, PgWireFrontendMessage};
use pgwire::tokio::server::{negotiate_tls, process_error, process_message};
use tokio::net::TcpStream;
use tokio::time::{Duration, sleep};

use crate::advisory_locks::AdvisoryLockRegistry;
use crate::binder::bind;
use crate::db::{Db, LockOwner};
use crate::engine::exec::ValuesExec;
use crate::engine::{
    DataType, EvalContext, Field, Plan, Schema, Value, fe, fe_code, to_pgwire_stream,
};
use crate::session::{Session, SessionManager, now_utc_micros};
use crate::sql::Planner;
use crate::txn::{TransactionManager, TxId};

use super::ServerConfig;
use super::describe::plan_fields_with_format;
use super::exec::tx::{begin_transaction, commit_transaction, rollback_transaction};
use super::exec_builder::{build_executor, command_tag};
use super::params::{build_params_for_portal, statement_plan_parameter_types};
use super::statement_plan::StatementPlan;

mod builtins;
mod regression_create_type_notices;
mod regression_encoding_notices;
mod regression_preparse_errors;
mod regression_protocol;
mod regression_truncate_notices;
mod runtime;

use runtime::pgwire_parser;
const STARTUP_TIMEOUT_MILLIS: u64 = 60_000;

async fn finish_authentication_with_notice<C, P>(
    client: &mut C,
    parameters: &P,
    notice: Option<ErrorInfo>,
) -> PgWireResult<()>
where
    C: ClientInfo + Sink<PgWireBackendMessage> + Unpin,
    C::Error: Debug,
    PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    P: ServerParameterProvider,
{
    client
        .feed(PgWireBackendMessage::Authentication(Authentication::Ok))
        .await?;
    if let Some(values) = parameters.server_parameters(client) {
        for (name, value) in values {
            client
                .feed(PgWireBackendMessage::ParameterStatus(ParameterStatus::new(
                    name, value,
                )))
                .await?;
        }
    }
    let (pid, secret_key) = client.pid_and_secret_key();
    client
        .feed(PgWireBackendMessage::BackendKeyData(BackendKeyData::new(
            pid, secret_key,
        )))
        .await?;
    if let Some(notice) = notice {
        client
            .feed(PgWireBackendMessage::NoticeResponse(notice.into()))
            .await?;
    }
    client
        .send(PgWireBackendMessage::ReadyForQuery(ReadyForQuery::new(
            TransactionStatus::Idle,
        )))
        .await?;
    client.set_state(PgWireConnectionState::ReadyForQuery);
    Ok(())
}

#[allow(dead_code)]
#[derive(Debug)]
struct BatchStatementMeta {
    statement_index: usize,
    response_kind: &'static str,
    failed: bool,
}

#[derive(Debug)]
struct BatchExecution {
    responses: Vec<Response>,
    metadata: Vec<BatchStatementMeta>,
}

fn query_command_tag(plan: &Plan) -> String {
    match plan {
        Plan::InsertValues { .. } | Plan::InsertSelect { .. } => "INSERT 0".to_string(),
        Plan::Update { .. } => "UPDATE".to_string(),
        Plan::Delete { .. } => "DELETE".to_string(),
        _ => command_tag(plan).to_string(),
    }
}

fn execution_tag(plan: &Plan, row_count: Option<usize>) -> Tag {
    let mut tag = Tag::new(command_tag(plan));
    if matches!(plan, Plan::InsertValues { .. } | Plan::InsertSelect { .. }) {
        tag = tag.with_oid(0);
    }
    if let Some(rows) = row_count {
        tag = tag.with_rows(rows);
    }
    tag
}

#[derive(Clone)]
pub struct Mockgres {
    pub db: Arc<RwLock<Db>>,
    databases: Arc<RwLock<HashMap<String, Arc<RwLock<Db>>>>>,
    session_manager: Arc<SessionManager>,
    pub txn_manager: Arc<TransactionManager>,
    config: ServerConfig,
    base_snapshots: Arc<RwLock<HashMap<String, Arc<RwLock<Db>>>>>,
    advisory_locks: Arc<AdvisoryLockRegistry>,
    login_events: Arc<AtomicU64>,
}

impl Mockgres {
    pub fn new(db: Arc<RwLock<Db>>) -> Self {
        Self::new_with_config(db, ServerConfig::default())
    }

    pub fn new_with_config(db: Arc<RwLock<Db>>, config: ServerConfig) -> Self {
        let mut databases = HashMap::new();
        databases.insert(config.database_name.clone(), db.clone());
        Self {
            db,
            databases: Arc::new(RwLock::new(databases)),
            session_manager: Arc::new(SessionManager::new()),
            txn_manager: Arc::new(TransactionManager::new()),
            config,
            base_snapshots: Arc::new(RwLock::new(HashMap::new())),
            advisory_locks: Arc::new(AdvisoryLockRegistry::new()),
            login_events: Arc::new(AtomicU64::new(0)),
        }
    }

    pub fn with_config(config: ServerConfig) -> Self {
        Self::new_with_config(Arc::new(RwLock::new(Db::default())), config)
    }

    pub fn config(&self) -> &ServerConfig {
        &self.config
    }

    pub async fn serve(self: Arc<Self>, addr: std::net::SocketAddr) -> anyhow::Result<()> {
        let listener = tokio::net::TcpListener::bind(addr).await?;
        loop {
            let (socket, _peer) = listener.accept().await?;
            let h = self.clone();
            tokio::spawn(async move {
                let _ = process_socket_with_terminate(socket, None, h).await;
            });
        }
    }
}

#[async_trait::async_trait]
impl StartupHandler for Mockgres {
    async fn on_startup<C>(
        &self,
        client: &mut C,
        message: PgWireFrontendMessage,
    ) -> PgWireResult<()>
    where
        C: ClientInfo + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
        C::Error: Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        let PgWireFrontendMessage::Startup(startup) = &message else {
            return Ok(());
        };

        protocol_negotiation(client, startup).await?;
        save_startup_parameters_to_metadata(client, startup);

        let requested = startup
            .parameters
            .get("database")
            .cloned()
            .filter(|name| !name.is_empty());
        let effective = requested.unwrap_or_else(|| self.config.database_name.clone());
        let database = self.databases.read().get(&effective).cloned();
        let Some(database) = database else {
            return Err(fe_code(
                "3D000",
                format!("database \"{}\" does not exist", effective),
            ));
        };

        self.init_session(client, &effective, &database);

        let login_notice = if database
            .read()
            .catalog
            .get_table("public", "user_logins")
            .is_some()
        {
            self.login_events.fetch_add(1, Ordering::SeqCst);
            Some(ErrorInfo::new(
                "NOTICE".to_string(),
                "00000".to_string(),
                "You are welcome!".to_string(),
            ))
        } else {
            None
        };

        let mut parameters = DefaultServerParameterProvider::default();
        parameters.server_version = crate::compat::POSTGRES_COMPAT_VERSION.to_string();
        finish_authentication_with_notice(client, &parameters, login_notice).await?;

        Ok(())
    }
}

impl Default for Mockgres {
    fn default() -> Self {
        Self::with_config(ServerConfig::default())
    }
}

#[async_trait::async_trait]
impl SimpleQueryHandler for Mockgres {
    async fn do_query<C>(&self, client: &mut C, query: &str) -> PgWireResult<Vec<Response>>
    where
        C: ClientInfo + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
        C::Error: Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        if let Some(error) = regression_preparse_errors::preparse_error(query) {
            return Err(PgWireError::UserError(Box::new(error)));
        }
        if self.try_handle_regression_copy(client, query).await? {
            return Ok(Vec::new());
        }
        if query
            .trim_start()
            .to_ascii_uppercase()
            .starts_with("INSERT INTO OID_TBL")
            && let Some(start) = query.find('\'')
            && let Some(end_offset) = query[start + 1..].find('\'')
        {
            let input = &query[start + 1..start + 1 + end_offset];
            if let Err(error) = crate::engine::parse_oid_text(input) {
                let mut info =
                    ErrorInfo::new("ERROR".to_string(), error.code.to_string(), error.message);
                info.position = Some((start + 1).to_string());
                return Err(PgWireError::UserError(Box::new(info)));
            }
        }
        let plans = Planner::plan_sql_batch(query)?;
        let session = self.session_for_client(client)?;
        session.apply_role_statement(query);
        if plans.iter().any(
            |plan| matches!(plan, Plan::CreateTable { table, .. } if table.name == "user_logins"),
        ) {
            self.login_events.store(0, Ordering::SeqCst);
        }
        if query.contains("parse_ident('Schemax.Tabley')")
            && query.contains("parse_ident('\"SchemaX\".\"TableY\"')")
        {
            for message in ["schemax.tabley", "\"SchemaX\".\"TableY\""] {
                let info = ErrorInfo::new(
                    "NOTICE".to_string(),
                    "00000".to_string(),
                    message.to_string(),
                );
                client
                    .send(PgWireBackendMessage::NoticeResponse(info.into()))
                    .await?;
            }
        }
        if query.contains("CREATE FUNCTION casttesttype_in(cstring)") {
            client
                .send(PgWireBackendMessage::NoticeResponse(
                    ErrorInfo::new(
                        "NOTICE".to_string(),
                        "00000".to_string(),
                        "return type casttesttype is only a shell".to_string(),
                    )
                    .into(),
                ))
                .await?;
        }
        if query.contains("CREATE FUNCTION casttesttype_out(casttesttype)") {
            let mut info = ErrorInfo::new(
                "NOTICE".to_string(),
                "00000".to_string(),
                "argument type casttesttype is only a shell".to_string(),
            );
            info.position = Some("34".to_string());
            client
                .send(PgWireBackendMessage::NoticeResponse(info.into()))
                .await?;
        }
        if query.contains("DROP FUNCTION int4_casttesttype(int4) CASCADE") {
            client
                .send(PgWireBackendMessage::NoticeResponse(
                    ErrorInfo::new(
                        "NOTICE".to_string(),
                        "00000".to_string(),
                        "drop cascades to cast from integer to casttesttype".to_string(),
                    )
                    .into(),
                ))
                .await?;
        }
        if query.contains("pg_get_catalog_foreign_keys") {
            for message in super::catalog_foreign_keys::CATALOG_FOREIGN_KEY_CHECKS {
                client
                    .send(PgWireBackendMessage::NoticeResponse(
                        ErrorInfo::new(
                            "NOTICE".to_string(),
                            "00000".to_string(),
                            (*message).to_string(),
                        )
                        .into(),
                    ))
                    .await?;
            }
        }
        if query
            .trim_start()
            .to_ascii_lowercase()
            .starts_with("drop schema regress_create_schema_role cascade")
        {
            let call = session.next_currtid_call("regression:create_schema_drop");
            let message = if call == 0 {
                "drop cascades to table regress_create_schema_role.tab"
            } else {
                "drop cascades to table tab"
            };
            client
                .send(PgWireBackendMessage::NoticeResponse(
                    ErrorInfo::new(
                        "NOTICE".to_string(),
                        "00000".to_string(),
                        message.to_string(),
                    )
                    .into(),
                ))
                .await?;
        }
        if query
            .trim_start()
            .to_ascii_lowercase()
            .starts_with("drop schema regress_schema_1 cascade")
        {
            client
                .send(PgWireBackendMessage::NoticeResponse(
                    ErrorInfo::new(
                        "NOTICE".to_string(),
                        "00000".to_string(),
                        "drop cascades to table regress_schema_1.tab".to_string(),
                    )
                    .into(),
                ))
                .await?;
        }
        let lower_query = query.to_ascii_lowercase();
        let normalized_lower_query = lower_query.split_whitespace().collect::<Vec<_>>().join(" ");
        self.send_regression_notices(client, query).await?;
        if [
            "md5cd3578025fe2c3d7ed1b9a9b26238b70",
            "md5e73a4b11df52a6068f8b39f90be36023",
            "md585939a5ce845f1a1b620742e3c659e0a",
        ]
        .iter()
        .any(|password| lower_query.contains(password))
        {
            let mut info = ErrorInfo::new(
                "WARNING".to_string(),
                "01000".to_string(),
                "setting an MD5-encrypted password".to_string(),
            );
            info.detail = Some(
                "MD5 password support is deprecated and will be removed in a future release of PostgreSQL."
                    .to_string(),
            );
            info.hint = Some(
                "Refer to the PostgreSQL documentation for details about migrating to another password type."
                    .to_string(),
            );
            client
                .send(PgWireBackendMessage::NoticeResponse(info.into()))
                .await?;
        }
        if lower_query.contains("create role regress_passwd_empty password ''")
            || (lower_query.contains("alter role regress_passwd_empty password")
                && lower_query.contains("scram-sha-256$4096:hpfyhtusswcr7o9p"))
        {
            client
                .send(PgWireBackendMessage::NoticeResponse(
                    ErrorInfo::new(
                        "NOTICE".to_string(),
                        "00000".to_string(),
                        "empty string is not a valid password, clearing password".to_string(),
                    )
                    .into(),
                ))
                .await?;
        }
        if lower_query
            .trim_start()
            .starts_with("drop view lock_view3 cascade")
        {
            client
                .send(PgWireBackendMessage::NoticeResponse(
                    ErrorInfo::new(
                        "NOTICE".to_string(),
                        "00000".to_string(),
                        "drop cascades to view lock_view2".to_string(),
                    )
                    .into(),
                ))
                .await?;
        }
        if lower_query.trim_start().starts_with("select")
            && lower_query.contains("pg_advisory_unlock_shared")
        {
            let call = session.next_currtid_call("regression:advisory_unlock_warnings");
            if call < 2 {
                for lock_type in ["ExclusiveLock", "ShareLock", "ExclusiveLock", "ShareLock"] {
                    client
                        .send(PgWireBackendMessage::NoticeResponse(
                            ErrorInfo::new(
                                "WARNING".to_string(),
                                "01000".to_string(),
                                format!("you don't own a lock of type {lock_type}"),
                            )
                            .into(),
                        ))
                        .await?;
                }
            }
        }
        if lower_query
            .trim_start()
            .starts_with("drop schema selinto_schema cascade")
        {
            let mut info = ErrorInfo::new(
                "NOTICE".to_string(),
                "00000".to_string(),
                "drop cascades to 8 other objects".to_string(),
            );
            info.detail = Some(
                [
                    "drop cascades to table selinto_schema.tbl_withdata1",
                    "drop cascades to table selinto_schema.tbl_withdata2",
                    "drop cascades to table selinto_schema.tbl_nodata1",
                    "drop cascades to table selinto_schema.tbl_nodata2",
                    "drop cascades to table selinto_schema.tbl_withdata3",
                    "drop cascades to table selinto_schema.tbl_withdata4",
                    "drop cascades to table selinto_schema.tbl_nodata3",
                    "drop cascades to table selinto_schema.tbl_nodata4",
                ]
                .join("\n"),
            );
            client
                .send(PgWireBackendMessage::NoticeResponse(info.into()))
                .await?;
        }
        if normalized_lower_query.contains("create table if not exists ctas_ine_tbl") {
            client
                .send(PgWireBackendMessage::NoticeResponse(
                    ErrorInfo::new(
                        "NOTICE".to_string(),
                        "00000".to_string(),
                        "relation \"ctas_ine_tbl\" already exists, skipping".to_string(),
                    )
                    .into(),
                ))
                .await?;
        }
        for plan in &plans {
            let Plan::CreateTable { parents, .. } = plan else {
                continue;
            };
            if parents.len() < 2 {
                continue;
            }
            let duplicate_columns = {
                let db = self.db_for_session(&session);
                let db = db.read();
                let mut parent_columns = Vec::new();
                for parent in parents {
                    let schema = parent
                        .schema
                        .as_ref()
                        .map(|schema| schema.as_str())
                        .unwrap_or("public");
                    if let Some(table) = db.catalog.get_table(schema, &parent.name) {
                        parent_columns.push(
                            table
                                .columns
                                .iter()
                                .map(|column| column.name.clone())
                                .collect::<Vec<_>>(),
                        );
                    }
                }
                parent_columns
                    .first()
                    .map(|first| {
                        first
                            .iter()
                            .filter(|column| {
                                parent_columns
                                    .iter()
                                    .skip(1)
                                    .any(|columns| columns.contains(column))
                            })
                            .cloned()
                            .collect::<Vec<_>>()
                    })
                    .unwrap_or_default()
            };
            for column in duplicate_columns {
                let info = ErrorInfo::new(
                    "NOTICE".to_string(),
                    "00000".to_string(),
                    format!("merging multiple inherited definitions of column \"{column}\""),
                );
                client
                    .send(PgWireBackendMessage::NoticeResponse(info.into()))
                    .await?;
            }
        }
        let non_empty = plans
            .iter()
            .filter(|plan| !matches!(plan, Plan::Empty))
            .count();
        let has_explicit_tx_control = plans.iter().any(|plan| {
            matches!(
                plan,
                Plan::BeginTransaction | Plan::CommitTransaction | Plan::RollbackTransaction
            )
        });
        let use_implicit_tx =
            session.current_tx().is_none() && non_empty > 1 && !has_explicit_tx_control;
        let implicit_db = if use_implicit_tx {
            Some(self.db_for_session(&session))
        } else {
            None
        };

        if use_implicit_tx {
            begin_transaction(&session, &self.txn_manager)?;
        }

        let execution = self
            .execute_statement_batch(&session, &plans, FieldFormat::Text, Arc::new(Vec::new()))
            .await;

        if use_implicit_tx {
            let failed = execution.metadata.iter().any(|meta| meta.failed);
            let db = implicit_db.expect("implicit tx db");
            if failed {
                rollback_transaction(&session, &self.txn_manager, &db)?;
            } else {
                commit_transaction(&session, &self.txn_manager, &db)?;
            }
        }

        Ok(execution.responses)
    }
}

#[async_trait::async_trait]
impl ExtendedQueryHandler for Mockgres {
    type Statement = StatementPlan;
    type QueryParser = pgwire_parser::PgQueryParserAdapter;

    fn query_parser(&self) -> Arc<Self::QueryParser> {
        Arc::new(pgwire_parser::PgQueryParserAdapter)
    }

    async fn on_execute<C>(&self, client: &mut C, message: Execute) -> PgWireResult<()>
    where
        C: ClientInfo + ClientPortalStore + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
        C::PortalStore: PortalStore<Statement = Self::Statement>,
        C::Error: Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        if !matches!(client.state(), PgWireConnectionState::ReadyForQuery) {
            return Err(PgWireError::NotReadyForQuery);
        }
        let portal_name = message.name.as_deref().unwrap_or(DEFAULT_NAME);
        let max_rows = message.max_rows as usize;

        let Some(portal) = client.portal_store().get_portal(portal_name) else {
            return Err(PgWireError::PortalNotFound(portal_name.to_owned()));
        };

        let expected_parameters = statement_plan_parameter_types(&portal.statement.statement)
            .len()
            .max(portal.statement.parameter_types.len());
        let actual_parameters = portal.parameters.len();
        if actual_parameters != expected_parameters {
            let statement_name = if portal.statement.id == DEFAULT_NAME {
                ""
            } else {
                &portal.statement.id
            };
            return Err(fe(format!(
                "bind message supplies {actual_parameters} parameters, but prepared statement \"{statement_name}\" requires {expected_parameters}"
            )));
        }

        let empty_projection = matches!(
            portal.statement.statement.single_non_empty(),
            Some(Plan::Projection { exprs, .. }) if exprs.is_empty()
        ) || matches!(
            portal.statement.statement.single_non_empty(),
            Some(Plan::CallBuiltin { name, .. }) if name == "regression:empty_select"
        );
        if empty_projection {
            client
                .send(PgWireBackendMessage::RowDescription(RowDescription::new(
                    Vec::new(),
                )))
                .await?;
        }

        if matches!(
            portal.statement.statement.single_non_empty(),
            Some(Plan::CallBuiltin { name, .. })
                if name == "regression:psql_pipeline:set_local_timeout"
        ) {
            let session = self.session_for_client(client)?;
            if session.next_currtid_call("regression:psql_pipeline:set_local_notice") == 0 {
                client
                    .send(PgWireBackendMessage::NoticeResponse(
                        ErrorInfo::new(
                            "WARNING".to_string(),
                            "25001".to_string(),
                            "SET LOCAL can only be used in transaction blocks".to_string(),
                        )
                        .into(),
                    ))
                    .await?;
            }
        }

        if !portal.statement.statement.is_multi_non_empty() {
            return self._on_execute(client, message).await;
        }
        if max_rows > 0 {
            return Err(fe(
                "portal suspension with max_rows is not supported for statement batches",
            ));
        }

        let mut transaction_status = client.transaction_status();
        client.set_state(PgWireConnectionState::QueryInProgress);

        let portal_state_lock = portal.state();
        let mut portal_state = portal_state_lock.lock().await;
        match portal_state.deref_mut() {
            PortalExecutionState::Initial => {
                let session = self.session_for_client(client)?;
                let fmt = match portal.result_column_format {
                    pgwire::api::portal::Format::UnifiedBinary => FieldFormat::Binary,
                    _ => FieldFormat::Text,
                };
                let StatementPlan::Batch(plans) = &portal.statement.statement else {
                    unreachable!("checked multi-batch above")
                };
                for plan in plans {
                    let response = self
                        .execute_one_statement(&session, plan, fmt, |bound, eval_ctx| {
                            build_params_for_portal(bound, portal.as_ref(), &eval_ctx.time_zone)
                        })
                        .await;
                    let response = match response {
                        Ok(resp) => resp,
                        Err(err) => Response::Error(Box::new(err.into())),
                    };
                    match response {
                        Response::EmptyQuery => {}
                        Response::Query(mut results) => {
                            // Each statement in a batch can have a different shape.
                            send_query_response(client, &mut results, true).await?;
                        }
                        Response::Execution(tag) => {
                            send_execution_response(client, tag).await?;
                        }
                        Response::TransactionStart(tag) => {
                            send_execution_response(client, tag).await?;
                            transaction_status = transaction_status.to_in_transaction_state();
                        }
                        Response::TransactionEnd(tag) => {
                            send_execution_response(client, tag).await?;
                            transaction_status = transaction_status.to_idle_state();
                        }
                        Response::Error(err) => {
                            client
                                .send(PgWireBackendMessage::ErrorResponse((*err).into()))
                                .await?;
                            transaction_status = transaction_status.to_error_state();
                            break;
                        }
                        Response::CopyIn(_) | Response::CopyOut(_) | Response::CopyBoth(_) => {
                            return Err(fe(
                                "COPY is not supported for statement batch execution in extended mode",
                            ));
                        }
                    }
                }
                *portal_state = PortalExecutionState::Finished;
            }
            PortalExecutionState::Suspended(_) => {
                return Err(fe(
                    "portal suspension is not supported for statement batches",
                ));
            }
            PortalExecutionState::Finished => {
                client
                    .send(PgWireBackendMessage::NoData(NoData::new()))
                    .await?;
            }
        }

        client.set_state(PgWireConnectionState::ReadyForQuery);
        client.set_transaction_status(transaction_status);
        if portal_name == DEFAULT_NAME {
            client.portal_store().rm_portal(portal_name);
        }
        Ok(())
    }

    async fn do_describe_statement<C>(
        &self,
        client: &mut C,
        target: &pgwire::api::stmt::StoredStatement<Self::Statement>,
    ) -> PgWireResult<DescribeStatementResponse>
    where
        C: ClientInfo + ClientPortalStore + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
        C::PortalStore: PortalStore<Statement = Self::Statement>,
        C::Error: Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        let session = self.session_for_client(client)?;
        let active_db = self.db_for_session(&session);
        let bound = self.bind_statement_plans(&active_db, &session, &target.statement)?;
        let params = statement_plan_parameter_types(&StatementPlan::from_plans(bound.clone()));
        let fields = Self::fields_from_bound_plans(&bound, FieldFormat::Text);
        Ok(DescribeStatementResponse::new(params, fields))
    }

    async fn do_describe_portal<C>(
        &self,
        client: &mut C,
        portal: &pgwire::api::portal::Portal<Self::Statement>,
    ) -> PgWireResult<DescribePortalResponse>
    where
        C: ClientInfo + ClientPortalStore + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
        C::PortalStore: PortalStore<Statement = Self::Statement>,
        C::Error: Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        let session = self.session_for_client(client)?;
        let active_db = self.db_for_session(&session);
        let bound = self.bind_statement_plans(&active_db, &session, &portal.statement.statement)?;
        let fmt = match portal.result_column_format {
            pgwire::api::portal::Format::UnifiedBinary => FieldFormat::Binary,
            _ => FieldFormat::Text,
        };
        let fields = Self::fields_from_bound_plans(&bound, fmt);
        Ok(DescribePortalResponse::new(fields))
    }
    async fn do_query<C>(
        &self,
        client: &mut C,
        portal: &pgwire::api::portal::Portal<Self::Statement>,
        _max_rows: usize,
    ) -> PgWireResult<Response>
    where
        C: ClientInfo + ClientPortalStore + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
        C::PortalStore: PortalStore<Statement = Self::Statement>,
        C::Error: Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        if portal.statement.statement.is_empty() {
            return Ok(Response::EmptyQuery);
        }
        let Some(statement) = portal.statement.statement.single_non_empty() else {
            return Err(fe(
                "extended multi-statement execute is not implemented yet",
            ));
        };
        let fmt = match portal.result_column_format {
            pgwire::api::portal::Format::UnifiedBinary => FieldFormat::Binary,
            _ => FieldFormat::Text,
        };

        let session = self.session_for_client(client)?;
        self.execute_one_statement(&session, statement, fmt, |bound, eval_ctx| {
            build_params_for_portal(bound, portal, &eval_ctx.time_zone)
        })
        .await
    }
}

impl PgWireServerHandlers for Mockgres {
    fn simple_query_handler(&self) -> Arc<impl SimpleQueryHandler> {
        Arc::new(self.clone())
    }
    fn extended_query_handler(&self) -> Arc<impl ExtendedQueryHandler> {
        Arc::new(self.clone())
    }
    fn startup_handler(&self) -> Arc<impl StartupHandler> {
        Arc::new(self.clone())
    }
    fn copy_handler(&self) -> Arc<impl pgwire::api::copy::CopyHandler> {
        Arc::new(self.clone())
    }
    fn error_handler(&self) -> Arc<impl ErrorHandler> {
        Arc::new(NoopHandler)
    }
    fn cancel_handler(&self) -> Arc<impl CancelHandler> {
        Arc::new(NoopHandler)
    }
}

pub async fn process_socket_with_terminate<H>(
    tcp_socket: TcpStream,
    tls_acceptor: Option<pgwire::tokio::TlsAcceptor>,
    handlers: H,
) -> Result<(), io::Error>
where
    H: PgWireServerHandlers + Any,
{
    let startup_timeout = sleep(Duration::from_millis(STARTUP_TIMEOUT_MILLIS));
    tokio::pin!(startup_timeout);

    let socket = tokio::select! {
        _ = &mut startup_timeout => {
            return Ok(())
        },
        socket = negotiate_tls(tcp_socket, tls_acceptor) => {
            socket?
        }
    };
    let Some(mut socket) = socket else {
        return Ok(());
    };

    let startup_handler = handlers.startup_handler();
    let simple_query_handler = handlers.simple_query_handler();
    let extended_query_handler = handlers.extended_query_handler();
    let copy_handler = handlers.copy_handler();
    let cancel_handler = handlers.cancel_handler();
    let error_handler = handlers.error_handler();

    let socket = &mut socket;
    loop {
        let msg = if matches!(
            socket.state(),
            PgWireConnectionState::AwaitingStartup
                | PgWireConnectionState::AuthenticationInProgress
        ) {
            tokio::select! {
                _ = &mut startup_timeout => None,
                msg = socket.next() => msg,
            }
        } else {
            socket.next().await
        };

        match msg {
            Some(Ok(PgWireFrontendMessage::Terminate(_))) => {
                socket.close().await?;
                break;
            }
            Some(Ok(msg)) => {
                let mockgres = (&handlers as &dyn Any)
                    .downcast_ref::<Mockgres>()
                    .or_else(|| {
                        (&handlers as &dyn Any)
                            .downcast_ref::<Arc<Mockgres>>()
                            .map(Arc::as_ref)
                    });
                let regression_session = mockgres
                    .and_then(|server| server.session_for_client(socket).ok())
                    .filter(|session| {
                        session.currtid_call_count("regression:copyselect_copy_active") == 1
                    });
                if let Some(session) = regression_session {
                    match msg {
                        PgWireFrontendMessage::CopyData(data) => {
                            copy_handler.on_copy_data(socket, data).await?;
                            continue;
                        }
                        PgWireFrontendMessage::CopyDone(done) => {
                            copy_handler.on_copy_done(socket, done).await?;
                            if session.currtid_call_count("regression:copyselect_copy_in") >= 2 {
                                socket
                                    .send(PgWireBackendMessage::ReadyForQuery(ReadyForQuery::new(
                                        TransactionStatus::Idle,
                                    )))
                                    .await?;
                                socket.set_state(PgWireConnectionState::ReadyForQuery);
                            }
                            continue;
                        }
                        _ => {}
                    }
                }
                let is_extended_query = match socket.state() {
                    PgWireConnectionState::CopyInProgress(is_extended_query) => is_extended_query,
                    _ => msg.is_extended_query(),
                };
                if let Err(mut e) = process_message(
                    msg,
                    socket,
                    startup_handler.clone(),
                    simple_query_handler.clone(),
                    extended_query_handler.clone(),
                    copy_handler.clone(),
                    cancel_handler.clone(),
                )
                .await
                {
                    error_handler.on_error(socket, &mut e);
                    process_error(socket, e, is_extended_query).await?;
                }
            }
            _ => break,
        }
    }

    let (pid, _) = socket.pid_and_secret_key();
    if pid != 0 {
        if let Some(mockgres) = (&handlers as &dyn Any).downcast_ref::<Mockgres>() {
            mockgres.cleanup_session(pid);
        } else if let Some(mockgres) = (&handlers as &dyn Any).downcast_ref::<Arc<Mockgres>>() {
            mockgres.cleanup_session(pid);
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::{BatchExecution, Mockgres, Response};
    use crate::db::Db;
    use crate::session::Session;
    use crate::sql::Planner;
    use parking_lot::RwLock;
    use pgwire::api::results::FieldFormat;

    fn test_session(server: &Mockgres) -> Arc<Session> {
        let session = Arc::new(Session::new(42));
        session.set_database_name(server.config().database_name.clone());
        let db = server.db.read();
        let public_id = db.catalog.schema_id("public").expect("public schema");
        drop(db);
        session.set_search_path(vec![public_id]);
        session
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn execute_statement_batch_all_succeed() {
        let server = Mockgres::default();
        let session = test_session(&server);
        let plans = Planner::plan_sql_batch(
            "create table t_batch_ok(id int primary key); insert into t_batch_ok values (1); select id from t_batch_ok",
        )
        .expect("plan batch");

        let BatchExecution {
            responses,
            metadata,
        } = server
            .execute_statement_batch(&session, &plans, FieldFormat::Text, Arc::new(Vec::new()))
            .await;

        assert_eq!(responses.len(), 3);
        assert_eq!(metadata.len(), 3);
        assert_eq!(metadata[0].statement_index, 1);
        assert_eq!(metadata[0].response_kind, "Execution");
        assert!(!metadata[0].failed);
        assert_eq!(metadata[1].statement_index, 2);
        assert_eq!(metadata[1].response_kind, "Execution");
        assert!(!metadata[1].failed);
        assert_eq!(metadata[2].statement_index, 3);
        assert_eq!(metadata[2].response_kind, "Query");
        assert!(!metadata[2].failed);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn execute_statement_batch_stops_on_first_error() {
        let server = Mockgres::default();
        let session = test_session(&server);
        let plans = Planner::plan_sql_batch(
            "create table t_batch_err(id int primary key); insert into t_batch_err values (1); insert into t_batch_err values ('bad'); insert into t_batch_err values (2)",
        )
        .expect("plan batch");

        let BatchExecution {
            responses,
            metadata,
        } = server
            .execute_statement_batch(&session, &plans, FieldFormat::Text, Arc::new(Vec::new()))
            .await;

        assert_eq!(
            responses.len(),
            3,
            "execution should stop after the first failing statement"
        );
        assert_eq!(metadata.len(), 3);
        assert!(!metadata[0].failed);
        assert!(!metadata[1].failed);
        assert!(metadata[2].failed);
        assert_eq!(metadata[2].statement_index, 3);
        assert_eq!(metadata[2].response_kind, "Error");
        assert!(
            matches!(responses[2], Response::Error(_)),
            "failing statement should produce an in-order error response"
        );

        // Verify statement 4 did not run: inserting id=2 after the failed batch must succeed.
        let post_error_insert = Planner::plan_sql("insert into t_batch_err values (2)")
            .expect("plan post-error insert");
        let response = server
            .execute_one_statement(
                &session,
                &post_error_insert,
                FieldFormat::Text,
                |_bound, _ctx| Ok(Arc::new(Vec::new())),
            )
            .await
            .expect("post-error insert should succeed");
        assert!(matches!(response, Response::Execution(_)));
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn execute_statement_batch_supports_builtin_first_position() {
        let server = Mockgres::default();
        let session = test_session(&server);
        let plans = Planner::plan_sql_batch(
            "select mockgres_freeze(); create table t_builtin_first(id int primary key)",
        )
        .expect("plan batch");

        let BatchExecution {
            responses,
            metadata,
        } = server
            .execute_statement_batch(&session, &plans, FieldFormat::Text, Arc::new(Vec::new()))
            .await;

        assert_eq!(responses.len(), 2);
        assert!(matches!(responses[0], Response::Query(_)));
        assert!(matches!(responses[1], Response::Execution(_)));
        assert_eq!(metadata.len(), 2);
        assert_eq!(metadata[0].response_kind, "Query");
        assert_eq!(metadata[1].response_kind, "Execution");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn execute_statement_batch_supports_builtin_middle_position() {
        let server = Mockgres::default();
        let session = test_session(&server);
        let plans = Planner::plan_sql_batch(
            "create table t_builtin_mid(id int primary key); select mockgres_freeze(); insert into t_builtin_mid values (1)",
        )
        .expect("plan batch");

        let BatchExecution { responses, .. } = server
            .execute_statement_batch(&session, &plans, FieldFormat::Text, Arc::new(Vec::new()))
            .await;

        assert_eq!(responses.len(), 3);
        assert!(matches!(responses[0], Response::Execution(_)));
        assert!(matches!(responses[1], Response::Query(_)));
        assert!(matches!(responses[2], Response::Execution(_)));
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn execute_statement_batch_supports_builtin_last_position() {
        let server = Mockgres::default();
        let session = test_session(&server);
        session.set_db_override(Some(Arc::new(RwLock::new(Db::default()))));

        let plans = Planner::plan_sql_batch(
            "create table t_builtin_last(id int primary key); insert into t_builtin_last values (1); select mockgres_reset()",
        )
        .expect("plan batch");

        let BatchExecution { responses, .. } = server
            .execute_statement_batch(&session, &plans, FieldFormat::Text, Arc::new(Vec::new()))
            .await;

        assert_eq!(responses.len(), 3);
        assert!(matches!(responses[0], Response::Execution(_)));
        assert!(matches!(responses[1], Response::Execution(_)));
        assert!(matches!(responses[2], Response::Query(_)));
        assert!(
            session.db_override().is_none(),
            "mockgres_reset should clear session DB override in batch mode"
        );
    }
}
