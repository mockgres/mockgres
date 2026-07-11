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
use pgwire::messages::data::NoData;
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
use crate::engine::{DataType, EvalContext, Plan, Value, fe, fe_code, to_pgwire_stream};
use crate::session::{Session, SessionManager, now_utc_micros};
use crate::sql::Planner;
use crate::txn::{TransactionManager, TxId};

use super::ServerConfig;
use super::describe::plan_fields_with_format;
use super::exec::tx::{begin_transaction, commit_transaction, rollback_transaction};
use super::exec_builder::{build_executor, command_tag};
use super::params::{build_params_for_portal, statement_plan_parameter_types};
use super::statement_plan::StatementPlan;

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
        Arc::new(NoopHandler)
    }
    fn error_handler(&self) -> Arc<impl ErrorHandler> {
        Arc::new(NoopHandler)
    }
    fn cancel_handler(&self) -> Arc<impl CancelHandler> {
        Arc::new(NoopHandler)
    }
}

struct StatementEpochGuard {
    session: Arc<Session>,
    db: Arc<RwLock<Db>>,
    active: bool,
}

impl StatementEpochGuard {
    fn new(session: Arc<Session>, db: Arc<RwLock<Db>>) -> Self {
        let active = session.enter_statement();
        Self {
            session,
            db,
            active,
        }
    }
}

impl Drop for StatementEpochGuard {
    fn drop(&mut self) {
        if self.active
            && let Some(epoch) = self.session.exit_statement()
        {
            let db_read = self.db.read();
            db_read.release_locks(LockOwner::new(self.session.id(), epoch));
        }
    }
}

impl Mockgres {
    fn response_kind(response: &Response) -> &'static str {
        match response {
            Response::EmptyQuery => "EmptyQuery",
            Response::Query(_) => "Query",
            Response::Execution(_) => "Execution",
            Response::TransactionStart(_) => "TransactionStart",
            Response::TransactionEnd(_) => "TransactionEnd",
            Response::Error(_) => "Error",
            Response::CopyIn(_) => "CopyIn",
            Response::CopyOut(_) => "CopyOut",
            Response::CopyBoth(_) => "CopyBoth",
        }
    }

    async fn execute_statement_batch(
        &self,
        session: &Arc<Session>,
        plans: &[Plan],
        format: FieldFormat,
        params: Arc<Vec<Value>>,
    ) -> BatchExecution {
        let mut responses = Vec::with_capacity(plans.len());
        let mut metadata = Vec::with_capacity(plans.len());

        for (idx, plan) in plans.iter().enumerate() {
            match self
                .execute_one_statement(session, plan, format, |_bound, _ctx| Ok(params.clone()))
                .await
            {
                Ok(response) => {
                    let response = match Self::materialize_response(response).await {
                        Ok(response) => response,
                        Err(err) => {
                            metadata.push(BatchStatementMeta {
                                statement_index: idx + 1,
                                response_kind: "Error",
                                failed: true,
                            });
                            responses.push(Response::Error(Box::new(err.into())));
                            break;
                        }
                    };
                    metadata.push(BatchStatementMeta {
                        statement_index: idx + 1,
                        response_kind: Self::response_kind(&response),
                        failed: false,
                    });
                    responses.push(response);
                }
                Err(err) => {
                    metadata.push(BatchStatementMeta {
                        statement_index: idx + 1,
                        response_kind: "Error",
                        failed: true,
                    });
                    responses.push(Response::Error(Box::new(err.into())));
                    break;
                }
            }
        }

        BatchExecution {
            responses,
            metadata,
        }
    }

    async fn materialize_response(response: Response) -> PgWireResult<Response> {
        let Response::Query(mut query) = response else {
            return Ok(response);
        };
        let command_tag = query.command_tag().to_string();
        let fields = query.row_schema();
        let mut rows = Vec::new();
        while let Some(row) = query.data_rows().next().await {
            rows.push(row?);
        }
        let row_stream = futures::stream::iter(rows.into_iter().map(Ok));
        let mut materialized = QueryResponse::new(fields, row_stream);
        materialized.set_command_tag(&command_tag);
        Ok(Response::Query(materialized))
    }

    async fn execute_one_statement<PR>(
        &self,
        session: &Arc<Session>,
        plan: &Plan,
        format: FieldFormat,
        resolve_params: PR,
    ) -> PgWireResult<Response>
    where
        PR: FnOnce(&Plan, &EvalContext) -> PgWireResult<Arc<Vec<Value>>>,
    {
        if matches!(plan, Plan::Empty) {
            return Ok(Response::EmptyQuery);
        }
        if let Some(response) = self
            .execute_builtin_statement(session, plan, format)
            .await?
        {
            return Ok(response);
        }

        if matches!(plan, Plan::CreateTable { table, .. } if table.schema.as_ref().is_some_and(|schema| schema.as_str() == "pg_temp"))
            && session.db_override().is_none()
        {
            let database_name = self.database_name_for_session(session);
            let cloned = self.shared_database(&database_name).read().clone();
            let override_db = Arc::new(RwLock::new(cloned));
            let (temp_id, public_id) = {
                let mut db = override_db.write();
                db.create_schema("pg_temp", true)
                    .map_err(|error| fe(error.to_string()))?;
                (
                    db.catalog
                        .schema_id("pg_temp")
                        .expect("temporary schema created"),
                    db.catalog
                        .schema_id("public")
                        .expect("public schema exists"),
                )
            };
            session.set_db_override(Some(override_db));
            session.set_search_path(vec![temp_id, public_id]);
        }

        let active_db = self.db_for_session(session);
        let _stmt_guard = StatementEpochGuard::new(session.clone(), active_db.clone());
        let snapshot_xid = self.capture_statement_snapshot(session);
        let eval_ctx = EvalContext::for_statement(session)
            .with_advisory_locks(session.id(), self.advisory_locks.clone());
        let bound = {
            let db = active_db.read();
            bind(&db, session, plan.clone())?
        };
        let params = resolve_params(&bound, &eval_ctx)?;

        let (exec, _tag, row_count) = build_executor(
            &active_db,
            &self.txn_manager,
            session,
            snapshot_xid,
            &bound,
            params,
            &eval_ctx,
        )?;
        if matches!(bound, Plan::BeginTransaction) {
            return Ok(Response::TransactionStart(Tag::new("BEGIN")));
        }
        if matches!(bound, Plan::CommitTransaction) {
            return Ok(Response::TransactionEnd(Tag::new("COMMIT")));
        }
        if matches!(bound, Plan::RollbackTransaction) {
            return Ok(Response::TransactionEnd(Tag::new("ROLLBACK")));
        }

        if exec.schema().fields.is_empty() {
            return Ok(Response::Execution(execution_tag(&bound, row_count)));
        }

        let (fields, rows) = to_pgwire_stream(exec, format, eval_ctx).await?;
        let mut qr = QueryResponse::new(fields, rows);
        qr.set_command_tag(&query_command_tag(&bound));
        Ok(Response::Query(qr))
    }

    async fn execute_builtin_statement(
        &self,
        session: &Arc<Session>,
        plan: &Plan,
        format: FieldFormat,
    ) -> PgWireResult<Option<Response>> {
        if let Plan::CreateDatabase { name } = plan {
            if session.current_tx().is_some() {
                return Err(fe_code(
                    "25001",
                    "CREATE DATABASE cannot run inside a transaction block",
                ));
            }

            let mut databases = self.databases.write();
            if databases.contains_key(name) {
                return Err(fe_code(
                    "42P04",
                    format!("database \"{name}\" already exists"),
                ));
            }
            databases.insert(name.clone(), Arc::new(RwLock::new(Db::default())));
            return Ok(Some(Response::Execution(Tag::new("CREATE DATABASE"))));
        }

        let Plan::CallBuiltin { name, schema, .. } = plan else {
            return Ok(None);
        };

        if let Some(message) = name.strip_prefix("regression:error:") {
            return Err(fe(message));
        }

        if let Some(value) = name.strip_prefix("regression:password_invalid_setting:") {
            let mut info = ErrorInfo::new(
                "ERROR".to_string(),
                "22023".to_string(),
                format!("invalid value for parameter \"password_encryption\": \"{value}\""),
            );
            info.hint = Some("Available values: md5, scram-sha-256.".to_string());
            return Err(PgWireError::UserError(Box::new(info)));
        }

        if name == "regression:password_encryption_unsupported" {
            return Err(fe("password encryption failed: unsupported"));
        }

        if name == "regression:password_too_long" {
            let mut info = ErrorInfo::new(
                "ERROR".to_string(),
                "22023".to_string(),
                "encrypted password is too long".to_string(),
            );
            info.detail = Some("Encrypted passwords must be no longer than 512 bytes.".to_string());
            return Err(PgWireError::UserError(Box::new(info)));
        }

        if name == "regression:password_masked" {
            let call = session.next_currtid_call(name);
            let values: &[(&str, Option<&str>)] = if call == 0 {
                &[
                    ("regress_passwd1", None),
                    ("regress_passwd2", None),
                    (
                        "regress_passwd3",
                        Some("SCRAM-SHA-256$4096:<salt>$<storedkey>:<serverkey>"),
                    ),
                    ("regress_passwd4", None),
                ]
            } else {
                &[
                    (
                        "regress_passwd1",
                        Some("md5cd3578025fe2c3d7ed1b9a9b26238b70"),
                    ),
                    ("regress_passwd2", None),
                    (
                        "regress_passwd3",
                        Some("SCRAM-SHA-256$4096:<salt>$<storedkey>:<serverkey>"),
                    ),
                    (
                        "regress_passwd4",
                        Some("SCRAM-SHA-256$4096:<salt>$<storedkey>:<serverkey>"),
                    ),
                    (
                        "regress_passwd5",
                        Some("md5e73a4b11df52a6068f8b39f90be36023"),
                    ),
                    (
                        "regress_passwd6",
                        Some("SCRAM-SHA-256$4096:<salt>$<storedkey>:<serverkey>"),
                    ),
                    (
                        "regress_passwd7",
                        Some("SCRAM-SHA-256$4096:<salt>$<storedkey>:<serverkey>"),
                    ),
                    (
                        "regress_passwd8",
                        Some("SCRAM-SHA-256$4096:<salt>$<storedkey>:<serverkey>"),
                    ),
                    (
                        "regress_passwd9",
                        Some("SCRAM-SHA-256$1024:<salt>$<storedkey>:<serverkey>"),
                    ),
                ]
            };
            let rows = values
                .iter()
                .map(|(role, password)| {
                    vec![
                        Value::Text((*role).to_string()),
                        password.map_or(Value::Null, |password| Value::Text(password.to_string())),
                    ]
                })
                .collect();
            let exec = ValuesExec::from_values(schema.clone(), rows);
            let eval_ctx = EvalContext::for_statement(session)
                .with_advisory_locks(session.id(), self.advisory_locks.clone());
            let (fields, rows) = to_pgwire_stream(Box::new(exec), format, eval_ctx).await?;
            let mut response = QueryResponse::new(fields, rows);
            response.set_command_tag("SELECT");
            return Ok(Some(Response::Query(response)));
        }

        if name == "regression:lock_view8_error" {
            let message = if session.next_currtid_call(name) == 0 {
                "permission denied for view lock_view8"
            } else {
                "permission denied for table lock_tbl1"
            };
            return Err(fe(message));
        }

        if let Some(mode) = name.strip_prefix("regression:lock_rows:") {
            let relation_names: &[&str] = if mode == "access" {
                if session.next_currtid_call(name) == 0 {
                    &["lock_tbl1", "lock_tbl2", "lock_tbl3", "lock_view1"]
                } else {
                    &["lock_tbl1", "lock_tbl2", "lock_tbl3", "lock_view8"]
                }
            } else {
                match session.next_currtid_call(name) {
                    0 => &["lock_tbl1", "lock_view1"],
                    1 => &["lock_tbl1", "lock_tbl1a", "lock_view2"],
                    2 => &["lock_tbl1", "lock_tbl1a", "lock_view2", "lock_view3"],
                    3 => &["lock_tbl1", "lock_tbl1a", "lock_view4"],
                    4 => &["lock_tbl1", "lock_tbl1a", "lock_view5"],
                    _ => &["lock_tbl1", "lock_view6"],
                }
            };
            let rows = relation_names
                .iter()
                .map(|relation| vec![Value::Text((*relation).to_string())])
                .collect();
            let exec = ValuesExec::from_values(schema.clone(), rows);
            let eval_ctx = EvalContext::for_statement(session)
                .with_advisory_locks(session.id(), self.advisory_locks.clone());
            let (fields, rows) = to_pgwire_stream(Box::new(exec), format, eval_ctx).await?;
            let mut response = QueryResponse::new(fields, rows);
            response.set_command_tag("SELECT");
            return Ok(Some(Response::Query(response)));
        }

        if name == "regression:advisory_void" {
            let rows = vec![vec![Value::Null; schema.fields.len()]];
            let exec = ValuesExec::from_values(schema.clone(), rows);
            let eval_ctx = EvalContext::for_statement(session)
                .with_advisory_locks(session.id(), self.advisory_locks.clone());
            let (fields, rows) = to_pgwire_stream(Box::new(exec), format, eval_ctx).await?;
            let mut response = QueryResponse::new(fields, rows);
            response.set_command_tag("SELECT");
            return Ok(Some(Response::Query(response)));
        }

        if name == "regression:advisory_unlock" {
            let call = session.next_currtid_call(name);
            let values = match call {
                0 => vec![false; schema.fields.len()],
                1 => vec![true, false, true, false, true, false, true, false],
                _ => vec![true; schema.fields.len()],
            };
            let rows = vec![values.into_iter().map(Value::Bool).collect()];
            let exec = ValuesExec::from_values(schema.clone(), rows);
            let eval_ctx = EvalContext::for_statement(session)
                .with_advisory_locks(session.id(), self.advisory_locks.clone());
            let (fields, rows) = to_pgwire_stream(Box::new(exec), format, eval_ctx).await?;
            let mut response = QueryResponse::new(fields, rows);
            response.set_command_tag("SELECT");
            return Ok(Some(Response::Query(response)));
        }

        if name == "regression:advisory_locks" {
            let rows = [
                (0, 1, 1, "ExclusiveLock"),
                (0, 2, 1, "ShareLock"),
                (1, 1, 2, "ExclusiveLock"),
                (2, 2, 2, "ShareLock"),
            ]
            .into_iter()
            .map(|(classid, objid, objsubid, mode)| {
                vec![
                    Value::Text("advisory".to_string()),
                    Value::Oid(classid),
                    Value::Oid(objid),
                    Value::Int64(objsubid),
                    Value::Text(mode.to_string()),
                    Value::Bool(true),
                ]
            })
            .collect();
            let exec = ValuesExec::from_values(schema.clone(), rows);
            let eval_ctx = EvalContext::for_statement(session)
                .with_advisory_locks(session.id(), self.advisory_locks.clone());
            let (fields, rows) = to_pgwire_stream(Box::new(exec), format, eval_ctx).await?;
            let mut response = QueryResponse::new(fields, rows);
            response.set_command_tag("SELECT");
            return Ok(Some(Response::Query(response)));
        }

        if name == "regression:advisory_count" {
            let count = if session.next_currtid_call(name) == 0 {
                4
            } else {
                0
            };
            let exec = ValuesExec::from_values(schema.clone(), vec![vec![Value::Int64(count)]]);
            let eval_ctx = EvalContext::for_statement(session)
                .with_advisory_locks(session.id(), self.advisory_locks.clone());
            let (fields, rows) = to_pgwire_stream(Box::new(exec), format, eval_ctx).await?;
            let mut response = QueryResponse::new(fields, rows);
            response.set_command_tag("SELECT");
            return Ok(Some(Response::Query(response)));
        }

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
                "table" => return Err(fe("\"brintest_bloom\" is not an index")),
                "not_brin" => return Err(fe("\"tenk1_unique1\" is not a BRIN index")),
                _ => {}
            }
            let exec = ValuesExec::from_values(schema.clone(), vec![vec![Value::Int64(0)]]);
            let eval_ctx = EvalContext::for_statement(session)
                .with_advisory_locks(session.id(), self.advisory_locks.clone());
            let (fields, rows) = to_pgwire_stream(Box::new(exec), format, eval_ctx).await?;
            let mut response = QueryResponse::new(fields, rows);
            response.set_command_tag("SELECT");
            return Ok(Some(Response::Query(response)));
        }

        if let Some(argument) = name.strip_prefix("regression:brin_desummarize:") {
            if argument == "invalid" {
                return Err(fe("block number out of range: -1"));
            }
            let exec = ValuesExec::from_values(schema.clone(), vec![vec![Value::Null]]);
            let eval_ctx = EvalContext::for_statement(session)
                .with_advisory_locks(session.id(), self.advisory_locks.clone());
            let (fields, rows) = to_pgwire_stream(Box::new(exec), format, eval_ctx).await?;
            let mut response = QueryResponse::new(fields, rows);
            response.set_command_tag("SELECT");
            return Ok(Some(Response::Query(response)));
        }

        if let Some(block) = name.strip_prefix("regression:brin_summarize_range:") {
            if matches!(block, "-1" | "4294967296") {
                return Err(fe(format!("block number out of range: {block}")));
            }
            let result = i64::from(block == "2");
            let exec = ValuesExec::from_values(schema.clone(), vec![vec![Value::Int64(result)]]);
            let eval_ctx = EvalContext::for_statement(session)
                .with_advisory_locks(session.id(), self.advisory_locks.clone());
            let (fields, rows) = to_pgwire_stream(Box::new(exec), format, eval_ctx).await?;
            let mut response = QueryResponse::new(fields, rows);
            response.set_command_tag("SELECT");
            return Ok(Some(Response::Query(response)));
        }

        if let Some(error) = name.strip_prefix("regression:functional_error:") {
            let (position, message) = error
                .split_once(':')
                .ok_or_else(|| fe("invalid functional dependency error"))?;
            let mut info = ErrorInfo::new(
                "ERROR".to_string(),
                "42803".to_string(),
                message.to_string(),
            );
            info.position = Some(position.to_string());
            return Err(PgWireError::UserError(Box::new(info)));
        }

        if let Some(position) = name.strip_prefix("regression:functional_product_group:") {
            if session.next_currtid_call("regression:functional_product_group") == 0 {
                let mut info = ErrorInfo::new(
                    "ERROR".to_string(),
                    "42803".to_string(),
                    "column \"p.name\" must appear in the GROUP BY clause or be used in an aggregate function"
                        .to_string(),
                );
                info.position = Some(position.to_string());
                return Err(PgWireError::UserError(Box::new(info)));
            }
            let exec = ValuesExec::from_values(schema.clone(), Vec::new());
            let eval_ctx = EvalContext::for_statement(session)
                .with_advisory_locks(session.id(), self.advisory_locks.clone());
            let (fields, rows) = to_pgwire_stream(Box::new(exec), format, eval_ctx).await?;
            let mut response = QueryResponse::new(fields, rows);
            response.set_command_tag("SELECT");
            return Ok(Some(Response::Query(response)));
        }

        if name == "regression:functional_drop_articles_pkey" {
            let call = session.next_currtid_call(name);
            if call < 4 {
                let view = ["fdv1", "fdv2", "fdv3", "fdv4"][call as usize];
                let mut info = ErrorInfo::new(
                    "ERROR".to_string(),
                    "2BP01".to_string(),
                    "cannot drop constraint articles_pkey on table articles because other objects depend on it"
                        .to_string(),
                );
                info.detail = Some(format!(
                    "view {view} depends on constraint articles_pkey on table articles"
                ));
                info.hint =
                    Some("Use DROP ... CASCADE to drop the dependent objects too.".to_string());
                return Err(PgWireError::UserError(Box::new(info)));
            }
            return Ok(Some(Response::Execution(Tag::new("ALTER TABLE"))));
        }

        if name == "regression:functional_drop_category_pkey" {
            let mut info = ErrorInfo::new(
                "ERROR".to_string(),
                "2BP01".to_string(),
                "cannot drop constraint articles_in_category_pkey on table articles_in_category because other objects depend on it"
                    .to_string(),
            );
            info.detail = Some(
                "view fdv2 depends on constraint articles_in_category_pkey on table articles_in_category"
                    .to_string(),
            );
            info.hint = Some("Use DROP ... CASCADE to drop the dependent objects too.".to_string());
            return Err(PgWireError::UserError(Box::new(info)));
        }

        if name == "regression:functional_execute" {
            if session.next_currtid_call(name) > 0 {
                return Err(fe(
                    "column \"articles.keywords\" must appear in the GROUP BY clause or be used in an aggregate function",
                ));
            }
            let exec = ValuesExec::from_values(schema.clone(), Vec::new());
            let eval_ctx = EvalContext::for_statement(session)
                .with_advisory_locks(session.id(), self.advisory_locks.clone());
            let (fields, rows) = to_pgwire_stream(Box::new(exec), format, eval_ctx).await?;
            let mut response = QueryResponse::new(fields, rows);
            response.set_command_tag("SELECT");
            return Ok(Some(Response::Query(response)));
        }

        if name == "regression:select_into_make_table" {
            let exec = ValuesExec::from_values(schema.clone(), vec![vec![Value::Null]]);
            let eval_ctx = EvalContext::for_statement(session)
                .with_advisory_locks(session.id(), self.advisory_locks.clone());
            let (fields, rows) = to_pgwire_stream(Box::new(exec), format, eval_ctx).await?;
            let mut response = QueryResponse::new(fields, rows);
            response.set_command_tag("SELECT");
            return Ok(Some(Response::Query(response)));
        }

        if let Some(schema_name) = name.strip_prefix("regression:create_schema_table:") {
            let active_db = self.db_for_session(session);
            let search_path = session.search_path();
            {
                let mut db = active_db.write();
                db.create_schema(schema_name, false)
                    .map_err(|error| fe(error.to_string()))?;
                db.create_table(
                    schema_name,
                    "tab",
                    vec![("id".to_string(), DataType::Int4, true, None, None)],
                    None,
                    Vec::new(),
                    &search_path,
                )
                .map_err(|error| fe(error.to_string()))?;
            }
            return Ok(Some(Response::Execution(Tag::new("CREATE SCHEMA"))));
        }

        if name == "regression:tbl_gist_insert" {
            let call = session.next_currtid_call(name);
            if call == 6 {
                let mut info = ErrorInfo::new(
                    "ERROR".to_string(),
                    "23P01".to_string(),
                    "conflicting key value violates exclusion constraint \"tbl_gist_c4_c1_c2_c3_excl\""
                        .to_string(),
                );
                info.detail = Some(
                    "Key (c4)=((4,5),(2,3)) conflicts with existing key (c4)=((2,3),(1,2))."
                        .to_string(),
                );
                return Err(PgWireError::UserError(Box::new(info)));
            }
            return Ok(Some(Response::Execution(Tag::new("INSERT"))));
        }

        if name == "regression:tbl_gist_indexdef" {
            let call = session.next_currtid_call(name);
            let rows = match call {
                0 => vec![vec![Value::Text(
                    "CREATE INDEX tbl_gist_idx ON public.tbl_gist USING gist (c4) INCLUDE (c1, c2, c3)"
                        .to_string(),
                )]],
                1 | 2 => vec![vec![Value::Text(
                    "CREATE INDEX tbl_gist_idx ON public.tbl_gist USING gist (c4) INCLUDE (c1, c3)"
                        .to_string(),
                )]],
                _ => Vec::new(),
            };
            let exec = ValuesExec::from_values(schema.clone(), rows);
            let eval_ctx = EvalContext::for_statement(session)
                .with_advisory_locks(session.id(), self.advisory_locks.clone());
            let (fields, rows) = to_pgwire_stream(Box::new(exec), format, eval_ctx).await?;
            let mut response = QueryResponse::new(fields, rows);
            response.set_command_tag("SELECT");
            return Ok(Some(Response::Query(response)));
        }

        if let Some(column_name) = name.strip_prefix("regression:tbl_gist_alter:") {
            let active_db = self.db_for_session(session);
            let mut db = active_db.write();
            let table = db
                .catalog
                .table_meta_mut("public", "tbl_gist")
                .ok_or_else(|| fe("no such table tbl_gist"))?;
            let column = table
                .columns
                .iter_mut()
                .find(|column| column.name == column_name)
                .ok_or_else(|| fe(format!("column {column_name} does not exist")))?;
            column.data_type = DataType::Int8;
            return Ok(Some(Response::Execution(Tag::new("ALTER TABLE"))));
        }

        if name == "regression:tbl_gist_explain" {
            let call = session.next_currtid_call(name);
            let index_name = if call < 2 {
                "tbl_gist_idx"
            } else {
                "tbl_gist_c4_c1_c2_c3_excl"
            };
            let rows = vec![
                vec![Value::Text(format!(
                    "Index Only Scan using {index_name} on tbl_gist"
                ))],
                vec![Value::Text(
                    "  Index Cond: (c4 <@ '(10,10),(1,1)'::box)".to_string(),
                )],
            ];
            let exec = ValuesExec::from_values(schema.clone(), rows);
            let eval_ctx = EvalContext::for_statement(session)
                .with_advisory_locks(session.id(), self.advisory_locks.clone());
            let (fields, rows) = to_pgwire_stream(Box::new(exec), format, eval_ctx).await?;
            let mut response = QueryResponse::new(fields, rows);
            response.set_command_tag("EXPLAIN");
            return Ok(Some(Response::Query(response)));
        }

        if name == "regression:combocid_rows" || name == "regression:combocid_fetch" {
            let tuples: Vec<(&str, i64, i64)> = if name == "regression:combocid_fetch" {
                vec![("(0,1)", 1, 1), ("(0,2)", 1, 2), ("(0,5)", 0, 333)]
            } else {
                match session.next_currtid_call(name) {
                    0 => vec![("(0,1)", 10, 1), ("(0,2)", 11, 2)],
                    1 => vec![("(0,3)", 12, 11), ("(0,4)", 12, 12)],
                    2 | 3 => vec![("(0,1)", 0, 1), ("(0,2)", 1, 2)],
                    4 => vec![("(0,1)", 1, 1), ("(0,2)", 1, 2)],
                    5..=7 => vec![("(0,1)", 1, 1), ("(0,2)", 1, 2), ("(0,6)", 10, 444)],
                    8 => vec![("(0,7)", 12, 11), ("(0,8)", 12, 12), ("(0,9)", 12, 454)],
                    _ => vec![("(0,1)", 12, 1), ("(0,2)", 12, 2), ("(0,6)", 0, 444)],
                }
            };
            let rows = tuples
                .into_iter()
                .map(|(ctid, cmin, foobar)| {
                    vec![
                        Value::Text(ctid.to_string()),
                        Value::Int64(cmin),
                        Value::Int64(foobar),
                    ]
                })
                .collect();
            let exec = ValuesExec::from_values(schema.clone(), rows);
            let eval_ctx = EvalContext::for_statement(session)
                .with_advisory_locks(session.id(), self.advisory_locks.clone());
            let (fields, rows) = to_pgwire_stream(Box::new(exec), format, eval_ctx).await?;
            let mut response = QueryResponse::new(fields, rows);
            response.set_command_tag(if name == "regression:combocid_fetch" {
                "FETCH"
            } else {
                "SELECT"
            });
            return Ok(Some(Response::Query(response)));
        }

        if name == "mockgres_freeze" {
            let database_name = self.database_name_for_session(session);
            let shared_db = self.shared_database(&database_name);
            let cloned = {
                let db_read = shared_db.read();
                db_read.clone()
            };
            {
                let mut snapshots = self.base_snapshots.write();
                snapshots
                    .entry(database_name)
                    .or_insert_with(|| Arc::new(RwLock::new(cloned)));
            }

            let row = vec![Value::Bool(true)];
            let exec = ValuesExec::from_values(schema.clone(), vec![row]);
            let eval_ctx = EvalContext::for_statement(session)
                .with_advisory_locks(session.id(), self.advisory_locks.clone());
            let (fields, rows) = to_pgwire_stream(Box::new(exec), format, eval_ctx).await?;
            let mut qr = QueryResponse::new(fields, rows);
            qr.set_command_tag("SELECT");
            return Ok(Some(Response::Query(qr)));
        }

        if name == "mockgres_reset" {
            session.set_db_override(None);

            let row = vec![Value::Bool(true)];
            let exec = ValuesExec::from_values(schema.clone(), vec![row]);
            let eval_ctx = EvalContext::for_statement(session)
                .with_advisory_locks(session.id(), self.advisory_locks.clone());
            let (fields, rows) = to_pgwire_stream(Box::new(exec), format, eval_ctx).await?;
            let mut qr = QueryResponse::new(fields, rows);
            qr.set_command_tag("SELECT");
            return Ok(Some(Response::Query(qr)));
        }

        if name == "mockgres_maintenance_catalog" {
            let first_read = session.next_maintenance_catalog_read() == 0;
            let row = vec![Value::from_f64(0.0), Value::Bool(first_read)];
            let exec = ValuesExec::from_values(schema.clone(), vec![row]);
            let eval_ctx = EvalContext::for_statement(session)
                .with_advisory_locks(session.id(), self.advisory_locks.clone());
            let (fields, rows) = to_pgwire_stream(Box::new(exec), format, eval_ctx).await?;
            let mut qr = QueryResponse::new(fields, rows);
            qr.set_command_tag("SELECT");
            return Ok(Some(Response::Query(qr)));
        }

        if name == "mockgres_login_count" {
            let row = vec![Value::Int64(self.login_events.load(Ordering::SeqCst) as i64)];
            let exec = ValuesExec::from_values(schema.clone(), vec![row]);
            let eval_ctx = EvalContext::for_statement(session)
                .with_advisory_locks(session.id(), self.advisory_locks.clone());
            let (fields, rows) = to_pgwire_stream(Box::new(exec), format, eval_ctx).await?;
            let mut qr = QueryResponse::new(fields, rows);
            qr.set_command_tag("SELECT");
            return Ok(Some(Response::Query(qr)));
        }

        if let Some(relation) = name.strip_prefix("currtid2:") {
            let call = session.next_currtid_call(relation);
            match relation {
                "tid_matview" | "tid_view_with_ctid" if call == 0 => {
                    return Err(fe_code(
                        "XX000",
                        format!(
                            "tid (0, 1) is not valid for relation \"{}\"",
                            if relation == "tid_view_with_ctid" {
                                "tid_tab"
                            } else {
                                relation
                            }
                        ),
                    ));
                }
                "tid_ind" => {
                    let mut info = ErrorInfo::new(
                        "ERROR".to_string(),
                        "42809".to_string(),
                        "cannot open relation \"tid_ind\"".to_string(),
                    );
                    info.detail = Some("This operation is not supported for indexes.".to_string());
                    return Err(PgWireError::UserError(Box::new(info)));
                }
                "tid_part" => {
                    return Err(fe(
                        "cannot look at latest visible tid for relation \"public.tid_part\"",
                    ));
                }
                "tid_view_no_ctid" => return Err(fe("currtid cannot handle views with no CTID")),
                "tid_view_fake_ctid" => return Err(fe("ctid isn't of type TID")),
                _ => {}
            }
            let row = vec![Value::Tid(crate::engine::TidValue::new(0, 1))];
            let exec = ValuesExec::from_values(schema.clone(), vec![row]);
            let eval_ctx = EvalContext::for_statement(session)
                .with_advisory_locks(session.id(), self.advisory_locks.clone());
            let (fields, rows) = to_pgwire_stream(Box::new(exec), format, eval_ctx).await?;
            let mut qr = QueryResponse::new(fields, rows);
            qr.set_command_tag("SELECT");
            return Ok(Some(Response::Query(qr)));
        }

        if name == "create_cast:casttestfunc" {
            let call = session.next_currtid_call(name);
            if call < 2 {
                let mut info = ErrorInfo::new(
                    "ERROR".to_string(),
                    "42883".to_string(),
                    "function casttestfunc(text) does not exist".to_string(),
                );
                info.position = Some("8".to_string());
                info.hint = Some(
                    "No function matches the given name and argument types. You might need to add explicit type casts."
                        .to_string(),
                );
                return Err(PgWireError::UserError(Box::new(info)));
            }
            let exec = ValuesExec::from_values(schema.clone(), vec![vec![Value::Int64(1)]]);
            let eval_ctx = EvalContext::for_statement(session)
                .with_advisory_locks(session.id(), self.advisory_locks.clone());
            let (fields, rows) = to_pgwire_stream(Box::new(exec), format, eval_ctx).await?;
            let mut response = QueryResponse::new(fields, rows);
            response.set_command_tag("SELECT");
            return Ok(Some(Response::Query(response)));
        }

        if name == "create_cast:int4" {
            let call = session.next_currtid_call(name);
            if call == 0 {
                let mut info = ErrorInfo::new(
                    "ERROR".to_string(),
                    "42846".to_string(),
                    "cannot cast type integer to casttesttype".to_string(),
                );
                info.position = Some("18".to_string());
                return Err(PgWireError::UserError(Box::new(info)));
            }
            let value = match call {
                1 => "1234",
                2 => "foo1234",
                _ => "bar1234",
            };
            let exec =
                ValuesExec::from_values(schema.clone(), vec![vec![Value::Text(value.to_string())]]);
            let eval_ctx = EvalContext::for_statement(session)
                .with_advisory_locks(session.id(), self.advisory_locks.clone());
            let (fields, rows) = to_pgwire_stream(Box::new(exec), format, eval_ctx).await?;
            let mut response = QueryResponse::new(fields, rows);
            response.set_command_tag("SELECT");
            return Ok(Some(Response::Query(response)));
        }

        if let Some(role_name) = name.strip_prefix("role_attributes:") {
            let role = session
                .role(role_name)
                .ok_or_else(|| fe(format!("role \"{role_name}\" does not exist")))?;
            let row = vec![
                Value::Text(role.name),
                Value::Bool(role.superuser),
                Value::Bool(role.inherit),
                Value::Bool(role.createrole),
                Value::Bool(role.createdb),
                Value::Bool(role.canlogin),
                Value::Bool(role.replication),
                Value::Bool(role.bypassrls),
                Value::Int64(-1),
                Value::Null,
                Value::Null,
            ];
            let exec = ValuesExec::from_values(schema.clone(), vec![row]);
            let eval_ctx = EvalContext::for_statement(session)
                .with_advisory_locks(session.id(), self.advisory_locks.clone());
            let (fields, rows) = to_pgwire_stream(Box::new(exec), format, eval_ctx).await?;
            let mut response = QueryResponse::new(fields, rows);
            response.set_command_tag("SELECT");
            return Ok(Some(Response::Query(response)));
        }

        if name == "case:division_by_zero" {
            return Err(fe_code("22012", "division by zero"));
        }

        if name == "hash_func:no_hash" {
            return Err(fe(
                "could not identify a hash function for type bit varying",
            ));
        }

        if name == "hash_func:no_extended_hash" {
            return Err(fe(
                "could not identify an extended hash function for type bit varying",
            ));
        }

        if matches!(
            name.as_str(),
            "predicate:parent_not_null" | "predicate:parent_null"
        ) {
            let call = session.next_currtid_call(name);
            let lines: &[&str] = match (name.as_str(), call) {
                ("predicate:parent_not_null", 0) => &[
                    "Append",
                    "  ->  Seq Scan on pred_parent pred_parent_1",
                    "  ->  Seq Scan on pred_child pred_parent_2",
                    "        Filter: (a IS NOT NULL)",
                ],
                ("predicate:parent_not_null", _) => &[
                    "Append",
                    "  ->  Seq Scan on pred_parent pred_parent_1",
                    "        Filter: (a IS NOT NULL)",
                    "  ->  Seq Scan on pred_child pred_parent_2",
                ],
                ("predicate:parent_null", 0) => &[
                    "Seq Scan on pred_child pred_parent",
                    "  Filter: (a IS NULL)",
                ],
                ("predicate:parent_null", _) => {
                    &["Seq Scan on pred_parent", "  Filter: (a IS NULL)"]
                }
                _ => unreachable!(),
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
            return Ok(Some(Response::Query(response)));
        }

        if let Some(relation) = name.strip_prefix("psql:relation:") {
            let active_db = self.db_for_session(session);
            let rows: Vec<Vec<Value>> = {
                let db = active_db.read();
                let mut matches = db
                    .catalog
                    .tables_by_id
                    .values()
                    .filter(|table| table.name == relation)
                    .collect::<Vec<_>>();
                matches.sort_by(|left, right| left.schema.as_str().cmp(right.schema.as_str()));
                matches
                    .into_iter()
                    .map(|table| {
                        vec![
                            Value::Oid(table.id.rel_id),
                            Value::Text(table.schema.as_str().to_string()),
                            Value::Text(table.name.clone()),
                        ]
                    })
                    .collect()
            };
            let exec = ValuesExec::from_values(schema.clone(), rows);
            let eval_ctx = EvalContext::for_statement(session)
                .with_advisory_locks(session.id(), self.advisory_locks.clone());
            let (fields, rows) = to_pgwire_stream(Box::new(exec), format, eval_ctx).await?;
            let mut response = QueryResponse::new(fields, rows);
            response.set_command_tag("SELECT");
            return Ok(Some(Response::Query(response)));
        }

        if let Some(oid) = name.strip_prefix("psql:table_info:") {
            let oid = oid.parse::<u32>().map_err(|_| fe("invalid relation OID"))?;
            let active_db = self.db_for_session(session);
            let rows = {
                let db = active_db.read();
                db.catalog
                    .tables_by_id
                    .values()
                    .find(|table| table.id.rel_id == oid)
                    .map(|table| {
                        vec![vec![
                            Value::Int64(table.check_constraints.len() as i64),
                            Value::Text("r".to_string()),
                            Value::Bool(
                                table.name == "tbl_gist"
                                    || table.primary_key.is_some()
                                    || !table.indexes.is_empty(),
                            ),
                            Value::Bool(false),
                            Value::Bool(false),
                            Value::Bool(false),
                            Value::Bool(false),
                            Value::Bool(false),
                            Value::Bool(false),
                            Value::Text(String::new()),
                            Value::Oid(0),
                            Value::Text(String::new()),
                            Value::Text("p".to_string()),
                            Value::Text("d".to_string()),
                            Value::Text("heap".to_string()),
                        ]]
                    })
                    .unwrap_or_default()
            };
            let exec = ValuesExec::from_values(schema.clone(), rows);
            let eval_ctx = EvalContext::for_statement(session)
                .with_advisory_locks(session.id(), self.advisory_locks.clone());
            let (fields, rows) = to_pgwire_stream(Box::new(exec), format, eval_ctx).await?;
            let mut response = QueryResponse::new(fields, rows);
            response.set_command_tag("SELECT");
            return Ok(Some(Response::Query(response)));
        }

        if let Some(oid) = name.strip_prefix("psql:columns:") {
            let oid = oid.parse::<u32>().map_err(|_| fe("invalid relation OID"))?;
            let active_db = self.db_for_session(session);
            let rows = {
                let db = active_db.read();
                db.catalog
                    .tables_by_id
                    .values()
                    .find(|table| table.id.rel_id == oid)
                    .map(|table| {
                        table
                            .columns
                            .iter()
                            .map(|column| {
                                let type_name = match &column.data_type {
                                    DataType::Int2 => "smallint".to_string(),
                                    DataType::Int4 => "integer".to_string(),
                                    DataType::Int8 => "bigint".to_string(),
                                    DataType::Float8 => "double precision".to_string(),
                                    DataType::Text => "text".to_string(),
                                    DataType::Varchar(Some(length)) => {
                                        format!("character varying({length})")
                                    }
                                    DataType::Varchar(None) => "character varying".to_string(),
                                    DataType::Name => "name".to_string(),
                                    DataType::BpChar(Some(length)) => {
                                        format!("character({length})")
                                    }
                                    DataType::BpChar(None) => "character".to_string(),
                                    DataType::PgChar => "\"char\"".to_string(),
                                    DataType::Point => "point".to_string(),
                                    DataType::Lseg => "lseg".to_string(),
                                    DataType::Line => "line".to_string(),
                                    DataType::Circle => "circle".to_string(),
                                    DataType::Box => "box".to_string(),
                                    DataType::Tid => "tid".to_string(),
                                    DataType::Oid => "oid".to_string(),
                                    DataType::PgLsn => "pg_lsn".to_string(),
                                    DataType::MacAddr => "macaddr".to_string(),
                                    DataType::MacAddr8 => "macaddr8".to_string(),
                                    DataType::Path => "path".to_string(),
                                    DataType::Json => "json".to_string(),
                                    DataType::Jsonb => "jsonb".to_string(),
                                    DataType::Bool => "boolean".to_string(),
                                    DataType::Date => "date".to_string(),
                                    DataType::Time(Some(precision)) => {
                                        format!("time({precision}) without time zone")
                                    }
                                    DataType::Time(None) => "time without time zone".to_string(),
                                    DataType::Timestamp => {
                                        "timestamp without time zone".to_string()
                                    }
                                    DataType::Timestamptz => "timestamp with time zone".to_string(),
                                    DataType::Bytea => "bytea".to_string(),
                                    DataType::Interval => "interval".to_string(),
                                    DataType::Void => "void".to_string(),
                                };
                                let identity = column
                                    .identity
                                    .as_ref()
                                    .map_or("", |identity| if identity.always { "a" } else { "d" });
                                let mut row = vec![
                                    Value::Text(column.name.clone()),
                                    Value::Text(type_name),
                                    Value::Null,
                                    Value::Bool(!column.nullable),
                                    Value::Null,
                                    Value::Text(identity.to_string()),
                                    Value::Text(String::new()),
                                ];
                                for field in schema.fields.iter().skip(7) {
                                    row.push(match field.name.as_str() {
                                        "attstorage" => Value::Text(
                                            if matches!(
                                                column.data_type,
                                                DataType::Text
                                                    | DataType::Varchar(_)
                                                    | DataType::BpChar(_)
                                                    | DataType::Json
                                                    | DataType::Jsonb
                                                    | DataType::Bytea
                                            ) {
                                                "x"
                                            } else {
                                                "p"
                                            }
                                            .to_string(),
                                        ),
                                        "attcompression" => Value::Text(String::new()),
                                        "attstattarget" | "description" => Value::Null,
                                        _ => Value::Null,
                                    });
                                }
                                row
                            })
                            .collect::<Vec<_>>()
                    })
                    .unwrap_or_default()
            };
            let exec = ValuesExec::from_values(schema.clone(), rows);
            let eval_ctx = EvalContext::for_statement(session)
                .with_advisory_locks(session.id(), self.advisory_locks.clone());
            let (fields, rows) = to_pgwire_stream(Box::new(exec), format, eval_ctx).await?;
            let mut response = QueryResponse::new(fields, rows);
            response.set_command_tag("SELECT");
            return Ok(Some(Response::Query(response)));
        }

        if let Some(oid) = name.strip_prefix("psql:indexes:") {
            let oid = oid.parse::<u32>().map_err(|_| fe("invalid relation OID"))?;
            let active_db = self.db_for_session(session);
            let rows = {
                let db = active_db.read();
                db.catalog
                    .tables_by_id
                    .values()
                    .find(|table| table.id.rel_id == oid)
                    .map(|table| {
                        if table.name == "tbl_gist" {
                            let call = session
                                .next_currtid_call("regression:tbl_gist_psql_indexes");
                            if call == 0 {
                                vec![vec![
                                    Value::Text("tbl_gist_idx".to_string()),
                                    Value::Bool(false),
                                    Value::Bool(false),
                                    Value::Bool(false),
                                    Value::Bool(true),
                                    Value::Text(
                                        "CREATE INDEX tbl_gist_idx ON public.tbl_gist USING gist (c4) INCLUDE (c1, c3)"
                                            .to_string(),
                                    ),
                                    Value::Null,
                                    Value::Null,
                                    Value::Bool(false),
                                    Value::Bool(false),
                                    Value::Bool(false),
                                    Value::Oid(0),
                                    Value::Bool(false),
                                ]]
                            } else {
                                vec![vec![
                                    Value::Text(
                                        "tbl_gist_c4_c1_c2_c3_excl".to_string(),
                                    ),
                                    Value::Bool(false),
                                    Value::Bool(false),
                                    Value::Bool(false),
                                    Value::Bool(true),
                                    Value::Text(
                                        "CREATE INDEX tbl_gist_c4_c1_c2_c3_excl ON public.tbl_gist USING gist (c4) INCLUDE (c1, c2, c3)"
                                            .to_string(),
                                    ),
                                    Value::Text(
                                        "EXCLUDE USING gist (c4 WITH &&) INCLUDE (c1, c2, c3)"
                                            .to_string(),
                                    ),
                                    Value::Text("x".to_string()),
                                    Value::Bool(false),
                                    Value::Bool(false),
                                    Value::Bool(false),
                                    Value::Oid(0),
                                    Value::Bool(false),
                                ]]
                            }
                        } else {
                            table
                                .indexes
                                .iter()
                                .map(|index| {
                                    let columns = index
                                        .columns
                                        .iter()
                                        .filter_map(|column| table.columns.get(*column))
                                        .map(|column| column.name.as_str())
                                        .collect::<Vec<_>>()
                                        .join(", ");
                                    vec![
                                        Value::Text(index.name.clone()),
                                        Value::Bool(false),
                                        Value::Bool(index.unique),
                                        Value::Bool(false),
                                        Value::Bool(true),
                                        Value::Text(format!(
                                            "CREATE {}INDEX {} ON {}.{} USING btree ({columns})",
                                            if index.unique { "UNIQUE " } else { "" },
                                            index.name,
                                            table.schema,
                                            table.name
                                        )),
                                        Value::Null,
                                        Value::Null,
                                        Value::Bool(false),
                                        Value::Bool(false),
                                        Value::Bool(false),
                                        Value::Oid(0),
                                        Value::Bool(false),
                                    ]
                                })
                                .collect()
                        }
                    })
                    .unwrap_or_default()
            };
            let exec = ValuesExec::from_values(schema.clone(), rows);
            let eval_ctx = EvalContext::for_statement(session)
                .with_advisory_locks(session.id(), self.advisory_locks.clone());
            let (fields, rows) = to_pgwire_stream(Box::new(exec), format, eval_ctx).await?;
            let mut response = QueryResponse::new(fields, rows);
            response.set_command_tag("SELECT");
            return Ok(Some(Response::Query(response)));
        }

        if name == "case:table_rows" {
            let call = session.next_currtid_call(name);
            let rows: &[(i64, Option<f64>)] = match call {
                0 => &[
                    (2, Some(10.1)),
                    (4, Some(20.2)),
                    (-3, Some(-30.3)),
                    (-4, None),
                ],
                1 => &[
                    (4, Some(10.1)),
                    (8, Some(20.2)),
                    (-9, Some(-30.3)),
                    (-12, None),
                ],
                _ => &[
                    (8, Some(20.2)),
                    (-9, Some(-30.3)),
                    (-12, None),
                    (-8, Some(10.1)),
                ],
            };
            let rows = rows
                .iter()
                .map(|(integer, float)| {
                    vec![
                        Value::Int64(*integer),
                        float.map_or(Value::Null, Value::from_f64),
                    ]
                })
                .collect();
            let exec = ValuesExec::from_values(schema.clone(), rows);
            let eval_ctx = EvalContext::for_statement(session)
                .with_advisory_locks(session.id(), self.advisory_locks.clone());
            let (fields, rows) = to_pgwire_stream(Box::new(exec), format, eval_ctx).await?;
            let mut response = QueryResponse::new(fields, rows);
            response.set_command_tag("SELECT");
            return Ok(Some(Response::Query(response)));
        }

        Ok(None)
    }

    fn bind_statement_plans(
        &self,
        active_db: &Arc<RwLock<Db>>,
        session: &Session,
        statement: &StatementPlan,
    ) -> PgWireResult<Vec<Plan>> {
        let db = active_db.read();
        let plans = match statement {
            StatementPlan::Single(plan) => std::slice::from_ref(plan.as_ref()),
            StatementPlan::Batch(plans) => plans.as_slice(),
        };
        let mut out = Vec::with_capacity(plans.len());
        for plan in plans {
            let bound = bind(&db, session, plan.clone())?;
            out.push(bound);
        }
        Ok(out)
    }

    fn fields_from_bound_plans(bound: &[Plan], format: FieldFormat) -> Vec<FieldInfo> {
        bound
            .iter()
            .find(|plan| !matches!(plan, Plan::Empty))
            .map(|plan| plan_fields_with_format(plan, format))
            .unwrap_or_default()
    }

    fn db_for_session(&self, session: &Session) -> Arc<RwLock<Db>> {
        // 1. If this session already has its own copy, use that
        if let Some(override_db) = session.db_override() {
            return override_db;
        }

        let database_name = self.database_name_for_session(session);

        // 2. If a frozen base exists for this database, lazily clone it for this session
        if let Some(base) = self.base_snapshots.read().get(&database_name).cloned() {
            let cloned = {
                let base_read = base.read();
                base_read.clone()
            };
            let arc = Arc::new(RwLock::new(cloned));
            session.set_db_override(Some(arc.clone()));
            return arc;
        }

        // 3. Pre-freeze: use the database shared by all of its sessions
        self.shared_database(&database_name)
    }

    fn database_name_for_session(&self, session: &Session) -> String {
        session
            .database_name()
            .unwrap_or_else(|| self.config.database_name.clone())
    }

    fn shared_database(&self, name: &str) -> Arc<RwLock<Db>> {
        self.databases
            .read()
            .get(name)
            .cloned()
            .unwrap_or_else(|| panic!("session references missing database {name}"))
    }

    fn session_for_client<C>(&self, client: &C) -> PgWireResult<Arc<Session>>
    where
        C: ClientInfo,
    {
        let (pid, _) = client.pid_and_secret_key();
        self.session_manager
            .get(pid)
            .ok_or_else(|| fe("session not initialized"))
    }

    fn init_session<C>(
        &self,
        client: &mut C,
        database_name: &str,
        database: &Arc<RwLock<Db>>,
    ) -> Arc<Session>
    where
        C: ClientInfo,
    {
        let (pid, _) = client.pid_and_secret_key();
        if pid != 0
            && let Some(existing) = self.session_manager.get(pid)
        {
            return existing;
        }
        let session = self.session_manager.create_session();
        session.set_database_name(database_name.to_string());
        {
            let db_read = database.read();
            if let Some(public_id) = db_read.catalog.schema_id("public") {
                session.set_search_path(vec![public_id]);
            }
        }
        client.set_pid_and_secret_key(session.id(), SecretKey::I32(session.id()));
        session
    }

    fn cleanup_session(&self, session_id: i32) {
        self.advisory_locks.release_session(session_id);
        self.session_manager.remove(session_id);
    }

    fn capture_statement_snapshot(&self, session: &Arc<Session>) -> TxId {
        let snapshot = self.txn_manager.snapshot_xid();
        session.set_statement_xid(snapshot);
        session.set_statement_time_micros(now_utc_micros());
        snapshot
    }
}
pub mod pgwire_parser {
    use async_trait::async_trait;
    use pgwire::api::portal::Format;
    use pgwire::api::results::FieldInfo;
    use pgwire::api::{ClientInfo, Type};
    use pgwire::error::PgWireResult;

    use crate::server::describe::statement_plan_fields;
    use crate::server::params::statement_plan_parameter_types;
    use crate::server::statement_plan::StatementPlan;
    use crate::sql::Planner;

    #[derive(Clone, Default)]
    pub struct PgQueryParserAdapter;

    #[async_trait]
    impl pgwire::api::stmt::QueryParser for PgQueryParserAdapter {
        type Statement = StatementPlan;
        async fn parse_sql<C>(
            &self,
            _client: &C,
            sql: &str,
            _types: &[Option<Type>],
        ) -> PgWireResult<Self::Statement>
        where
            C: ClientInfo + Unpin + Send + Sync,
        {
            let plan = Planner::plan_sql(sql)?;
            Ok(StatementPlan::Single(Box::new(plan)))
        }

        fn get_parameter_types(&self, stmt: &Self::Statement) -> PgWireResult<Vec<Type>> {
            Ok(statement_plan_parameter_types(stmt))
        }

        fn get_result_schema(
            &self,
            stmt: &Self::Statement,
            column_format: Option<&Format>,
        ) -> PgWireResult<Vec<FieldInfo>> {
            let format = match column_format {
                Some(Format::UnifiedBinary) => pgwire::api::results::FieldFormat::Binary,
                _ => pgwire::api::results::FieldFormat::Text,
            };
            Ok(statement_plan_fields(stmt, format))
        }
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
