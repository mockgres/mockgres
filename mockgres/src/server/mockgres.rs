use std::fmt::Debug;
use std::sync::Arc;

use futures::Sink;
use parking_lot::RwLock;
use pgwire::api::portal::Format;
use pgwire::api::{
    ClientInfo, ClientPortalStore, ErrorHandler, NoopHandler, PgWireServerHandlers,
    auth::{noop::NoopStartupHandler, StartupHandler},
    cancel::CancelHandler,
    query::{ExtendedQueryHandler, SimpleQueryHandler},
    results::{
        DescribePortalResponse, DescribeStatementResponse, FieldFormat, FieldInfo, QueryResponse,
        Response,
    },
    store::PortalStore,
};
use pgwire::error::{PgWireError, PgWireResult};
use pgwire::messages::{PgWireBackendMessage, PgWireFrontendMessage};

use crate::binder::bind;
use crate::db::Db;
use crate::engine::{Plan, Value, to_pgwire_stream};
use crate::parser::Planner;

use super::describe::plan_fields;
use super::exec_builder::{build_executor, command_tag};
use super::params::{build_params_for_portal, plan_parameter_types};

#[derive(Clone)]
pub struct Mockgres {
    pub db: Arc<RwLock<Db>>,
}

impl Mockgres {
    pub fn new(db: Arc<RwLock<Db>>) -> Self {
        Self { db }
    }

    pub async fn serve(self: Arc<Self>, addr: std::net::SocketAddr) -> anyhow::Result<()> {
        let listener = tokio::net::TcpListener::bind(addr).await?;
        loop {
            let (socket, _peer) = listener.accept().await?;
            let h = self.clone();
            tokio::spawn(async move {
                let _ = pgwire::tokio::process_socket(socket, None, h).await;
            });
        }
    }
}

#[async_trait::async_trait]
impl NoopStartupHandler for Mockgres {
    async fn post_startup<C>(
        &self,
        _client: &mut C,
        _message: PgWireFrontendMessage,
    ) -> PgWireResult<()>
    where
        C: ClientInfo + Sink<PgWireBackendMessage> + Unpin + Send,
        C::Error: Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        Ok(())
    }
}

impl Default for Mockgres {
    fn default() -> Self {
        Self::new(Arc::new(RwLock::new(Db::default())))
    }
}

#[async_trait::async_trait]
impl SimpleQueryHandler for Mockgres {
    async fn do_query<'a, C>(&self, _client: &mut C, query: &str) -> PgWireResult<Vec<Response<'a>>>
    where
        C: ClientInfo + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
        C::Error: Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        match Planner::plan_sql(query) {
            Ok(lp0) => {
                // bind (names -> positions) using catalog
                let db_read = self.db.read();
                let bound = bind(&db_read, lp0)?;
                drop(db_read);

                let params: Arc<Vec<Value>> = Arc::new(Vec::new());
                let (exec, tag, row_count) = build_executor(&self.db, &bound, params)?;
                let (fields, rows) = to_pgwire_stream(exec, FieldFormat::Text)?;
                let mut qr = QueryResponse::new(fields, rows);
                if let Some(t) = tag {
                    // explicit tag from executor (e.g., insert)
                    qr.set_command_tag(&t);
                } else if let Some(n) = row_count {
                    // dynamic select row count
                    qr.set_command_tag(&format!("SELECT {}", n));
                } else {
                    qr.set_command_tag(command_tag(&bound));
                }
                Ok(vec![Response::Query(qr)])
            }
            Err(e) => Err(e),
        }
    }
}

#[async_trait::async_trait]
impl ExtendedQueryHandler for Mockgres {
    type Statement = Plan;
    type QueryParser = pgwire_parser::PgQueryParserAdapter;

    fn query_parser(&self) -> Arc<Self::QueryParser> {
        Arc::new(pgwire_parser::PgQueryParserAdapter::default())
    }

    async fn do_describe_statement<C>(
        &self,
        _client: &mut C,
        target: &pgwire::api::stmt::StoredStatement<Self::Statement>,
    ) -> PgWireResult<DescribeStatementResponse>
    where
        C: ClientInfo + ClientPortalStore + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
        C::PortalStore: PortalStore<Statement = Self::Statement>,
        C::Error: Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        let db = self.db.read();
        let bound = bind(&db, target.statement.clone())?;
        drop(db);
        let params = plan_parameter_types(&bound);
        let fields = plan_fields(&bound);
        Ok(DescribeStatementResponse::new(params, fields))
    }

    async fn do_describe_portal<C>(
        &self,
        _client: &mut C,
        portal: &pgwire::api::portal::Portal<Self::Statement>,
    ) -> PgWireResult<DescribePortalResponse>
    where
        C: ClientInfo + ClientPortalStore + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
        C::PortalStore: PortalStore<Statement = Self::Statement>,
        C::Error: Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        let fields = self.describe_plan(&portal.statement.statement)?;
        Ok(DescribePortalResponse::new(fields))
    }
    async fn do_query<'a, C>(
        &self,
        _client: &mut C,
        portal: &pgwire::api::portal::Portal<Self::Statement>,
        _max_rows: usize,
    ) -> PgWireResult<Response<'a>>
    where
        C: ClientInfo + ClientPortalStore + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
        C::PortalStore: PortalStore<Statement = Self::Statement>,
        C::Error: Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        let fmt = match portal.result_column_format {
            Format::UnifiedBinary => FieldFormat::Binary,
            _ => FieldFormat::Text,
        };

        let db = self.db.read();
        let bound = bind(&db, portal.statement.statement.clone())?;
        drop(db);

        let params = build_params_for_portal(&bound, portal)?;
        let (exec, tag, row_count) = build_executor(&self.db, &bound, params.clone())?;
        let (fields, rows) = to_pgwire_stream(exec, fmt)?;
        let mut qr = QueryResponse::new(fields, rows);
        if let Some(t) = tag {
            qr.set_command_tag(&t);
        } else if let Some(n) = row_count {
            qr.set_command_tag(&format!("SELECT {}", n));
        } else {
            qr.set_command_tag(command_tag(&bound));
        }
        Ok(Response::Query(qr))
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

impl Mockgres {
    fn describe_plan(&self, plan: &Plan) -> PgWireResult<Vec<FieldInfo>> {
        let db = self.db.read();
        let bound = bind(&db, plan.clone())?;
        Ok(plan_fields(&bound))
    }
}

/// Pgwire adapter: parse -> our `Plan`
pub mod pgwire_parser {
    use async_trait::async_trait;
    use pgwire::api::{ClientInfo, Type};
    use pgwire::error::PgWireResult;

    use crate::engine::Plan;
    use crate::parser::Planner;

    #[derive(Clone, Default)]
    pub struct PgQueryParserAdapter;

    #[async_trait]
    impl pgwire::api::stmt::QueryParser for PgQueryParserAdapter {
        type Statement = Plan;
        async fn parse_sql<C>(
            &self,
            _client: &C,
            sql: &str,
            _types: &[Type],
        ) -> PgWireResult<Self::Statement>
        where
            C: ClientInfo + Unpin + Send + Sync,
        {
            Planner::plan_sql(sql)
        }
    }
}
