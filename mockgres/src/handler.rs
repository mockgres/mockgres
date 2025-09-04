use std::fmt::Debug;
use std::sync::Arc;

use futures::Sink;
use parking_lot::RwLock;
use pgwire::api::portal::Format;
use pgwire::api::{
    ClientInfo, ClientPortalStore, ErrorHandler, NoopHandler, PgWireServerHandlers,
    auth::{StartupHandler, noop::NoopStartupHandler},
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
use crate::engine::{ExecNode, Expr, Plan, Schema, SeqScanExec, to_pgwire_stream, SortKey, OrderExec, LimitExec, FilterPred, FilterExec, fe};
use crate::engine::{ProjectExec, ValuesExec};
use crate::parser::Planner;

#[derive(Clone)]
pub struct Mockgres {
    pub db: Arc<RwLock<Db>>,
}

impl Mockgres {
    pub fn new(db: Arc<RwLock<Db>>) -> Self {
        Self { db }
    }

    pub async fn serve(
        self: Arc<Self>,
        addr: std::net::SocketAddr,
    ) -> anyhow::Result<()> {
        let listener = tokio::net::TcpListener::bind(addr).await?;
        loop {
            let (socket, _peer) = listener.accept().await?;
            let h = self.clone();
            tokio::spawn(async move {
                let _ = pgwire::tokio::process_socket(socket, None, h).await;
            });
        }
    }

    // default/fallback command tags used when the executor didn't provide one
    fn command_tag(plan: &Plan) -> &'static str {
        match plan {
            Plan::Values { .. } |
            Plan::SeqScan { .. } |
            Plan::Projection { .. } |
            Plan::Filter { .. } |
            Plan::Order { .. } |
            Plan::Limit { .. } => "SELECT 0",

            Plan::CreateTable { .. } => "CREATE TABLE",
            Plan::InsertValues { .. } => "INSERT 0",
            Plan::UnboundSeqScan { .. } => "SELECT 0",
        }
    }

    // build a physical plan
    fn build_executor(&self, p: &Plan) -> PgWireResult<(Box<dyn ExecNode>, Option<String>, Option<usize>)> {
        match p {
            Plan::Values { rows, schema } => {
                let cnt = rows.len();
                Ok((Box::new(ValuesExec::new(schema.clone(), rows.clone())?), None, Some(cnt)))
            }

            Plan::Projection { input, exprs, schema } => {
                let (child, _tag, cnt) = self.build_executor(input)?;
                Ok((Box::new(ProjectExec::new(schema.clone(), child, exprs.clone())), None, cnt))
            }

            Plan::SeqScan { table, cols, schema } => {
                let db = self.db.read();
                let schema_name = table.schema.as_deref().unwrap_or("public");
                let _tm = db.resolve_table(schema_name, &table.name).map_err(|e| fe(e.to_string()))?;
                let positions: Vec<usize> = cols.iter().map(|(i, _)| *i).collect();
                let (rows, _) = if positions.is_empty() && schema.fields.is_empty() {
                    (vec![], vec![])
                } else {
                    db.scan_bound_positions(schema_name, &table.name, &positions)
                        .map_err(|e| fe(e.to_string()))?
                };
                drop(db);
                let cnt = rows.len();
                Ok((Box::new(SeqScanExec::new(schema.clone(), rows)), None, Some(cnt)))
            }

            // wrappers
            Plan::Filter { input, pred, project_prefix_len } => {
                let (child, _tag, _cnt) = self.build_executor(input)?;
                let (idx, op, rhs) = match pred {
                    FilterPred::ByIndex { idx, op, rhs } => (*idx, *op, rhs.clone()),
                    FilterPred::ByName  { col, op, rhs } => {
                        let i = child.schema().fields.iter().position(|f| f.name == *col)
                            .ok_or_else(|| fe(format!("unknown column in filter: {}", col)))?;
                        (i, *op, rhs.clone())
                    }
                };

                // build the filter exec
                let child_schema = child.schema().clone();
                let mut node: Box<dyn ExecNode> =
                    Box::new(FilterExec::new(child_schema.clone(), child, idx, op, rhs));

                // if parser widened selection for where, drop the extra columns here
                if let Some(n) = *project_prefix_len {
                    // project first n fields from the child schema
                    let proj_fields = child_schema.fields[..n].to_vec();
                    let proj_schema = Schema { fields: proj_fields.clone() };
                    let exprs = (0..n)
                        .map(|i| (Expr::Column(i), proj_fields[i].name.clone()))
                        .collect();
                    node = Box::new(ProjectExec::new(proj_schema, node, exprs));
                }

                Ok((node, None, None))
            }

            Plan::Order { input, keys } => {
                let (child, _tag, cnt) = self.build_executor(input)?;
                let mut idx_keys = Vec::with_capacity(keys.len());
                for k in keys {
                    match k {
                        SortKey::ByIndex { idx, asc } => idx_keys.push((*idx, *asc)),
                        SortKey::ByName  { col, asc } => {
                            let i = child.schema().fields.iter().position(|f| f.name == *col)
                                .ok_or_else(|| fe(format!("unknown column in order by: {}", col)))?;
                            idx_keys.push((i, *asc));
                        }
                    }
                }
                let schema = child.schema().clone();
                let exec = Box::new(OrderExec::new(schema, child, idx_keys)?);
                Ok((exec, None, cnt))
            }

            Plan::Limit { input, limit } => {
                let (child, _tag, cnt) = self.build_executor(input)?;
                let out_cnt = cnt.map(|c| c.min(*limit));
                let schema = child.schema().clone();
                Ok((Box::new(LimitExec::new(schema, child, *limit)), None, out_cnt))
            }

            Plan::CreateTable { table, cols, pk } => {
                let mut db = self.db.write();
                let schema_name = table.schema.as_deref().unwrap_or("public");
                db.create_table(schema_name, &table.name, cols.clone(), pk.clone())
                    .map_err(|e| fe(e.to_string()))?;
                drop(db);
                Ok((Box::new(ValuesExec::new(Schema{fields:vec![]}, vec![])?), Some("CREATE TABLE".into()), None))
            }

            Plan::InsertValues { table, rows } => {
                // realize constants only
                let mut realized = Vec::with_capacity(rows.len());
                for r in rows {
                    let mut rr = Vec::with_capacity(r.len());
                    for e in r {
                        match e {
                            Expr::Literal(v) => rr.push(v.clone()),
                            _ => return Err(fe("insert supports constants only")),
                        }
                    }
                    realized.push(rr);
                }
                let mut db = self.db.write();
                let schema_name = table.schema.as_deref().unwrap_or("public");
                let inserted = db.insert_full_rows(schema_name, &table.name, realized)
                    .map_err(|e| fe(e.to_string()))?;
                drop(db);
                let tag = format!("INSERT 0 {}", inserted);
                Ok((Box::new(ValuesExec::new(Schema{fields:vec![]}, vec![])?), Some(tag), None))
            }
            Plan::UnboundSeqScan { .. } => {
                Err(fe("unbound plan; call binder first"))
            }
        }
    }

    fn describe_plan(&self, plan: &Plan) -> PgWireResult<Vec<FieldInfo>> {
        let db = self.db.read();
        let bound = bind(&db, plan.clone())?;
        let fields = bound
            .schema()
            .fields
            .iter()
            .map(|f| {
                FieldInfo::new(
                    f.name.clone(),
                    None,
                    None,
                    f.data_type.to_pg(),
                    FieldFormat::Text,
                )
            })
            .collect();
        Ok(fields)
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

                let (exec, tag, row_count) = self.build_executor(&bound)?;
                let (fields, rows) = to_pgwire_stream(exec, FieldFormat::Text)?;
                let mut qr = QueryResponse::new(fields, rows);
                if let Some(t) = tag {
                    // explicit tag from executor (e.g., insert)
                    qr.set_command_tag(&t);
                } else if let Some(n) = row_count {
                    // dynamic select row count
                    qr.set_command_tag(&format!("SELECT {}", n));
                } else {
                    qr.set_command_tag(Self::command_tag(&bound));
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
        C: ClientInfo
        + ClientPortalStore
        + Sink<PgWireBackendMessage>
        + Unpin
        + Send
        + Sync,
        C::PortalStore: PortalStore<Statement = Self::Statement>,
        C::Error: Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        let fields = self.describe_plan(&target.statement)?;
        Ok(DescribeStatementResponse::new(vec![], fields))
    }

    async fn do_describe_portal<C>(
        &self,
        _client: &mut C,
        portal: &pgwire::api::portal::Portal<Self::Statement>,
    ) -> PgWireResult<DescribePortalResponse>
    where
        C: ClientInfo
        + ClientPortalStore
        + Sink<PgWireBackendMessage>
        + Unpin
        + Send
        + Sync,
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

        let (exec, tag, row_count) = self.build_executor(&bound)?;
        let (fields, rows) = to_pgwire_stream(exec, fmt)?;
        let mut qr = QueryResponse::new(fields, rows);
        if let Some(t) = tag {
            qr.set_command_tag(&t);
        } else if let Some(n) = row_count {
            qr.set_command_tag(&format!("SELECT {}", n));
        } else {
            qr.set_command_tag(Self::command_tag(&bound));
        }
        Ok(Response::Query(qr))
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
