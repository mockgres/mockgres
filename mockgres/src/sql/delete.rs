use crate::catalog::SchemaName;
use crate::engine::{ObjName, Plan, fe};
use pg_query::protobuf::DeleteStmt;
use pgwire::error::PgWireResult;

use super::expr::parse_bool_expr;
use super::returning::parse_returning_clause;

pub fn plan_delete(mut del: DeleteStmt) -> PgWireResult<Plan> {
    let with_clause = del.with_clause.take();
    let rv = del.relation.ok_or_else(|| fe("missing target table"))?;
    let schema = if rv.schemaname.is_empty() {
        None
    } else {
        Some(SchemaName::new(rv.schemaname))
    };
    let table = ObjName {
        schema,
        name: rv.relname,
    };
    let filter = if let Some(w) = del.where_clause.as_ref().and_then(|n| n.node.as_ref()) {
        Some(parse_bool_expr(w)?)
    } else {
        None
    };
    let returning = parse_returning_clause(&del.returning_list)?;
    super::cte::wrap_with_clause(
        with_clause,
        Plan::Delete {
            table,
            filter,
            returning,
            returning_schema: None,
        },
    )
}
