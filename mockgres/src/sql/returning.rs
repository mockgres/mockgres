use crate::engine::{ReturningClause, ReturningExpr, fe};
use pg_query::NodeEnum;
use pgwire::error::PgWireResult;

use super::expr::{derive_expr_name, parse_scalar_expr};

pub(crate) fn parse_returning_clause(
    returning_list: &[pg_query::Node],
) -> PgWireResult<Option<ReturningClause>> {
    if returning_list.is_empty() {
        return Ok(None);
    }
    let mut exprs = Vec::with_capacity(returning_list.len());
    for item in returning_list {
        let rt = item
            .node
            .as_ref()
            .and_then(|n| {
                if let NodeEnum::ResTarget(rt) = n {
                    Some(rt)
                } else {
                    None
                }
            })
            .ok_or_else(|| fe("bad RETURNING target"))?;
        if let Some(NodeEnum::ColumnRef(cr)) = rt.val.as_ref().and_then(|n| n.node.as_ref()) {
            if cr
                .fields
                .get(0)
                .and_then(|f| f.node.as_ref())
                .map(|n| matches!(n, NodeEnum::AStar(_)))
                .unwrap_or(false)
            {
                exprs.push(ReturningExpr::Star);
                continue;
            }
        }
        let expr_node = rt
            .val
            .as_ref()
            .and_then(|n| n.node.as_ref())
            .ok_or_else(|| fe("missing RETURNING expression"))?;
        let expr = parse_scalar_expr(expr_node)?;
        let alias = if rt.name.is_empty() {
            derive_expr_name(&expr)
        } else {
            rt.name.clone()
        };
        exprs.push(ReturningExpr::Expr { expr, alias });
    }
    Ok(Some(ReturningClause { exprs }))
}
