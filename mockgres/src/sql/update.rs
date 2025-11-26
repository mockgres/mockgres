use crate::catalog::SchemaName;
use crate::engine::{ObjName, Plan, UpdateSet, fe};
use pg_query::NodeEnum;
use pg_query::protobuf::UpdateStmt;
use pgwire::error::PgWireResult;

use super::dml::extract_col_name;
use super::expr::{parse_bool_expr, parse_scalar_expr};
use super::returning::parse_returning_clause;

pub fn plan_update(upd: UpdateStmt) -> PgWireResult<Plan> {
    let rv = upd.relation.ok_or_else(|| fe("missing target table"))?;
    let schema = if rv.schemaname.is_empty() {
        None
    } else {
        Some(SchemaName::new(rv.schemaname))
    };
    let table = ObjName {
        schema,
        name: rv.relname,
    };

    let mut sets = Vec::new();
    for tgt in upd.target_list {
        let NodeEnum::ResTarget(rt) = tgt.node.unwrap() else {
            return Err(fe("bad update target"));
        };
        let col_name = if !rt.name.is_empty() {
            rt.name.clone()
        } else {
            extract_col_name(&rt)?
        };
        let expr_node = rt
            .val
            .as_ref()
            .and_then(|n| n.node.as_ref())
            .ok_or_else(|| fe("missing update value"))?;
        let expr = parse_scalar_expr(expr_node)?;
        sets.push(UpdateSet::ByName(col_name, expr));
    }
    if sets.is_empty() {
        return Err(fe("UPDATE requires SET clauses"));
    }

    let filter = if let Some(w) = upd.where_clause.as_ref().and_then(|n| n.node.as_ref()) {
        Some(parse_bool_expr(w)?)
    } else {
        None
    };
    let returning = parse_returning_clause(&upd.returning_list)?;
    Ok(Plan::Update {
        table,
        sets,
        filter,
        returning,
        returning_schema: None,
    })
}

pub fn parse_update_target_list(
    target_list: &[pg_query::protobuf::Node],
) -> PgWireResult<Vec<UpdateSet>> {
    let mut sets = Vec::new();
    for tgt in target_list {
        let Some(NodeEnum::ResTarget(rt)) = tgt.node.as_ref() else {
            return Err(fe("bad update target"));
        };
        let col_name = if !rt.name.is_empty() {
            rt.name.clone()
        } else {
            extract_col_name(rt)?
        };
        let expr_node = rt
            .val
            .as_ref()
            .and_then(|n| n.node.as_ref())
            .ok_or_else(|| fe("missing update value"))?;
        let expr = parse_scalar_expr(expr_node)?;
        sets.push(UpdateSet::ByName(col_name, expr));
    }
    if sets.is_empty() {
        return Err(fe("UPDATE requires SET clauses"));
    }
    Ok(sets)
}
