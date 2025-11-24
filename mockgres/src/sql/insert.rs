use crate::catalog::SchemaName;
use crate::engine::{InsertSource, ObjName, Plan, ScalarExpr, fe, fe_code};
use pg_query::NodeEnum;
use pg_query::protobuf::{InsertStmt, OverridingKind};
use pgwire::error::PgWireResult;

use super::dml::extract_col_name;
use super::expr::parse_scalar_expr;
use super::returning::parse_returning_clause;

pub fn plan_insert(ins: InsertStmt) -> PgWireResult<Plan> {
    let rv = ins.relation.ok_or_else(|| fe("missing target table"))?;
    let schema = if rv.schemaname.is_empty() {
        None
    } else {
        Some(SchemaName::new(rv.schemaname))
    };
    let table = ObjName {
        schema,
        name: rv.relname,
    };
    let insert_columns = parse_insert_columns(&ins.cols)?;
    let override_system_value = match OverridingKind::try_from(ins.r#override)
        .unwrap_or(OverridingKind::OverridingNotSet)
    {
        OverridingKind::Undefined | OverridingKind::OverridingNotSet => false,
        OverridingKind::OverridingSystemValue => true,
        OverridingKind::OverridingUserValue => {
            return Err(fe_code("0A000", "OVERRIDING USER VALUE is not supported"));
        }
    };
    let sel = ins
        .select_stmt
        .and_then(|n| n.node)
        .ok_or_else(|| fe("INSERT needs VALUES"))?;
    let NodeEnum::SelectStmt(sel2) = sel else {
        return Err(fe("only VALUES supported"));
    };
    let mut all_rows: Vec<Vec<InsertSource>> = Vec::new();
    for v in sel2.values_lists {
        let NodeEnum::List(vlist) = v.node.unwrap() else {
            continue;
        };
        let mut row = Vec::new();
        for cell in vlist.items {
            let n = cell.node.unwrap();
            if matches!(n, NodeEnum::SetToDefault(_)) {
                row.push(InsertSource::Default);
            } else {
                let expr = parse_insert_value_expr(&n)?;
                row.push(InsertSource::Expr(expr));
            }
        }
        all_rows.push(row);
    }
    let returning = parse_returning_clause(&ins.returning_list)?;
    Ok(Plan::InsertValues {
        table,
        columns: insert_columns,
        rows: all_rows,
        override_system_value,
        returning,
        returning_schema: None,
    })
}

fn parse_insert_columns(cols: &[pg_query::Node]) -> PgWireResult<Option<Vec<String>>> {
    if cols.is_empty() {
        return Ok(None);
    }
    let mut out = Vec::with_capacity(cols.len());
    for c in cols {
        let node = c.node.as_ref().ok_or_else(|| fe("bad insert column"))?;
        let name = match node {
            NodeEnum::ResTarget(rt) => {
                if !rt.name.is_empty() {
                    rt.name.clone()
                } else {
                    extract_col_name(rt)?
                }
            }
            NodeEnum::String(s) => s.sval.clone(),
            _ => return Err(fe("bad insert column")),
        };
        if out.iter().any(|existing| existing == &name) {
            return Err(fe(format!("duplicate insert column: {name}")));
        }
        out.push(name);
    }
    Ok(Some(out))
}

fn parse_insert_value_expr(node: &NodeEnum) -> PgWireResult<ScalarExpr> {
    let expr = parse_scalar_expr(node)?;
    sanitize_insert_expr(expr)
}

fn sanitize_insert_expr(expr: ScalarExpr) -> PgWireResult<ScalarExpr> {
    match expr {
        ScalarExpr::Column(..) => Err(fe("INSERT expressions cannot reference columns")),
        ScalarExpr::BinaryOp { op, left, right } => Ok(ScalarExpr::BinaryOp {
            op,
            left: Box::new(sanitize_insert_expr(*left)?),
            right: Box::new(sanitize_insert_expr(*right)?),
        }),
        ScalarExpr::UnaryOp { op, expr } => Ok(ScalarExpr::UnaryOp {
            op,
            expr: Box::new(sanitize_insert_expr(*expr)?),
        }),
        ScalarExpr::Func { func, args } => Ok(ScalarExpr::Func {
            func,
            args: args
                .into_iter()
                .map(sanitize_insert_expr)
                .collect::<PgWireResult<Vec<_>>>()?,
        }),
        ScalarExpr::Cast { expr, ty } => Ok(ScalarExpr::Cast {
            expr: Box::new(sanitize_insert_expr(*expr)?),
            ty,
        }),
        other => Ok(other),
    }
}
