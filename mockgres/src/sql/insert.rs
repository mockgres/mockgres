use crate::catalog::SchemaName;
use crate::engine::{
    InsertSource, ObjName, OnConflictAction, OnConflictTarget, Plan, ScalarExpr, fe, fe_code,
};
use pg_query::NodeEnum;
use pg_query::protobuf::{
    InsertStmt, OnConflictAction as PgOnConflictAction, OnConflictClause, OverridingKind,
};
use pgwire::error::PgWireResult;

use super::dml::extract_col_name;
use super::expr::{parse_bool_expr, parse_scalar_expr};
use super::returning::parse_returning_clause;
use super::tokens::parse_index_columns;
use super::update::parse_update_target_list;

pub fn plan_insert(mut ins: InsertStmt) -> PgWireResult<Plan> {
    let with_clause = ins.with_clause.take();
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
        .ok_or_else(|| fe("INSERT needs VALUES or SELECT"))?;
    let NodeEnum::SelectStmt(sel2) = sel else {
        return Err(fe("only VALUES or SELECT are supported for INSERT"));
    };
    let select_stmt = *sel2;
    let on_conflict = parse_on_conflict_clause(&ins.on_conflict_clause)?;
    let returning = parse_returning_clause(&ins.returning_list)?;
    let plan = if select_stmt.values_lists.is_empty() {
        Plan::InsertSelect {
            table,
            columns: insert_columns,
            select: Box::new(super::dml::plan_select(select_stmt)?),
            override_system_value,
            on_conflict,
            returning,
            returning_schema: None,
        }
    } else {
        let mut all_rows: Vec<Vec<InsertSource>> = Vec::new();
        for v in select_stmt.values_lists {
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
        Plan::InsertValues {
            table,
            columns: insert_columns,
            rows: all_rows,
            override_system_value,
            on_conflict,
            returning,
            returning_schema: None,
        }
    };
    super::cte::wrap_with_clause(with_clause, plan)
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

fn parse_on_conflict_clause(
    clause: &Option<Box<OnConflictClause>>,
) -> PgWireResult<Option<OnConflictAction>> {
    let Some(occ) = clause.as_ref() else {
        return Ok(None);
    };

    let action = PgOnConflictAction::try_from(occ.action)
        .map_err(|_| fe("unsupported ON CONFLICT action"))?;
    match action {
        PgOnConflictAction::OnconflictNothing => {
            if !occ.target_list.is_empty() || occ.where_clause.is_some() {
                return Err(fe_code("0A000", "ON CONFLICT DO UPDATE is not supported"));
            }
            let target = if let Some(infer) = occ.infer.as_ref() {
                if !infer.index_elems.is_empty() {
                    let cols = parse_index_columns(&infer.index_elems)?;
                    OnConflictTarget::Columns(cols)
                } else if !infer.conname.is_empty() {
                    OnConflictTarget::Constraint(infer.conname.clone())
                } else {
                    OnConflictTarget::None
                }
            } else {
                OnConflictTarget::None
            };
            Ok(Some(OnConflictAction::DoNothing { target }))
        }
        PgOnConflictAction::OnconflictUpdate => {
            let target = if let Some(infer) = occ.infer.as_ref() {
                if !infer.index_elems.is_empty() {
                    let cols = parse_index_columns(&infer.index_elems)?;
                    OnConflictTarget::Columns(cols)
                } else if !infer.conname.is_empty() {
                    OnConflictTarget::Constraint(infer.conname.clone())
                } else {
                    OnConflictTarget::None
                }
            } else {
                OnConflictTarget::None
            };
            let sets = parse_update_target_list(&occ.target_list)?;
            let where_clause =
                if let Some(w) = occ.where_clause.as_ref().and_then(|n| n.node.as_ref()) {
                    Some(parse_bool_expr(w)?)
                } else {
                    None
                };
            Ok(Some(OnConflictAction::DoUpdate {
                target,
                sets,
                where_clause,
            }))
        }
        PgOnConflictAction::OnconflictNone | PgOnConflictAction::Undefined => {
            Err(fe("ON CONFLICT action required"))
        }
    }
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
