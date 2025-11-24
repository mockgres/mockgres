use crate::catalog::SchemaId;
use crate::db::Db;
use crate::engine::{
    BoolExpr, ColumnRefName, DataType, Field, ScalarBinaryOp, ScalarExpr, ScalarFunc, Schema,
    Value, fe, fe_code,
};
use pgwire::error::PgWireResult;

use super::{BindTimeContext, bind_time_scalar_func, current_schema_name, schema_names_for_path};

pub(crate) fn bind_bool_expr(
    expr: &BoolExpr,
    schema: &Schema,
    db: &Db,
    search_path: &[SchemaId],
    current_database: Option<&str>,
    time_ctx: BindTimeContext,
) -> PgWireResult<BoolExpr> {
    Ok(match expr {
        BoolExpr::Literal(b) => BoolExpr::Literal(*b),
        BoolExpr::Comparison { lhs, op, rhs } => {
            let mut lhs_bound = bind_scalar_expr(
                lhs,
                schema,
                None,
                db,
                search_path,
                current_database,
                time_ctx,
            )?;
            let mut rhs_bound = bind_scalar_expr(
                rhs,
                schema,
                None,
                db,
                search_path,
                current_database,
                time_ctx,
            )?;
            let lhs_hint = scalar_expr_type(&lhs_bound, schema);
            let rhs_hint = scalar_expr_type(&rhs_bound, schema);
            apply_param_hint(&mut lhs_bound, rhs_hint.as_ref());
            apply_param_hint(&mut rhs_bound, lhs_hint.as_ref());
            BoolExpr::Comparison {
                lhs: lhs_bound,
                op: *op,
                rhs: rhs_bound,
            }
        }
        BoolExpr::And(exprs) => BoolExpr::And(
            exprs
                .iter()
                .map(|e| bind_bool_expr(e, schema, db, search_path, current_database, time_ctx))
                .collect::<PgWireResult<Vec<_>>>()?,
        ),
        BoolExpr::Or(exprs) => BoolExpr::Or(
            exprs
                .iter()
                .map(|e| bind_bool_expr(e, schema, db, search_path, current_database, time_ctx))
                .collect::<PgWireResult<Vec<_>>>()?,
        ),
        BoolExpr::Not(inner) => BoolExpr::Not(Box::new(bind_bool_expr(
            inner,
            schema,
            db,
            search_path,
            current_database,
            time_ctx,
        )?)),
        BoolExpr::IsNull { expr, negated } => BoolExpr::IsNull {
            expr: bind_scalar_expr(
                expr,
                schema,
                None,
                db,
                search_path,
                current_database,
                time_ctx,
            )?,
            negated: *negated,
        },
    })
}

pub(crate) fn bind_scalar_expr(
    expr: &ScalarExpr,
    schema: &Schema,
    hint: Option<&DataType>,
    db: &Db,
    search_path: &[SchemaId],
    current_database: Option<&str>,
    time_ctx: BindTimeContext,
) -> PgWireResult<ScalarExpr> {
    Ok(match expr {
        ScalarExpr::Literal(v) => ScalarExpr::Literal(v.clone()),
        ScalarExpr::Column(colref) => {
            let idx = resolve_column_reference(schema, colref)?;
            ScalarExpr::ColumnIdx(idx)
        }
        ScalarExpr::ColumnIdx(i) => ScalarExpr::ColumnIdx(*i),
        ScalarExpr::Param { idx, ty } => ScalarExpr::Param {
            idx: *idx,
            ty: ty.clone().or_else(|| hint.cloned()),
        },
        ScalarExpr::BinaryOp { op, left, right } => ScalarExpr::BinaryOp {
            op: *op,
            left: Box::new(bind_scalar_expr(
                left,
                schema,
                hint,
                db,
                search_path,
                current_database,
                time_ctx,
            )?),
            right: Box::new(bind_scalar_expr(
                right,
                schema,
                hint,
                db,
                search_path,
                current_database,
                time_ctx,
            )?),
        },
        ScalarExpr::UnaryOp { op, expr } => ScalarExpr::UnaryOp {
            op: *op,
            expr: Box::new(bind_scalar_expr(
                expr,
                schema,
                hint,
                db,
                search_path,
                current_database,
                time_ctx,
            )?),
        },
        ScalarExpr::Cast { expr, ty } => ScalarExpr::Cast {
            expr: Box::new(bind_scalar_expr(
                expr,
                schema,
                Some(ty),
                db,
                search_path,
                current_database,
                time_ctx,
            )?),
            ty: ty.clone(),
        },
        ScalarExpr::Func { func, args } => {
            let bound_args = args
                .iter()
                .map(|a| {
                    bind_scalar_expr(a, schema, hint, db, search_path, current_database, time_ctx)
                })
                .collect::<PgWireResult<Vec<_>>>()?;
            if let Some(result) = bind_time_scalar_func(*func, &bound_args, time_ctx) {
                return result;
            }
            match func {
                ScalarFunc::CurrentSchema => {
                    if !bound_args.is_empty() {
                        return Err(fe("current_schema() takes no arguments"));
                    }
                    ScalarExpr::Literal(Value::Text(current_schema_name(db, search_path)))
                }
                ScalarFunc::CurrentSchemas => {
                    if bound_args.len() != 1 {
                        return Err(fe("current_schemas(boolean) requires one argument"));
                    }
                    match &bound_args[0] {
                        ScalarExpr::Literal(Value::Bool(_)) => {
                            let names = schema_names_for_path(db, search_path);
                            let array_text = format!("{{{}}}", names.join(","));
                            ScalarExpr::Literal(Value::Text(array_text))
                        }
                        _ => return Err(fe("current_schemas argument must be boolean literal")),
                    }
                }
                ScalarFunc::CurrentDatabase => {
                    if !bound_args.is_empty() {
                        return Err(fe("current_database() takes no arguments"));
                    }
                    let Some(name) = current_database else {
                        return Err(fe("current_database() is not available in this context"));
                    };
                    ScalarExpr::Literal(Value::Text(name.to_string()))
                }
                _ => ScalarExpr::Func {
                    func: *func,
                    args: bound_args,
                },
            }
        }
    })
}

pub(crate) fn resolve_column_reference(
    schema: &Schema,
    colref: &ColumnRefName,
) -> PgWireResult<usize> {
    let mut matches = schema
        .fields
        .iter()
        .enumerate()
        .filter(|(_, field)| column_ref_matches(field, colref));
    let Some((idx, _)) = matches.next() else {
        return Err(fe_code("42703", format!("unknown column: {colref}")));
    };
    if matches.next().is_some() {
        return Err(fe_code(
            "42702",
            format!("column reference \"{colref}\" is ambiguous"),
        ));
    }
    Ok(idx)
}

pub(crate) fn column_ref_matches(field: &Field, colref: &ColumnRefName) -> bool {
    if field.name != colref.column {
        return false;
    }
    if colref.schema.is_none() && colref.relation.is_none() {
        return true;
    }
    let Some(origin) = &field.origin else {
        return false;
    };
    if let Some(schema_name) = &colref.schema {
        if origin.schema.as_ref() != Some(schema_name) {
            return false;
        }
        if let Some(table_name) = &colref.relation {
            return origin.table.as_ref() == Some(table_name);
        }
        return false;
    }
    if let Some(rel_name) = &colref.relation {
        if origin.alias.as_ref() == Some(rel_name) {
            return true;
        }
        if origin.table.as_ref() == Some(rel_name) {
            return true;
        }
        return false;
    }
    true
}

pub(crate) fn scalar_expr_type(expr: &ScalarExpr, schema: &Schema) -> Option<DataType> {
    match expr {
        ScalarExpr::ColumnIdx(i) => Some(schema.field(*i).data_type.clone()),
        ScalarExpr::Param { ty, .. } => ty.clone(),
        ScalarExpr::Literal(v) => match v {
            Value::Int64(i) => {
                if *i >= i32::MIN as i64 && *i <= i32::MAX as i64 {
                    Some(DataType::Int4)
                } else {
                    Some(DataType::Int8)
                }
            }
            Value::Float64Bits(_) => Some(DataType::Float8),
            Value::Text(_) => Some(DataType::Text),
            Value::Bool(_) => Some(DataType::Bool),
            Value::Date(_) => Some(DataType::Date),
            Value::TimestampMicros(_) => Some(DataType::Timestamp),
            Value::TimestamptzMicros(_) => Some(DataType::Timestamptz),
            Value::Bytes(_) => Some(DataType::Bytea),
            Value::Null => None,
        },
        ScalarExpr::BinaryOp { op, left, right } => match op {
            ScalarBinaryOp::Concat => Some(DataType::Text),
            ScalarBinaryOp::Add
            | ScalarBinaryOp::Sub
            | ScalarBinaryOp::Mul
            | ScalarBinaryOp::Div => {
                let l = scalar_expr_type(left.as_ref(), schema);
                let r = scalar_expr_type(right.as_ref(), schema);
                match (l, r) {
                    (Some(DataType::Float8), _) | (_, Some(DataType::Float8)) => {
                        Some(DataType::Float8)
                    }
                    (Some(DataType::Int8), _) | (_, Some(DataType::Int8)) => Some(DataType::Int8),
                    (Some(DataType::Int4), Some(DataType::Int4)) => Some(DataType::Int4),
                    (Some(DataType::Int4), None) | (None, Some(DataType::Int4)) => {
                        Some(DataType::Int4)
                    }
                    (Some(dt), None) | (None, Some(dt)) => Some(dt),
                    _ => Some(DataType::Float8),
                }
            }
        },
        ScalarExpr::UnaryOp { expr, .. } => scalar_expr_type(expr.as_ref(), schema),
        ScalarExpr::Cast { ty, .. } => Some(ty.clone()),
        ScalarExpr::Func { func, args } => match func {
            ScalarFunc::Upper
            | ScalarFunc::Lower
            | ScalarFunc::CurrentSchema
            | ScalarFunc::CurrentDatabase => Some(DataType::Text),
            ScalarFunc::CurrentSchemas => Some(DataType::Text),
            ScalarFunc::Length => Some(DataType::Int4),
            ScalarFunc::Now
            | ScalarFunc::CurrentTimestamp
            | ScalarFunc::StatementTimestamp
            | ScalarFunc::TransactionTimestamp
            | ScalarFunc::ClockTimestamp => Some(DataType::Timestamptz),
            ScalarFunc::CurrentDate => Some(DataType::Date),
            ScalarFunc::Coalesce => args
                .iter()
                .filter_map(|a| scalar_expr_type(a, schema))
                .next(),
        },
        _ => None,
    }
}

pub(crate) fn apply_param_hint(expr: &mut ScalarExpr, hint: Option<&DataType>) {
    if let (ScalarExpr::Param { ty, .. }, Some(dt)) = (expr, hint) {
        if ty.is_none() {
            *ty = Some(dt.clone());
        }
    }
}
