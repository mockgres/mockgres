use crate::db::Db;
use crate::engine::{
    BoolExpr, DataType, Expr, Field, Plan, ScalarExpr, Schema, Selection, UpdateSet, Value, fe,
};

pub fn bind(db: &Db, p: Plan) -> pgwire::error::PgWireResult<Plan> {
    match p {
        Plan::UnboundSeqScan { table, selection } => {
            let schema_name = table.schema.as_deref().unwrap_or("public");
            let tm = db
                .resolve_table(schema_name, &table.name)
                .map_err(|e| fe(e.to_string()))?;

            // build (idx, Field) for executor + compose output schema
            let cols: Vec<(usize, Field)> = match selection {
                Selection::Star => tm
                    .columns
                    .iter()
                    .enumerate()
                    .map(|(i, c)| {
                        (
                            i,
                            Field {
                                name: c.name.clone(),
                                data_type: c.data_type.clone(),
                            },
                        )
                    })
                    .collect(),
                Selection::Columns(names) => {
                    let mut out = Vec::with_capacity(names.len());
                    for n in names {
                        let i =
                            tm.columns.iter().position(|c| c.name == n).ok_or_else(|| {
                                crate::engine::fe(format!("unknown column: {}", n))
                            })?;
                        out.push((
                            i,
                            Field {
                                name: n,
                                data_type: tm.columns[i].data_type.clone(),
                            },
                        ));
                    }
                    out
                }
            };
            let schema = Schema {
                fields: cols.iter().map(|(_, f)| f.clone()).collect(),
            };

            Ok(Plan::SeqScan {
                table,
                cols,
                schema,
            })
        }

        // wrappers: bind child; nothing else to do
        Plan::Filter {
            input,
            expr,
            project_prefix_len,
        } => {
            let child = bind(db, *input)?;
            let bound_expr = bind_bool_expr(&expr, child.schema())?;
            let mut plan = Plan::Filter {
                input: Box::new(child),
                expr: bound_expr,
                project_prefix_len: None,
            };
            if let Some(n) = project_prefix_len {
                let schema = plan.schema().clone();
                let fields = schema.fields[..n].to_vec();
                let proj_exprs = (0..n)
                    .map(|i| (Expr::Column(i), fields[i].name.clone()))
                    .collect();
                plan = Plan::Projection {
                    input: Box::new(plan),
                    exprs: proj_exprs,
                    schema: Schema { fields },
                };
            }
            Ok(plan)
        }
        Plan::Update {
            table,
            sets,
            filter,
        } => {
            let schema_name = table.schema.as_deref().unwrap_or("public");
            let tm = db
                .resolve_table(schema_name, &table.name)
                .map_err(|e| fe(e.to_string()))?;
            let schema = Schema {
                fields: tm
                    .columns
                    .iter()
                    .map(|c| Field {
                        name: c.name.clone(),
                        data_type: c.data_type.clone(),
                    })
                    .collect(),
            };
            let mut bound_sets = Vec::with_capacity(sets.len());
            for set in sets {
                match set {
                    UpdateSet::ByIndex(idx, expr) => {
                        let bound_expr = bind_scalar_expr(&expr, &schema, None)?;
                        bound_sets.push(UpdateSet::ByIndex(idx, bound_expr));
                    }
                    UpdateSet::ByName(name, expr) => {
                        let idx = tm
                            .columns
                            .iter()
                            .position(|c| c.name == name)
                            .ok_or_else(|| fe(format!("unknown column in UPDATE: {name}")))?;
                        let bound_expr = bind_scalar_expr(&expr, &schema, None)?;
                        bound_sets.push(UpdateSet::ByIndex(idx, bound_expr));
                    }
                }
            }
            let bound_filter = match filter {
                Some(f) => Some(bind_bool_expr(&f, &schema)?),
                None => None,
            };
            Ok(Plan::Update {
                table,
                sets: bound_sets,
                filter: bound_filter,
            })
        }
        Plan::Delete { table, filter } => {
            let schema_name = table.schema.as_deref().unwrap_or("public");
            let tm = db
                .resolve_table(schema_name, &table.name)
                .map_err(|e| fe(e.to_string()))?;
            let schema = Schema {
                fields: tm
                    .columns
                    .iter()
                    .map(|c| Field {
                        name: c.name.clone(),
                        data_type: c.data_type.clone(),
                    })
                    .collect(),
            };
            let bound_filter = match filter {
                Some(f) => Some(bind_bool_expr(&f, &schema)?),
                None => None,
            };
            Ok(Plan::Delete {
                table,
                filter: bound_filter,
            })
        }
        Plan::Order { input, keys } => {
            let child = bind(db, *input)?;
            Ok(Plan::Order {
                input: Box::new(child),
                keys,
            })
        }
        Plan::Limit { input, limit } => {
            let child = bind(db, *input)?;
            Ok(Plan::Limit {
                input: Box::new(child),
                limit,
            })
        }

        other => Ok(other),
    }
}

fn bind_bool_expr(expr: &BoolExpr, schema: &Schema) -> pgwire::error::PgWireResult<BoolExpr> {
    Ok(match expr {
        BoolExpr::Literal(b) => BoolExpr::Literal(*b),
        BoolExpr::Comparison { lhs, op, rhs } => {
            let mut lhs_bound = bind_scalar_expr(lhs, schema, None)?;
            let mut rhs_bound = bind_scalar_expr(rhs, schema, None)?;
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
                .map(|e| bind_bool_expr(e, schema))
                .collect::<pgwire::error::PgWireResult<Vec<_>>>()?,
        ),
        BoolExpr::Or(exprs) => BoolExpr::Or(
            exprs
                .iter()
                .map(|e| bind_bool_expr(e, schema))
                .collect::<pgwire::error::PgWireResult<Vec<_>>>()?,
        ),
        BoolExpr::Not(inner) => BoolExpr::Not(Box::new(bind_bool_expr(inner, schema)?)),
        BoolExpr::IsNull { expr, negated } => BoolExpr::IsNull {
            expr: bind_scalar_expr(expr, schema, None)?,
            negated: *negated,
        },
    })
}

fn bind_scalar_expr(
    expr: &ScalarExpr,
    schema: &Schema,
    hint: Option<&DataType>,
) -> pgwire::error::PgWireResult<ScalarExpr> {
    Ok(match expr {
        ScalarExpr::Literal(v) => ScalarExpr::Literal(v.clone()),
        ScalarExpr::Column(name) => {
            let idx = schema
                .fields
                .iter()
                .position(|f| f.name == *name)
                .ok_or_else(|| fe(format!("unknown column: {}", name)))?;
            ScalarExpr::ColumnIdx(idx)
        }
        ScalarExpr::ColumnIdx(i) => ScalarExpr::ColumnIdx(*i),
        ScalarExpr::Param { idx, ty } => ScalarExpr::Param {
            idx: *idx,
            ty: ty.clone().or_else(|| hint.cloned()),
        },
    })
}

fn scalar_expr_type(expr: &ScalarExpr, schema: &Schema) -> Option<DataType> {
    match expr {
        ScalarExpr::ColumnIdx(i) => Some(schema.field(*i).data_type.clone()),
        ScalarExpr::Param { ty, .. } => ty.clone(),
        ScalarExpr::Literal(v) => match v {
            Value::Int64(_) => Some(DataType::Int8),
            Value::Float64Bits(_) => Some(DataType::Float8),
            Value::Text(_) => Some(DataType::Text),
            Value::Bool(_) => Some(DataType::Bool),
            Value::Date(_) => Some(DataType::Date),
            Value::TimestampMicros(_) => Some(DataType::Timestamp),
            Value::Bytes(_) => Some(DataType::Bytea),
            Value::Null => None,
        },
        _ => None,
    }
}

fn apply_param_hint(expr: &mut ScalarExpr, hint: Option<&DataType>) {
    if let (ScalarExpr::Param { ty, .. }, Some(dt)) = (expr, hint) {
        if ty.is_none() {
            *ty = Some(dt.clone());
        }
    }
}
