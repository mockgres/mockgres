use std::collections::{HashMap, HashSet};

use crate::catalog::{SchemaId, TableMeta};
use crate::db::Db;
use crate::engine::{
    AggCall, AggFunc, DataType, DbDdlKind, Field, FieldOrigin, InsertSource, LockSpec, ObjName,
    OnConflictAction, Plan, ScalarExpr, Schema, Selection, SortKey, SqlError, UpdateSet, fe,
    fe_code,
};
use crate::session::Session;
use anyhow::Error;
use pgwire::error::{PgWireError, PgWireResult};

mod expr;
mod returning;
mod time;

use self::expr::{
    bind_bool_expr, bind_bool_expr_allow_excluded, bind_scalar_expr,
    bind_scalar_expr_allow_excluded, scalar_expr_type,
};
use self::returning::bind_returning_clause;
pub(super) use self::time::{BindTimeContext, bind_time_scalar_func};

#[derive(Clone)]
struct CteBinding {
    schema: Schema,
}

type CteScope = HashMap<String, CteBinding>;

pub fn bind(db: &Db, session: &Session, p: Plan) -> PgWireResult<Plan> {
    let search_path = session.search_path();
    let current_database = session.database_name();
    let time_ctx =
        BindTimeContext::new(session.statement_time_micros(), session.txn_start_micros());
    let cte_scope = CteScope::new();
    bind_with_search_path(
        db,
        &search_path,
        current_database.as_deref(),
        time_ctx,
        &cte_scope,
        p,
    )
}

fn bind_with_search_path(
    db: &Db,
    search_path: &[SchemaId],
    current_database: Option<&str>,
    time_ctx: BindTimeContext,
    cte_scope: &CteScope,
    p: Plan,
) -> PgWireResult<Plan> {
    match p {
        Plan::Empty => Ok(Plan::Empty),
        Plan::With { ctes, body } => {
            let mut scoped = cte_scope.clone();
            let mut pending = ctes;
            let mut bound_ctes = Vec::with_capacity(pending.len());
            while !pending.is_empty() {
                let unresolved_names: HashSet<String> =
                    pending.iter().map(|cte| cte.name.clone()).collect();
                let mut next_pending = Vec::new();
                let mut made_progress = false;
                for mut cte in pending {
                    match bind_with_search_path(
                        db,
                        search_path,
                        current_database,
                        time_ctx,
                        &scoped,
                        *cte.plan.clone(),
                    ) {
                        Ok(bound_plan) => {
                            let mut output_schema = bound_plan.schema().clone();
                            if let Some(output_columns) = &cte.output_columns {
                                if output_columns.len() != output_schema.fields.len() {
                                    return Err(fe_code(
                                        "42601",
                                        format!(
                                            "CTE \"{}\" has {} columns but {} column aliases were provided",
                                            cte.name,
                                            output_schema.fields.len(),
                                            output_columns.len()
                                        ),
                                    ));
                                }
                                for (field, alias) in
                                    output_schema.fields.iter_mut().zip(output_columns)
                                {
                                    field.name = alias.clone();
                                }
                            }
                            cte.plan = Box::new(bound_plan);
                            cte.schema = Some(output_schema.clone());
                            scoped.insert(
                                cte.name.clone(),
                                CteBinding {
                                    schema: output_schema,
                                },
                            );
                            bound_ctes.push(cte);
                            made_progress = true;
                        }
                        Err(err)
                            if should_defer_cte_binding(&err, &unresolved_names, &cte.name) =>
                        {
                            next_pending.push(cte);
                        }
                        Err(err) => return Err(err),
                    }
                }
                if !made_progress {
                    return Err(fe_code(
                        "0A000",
                        "circular CTE dependencies are not supported",
                    ));
                }
                pending = next_pending;
            }
            let bound_body =
                bind_with_search_path(db, search_path, current_database, time_ctx, &scoped, *body)?;
            Ok(Plan::With {
                ctes: bound_ctes,
                body: Box::new(bound_body),
            })
        }
        Plan::UnboundSeqScan {
            mut table,
            alias,
            selection,
            lock,
        } => {
            if table.schema.is_none()
                && let Some(cte) = cte_scope.get(&table.name)
            {
                let cte_schema = &cte.schema;
                let base_origin = Some(FieldOrigin {
                    schema: None,
                    table: Some(table.name.clone()),
                    alias: alias.clone(),
                });
                let cols: Vec<(usize, Field)> = match selection {
                    Selection::Star => cte_schema
                        .fields
                        .iter()
                        .enumerate()
                        .map(|(i, c)| {
                            (
                                i,
                                Field {
                                    name: c.name.clone(),
                                    data_type: c.data_type.clone(),
                                    origin: base_origin.clone(),
                                },
                            )
                        })
                        .collect(),
                    Selection::Columns(names) => {
                        let mut out = Vec::with_capacity(names.len());
                        for n in names {
                            let i = cte_schema
                                .fields
                                .iter()
                                .position(|c| c.name == n)
                                .ok_or_else(|| fe_code("42703", format!("unknown column: {n}")))?;
                            if out.iter().any(|(existing_idx, _)| *existing_idx == i) {
                                continue;
                            }
                            out.push((
                                i,
                                Field {
                                    name: n,
                                    data_type: cte_schema.fields[i].data_type.clone(),
                                    origin: base_origin.clone(),
                                },
                            ));
                        }
                        out
                    }
                };
                let schema = Schema {
                    fields: cols.iter().map(|(_, f)| f.clone()).collect(),
                };
                return Ok(Plan::CteScan {
                    name: table.name,
                    cols,
                    schema,
                });
            }
            let tm = resolve_table_meta(db, search_path, &table).map_err(map_catalog_err)?;
            if table.schema.is_none() {
                table.schema = Some(tm.schema.clone());
            }

            let schema_name = tm.schema.as_str().to_string();
            let table_name = tm.name.clone();
            let alias_clone = alias.clone();
            let base_origin = Some(FieldOrigin {
                schema: Some(schema_name),
                table: Some(table_name),
                alias: alias_clone,
            });

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
                                origin: base_origin.clone(),
                            },
                        )
                    })
                    .collect(),
                Selection::Columns(names) => {
                    let mut out = Vec::with_capacity(names.len());
                    for n in names {
                        let i = tm
                            .columns
                            .iter()
                            .position(|c| c.name == n)
                            .ok_or_else(|| fe_code("42703", format!("unknown column: {n}")))?;
                        if out.iter().any(|(existing_idx, _)| *existing_idx == i) {
                            continue;
                        }
                        out.push((
                            i,
                            Field {
                                name: n,
                                data_type: tm.columns[i].data_type.clone(),
                                origin: base_origin.clone(),
                            },
                        ));
                    }
                    out
                }
            };
            let mut schema = Schema {
                fields: cols.iter().map(|(_, f)| f.clone()).collect(),
            };
            if lock.is_some() {
                schema.fields.push(Field {
                    name: "__mockgres_row_id".to_string(),
                    data_type: DataType::Int8,
                    origin: None,
                });
            }
            let lock = lock.map(|req| LockSpec {
                mode: req.mode,
                skip_locked: req.skip_locked,
                nowait: req.nowait,
                target: tm.id,
            });

            Ok(Plan::SeqScan {
                table,
                cols,
                schema,
                lock,
            })
        }
        Plan::Alias {
            input,
            alias,
            schema: _,
        } => {
            let child = bind_with_search_path(
                db,
                search_path,
                current_database,
                time_ctx,
                cte_scope,
                *input,
            )?;
            let mut fields = Vec::with_capacity(child.schema().fields.len());
            let mut exprs = Vec::with_capacity(child.schema().fields.len());
            for (idx, f) in child.schema().fields.iter().enumerate() {
                fields.push(Field {
                    name: f.name.clone(),
                    data_type: f.data_type.clone(),
                    origin: Some(FieldOrigin {
                        schema: None,
                        table: None,
                        alias: Some(alias.alias.clone()),
                    }),
                });
                exprs.push((ScalarExpr::ColumnIdx(idx), f.name.clone()));
            }
            Ok(Plan::Projection {
                input: Box::new(child),
                exprs,
                schema: Schema { fields },
            })
        }
        Plan::UnboundJoin {
            left,
            right,
            join_type,
            on,
        } => {
            let left_bound = bind_with_search_path(
                db,
                search_path,
                current_database,
                time_ctx,
                cte_scope,
                *left,
            )?;
            let right_bound = bind_with_search_path(
                db,
                search_path,
                current_database,
                time_ctx,
                cte_scope,
                *right,
            )?;
            let mut fields = left_bound.schema().fields.clone();
            fields.extend(right_bound.schema().fields.clone());
            let schema = Schema { fields };
            let bound_on = if let Some(expr) = on.as_ref() {
                Some(bind_bool_expr(
                    expr,
                    &schema,
                    db,
                    search_path,
                    current_database,
                    time_ctx,
                    cte_scope,
                )?)
            } else {
                None
            };
            Ok(Plan::Join {
                left: Box::new(left_bound),
                right: Box::new(right_bound),
                on: bound_on,
                join_type,
                schema,
            })
        }

        Plan::Projection {
            input,
            exprs,
            schema: _,
        } => {
            let child = bind_with_search_path(
                db,
                search_path,
                current_database,
                time_ctx,
                cte_scope,
                *input,
            )?;
            let mut bound_exprs = Vec::with_capacity(exprs.len());
            let mut fields = Vec::with_capacity(exprs.len());
            for (expr, name) in exprs {
                let bound = bind_scalar_expr(
                    &expr,
                    child.schema(),
                    None,
                    db,
                    search_path,
                    current_database,
                    time_ctx,
                )?;
                let dt = scalar_expr_type(&bound, child.schema()).unwrap_or(DataType::Text);
                fields.push(Field {
                    name: name.clone(),
                    data_type: dt,
                    origin: None,
                });
                bound_exprs.push((bound, name.clone()));
            }
            Ok(Plan::Projection {
                input: Box::new(child),
                exprs: bound_exprs,
                schema: Schema { fields },
            })
        }

        Plan::Aggregate {
            input,
            group_exprs,
            agg_exprs,
            schema: _,
        } => {
            let child = bind_with_search_path(
                db,
                search_path,
                current_database,
                time_ctx,
                cte_scope,
                *input,
            )?;
            let child_schema = child.schema().clone();
            let mut bound_group_exprs = Vec::with_capacity(group_exprs.len());
            let mut fields = Vec::new();
            for (expr, name) in group_exprs {
                let bound = bind_scalar_expr(
                    &expr,
                    &child_schema,
                    None,
                    db,
                    search_path,
                    current_database,
                    time_ctx,
                )?;
                let dt = scalar_expr_type(&bound, &child_schema).unwrap_or(DataType::Text);
                let origin = if let ScalarExpr::ColumnIdx(idx) = &bound {
                    child_schema.fields.get(*idx).and_then(|f| f.origin.clone())
                } else {
                    None
                };
                fields.push(Field {
                    name: name.clone(),
                    data_type: dt.clone(),
                    origin,
                });
                bound_group_exprs.push((bound, name));
            }

            let mut bound_agg_exprs = Vec::with_capacity(agg_exprs.len());
            for (agg_call, name) in agg_exprs {
                let bound_expr = if let Some(expr) = agg_call.expr {
                    Some(bind_scalar_expr(
                        &expr,
                        &child_schema,
                        None,
                        db,
                        search_path,
                        current_database,
                        time_ctx,
                    )?)
                } else {
                    None
                };
                let dt = match agg_call.func {
                    AggFunc::Count => DataType::Int8,
                    AggFunc::Sum => match bound_expr
                        .as_ref()
                        .and_then(|e| scalar_expr_type(e, &child_schema))
                    {
                        Some(DataType::Float8) => DataType::Float8,
                        Some(DataType::Int4) | Some(DataType::Int8) => DataType::Int8,
                        _ => DataType::Float8,
                    },
                    AggFunc::Avg => DataType::Float8,
                    AggFunc::Min | AggFunc::Max => bound_expr
                        .as_ref()
                        .and_then(|e| scalar_expr_type(e, &child_schema))
                        .unwrap_or(DataType::Text),
                };
                fields.push(Field {
                    name: name.clone(),
                    data_type: dt.clone(),
                    origin: None,
                });
                bound_agg_exprs.push((
                    AggCall {
                        func: agg_call.func,
                        expr: bound_expr,
                    },
                    name,
                ));
            }

            Ok(Plan::Aggregate {
                input: Box::new(child),
                group_exprs: bound_group_exprs,
                agg_exprs: bound_agg_exprs,
                schema: Schema { fields },
            })
        }

        Plan::CountRows { input, schema } => {
            let child = bind_with_search_path(
                db,
                search_path,
                current_database,
                time_ctx,
                cte_scope,
                *input,
            )?;
            Ok(Plan::CountRows {
                input: Box::new(child),
                schema,
            })
        }
        Plan::LockRows {
            mut table,
            input,
            lock,
            row_id_idx: _,
            schema: _,
        } => {
            let bound_child = bind_with_search_path(
                db,
                search_path,
                current_database,
                time_ctx,
                cte_scope,
                *input,
            )?;
            if matches!(bound_child, Plan::CteScan { .. }) {
                return Ok(bound_child);
            }
            let child_schema = bound_child.schema().clone();
            if child_schema.fields.is_empty() {
                return Err(fe("FOR UPDATE requires row identifier column"));
            }
            let row_id_idx = child_schema.fields.len() - 1;
            let mut visible_fields = child_schema.fields.clone();
            visible_fields.remove(row_id_idx);
            let schema = Schema {
                fields: visible_fields,
            };
            let tm = resolve_table_meta(db, search_path, &table).map_err(map_catalog_err)?;
            if table.schema.is_none() {
                table.schema = Some(tm.schema.clone());
            }
            Ok(Plan::LockRows {
                table,
                input: Box::new(bound_child),
                lock: LockSpec {
                    mode: lock.mode,
                    skip_locked: lock.skip_locked,
                    nowait: lock.nowait,
                    target: tm.id,
                },
                row_id_idx,
                schema,
            })
        }

        // wrappers: bind child; nothing else to do
        Plan::Filter {
            input,
            expr,
            project_prefix_len,
        } => {
            let child = bind_with_search_path(
                db,
                search_path,
                current_database,
                time_ctx,
                cte_scope,
                *input,
            )?;
            let bound_expr = bind_bool_expr(
                &expr,
                child.schema(),
                db,
                search_path,
                current_database,
                time_ctx,
                cte_scope,
            )?;
            let mut plan = Plan::Filter {
                input: Box::new(child),
                expr: bound_expr,
                project_prefix_len: None,
            };
            if let Some(n) = project_prefix_len {
                if n == 0 {
                    return Ok(plan);
                }
                let schema = plan.schema().clone();
                let fields = schema.fields[..n].to_vec();
                let proj_exprs = (0..n)
                    .map(|i| (ScalarExpr::ColumnIdx(i), fields[i].name.clone()))
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
            mut table,
            table_alias,
            sets,
            filter,
            from,
            mut from_schema,
            mut returning,
            returning_schema: _,
        } => {
            let tm = resolve_table_meta(db, search_path, &table).map_err(map_catalog_err)?;
            if table.schema.is_none() {
                table.schema = Some(tm.schema.clone());
            }
            let schema_origin = Some(FieldOrigin {
                schema: Some(tm.schema.as_str().to_string()),
                table: Some(tm.name.clone()),
                alias: table_alias.clone(),
            });
            let target_schema = Schema {
                fields: tm
                    .columns
                    .iter()
                    .map(|c| Field {
                        name: c.name.clone(),
                        data_type: c.data_type.clone(),
                        origin: schema_origin.clone(),
                    })
                    .collect(),
            };
            let (bound_from, combined_schema) = if let Some(plan) = from {
                let bound = bind_with_search_path(
                    db,
                    search_path,
                    current_database,
                    time_ctx,
                    cte_scope,
                    *plan,
                )?;
                let mut fields = target_schema.fields.clone();
                fields.extend(bound.schema().fields.clone());
                from_schema = Some(bound.schema().clone());
                (Some(bound), Schema { fields })
            } else {
                (None, target_schema.clone())
            };
            let bound_sets = bind_update_sets(
                sets,
                &combined_schema,
                tm,
                db,
                search_path,
                current_database,
                time_ctx,
                false,
            )?;
            let bound_filter = match filter {
                Some(f) => Some(bind_bool_expr(
                    &f,
                    &combined_schema,
                    db,
                    search_path,
                    current_database,
                    time_ctx,
                    cte_scope,
                )?),
                None => None,
            };
            let returning_schema = match returning.as_mut() {
                Some(clause) => Some(bind_returning_clause(
                    clause,
                    &combined_schema,
                    db,
                    search_path,
                    current_database,
                    time_ctx,
                )?),
                None => None,
            };
            Ok(Plan::Update {
                table,
                table_alias,
                sets: bound_sets,
                filter: bound_filter,
                from: bound_from.map(Box::new),
                from_schema,
                returning,
                returning_schema,
            })
        }
        Plan::Delete {
            mut table,
            filter,
            mut returning,
            returning_schema: _,
        } => {
            let tm = resolve_table_meta(db, search_path, &table).map_err(map_catalog_err)?;
            if table.schema.is_none() {
                table.schema = Some(tm.schema.clone());
            }
            let schema_origin = Some(FieldOrigin {
                schema: Some(tm.schema.as_str().to_string()),
                table: Some(tm.name.clone()),
                alias: None,
            });
            let schema = Schema {
                fields: tm
                    .columns
                    .iter()
                    .map(|c| Field {
                        name: c.name.clone(),
                        data_type: c.data_type.clone(),
                        origin: schema_origin.clone(),
                    })
                    .collect(),
            };
            let bound_filter = match filter {
                Some(f) => Some(bind_bool_expr(
                    &f,
                    &schema,
                    db,
                    search_path,
                    current_database,
                    time_ctx,
                    cte_scope,
                )?),
                None => None,
            };
            let returning_schema = match returning.as_mut() {
                Some(clause) => Some(bind_returning_clause(
                    clause,
                    &schema,
                    db,
                    search_path,
                    current_database,
                    time_ctx,
                )?),
                None => None,
            };
            Ok(Plan::Delete {
                table,
                filter: bound_filter,
                returning,
                returning_schema,
            })
        }
        Plan::Order { input, keys } => {
            let child = bind_with_search_path(
                db,
                search_path,
                current_database,
                time_ctx,
                cte_scope,
                *input,
            )?;
            let child_schema = child.schema().clone();
            let mut bound_keys = Vec::with_capacity(keys.len());
            for key in keys {
                match key {
                    SortKey::ByName {
                        col,
                        asc,
                        nulls_first,
                    } => {
                        let idx = child_schema
                            .fields
                            .iter()
                            .position(|f| f.name == col)
                            .ok_or_else(|| fe_code("42703", format!("unknown column: {col}")))?;
                        bound_keys.push(SortKey::ByIndex {
                            idx,
                            asc,
                            nulls_first,
                        });
                    }
                    SortKey::Expr {
                        expr,
                        asc,
                        nulls_first,
                    } => {
                        let bound = bind_scalar_expr(
                            &expr,
                            &child_schema,
                            None,
                            db,
                            search_path,
                            current_database,
                            time_ctx,
                        )?;
                        bound_keys.push(SortKey::Expr {
                            expr: bound,
                            asc,
                            nulls_first,
                        });
                    }
                    other => bound_keys.push(other),
                }
            }
            Ok(Plan::Order {
                input: Box::new(child),
                keys: bound_keys,
            })
        }
        Plan::Limit {
            input,
            limit,
            offset,
        } => {
            let child = bind_with_search_path(
                db,
                search_path,
                current_database,
                time_ctx,
                cte_scope,
                *input,
            )?;
            Ok(Plan::Limit {
                input: Box::new(child),
                limit,
                offset,
            })
        }
        Plan::CallBuiltin { name, args, schema } => Ok(Plan::CallBuiltin { name, args, schema }),
        Plan::InsertValues {
            mut table,
            columns,
            rows,
            override_system_value,
            on_conflict,
            mut returning,
            returning_schema: _,
        } => {
            let tm = resolve_table_meta(db, search_path, &table).map_err(map_catalog_err)?;
            if table.schema.is_none() {
                table.schema = Some(tm.schema.clone());
            }
            let schema_origin = Some(FieldOrigin {
                schema: Some(tm.schema.as_str().to_string()),
                table: Some(tm.name.clone()),
                alias: None,
            });
            let table_schema = Schema {
                fields: tm
                    .columns
                    .iter()
                    .map(|c| Field {
                        name: c.name.clone(),
                        data_type: c.data_type.clone(),
                        origin: schema_origin.clone(),
                    })
                    .collect(),
            };
            let bound_on_conflict = match on_conflict {
                Some(OnConflictAction::DoUpdate {
                    target,
                    sets,
                    where_clause,
                }) => {
                    let bound_sets = bind_update_sets(
                        sets,
                        &table_schema,
                        tm,
                        db,
                        search_path,
                        current_database,
                        time_ctx,
                        true,
                    )?;
                    let bound_where = where_clause
                        .map(|c| {
                            bind_bool_expr_allow_excluded(
                                &c,
                                &table_schema,
                                db,
                                search_path,
                                current_database,
                                time_ctx,
                                cte_scope,
                            )
                        })
                        .transpose()?;
                    Some(OnConflictAction::DoUpdate {
                        target,
                        sets: bound_sets,
                        where_clause: bound_where,
                    })
                }
                Some(OnConflictAction::DoNothing { target }) => {
                    Some(OnConflictAction::DoNothing { target })
                }
                None => None,
            };
            let column_positions = if let Some(cols) = &columns {
                let mut seen = HashSet::new();
                let mut positions = Vec::with_capacity(cols.len());
                for col in cols {
                    let idx = tm
                        .columns
                        .iter()
                        .position(|c| c.name == *col)
                        .ok_or_else(|| fe_code("42703", format!("unknown column: {col}")))?;
                    if !seen.insert(idx) {
                        return Err(fe_code("42701", format!("column {col} specified twice")));
                    }
                    positions.push(idx);
                }
                positions
            } else {
                (0..table_schema.len()).collect()
            };
            let expected_len = column_positions.len();
            let mut bound_rows = Vec::with_capacity(rows.len());
            for row in rows {
                if row.len() != expected_len {
                    let msg = if columns.is_some() {
                        format!(
                            "INSERT has {} target columns but {} expressions",
                            expected_len,
                            row.len()
                        )
                    } else {
                        format!(
                            "INSERT expects {} expressions, got {}",
                            expected_len,
                            row.len()
                        )
                    };
                    return Err(fe_code("21P01", msg));
                }
                let mut bound_row = Vec::with_capacity(row.len());
                for (expr_idx, src) in row.into_iter().enumerate() {
                    let target_idx = column_positions[expr_idx];
                    match src {
                        InsertSource::Default => bound_row.push(InsertSource::Default),
                        InsertSource::Expr(expr) => {
                            let field = table_schema.field(target_idx);
                            let hint = match field.data_type {
                                DataType::Int4
                                | DataType::Int8
                                | DataType::Float8
                                | DataType::Bool
                                | DataType::Date
                                | DataType::Timestamp
                                | DataType::Timestamptz
                                | DataType::Bytea => Some(&field.data_type),
                                _ => None,
                            };
                            let bound = bind_scalar_expr(
                                &expr,
                                &table_schema,
                                hint,
                                db,
                                search_path,
                                current_database,
                                time_ctx,
                            )?;
                            bound_row.push(InsertSource::Expr(bound));
                        }
                    }
                }
                bound_rows.push(bound_row);
            }
            let returning_schema = match returning.as_mut() {
                Some(clause) => Some(bind_returning_clause(
                    clause,
                    &table_schema,
                    db,
                    search_path,
                    current_database,
                    time_ctx,
                )?),
                None => None,
            };
            Ok(Plan::InsertValues {
                table,
                columns,
                rows: bound_rows,
                override_system_value,
                on_conflict: bound_on_conflict,
                returning,
                returning_schema,
            })
        }
        Plan::InsertSelect {
            mut table,
            columns,
            select,
            override_system_value,
            on_conflict,
            mut returning,
            returning_schema: _,
        } => {
            let tm = resolve_table_meta(db, search_path, &table).map_err(map_catalog_err)?;
            if table.schema.is_none() {
                table.schema = Some(tm.schema.clone());
            }
            let schema_origin = Some(FieldOrigin {
                schema: Some(tm.schema.as_str().to_string()),
                table: Some(tm.name.clone()),
                alias: None,
            });
            let table_schema = Schema {
                fields: tm
                    .columns
                    .iter()
                    .map(|c| Field {
                        name: c.name.clone(),
                        data_type: c.data_type.clone(),
                        origin: schema_origin.clone(),
                    })
                    .collect(),
            };
            let bound_on_conflict = match on_conflict {
                Some(OnConflictAction::DoUpdate {
                    target,
                    sets,
                    where_clause,
                }) => {
                    let bound_sets = bind_update_sets(
                        sets,
                        &table_schema,
                        tm,
                        db,
                        search_path,
                        current_database,
                        time_ctx,
                        true,
                    )?;
                    let bound_where = where_clause
                        .map(|c| {
                            bind_bool_expr_allow_excluded(
                                &c,
                                &table_schema,
                                db,
                                search_path,
                                current_database,
                                time_ctx,
                                cte_scope,
                            )
                        })
                        .transpose()?;
                    Some(OnConflictAction::DoUpdate {
                        target,
                        sets: bound_sets,
                        where_clause: bound_where,
                    })
                }
                Some(OnConflictAction::DoNothing { target }) => {
                    Some(OnConflictAction::DoNothing { target })
                }
                None => None,
            };
            let bound_select = bind_with_search_path(
                db,
                search_path,
                current_database,
                time_ctx,
                cte_scope,
                *select,
            )?;
            let expected_len = columns.as_ref().map_or(table_schema.len(), Vec::len);
            let actual_len = bound_select.schema().len();
            if actual_len != expected_len {
                let msg = if columns.is_some() {
                    format!(
                        "INSERT has {} target columns but {} expressions",
                        expected_len, actual_len
                    )
                } else {
                    format!(
                        "INSERT expects {} expressions, got {}",
                        expected_len, actual_len
                    )
                };
                return Err(fe_code("21P01", msg));
            }
            let returning_schema = match returning.as_mut() {
                Some(clause) => Some(bind_returning_clause(
                    clause,
                    &table_schema,
                    db,
                    search_path,
                    current_database,
                    time_ctx,
                )?),
                None => None,
            };
            Ok(Plan::InsertSelect {
                table,
                columns,
                select: Box::new(bound_select),
                override_system_value,
                on_conflict: bound_on_conflict,
                returning,
                returning_schema,
            })
        }
        Plan::CreateDatabase { name } => Ok(Plan::UnsupportedDbDDL {
            kind: DbDdlKind::Create,
            name,
        }),
        Plan::DropDatabase { name } => Ok(Plan::UnsupportedDbDDL {
            kind: DbDdlKind::Drop,
            name,
        }),
        Plan::AlterDatabase { name } => Ok(Plan::UnsupportedDbDDL {
            kind: DbDdlKind::Alter,
            name,
        }),
        Plan::UnsupportedDbDDL { .. } => Ok(p),

        other => Ok(other),
    }
}

fn resolve_table_meta<'a>(
    db: &'a Db,
    search_path: &[SchemaId],
    table: &ObjName,
) -> anyhow::Result<&'a crate::catalog::TableMeta> {
    if let Some(schema) = &table.schema {
        db.resolve_table(schema.as_str(), &table.name)
    } else {
        db.resolve_table_in_search_path(search_path, &table.name)
    }
}

pub(super) fn current_schema_name(db: &Db, search_path: &[SchemaId]) -> String {
    for schema_id in search_path {
        if let Some(entry) = db.catalog.schemas.get(schema_id) {
            return entry.name.as_str().to_string();
        }
    }
    "public".to_string()
}

pub(super) fn schema_names_for_path(db: &Db, search_path: &[SchemaId]) -> Vec<String> {
    let mut names = Vec::new();
    for schema_id in search_path {
        if let Some(entry) = db.catalog.schemas.get(schema_id) {
            names.push(entry.name.as_str().to_string());
        }
    }
    if names.is_empty() {
        names.push("public".to_string());
    }
    names
}

fn map_catalog_err(err: Error) -> PgWireError {
    if let Some(sql) = err.downcast_ref::<SqlError>() {
        fe_code(sql.code, sql.message.clone())
    } else {
        fe(err.to_string())
    }
}

fn should_defer_cte_binding(
    err: &PgWireError,
    unresolved_names: &HashSet<String>,
    cte_name: &str,
) -> bool {
    let msg = err.to_string();
    if !msg.contains("42P01") && !msg.contains("no such table") {
        return false;
    }
    unresolved_names
        .iter()
        .filter(|name| name.as_str() != cte_name)
        .any(|name| cte_name_appears_in_relation_error(&msg, name))
        || cte_name_appears_in_relation_error(&msg, cte_name)
}

fn cte_name_appears_in_relation_error(msg: &str, cte_name: &str) -> bool {
    msg.contains(&format!("no such table {cte_name}"))
        || msg.contains(&format!("no such table public.{cte_name}"))
        || msg.contains(&format!(".{cte_name}"))
}

#[allow(clippy::too_many_arguments)]
fn bind_update_sets(
    sets: Vec<UpdateSet>,
    schema: &Schema,
    tm: &TableMeta,
    db: &Db,
    search_path: &[SchemaId],
    current_database: Option<&str>,
    time_ctx: BindTimeContext,
    allow_excluded: bool,
) -> PgWireResult<Vec<UpdateSet>> {
    let mut bound_sets = Vec::with_capacity(sets.len());
    for set in sets {
        match set {
            UpdateSet::ByIndex(idx, expr) => {
                let hint = schema.field(idx).data_type.clone();
                let bound_expr = if allow_excluded {
                    bind_scalar_expr_allow_excluded(
                        &expr,
                        schema,
                        Some(&hint),
                        db,
                        search_path,
                        current_database,
                        time_ctx,
                    )?
                } else {
                    bind_scalar_expr(
                        &expr,
                        schema,
                        Some(&hint),
                        db,
                        search_path,
                        current_database,
                        time_ctx,
                    )?
                };
                bound_sets.push(UpdateSet::ByIndex(idx, bound_expr));
            }
            UpdateSet::ByName(name, expr) => {
                let idx = tm
                    .columns
                    .iter()
                    .position(|c| c.name == name)
                    .ok_or_else(|| fe_code("42703", format!("unknown column in UPDATE: {name}")))?;
                let hint = schema.field(idx).data_type.clone();
                let bound_expr = if allow_excluded {
                    bind_scalar_expr_allow_excluded(
                        &expr,
                        schema,
                        Some(&hint),
                        db,
                        search_path,
                        current_database,
                        time_ctx,
                    )?
                } else {
                    bind_scalar_expr(
                        &expr,
                        schema,
                        Some(&hint),
                        db,
                        search_path,
                        current_database,
                        time_ctx,
                    )?
                };
                bound_sets.push(UpdateSet::ByIndex(idx, bound_expr));
            }
        }
    }
    if bound_sets.is_empty() {
        return Err(fe("UPDATE requires SET clauses"));
    }
    Ok(bound_sets)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::Db;
    use crate::engine::DataType;
    use crate::session::Session;
    use crate::sql::Planner;

    fn make_session(db: &Db) -> Session {
        let session = Session::new(7);
        let public_id = db.catalog.schema_id("public").expect("public schema");
        session.set_search_path(vec![public_id]);
        session
    }

    fn contains_cte_scan(plan: &Plan, name: &str) -> bool {
        match plan {
            Plan::CteScan {
                name: scan_name, ..
            } => scan_name == name,
            Plan::With { ctes, body } => {
                ctes.iter().any(|cte| contains_cte_scan(&cte.plan, name))
                    || contains_cte_scan(body, name)
            }
            Plan::Projection { input, .. }
            | Plan::Filter { input, .. }
            | Plan::Order { input, .. }
            | Plan::Limit { input, .. }
            | Plan::CountRows { input, .. }
            | Plan::LockRows { input, .. }
            | Plan::Alias { input, .. }
            | Plan::Aggregate { input, .. } => contains_cte_scan(input, name),
            Plan::Join { left, right, .. } | Plan::UnboundJoin { left, right, .. } => {
                contains_cte_scan(left, name) || contains_cte_scan(right, name)
            }
            Plan::Update { from, .. } => from
                .as_ref()
                .is_some_and(|from_plan| contains_cte_scan(from_plan, name)),
            Plan::InsertSelect { select, .. } => contains_cte_scan(select, name),
            _ => false,
        }
    }

    fn contains_seq_scan(plan: &Plan, table_name: &str) -> bool {
        match plan {
            Plan::SeqScan { table, .. } => table.name == table_name,
            Plan::With { ctes, body } => {
                ctes.iter()
                    .any(|cte| contains_seq_scan(&cte.plan, table_name))
                    || contains_seq_scan(body, table_name)
            }
            Plan::Projection { input, .. }
            | Plan::Filter { input, .. }
            | Plan::Order { input, .. }
            | Plan::Limit { input, .. }
            | Plan::CountRows { input, .. }
            | Plan::LockRows { input, .. }
            | Plan::Alias { input, .. }
            | Plan::Aggregate { input, .. } => contains_seq_scan(input, table_name),
            Plan::Join { left, right, .. } | Plan::UnboundJoin { left, right, .. } => {
                contains_seq_scan(left, table_name) || contains_seq_scan(right, table_name)
            }
            Plan::Update { from, .. } => from
                .as_ref()
                .is_some_and(|from_plan| contains_seq_scan(from_plan, table_name)),
            Plan::InsertSelect { select, .. } => contains_seq_scan(select, table_name),
            _ => false,
        }
    }

    #[test]
    fn later_ctes_can_reference_earlier_ctes() {
        let db = Db::default();
        let session = make_session(&db);
        let plan = Planner::plan_sql(
            "with first as (select 1 as id), second as (select id from first) select id from second",
        )
        .expect("plan");
        let bound = bind(&db, &session, plan).expect("bind");
        assert!(contains_cte_scan(&bound, "first"));
        assert!(contains_cte_scan(&bound, "second"));
    }

    #[test]
    fn earlier_ctes_can_reference_later_ctes() {
        let db = Db::default();
        let session = make_session(&db);
        let plan = Planner::plan_sql(
            "with second as (select id from first), first as (select 1 as id) select id from second",
        )
        .expect("plan");
        let bound = bind(&db, &session, plan).expect("bind");
        assert!(contains_cte_scan(&bound, "first"));
        assert!(contains_cte_scan(&bound, "second"));
    }

    #[test]
    fn cte_scope_is_statement_local() {
        let db = Db::default();
        let session = make_session(&db);
        let with_plan = Planner::plan_sql("with scoped as (select 1 as id) select id from scoped")
            .expect("plan");
        bind(&db, &session, with_plan).expect("bind with cte");

        let plain_plan = Planner::plan_sql("select id from scoped").expect("plan plain");
        let err = bind(&db, &session, plain_plan).expect_err("expected unknown table");
        assert!(
            err.to_string().contains("no such table"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn cte_name_shadows_catalog_table_with_same_name() {
        let mut db = Db::default();
        let public_id = db.catalog.schema_id("public").expect("public schema");
        db.create_table(
            "public",
            "dupe",
            vec![("id".to_string(), DataType::Int4, true, None, None)],
            None,
            Vec::new(),
            &[public_id],
        )
        .expect("create table");
        let session = make_session(&db);

        let with_plan =
            Planner::plan_sql("with dupe as (select 1 as id) select id from dupe").expect("plan");
        let bound_with = bind(&db, &session, with_plan).expect("bind");
        assert!(contains_cte_scan(&bound_with, "dupe"));

        let plain_plan = Planner::plan_sql("select id from dupe").expect("plan plain");
        let bound_plain = bind(&db, &session, plain_plan).expect("bind plain");
        assert!(contains_seq_scan(&bound_plain, "dupe"));
    }

    #[test]
    fn cte_column_alias_count_must_match_projection_width() {
        let db = Db::default();
        let session = make_session(&db);
        let plan = Planner::plan_sql("with c(a, b) as (select 1) select a from c").expect("plan");
        let err = bind(&db, &session, plan).expect_err("expected alias mismatch error");
        assert!(
            err.to_string()
                .contains("CTE \"c\" has 1 columns but 2 column aliases were provided"),
            "unexpected error: {err}"
        );
    }
}
