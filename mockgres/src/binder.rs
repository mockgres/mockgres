use std::collections::HashSet;

use crate::catalog::SchemaId;
use crate::db::Db;
use crate::engine::{
    AggCall, AggFunc, BoolExpr, ColumnRefName, DataType, DbDdlKind, Field, FieldOrigin,
    InsertSource, LockSpec, ObjName, Plan, ReturningClause, ReturningExpr, ScalarBinaryOp,
    ScalarExpr, ScalarFunc, Schema, Selection, SortKey, SqlError, UpdateSet, Value, fe, fe_code,
};
use crate::session::{Session, now_utc_micros};
use crate::types::timestamp_micros_to_date_days;
use anyhow::Error;
use pgwire::error::{PgWireError, PgWireResult};

#[derive(Clone, Copy)]
struct BindTimeContext {
    statement_time_micros: Option<i64>,
    txn_start_micros: Option<i64>,
}

impl BindTimeContext {
    fn new(statement_time_micros: Option<i64>, txn_start_micros: Option<i64>) -> Self {
        Self {
            statement_time_micros,
            txn_start_micros,
        }
    }

    fn statement_or_now(&self) -> i64 {
        self.statement_time_micros.unwrap_or_else(now_utc_micros)
    }

    fn txn_or_statement_or_now(&self) -> i64 {
        self.txn_start_micros
            .or(self.statement_time_micros)
            .unwrap_or_else(now_utc_micros)
    }
}

pub fn bind(db: &Db, session: &Session, p: Plan) -> PgWireResult<Plan> {
    let search_path = session.search_path();
    let current_database = session.database_name();
    let time_ctx =
        BindTimeContext::new(session.statement_time_micros(), session.txn_start_micros());
    bind_with_search_path(db, &search_path, current_database.as_deref(), time_ctx, p)
}

fn bind_with_search_path(
    db: &Db,
    search_path: &[SchemaId],
    current_database: Option<&str>,
    time_ctx: BindTimeContext,
    p: Plan,
) -> PgWireResult<Plan> {
    match p {
        Plan::UnboundSeqScan {
            mut table,
            alias,
            selection,
            lock,
        } => {
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
        Plan::UnboundJoin { left, right } => {
            let left_bound =
                bind_with_search_path(db, search_path, current_database, time_ctx, *left)?;
            let right_bound =
                bind_with_search_path(db, search_path, current_database, time_ctx, *right)?;
            let mut fields = left_bound.schema().fields.clone();
            fields.extend(right_bound.schema().fields.clone());
            Ok(Plan::Join {
                left: Box::new(left_bound),
                right: Box::new(right_bound),
                schema: Schema { fields },
            })
        }

        Plan::Projection {
            input,
            exprs,
            schema: _,
        } => {
            let child = bind_with_search_path(db, search_path, current_database, time_ctx, *input)?;
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
            let child =
                bind_with_search_path(db, search_path, current_database, time_ctx, *input)?;
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
                    child_schema
                        .fields
                        .get(*idx)
                        .and_then(|f| f.origin.clone())
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
            let child = bind_with_search_path(db, search_path, current_database, time_ctx, *input)?;
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
            let bound_child =
                bind_with_search_path(db, search_path, current_database, time_ctx, *input)?;
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
            let child = bind_with_search_path(db, search_path, current_database, time_ctx, *input)?;
            let bound_expr = bind_bool_expr(
                &expr,
                child.schema(),
                db,
                search_path,
                current_database,
                time_ctx,
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
            sets,
            filter,
            mut returning,
            mut returning_schema,
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
            let mut bound_sets = Vec::with_capacity(sets.len());
            for set in sets {
                match set {
                    UpdateSet::ByIndex(idx, expr) => {
                        let hint = schema.field(idx).data_type.clone();
                        let bound_expr = bind_scalar_expr(
                            &expr,
                            &schema,
                            Some(&hint),
                            db,
                            search_path,
                            current_database,
                            time_ctx,
                        )?;
                        bound_sets.push(UpdateSet::ByIndex(idx, bound_expr));
                    }
                    UpdateSet::ByName(name, expr) => {
                        let idx =
                            tm.columns
                                .iter()
                                .position(|c| c.name == name)
                                .ok_or_else(|| {
                                    fe_code("42703", format!("unknown column in UPDATE: {name}"))
                                })?;
                        let hint = schema.field(idx).data_type.clone();
                        let bound_expr = bind_scalar_expr(
                            &expr,
                            &schema,
                            Some(&hint),
                            db,
                            search_path,
                            current_database,
                            time_ctx,
                        )?;
                        bound_sets.push(UpdateSet::ByIndex(idx, bound_expr));
                    }
                }
            }
            let bound_filter = match filter {
                Some(f) => Some(bind_bool_expr(
                    &f,
                    &schema,
                    db,
                    search_path,
                    current_database,
                    time_ctx,
                )?),
                None => None,
            };
            returning_schema = match returning.as_mut() {
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
            Ok(Plan::Update {
                table,
                sets: bound_sets,
                filter: bound_filter,
                returning,
                returning_schema,
            })
        }
        Plan::Delete {
            mut table,
            filter,
            mut returning,
            mut returning_schema,
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
                )?),
                None => None,
            };
            returning_schema = match returning.as_mut() {
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
            let child = bind_with_search_path(db, search_path, current_database, time_ctx, *input)?;
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
            let child = bind_with_search_path(db, search_path, current_database, time_ctx, *input)?;
            Ok(Plan::Limit {
                input: Box::new(child),
                limit,
                offset,
            })
        }
        Plan::InsertValues {
            mut table,
            columns,
            rows,
            override_system_value,
            mut returning,
            mut returning_schema,
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
            returning_schema = match returning.as_mut() {
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

fn current_schema_name(db: &Db, search_path: &[SchemaId]) -> String {
    for schema_id in search_path {
        if let Some(entry) = db.catalog.schemas.get(schema_id) {
            return entry.name.as_str().to_string();
        }
    }
    "public".to_string()
}

fn schema_names_for_path(db: &Db, search_path: &[SchemaId]) -> Vec<String> {
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

fn bind_returning_clause(
    clause: &mut ReturningClause,
    schema: &Schema,
    db: &Db,
    search_path: &[SchemaId],
    current_database: Option<&str>,
    time_ctx: BindTimeContext,
) -> pgwire::error::PgWireResult<Schema> {
    let mut expanded = Vec::new();
    for item in clause.exprs.drain(..) {
        match item {
            ReturningExpr::Star => {
                for field in &schema.fields {
                    expanded.push(ReturningExpr::Expr {
                        expr: ScalarExpr::Column(ColumnRefName {
                            schema: None,
                            relation: None,
                            column: field.name.clone(),
                        }),
                        alias: field.name.clone(),
                    });
                }
            }
            ReturningExpr::Expr { expr, alias } => {
                expanded.push(ReturningExpr::Expr { expr, alias });
            }
        }
    }
    clause.exprs = expanded;
    let mut fields = Vec::with_capacity(clause.exprs.len());
    for item in clause.exprs.iter_mut() {
        if let ReturningExpr::Expr { expr, alias } = item {
            let bound = bind_scalar_expr(
                expr,
                schema,
                None,
                db,
                search_path,
                current_database,
                time_ctx,
            )?;
            let dt = scalar_expr_type(&bound, schema).unwrap_or(DataType::Text);
            fields.push(Field {
                name: alias.clone(),
                data_type: dt,
                origin: None,
            });
            *expr = bound;
        }
    }
    Ok(Schema { fields })
}

fn bind_bool_expr(
    expr: &BoolExpr,
    schema: &Schema,
    db: &Db,
    search_path: &[SchemaId],
    current_database: Option<&str>,
    time_ctx: BindTimeContext,
) -> pgwire::error::PgWireResult<BoolExpr> {
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
                .collect::<pgwire::error::PgWireResult<Vec<_>>>()?,
        ),
        BoolExpr::Or(exprs) => BoolExpr::Or(
            exprs
                .iter()
                .map(|e| bind_bool_expr(e, schema, db, search_path, current_database, time_ctx))
                .collect::<pgwire::error::PgWireResult<Vec<_>>>()?,
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

fn bind_scalar_expr(
    expr: &ScalarExpr,
    schema: &Schema,
    hint: Option<&DataType>,
    db: &Db,
    search_path: &[SchemaId],
    current_database: Option<&str>,
    time_ctx: BindTimeContext,
) -> pgwire::error::PgWireResult<ScalarExpr> {
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
                .collect::<pgwire::error::PgWireResult<Vec<_>>>()?;
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
                ScalarFunc::Now | ScalarFunc::CurrentTimestamp => {
                    if !bound_args.is_empty() {
                        return Err(fe("current_timestamp does not take arguments"));
                    }
                    ScalarExpr::Literal(Value::TimestamptzMicros(
                        time_ctx.txn_or_statement_or_now(),
                    ))
                }
                ScalarFunc::StatementTimestamp => {
                    if !bound_args.is_empty() {
                        return Err(fe("statement_timestamp() takes no arguments"));
                    }
                    ScalarExpr::Literal(Value::TimestamptzMicros(time_ctx.statement_or_now()))
                }
                ScalarFunc::TransactionTimestamp => {
                    if !bound_args.is_empty() {
                        return Err(fe("transaction_timestamp() takes no arguments"));
                    }
                    ScalarExpr::Literal(Value::TimestamptzMicros(
                        time_ctx.txn_or_statement_or_now(),
                    ))
                }
                ScalarFunc::ClockTimestamp => {
                    if !bound_args.is_empty() {
                        return Err(fe("clock_timestamp() takes no arguments"));
                    }
                    ScalarExpr::Literal(Value::TimestamptzMicros(now_utc_micros()))
                }
                ScalarFunc::CurrentDate => {
                    if !bound_args.is_empty() {
                        return Err(fe("current_date takes no arguments"));
                    }
                    let micros = time_ctx.statement_or_now();
                    let days = timestamp_micros_to_date_days(micros).map_err(fe)?;
                    ScalarExpr::Literal(Value::Date(days))
                }
                _ => ScalarExpr::Func {
                    func: *func,
                    args: bound_args,
                },
            }
        }
    })
}

fn resolve_column_reference(schema: &Schema, colref: &ColumnRefName) -> PgWireResult<usize> {
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

fn column_ref_matches(field: &Field, colref: &ColumnRefName) -> bool {
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

fn scalar_expr_type(expr: &ScalarExpr, schema: &Schema) -> Option<DataType> {
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

fn apply_param_hint(expr: &mut ScalarExpr, hint: Option<&DataType>) {
    if let (ScalarExpr::Param { ty, .. }, Some(dt)) = (expr, hint) {
        if ty.is_none() {
            *ty = Some(dt.clone());
        }
    }
}

fn map_catalog_err(err: Error) -> PgWireError {
    if let Some(sql) = err.downcast_ref::<SqlError>() {
        fe_code(sql.code, sql.message.clone())
    } else {
        fe(err.to_string())
    }
}
