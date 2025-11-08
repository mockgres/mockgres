use crate::engine::{
    DataType, Field, InsertSource, ObjName, Plan, ScalarExpr, Schema, Selection, SortKey,
    UpdateSet, Value, fe,
};
use pg_query::NodeEnum;
use pg_query::protobuf::a_const::Val;
use pg_query::protobuf::{DeleteStmt, InsertStmt, ResTarget, SelectStmt, UpdateStmt};
use pgwire::error::PgWireResult;

use super::expr::{
    collect_columns_from_bool_expr, collect_columns_from_scalar_expr, derive_expr_name,
    last_colref_name, parse_bool_expr, parse_scalar_expr,
};
use super::tokens::parse_numeric_const;

pub fn plan_select(mut sel: SelectStmt) -> PgWireResult<Plan> {
    if sel.from_clause.is_empty() {
        return plan_literal_select(sel);
    }
    if sel.from_clause.len() != 1 {
        return Err(fe("single table only"));
    }
    let table = parse_range_var(sel.from_clause.remove(0))?;

    let (selection, projection_items) = parse_select_list(&mut sel.target_list)?;

    let where_expr = if let Some(w) = sel.where_clause.as_ref().and_then(|n| n.node.as_ref()) {
        Some(parse_bool_expr(w)?)
    } else {
        None
    };

    let mut project_prefix_len: Option<usize> = None;
    let mut selection = selection;
    if let (Selection::Columns(cols), Some(expr)) = (&mut selection, &where_expr) {
        let mut needed = Vec::new();
        collect_columns_from_bool_expr(expr, &mut needed);
        ensure_columns_present(cols, needed, &mut project_prefix_len);
    }

    let mut order_keys: Option<Vec<SortKey>> = None;
    if !sel.sort_clause.is_empty() {
        let mut keys = parse_order_clause(&sel.sort_clause)?;
        if let Some(items) = &projection_items {
            rewrite_order_keys_for_projection(&mut keys, items);
        }
        if let Selection::Columns(cols) = &mut selection {
            let mut needed = Vec::new();
            collect_columns_from_order_keys(&keys, &mut needed);
            ensure_columns_present(cols, needed, &mut project_prefix_len);
        }
        order_keys = Some(keys);
    }

    let mut plan = Plan::UnboundSeqScan { table, selection };

    if let Some(pred) = where_expr {
        plan = Plan::Filter {
            input: Box::new(plan),
            expr: pred,
            project_prefix_len,
        };
    }

    if let Some(keys) = order_keys {
        plan = Plan::Order {
            input: Box::new(plan),
            keys,
        };
    }

    if let Some(limit_node) = sel.limit_count.as_ref().and_then(|n| n.node.as_ref()) {
        let lim = parse_limit_count(limit_node)?;
        plan = Plan::Limit {
            input: Box::new(plan),
            limit: lim,
        };
    }

    if let Some(exprs) = projection_items {
        let schema = Schema {
            fields: exprs
                .iter()
                .map(|(expr, name)| Field {
                    name: name.clone(),
                    data_type: infer_expr_type(expr),
                })
                .collect(),
        };
        plan = Plan::Projection {
            input: Box::new(plan),
            exprs,
            schema,
        };
    }

    Ok(plan)
}

fn plan_literal_select(sel: SelectStmt) -> PgWireResult<Plan> {
    let tl = sel.target_list;
    if tl.is_empty() {
        return Err(fe("at least one column required"));
    }
    let mut out_fields = Vec::with_capacity(tl.len());
    let mut out_exprs = Vec::with_capacity(tl.len());

    for t in tl {
        let tgt = t.node.as_ref().ok_or_else(|| fe("unexpected target"))?;
        let NodeEnum::ResTarget(rt) = tgt else {
            return Err(fe("unexpected target"));
        };
        let expr_node = rt
            .val
            .as_ref()
            .and_then(|n| n.node.as_ref())
            .ok_or_else(|| fe("missing expr"))?;
        let lit = parse_numeric_const(expr_node)?;
        let (dt, expr) = match lit {
            Value::Int64(i) => {
                let dt = if (i32::MIN as i64..=i32::MAX as i64).contains(&i) {
                    DataType::Int4
                } else {
                    DataType::Int8
                };
                (dt, ScalarExpr::Literal(Value::Int64(i)))
            }
            Value::Float64Bits(b) => (DataType::Float8, ScalarExpr::Literal(Value::Float64Bits(b))),
            Value::Null => return Err(fe("null not allowed here")),
            _ => return Err(fe("only numeric literals supported here")),
        };
        let name = if rt.name.is_empty() {
            "?column?".to_string()
        } else {
            rt.name.clone()
        };
        out_fields.push(Field {
            name: name.clone(),
            data_type: dt,
        });
        out_exprs.push((expr, name));
    }

    let input = Plan::Values {
        rows: vec![vec![]],
        schema: Schema { fields: vec![] },
    };
    let out_schema = Schema { fields: out_fields };
    Ok(Plan::Projection {
        input: Box::new(input),
        exprs: out_exprs,
        schema: out_schema,
    })
}

pub fn plan_insert(ins: InsertStmt) -> PgWireResult<Plan> {
    let rv = ins.relation.ok_or_else(|| fe("missing target table"))?;
    let table = ObjName {
        schema: if rv.schemaname.is_empty() {
            None
        } else {
            Some(rv.schemaname)
        },
        name: rv.relname,
    };
    let insert_columns = parse_insert_columns(&ins.cols)?;
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
    Ok(Plan::InsertValues {
        table,
        columns: insert_columns,
        rows: all_rows,
    })
}

pub fn plan_update(upd: UpdateStmt) -> PgWireResult<Plan> {
    let rv = upd.relation.ok_or_else(|| fe("missing target table"))?;
    let table = ObjName {
        schema: if rv.schemaname.is_empty() {
            None
        } else {
            Some(rv.schemaname)
        },
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
    Ok(Plan::Update {
        table,
        sets,
        filter,
    })
}

pub fn plan_delete(del: DeleteStmt) -> PgWireResult<Plan> {
    let rv = del.relation.ok_or_else(|| fe("missing target table"))?;
    let table = ObjName {
        schema: if rv.schemaname.is_empty() {
            None
        } else {
            Some(rv.schemaname)
        },
        name: rv.relname,
    };
    let filter = if let Some(w) = del.where_clause.as_ref().and_then(|n| n.node.as_ref()) {
        Some(parse_bool_expr(w)?)
    } else {
        None
    };
    Ok(Plan::Delete { table, filter })
}

fn parse_select_list(
    target_list: &mut Vec<pg_query::Node>,
) -> PgWireResult<(Selection, Option<Vec<(ScalarExpr, String)>>)> {
    if target_list.len() == 1 {
        let t = &target_list[0];
        let node = t.node.as_ref().ok_or_else(|| fe("missing target node"))?;
        if let NodeEnum::ResTarget(rt) = node {
            if let Some(NodeEnum::ColumnRef(cr)) = rt.val.as_ref().and_then(|n| n.node.as_ref()) {
                if cr
                    .fields
                    .get(0)
                    .and_then(|f| f.node.as_ref())
                    .map(|n| matches!(n, NodeEnum::AStar(_)))
                    .unwrap_or(false)
                {
                    return Ok((Selection::Star, None));
                }
            }
        }
    }

    let mut cols = Vec::new();
    let mut exprs = Vec::new();
    for t in target_list.drain(..) {
        let rt = t
            .node
            .as_ref()
            .and_then(|n| {
                if let NodeEnum::ResTarget(rt) = n {
                    Some(rt)
                } else {
                    None
                }
            })
            .ok_or_else(|| fe("bad target"))?;
        let expr_node = rt
            .val
            .as_ref()
            .and_then(|n| n.node.as_ref())
            .ok_or_else(|| fe("bad target expr"))?;
        let expr = parse_scalar_expr(expr_node)?;
        collect_columns_from_scalar_expr(&expr, &mut cols);
        let alias = if rt.name.is_empty() {
            derive_expr_name(&expr)
        } else {
            rt.name.clone()
        };
        exprs.push((expr, alias));
    }
    Ok((Selection::Columns(cols), Some(exprs)))
}

fn parse_range_var(node: pg_query::Node) -> PgWireResult<ObjName> {
    let from = node.node.ok_or_else(|| fe("missing from"))?;
    if let NodeEnum::RangeVar(rv) = from {
        Ok(ObjName {
            schema: if rv.schemaname.is_empty() {
                None
            } else {
                Some(rv.schemaname)
            },
            name: rv.relname,
        })
    } else {
        Err(fe("unsupported FROM"))
    }
}

fn parse_order_clause(clause: &[pg_query::Node]) -> PgWireResult<Vec<SortKey>> {
    let mut keys = Vec::with_capacity(clause.len());
    for sort in clause {
        let NodeEnum::SortBy(s) = sort.node.as_ref().ok_or_else(|| fe("bad order by"))? else {
            return Err(fe("bad order by node"));
        };
        let asc = match s.sortby_dir {
            1 | 2 => true,
            3 => false,
            _ => true,
        };
        let nulls_first = match s.sortby_nulls {
            2 => Some(true),
            3 => Some(false),
            _ => None,
        };
        let Some(expr) = s.node.as_ref().and_then(|n| n.node.as_ref()) else {
            return Err(fe("bad order by expr"));
        };
        let key = match expr {
            NodeEnum::AConst(ac) => {
                if let Some(Val::Ival(iv)) = ac.val.as_ref() {
                    if iv.ival <= 0 {
                        return Err(fe("order by position must be >= 1"));
                    }
                    SortKey::ByIndex {
                        idx: (iv.ival as usize) - 1,
                        asc,
                        nulls_first,
                    }
                } else {
                    return Err(fe("order by const must be integer"));
                }
            }
            NodeEnum::ColumnRef(cr) => SortKey::ByName {
                col: last_colref_name(cr)?,
                asc,
                nulls_first,
            },
            _ => {
                let expr = parse_scalar_expr(expr)?;
                SortKey::Expr {
                    expr,
                    asc,
                    nulls_first,
                }
            }
        };
        keys.push(key);
    }
    Ok(keys)
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
        ScalarExpr::Column(name) => {
            if name.eq_ignore_ascii_case("nan") {
                Ok(ScalarExpr::Literal(Value::from_f64(f64::NAN)))
            } else {
                Err(fe("INSERT expressions cannot reference columns"))
            }
        }
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
        other => Ok(other),
    }
}

fn rewrite_order_keys_for_projection(keys: &mut [SortKey], exprs: &[(ScalarExpr, String)]) {
    for key in keys {
        if let SortKey::ByIndex {
            idx,
            asc,
            nulls_first,
        } = *key
        {
            if let Some((expr, _)) = exprs.get(idx) {
                *key = SortKey::Expr {
                    expr: expr.clone(),
                    asc,
                    nulls_first,
                };
            }
        }
    }
}

fn collect_columns_from_order_keys(keys: &[SortKey], out: &mut Vec<String>) {
    for key in keys {
        match key {
            SortKey::ByName { col, .. } => out.push(col.clone()),
            SortKey::Expr { expr, .. } => collect_columns_from_scalar_expr(expr, out),
            SortKey::ByIndex { .. } => {}
        }
    }
}

fn ensure_columns_present(
    cols: &mut Vec<String>,
    needed: Vec<String>,
    project_prefix_len: &mut Option<usize>,
) {
    let start_len = cols.len();
    let mut added = false;
    for col in needed {
        if !cols.contains(&col) {
            cols.push(col);
            added = true;
        }
    }
    if project_prefix_len.is_none() && added {
        *project_prefix_len = Some(start_len);
    }
}

fn parse_limit_count(node: &NodeEnum) -> PgWireResult<usize> {
    match node {
        NodeEnum::AConst(c) => {
            if let Some(Val::Ival(iv)) = c.val.as_ref() {
                if iv.ival < 0 {
                    return Err(fe("limit must be non-negative"));
                }
                Ok(iv.ival as usize)
            } else {
                Err(fe("limit must be integer"))
            }
        }
        _ => Err(fe("unsupported limit expression")),
    }
}

fn extract_col_name(rt: &ResTarget) -> PgWireResult<String> {
    let Some(v) = rt.val.as_ref().and_then(|n| n.node.as_ref()) else {
        return Err(fe("bad column target"));
    };
    if let NodeEnum::ColumnRef(cr) = v {
        last_colref_name(cr)
    } else {
        Err(fe("only simple column names supported"))
    }
}

fn infer_expr_type(expr: &ScalarExpr) -> DataType {
    match expr {
        ScalarExpr::Literal(Value::Int64(_)) => DataType::Int8,
        ScalarExpr::Literal(Value::Float64Bits(_)) => DataType::Float8,
        ScalarExpr::Literal(Value::Text(_)) => DataType::Text,
        ScalarExpr::Literal(Value::Bool(_)) => DataType::Bool,
        ScalarExpr::Literal(Value::Date(_)) => DataType::Date,
        ScalarExpr::Literal(Value::TimestampMicros(_)) => DataType::Timestamp,
        ScalarExpr::Literal(Value::Bytes(_)) => DataType::Bytea,
        _ => DataType::Text,
    }
}
