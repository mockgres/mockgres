use crate::catalog::{SchemaName, TableId};
use crate::engine::{
    BoolExpr, DataType, Field, InsertSource, LockMode, LockRequest, LockSpec, ObjName, Plan,
    ReturningClause, ReturningExpr, ScalarExpr, Schema, Selection, SortKey, UpdateSet, Value, fe,
    fe_code,
};
use pg_query::NodeEnum;
use pg_query::protobuf::a_const::Val;
use pg_query::protobuf::{
    DeleteStmt, InsertStmt, LockClauseStrength, LockWaitPolicy, ResTarget, SelectStmt, UpdateStmt,
};
use pgwire::error::PgWireResult;

use super::expr::{
    collect_columns_from_bool_expr, collect_columns_from_scalar_expr, derive_expr_name,
    last_colref_name, parse_bool_expr, parse_scalar_expr,
};

pub fn plan_select(mut sel: SelectStmt) -> PgWireResult<Plan> {
    if sel.from_clause.is_empty() {
        return plan_literal_select(sel);
    }
    let mut count_star = false;
    let mut count_alias = "count".to_string();
    if sel.target_list.len() == 1 {
        if let Some(alias) = detect_count_star(sel.target_list.first().unwrap()) {
            count_star = true;
            count_alias = alias;
        }
    }
    let from_count = sel.from_clause.len();
    let has_join = sel.from_clause.iter().any(|node| from_item_is_join(node));
    let multi_from = from_count > 1 || has_join;
    let lock_request = parse_locking_clause(&mut sel.locking_clause, multi_from)?;

    let (mut selection, projection_items) = if count_star {
        (Selection::Star, None)
    } else {
        parse_select_list(&mut sel.target_list)?
    };

    let mut where_expr = if let Some(w) = sel.where_clause.as_ref().and_then(|n| n.node.as_ref()) {
        Some(parse_bool_expr(w)?)
    } else {
        None
    };

    let mut project_prefix_len: Option<usize> = None;
    if !multi_from {
        if let (Selection::Columns(cols), Some(expr)) = (&mut selection, &where_expr) {
            let mut needed = Vec::new();
            collect_columns_from_bool_expr(expr, &mut needed);
            ensure_columns_present(cols, needed, &mut project_prefix_len);
        }
    }

    let mut order_keys: Option<Vec<SortKey>> = None;
    if !sel.sort_clause.is_empty() {
        let mut keys = parse_order_clause(&sel.sort_clause)?;
        if let Some(items) = &projection_items {
            rewrite_order_keys_for_projection(&mut keys, items);
        }
        if !multi_from {
            if let Selection::Columns(cols) = &mut selection {
                let mut needed = Vec::new();
                collect_columns_from_order_keys(&keys, &mut needed);
                ensure_columns_present(cols, needed, &mut project_prefix_len);
            }
        }
        order_keys = Some(keys);
    }

    if lock_request.is_some() {
        project_prefix_len = None;
    }

    let mut from_nodes = sel.from_clause;
    let (mut plan, mut accumulated_on) = parse_from_item(from_nodes.remove(0))?;
    let mut first_table: Option<ObjName> = None;
    if !multi_from {
        plan = match plan {
            Plan::UnboundSeqScan { table, .. } => {
                first_table = Some(table.clone());
                Plan::UnboundSeqScan {
                    table,
                    selection,
                    lock: lock_request,
                }
            }
            other => other,
        };
    }
    for item in from_nodes {
        let (right, on) = parse_from_item(item)?;
        plan = Plan::UnboundJoin {
            left: Box::new(plan),
            right: Box::new(right),
        };
        if let Some(p) = on {
            accumulated_on = combine_bool_exprs(accumulated_on, p);
        }
    }

    if let Some(onp) = accumulated_on {
        where_expr = match where_expr {
            None => Some(onp),
            Some(existing) => Some(BoolExpr::And(vec![existing, onp])),
        };
    }

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

    if lock_request.is_some() && count_star {
        return Err(fe_code(
            "0A000",
            "FOR UPDATE with aggregates is not supported",
        ));
    }

    if let Some(req) = lock_request {
        let first_table = first_table.ok_or_else(|| {
            fe_code(
                "0A000",
                "FOR UPDATE is only supported for single-table SELECT statements",
            )
        })?;
        plan = Plan::LockRows {
            table: first_table,
            input: Box::new(plan),
            lock: LockSpec {
                mode: req.mode,
                skip_locked: req.skip_locked,
                nowait: req.nowait,
                target: TableId {
                    schema_id: 0,
                    rel_id: 0,
                },
            },
            row_id_idx: 0,
            schema: Schema { fields: vec![] },
        };
    }

    let mut limit_value = None;
    if let Some(limit_node) = sel.limit_count.as_ref().and_then(|n| n.node.as_ref()) {
        limit_value = Some(parse_limit_count(limit_node)?);
    }
    let mut offset_value = 0usize;
    if let Some(offset_node) = sel.limit_offset.as_ref().and_then(|n| n.node.as_ref()) {
        offset_value = parse_offset_count(offset_node)?;
    }
    if limit_value.is_some() || offset_value != 0 {
        plan = Plan::Limit {
            input: Box::new(plan),
            limit: limit_value,
            offset: offset_value,
        };
    }

    if count_star {
        let schema = Schema {
            fields: vec![Field {
                name: count_alias,
                data_type: DataType::Int8,
            }],
        };
        plan = Plan::CountRows {
            input: Box::new(plan),
            schema,
        };
    } else if let Some(exprs) = projection_items {
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

fn parse_locking_clause(
    locking_clause: &mut Vec<pg_query::Node>,
    multi_from: bool,
) -> PgWireResult<Option<LockRequest>> {
    if locking_clause.is_empty() {
        return Ok(None);
    }
    if multi_from {
        return Err(fe_code(
            "0A000",
            "FOR UPDATE is only supported for single-table SELECT statements",
        ));
    }
    if locking_clause.len() != 1 {
        return Err(fe_code(
            "0A000",
            "only one locking clause is supported per SELECT",
        ));
    }
    let clause_node = locking_clause
        .remove(0)
        .node
        .ok_or_else(|| fe("missing locking clause"))?;
    let NodeEnum::LockingClause(clause) = clause_node else {
        return Err(fe("malformed locking clause"));
    };
    if !clause.locked_rels.is_empty() {
        return Err(fe_code(
            "0A000",
            "locking specific relations is not supported",
        ));
    }
    let strength =
        LockClauseStrength::try_from(clause.strength).map_err(|_| fe("bad locking strength"))?;
    if strength != LockClauseStrength::LcsForupdate {
        return Err(fe_code("0A000", "only FOR UPDATE is supported"));
    }
    let wait_policy =
        LockWaitPolicy::try_from(clause.wait_policy).map_err(|_| fe("bad wait policy"))?;
    let (skip_locked, nowait) = match wait_policy {
        LockWaitPolicy::LockWaitBlock | LockWaitPolicy::Undefined => (false, false),
        LockWaitPolicy::LockWaitSkip => (true, false),
        LockWaitPolicy::LockWaitError => (false, true),
    };
    Ok(Some(LockRequest {
        mode: LockMode::Update,
        skip_locked,
        nowait,
    }))
}

fn detect_count_star(node: &pg_query::Node) -> Option<String> {
    let rt = node.node.as_ref().and_then(|n| match n {
        NodeEnum::ResTarget(rt) => Some(rt),
        _ => None,
    })?;
    let expr_node = rt.val.as_ref()?.node.as_ref()?;
    let NodeEnum::FuncCall(fc) = expr_node else {
        return None;
    };
    if !fc.agg_star {
        return None;
    }
    let name = fc.funcname.iter().find_map(|n| {
        n.node.as_ref().and_then(|nn| {
            if let NodeEnum::String(s) = nn {
                Some(s.sval.to_ascii_lowercase())
            } else {
                None
            }
        })
    })?;
    if name != "count" {
        return None;
    }
    if rt.name.is_empty() {
        Some("count".into())
    } else {
        Some(rt.name.clone())
    }
}

fn plan_literal_select(sel: SelectStmt) -> PgWireResult<Plan> {
    let tl = sel.target_list;
    if tl.is_empty() {
        return Err(fe("at least one column required"));
    }
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
        let expr = parse_scalar_expr(expr_node)?;
        let name = if rt.name.is_empty() {
            derive_expr_name(&expr)
        } else {
            rt.name.clone()
        };
        out_exprs.push((expr, name));
    }
    let input = Plan::Values {
        rows: vec![vec![]],
        schema: Schema { fields: vec![] },
    };
    Ok(Plan::Projection {
        input: Box::new(input),
        exprs: out_exprs,
        schema: Schema { fields: vec![] },
    })
}

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
        returning,
        returning_schema: None,
    })
}

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

pub fn plan_delete(del: DeleteStmt) -> PgWireResult<Plan> {
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
    Ok(Plan::Delete {
        table,
        filter,
        returning,
        returning_schema: None,
    })
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

fn parse_returning_clause(
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

fn from_item_is_join(node: &pg_query::Node) -> bool {
    matches!(node.node.as_ref(), Some(NodeEnum::JoinExpr(_)))
}

fn parse_from_item(node: pg_query::Node) -> PgWireResult<(Plan, Option<BoolExpr>)> {
    use pg_query::NodeEnum;
    let n = node.node.ok_or_else(|| fe("missing FROM item"))?;
    match n {
        NodeEnum::RangeVar(rv) => {
            let schema = if rv.schemaname.is_empty() {
                None
            } else {
                Some(SchemaName::new(rv.schemaname))
            };
            let table = ObjName {
                schema,
                name: rv.relname,
            };
            Ok((
                Plan::UnboundSeqScan {
                    table,
                    selection: Selection::Star,
                    lock: None,
                },
                None,
            ))
        }
        NodeEnum::JoinExpr(j) => {
            let jt = pg_query::protobuf::JoinType::try_from(j.jointype)
                .unwrap_or(pg_query::protobuf::JoinType::JoinInner);
            if jt != pg_query::protobuf::JoinType::JoinInner {
                return Err(fe_code("0A000", "only INNER JOIN is supported"));
            }
            if !j.using_clause.is_empty() {
                return Err(fe_code(
                    "0A000",
                    "JOIN ... USING (...) is not supported yet",
                ));
            }
            let left_node = *j.larg.ok_or_else(|| fe("join missing left"))?;
            let right_node = *j.rarg.ok_or_else(|| fe("join missing right"))?;
            let (left_plan, left_on) = parse_from_item(left_node)?;
            let (right_plan, right_on) = parse_from_item(right_node)?;
            let subtree = Plan::UnboundJoin {
                left: Box::new(left_plan),
                right: Box::new(right_plan),
            };
            let on_expr_node = j.quals.and_then(|n| n.node);
            let on_bool = if let Some(nn) = on_expr_node {
                Some(parse_bool_expr(&nn)?)
            } else {
                None
            };
            let combined_on = combine_optional_bools(vec![left_on, right_on, on_bool]);
            Ok((subtree, combined_on))
        }
        _ => Err(fe("unsupported FROM item")),
    }
}

fn combine_bool_exprs(existing: Option<BoolExpr>, next: BoolExpr) -> Option<BoolExpr> {
    match existing {
        None => Some(next),
        Some(prev) => Some(and_chain(vec![prev, next])),
    }
}

fn combine_optional_bools(parts: Vec<Option<BoolExpr>>) -> Option<BoolExpr> {
    let mut exprs = Vec::new();
    for part in parts.into_iter().flatten() {
        flatten_and(part, &mut exprs);
    }
    match exprs.len() {
        0 => None,
        1 => Some(exprs.remove(0)),
        _ => Some(BoolExpr::And(exprs)),
    }
}

fn and_chain(exprs: Vec<BoolExpr>) -> BoolExpr {
    let mut flat = Vec::new();
    for expr in exprs {
        flatten_and(expr, &mut flat);
    }
    BoolExpr::And(flat)
}

fn flatten_and(expr: BoolExpr, acc: &mut Vec<BoolExpr>) {
    if let BoolExpr::And(inner) = expr {
        acc.extend(inner);
    } else {
        acc.push(expr);
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
        ScalarExpr::Column(_) => Err(fe("INSERT expressions cannot reference columns")),
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

fn rewrite_order_keys_for_projection(keys: &mut [SortKey], exprs: &[(ScalarExpr, String)]) {
    for key in keys {
        match key {
            SortKey::ByIndex {
                idx,
                asc,
                nulls_first,
            } => {
                if let Some((expr, _)) = exprs.get(*idx) {
                    *key = SortKey::Expr {
                        expr: expr.clone(),
                        asc: *asc,
                        nulls_first: *nulls_first,
                    };
                }
            }
            SortKey::ByName {
                col,
                asc,
                nulls_first,
            } => {
                if let Some((expr, _)) = exprs.iter().find(|(_, name)| name == col) {
                    *key = SortKey::Expr {
                        expr: expr.clone(),
                        asc: *asc,
                        nulls_first: *nulls_first,
                    };
                }
            }
            SortKey::Expr { .. } => {}
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
    parse_nonnegative_count(node, "limit")
}

fn parse_offset_count(node: &NodeEnum) -> PgWireResult<usize> {
    parse_nonnegative_count(node, "offset")
}

fn parse_nonnegative_count(node: &NodeEnum, label: &str) -> PgWireResult<usize> {
    match node {
        NodeEnum::AConst(c) => {
            if let Some(Val::Ival(iv)) = c.val.as_ref() {
                if iv.ival < 0 {
                    return Err(fe(format!("{label} must be non-negative")));
                }
                Ok(iv.ival as usize)
            } else {
                Err(fe(format!("{label} must be integer")))
            }
        }
        _ => Err(fe(format!("unsupported {label} expression"))),
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
        ScalarExpr::Cast { ty, .. } => ty.clone(),
        _ => DataType::Text,
    }
}
