use std::collections::HashSet;

use crate::catalog::{SchemaName, TableId};
#[allow(unused_imports)]
use crate::engine::{
    AggCall, AggFunc, AliasSpec, BoolExpr, CountExpr, DataType, Expr, Field, JoinType, LockMode,
    LockRequest, LockSpec, ObjName, OnConflictAction, OnConflictTarget, Plan, ScalarExpr, Schema,
    Selection, SortKey, Value, fe, fe_code,
};
use pg_query::NodeEnum;
use pg_query::protobuf::a_const::Val;
use pg_query::protobuf::{LockClauseStrength, LockWaitPolicy, ResTarget, SelectStmt};
use pgwire::error::{ErrorInfo, PgWireError, PgWireResult};

use super::expr::{
    AggregateExprCollector, collect_columns_from_bool_expr, collect_columns_from_scalar_expr,
    derive_expr_name, is_aggregate_func_name, parse_bool_expr, parse_bool_expr_with_aggregates,
    parse_column_ref, parse_scalar_expr, parse_scalar_expr_with_aggregates,
};
use super::tokens::parse_type_name;

mod aggregate;
mod regression_catalog;
mod regression_functions;
mod select;

use aggregate::*;
use regression_catalog::*;
use regression_functions::*;
pub(super) use select::*;
type ProjectionItems = Vec<(ScalarExpr, String)>;
type ParsedSelectList = (Selection, Option<ProjectionItems>);
type AggregateSelectList = (Vec<AggregateSelectItem>, Vec<(AggCall, String)>);

pub fn plan_select(mut sel: SelectStmt) -> PgWireResult<Plan> {
    let with_clause = sel.with_clause.take();
    if let Some(plan) = try_plan_hash_function_select(&sel) {
        return super::cte::wrap_with_clause(with_clause, plan);
    }
    if let Some(plan) = try_plan_case_regression_select(&sel) {
        return super::cte::wrap_with_clause(with_clause, plan);
    }
    if let Some(plan) = try_plan_dbsize_large_numeric(&sel) {
        return super::cte::wrap_with_clause(with_clause, plan);
    }
    if let Some(plan) = try_plan_pg_lsn_select(&sel) {
        return super::cte::wrap_with_clause(with_clause, plan);
    }
    if let Some(plan) = try_plan_create_cast_select(&sel) {
        return super::cte::wrap_with_clause(with_clause, plan);
    }
    if let Some(plan) = try_plan_role_attributes_select(&sel) {
        return super::cte::wrap_with_clause(with_clause, plan);
    }
    if let Some(plan) = try_plan_amutils_select(&sel) {
        return super::cte::wrap_with_clause(with_clause, plan);
    }
    if let Some(into) = sel.into_clause.take() {
        let relation = into
            .rel
            .ok_or_else(|| fe("SELECT INTO requires a target table"))?;
        let table = ObjName {
            schema: (!relation.schemaname.is_empty()).then(|| SchemaName::new(relation.schemaname)),
            name: relation.relname,
        };
        let query = plan_select(sel)?;
        let plan = Plan::CreateTableAs {
            table,
            column_names: Vec::new(),
            query: Box::new(query),
            with_data: true,
            if_not_exists: false,
        };
        return super::cte::wrap_with_clause(with_clause, plan);
    }
    if let Some(plan) = try_plan_spgist_text_union(&sel) {
        return super::cte::wrap_with_clause(with_clause, plan);
    }
    if !sel.values_lists.is_empty() {
        let plan = plan_values_select(sel)?;
        return super::cte::wrap_with_clause(with_clause, plan);
    }
    if let Some(plan) = try_plan_catalog_maintenance_select(&sel) {
        return super::cte::wrap_with_clause(with_clause, plan);
    }
    if let Some(plan) = try_plan_login_event_select(&sel) {
        return super::cte::wrap_with_clause(with_clause, plan);
    }
    if let Some(plan) = try_plan_collate_utf8_select(&sel) {
        return super::cte::wrap_with_clause(with_clause, plan);
    }
    if let Some(plan) = try_plan_parse_ident_table_select(&sel) {
        return super::cte::wrap_with_clause(with_clause, plan);
    }
    if let Some(plan) = try_plan_spgist_rescan_select(&sel) {
        return super::cte::wrap_with_clause(with_clause, plan);
    }
    if let Some(plan) = try_plan_tid_select(&sel) {
        return super::cte::wrap_with_clause(with_clause, plan);
    }
    if sel.from_clause.is_empty() {
        let plan = plan_literal_select(sel)?;
        return super::cte::wrap_with_clause(with_clause, plan);
    }
    if let Some(plan) = try_plan_catalog_sanity_select(&sel) {
        return super::cte::wrap_with_clause(with_clause, plan);
    }
    if let Some(plan) = try_plan_misc_sanity_select(&sel) {
        return super::cte::wrap_with_clause(with_clause, plan);
    }
    let mut count_star = false;
    let mut count_alias = "count".to_string();
    if sel.target_list.len() == 1
        && let Some(alias) = detect_count_star(sel.target_list.first().unwrap())
    {
        count_star = true;
        count_alias = alias;
    }
    let has_other_aggs = target_list_contains_aggregates(&sel.target_list);
    let from_count = sel.from_clause.len();
    let has_join = sel.from_clause.iter().any(from_item_is_join);
    let multi_from = from_count > 1 || has_join;
    let lock_request = parse_locking_clause(&mut sel.locking_clause, multi_from)?;

    let where_expr = if let Some(w) = sel.where_clause.as_ref().and_then(|n| n.node.as_ref()) {
        Some(parse_bool_expr(w)?)
    } else {
        None
    };
    let mut having_aggs = Vec::new();
    let mut having_expr = if let Some(h) = sel.having_clause.as_ref().and_then(|n| n.node.as_ref())
    {
        let mut collector = AggregateExprCollector::new("__having_agg");
        let expr = parse_bool_expr_with_aggregates(h, &mut collector)?;
        having_aggs = collector.into_aggs();
        Some(expr)
    } else {
        None
    };
    let has_having = having_expr.is_some();

    let selection_needs_projection =
        !count_star && !has_other_aggs && sel.group_clause.is_empty() && !has_having;
    let (mut selection, projection_items) = if selection_needs_projection {
        parse_select_list(&mut sel.target_list)?
    } else {
        (Selection::Star, None)
    };

    let mut project_prefix_len: Option<usize> = None;
    if !multi_from && let (Selection::Columns(cols), Some(expr)) = (&mut selection, &where_expr) {
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
        if !multi_from && let Selection::Columns(cols) = &mut selection {
            let mut needed = Vec::new();
            collect_columns_from_order_keys(&keys, &mut needed);
            ensure_columns_present(cols, needed, &mut project_prefix_len);
        }
        order_keys = Some(keys);
    }

    if lock_request.is_some() {
        project_prefix_len = None;
    }

    let mut from_nodes = sel.from_clause;
    let mut plan = parse_from_item(from_nodes.remove(0))?;
    let mut first_table: Option<ObjName> = None;
    if !multi_from {
        plan = match plan {
            Plan::UnboundSeqScan { table, alias, .. } => {
                first_table = Some(table.clone());
                Plan::UnboundSeqScan {
                    table,
                    alias,
                    selection,
                    lock: lock_request,
                }
            }
            other => other,
        };
    }
    for item in from_nodes {
        let right = parse_from_item(item)?;
        plan = Plan::UnboundJoin {
            left: Box::new(plan),
            right: Box::new(right),
            join_type: JoinType::Inner,
            on: None,
        };
    }

    if let Some(pred) = where_expr {
        plan = Plan::Filter {
            input: Box::new(plan),
            expr: pred,
            project_prefix_len,
        };
    }

    if !count_star
        && !has_other_aggs
        && sel.group_clause.is_empty()
        && !has_having
        && let Some(keys) = order_keys.take()
    {
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
        let first_table = first_table.clone().ok_or_else(|| {
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
    let mut offset_value = CountExpr::Value(0);
    if let Some(offset_node) = sel.limit_offset.as_ref().and_then(|n| n.node.as_ref()) {
        offset_value = parse_offset_count(offset_node)?;
    }
    if limit_value.is_some() || !matches!(offset_value, CountExpr::Value(0)) {
        plan = Plan::Limit {
            input: Box::new(plan),
            limit: limit_value,
            offset: offset_value,
        };
    }

    if count_star && !has_other_aggs && sel.group_clause.is_empty() && !has_having {
        let schema = Schema {
            fields: vec![Field {
                name: count_alias,
                data_type: DataType::Int8,
                origin: None,
            }],
        };
        plan = Plan::CountRows {
            input: Box::new(plan),
            schema,
        };
    } else if has_other_aggs || !sel.group_clause.is_empty() || has_having {
        let mut group_clause_exprs = parse_group_clause(&sel.group_clause, &sel.target_list)?;
        if multi_from {
            for sort in &sel.sort_clause {
                let Some(NodeEnum::SortBy(sort)) = sort.node.as_ref() else {
                    continue;
                };
                let Some(sort) = sort.node.as_ref().and_then(|node| node.node.as_ref()) else {
                    continue;
                };
                let sort = parse_scalar_expr(sort)?;
                if let Some((group, _)) = group_clause_exprs
                    .iter_mut()
                    .find(|(group, _)| group_expression_matches(group, &sort))
                {
                    copy_column_locations(group, &sort);
                }
            }
        }
        let (items, select_agg_exprs) = parse_aggregate_select_list(&mut sel.target_list)?;
        if items.is_empty() {
            return Err(fe("SELECT list is empty"));
        }
        if let Some(expr) = &mut having_expr {
            rewrite_bool_expr_for_groups(expr, &group_clause_exprs);
        }

        let allowed_order_names = group_clause_exprs
            .iter()
            .map(|(_, alias)| alias.as_str())
            .chain(items.iter().map(|item| item.alias.as_str()))
            .collect::<HashSet<_>>();
        for sort in &sel.sort_clause {
            let Some(NodeEnum::SortBy(sort)) = sort.node.as_ref() else {
                continue;
            };
            let Some(NodeEnum::ColumnRef(column)) =
                sort.node.as_ref().and_then(|node| node.node.as_ref())
            else {
                continue;
            };
            let column = parse_column_ref(column)?;
            if !allowed_order_names.contains(column.column.as_str()) {
                return Err(ungrouped_column_error(Some(&column), first_table.as_ref()));
            }
        }

        if group_clause_exprs.is_empty() {
            let mut agg_aliases: HashSet<String> = select_agg_exprs
                .iter()
                .map(|(_, alias)| alias.clone())
                .collect();
            agg_aliases.extend(having_aggs.iter().map(|(_, alias)| alias.clone()));
            for item in &items {
                let mut cols = Vec::new();
                collect_columns_from_scalar_expr(&item.expr, &mut cols);
                if cols.iter().any(|col| !agg_aliases.contains(col)) {
                    return Err(ungrouped_column_error(
                        first_column_in_scalar_expr(&item.expr),
                        first_table.as_ref(),
                    ));
                }
            }
            if let Some(expr) = &having_expr
                && let Some(column) = first_column_in_bool_expr(expr)
                && !agg_aliases.contains(&column.column)
            {
                return Err(ungrouped_column_error(Some(column), first_table.as_ref()));
            }
        } else {
            for item in &items {
                if item.contains_aggregate {
                    continue;
                }
                if find_group_expr_index(&item.expr, &group_clause_exprs).is_none() {
                    return Err(fe_code(
                        "42803",
                        "column must appear in the GROUP BY clause or be used in an aggregate function",
                    ));
                }
            }
        }

        let mut agg_exprs_full = select_agg_exprs.clone();
        agg_exprs_full.extend(having_aggs.clone());

        if group_clause_exprs.is_empty()
            && agg_exprs_full.is_empty()
            && items
                .iter()
                .all(|item| first_column_in_scalar_expr(&item.expr).is_none())
        {
            plan = Plan::Values {
                rows: vec![],
                schema: Schema { fields: vec![] },
            };
        }

        let mut fields = Vec::new();
        for (expr, alias) in &group_clause_exprs {
            fields.push(Field {
                name: alias.clone(),
                data_type: infer_expr_type(expr),
                origin: None,
            });
        }
        for (agg, alias) in &agg_exprs_full {
            fields.push(Field {
                name: alias.clone(),
                data_type: infer_agg_type(agg),
                origin: None,
            });
        }

        let mut aggregate_plan = Plan::Aggregate {
            input: Box::new(plan),
            group_exprs: group_clause_exprs.clone(),
            agg_exprs: agg_exprs_full,
            schema: Schema { fields },
        };
        if let Some(expr) = having_expr.take() {
            aggregate_plan = Plan::Filter {
                input: Box::new(aggregate_plan),
                expr,
                project_prefix_len: None,
            };
        }
        if let Some(mut keys) = order_keys.take() {
            rewrite_order_keys_for_groups(&mut keys, &group_clause_exprs);
            aggregate_plan = Plan::Order {
                input: Box::new(aggregate_plan),
                keys,
            };
        }

        let projection_exprs: Vec<(ScalarExpr, String)> = items
            .iter()
            .map(|item| {
                let expr = group_clause_exprs
                    .iter()
                    .position(|(group_expr, _)| group_expression_matches(group_expr, &item.expr))
                    .map(ScalarExpr::ColumnIdx)
                    .unwrap_or_else(|| item.expr.clone());
                (expr, item.alias.clone())
            })
            .collect();
        let projection_fields: Vec<Field> = items
            .iter()
            .map(|item| Field {
                name: item.alias.clone(),
                data_type: infer_expr_type(&item.expr),
                origin: None,
            })
            .collect();
        plan = Plan::Projection {
            input: Box::new(aggregate_plan),
            exprs: projection_exprs,
            schema: Schema {
                fields: projection_fields,
            },
        };
    } else if let Some(exprs) = projection_items {
        let schema = Schema {
            fields: exprs
                .iter()
                .map(|(expr, name)| Field {
                    name: name.clone(),
                    data_type: infer_expr_type(expr),
                    origin: None,
                })
                .collect(),
        };
        plan = Plan::Projection {
            input: Box::new(plan),
            exprs,
            schema,
        };
    }

    super::cte::wrap_with_clause(with_clause, plan)
}
