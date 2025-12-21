use crate::engine::{
    AggCall, AggFunc, BoolExpr, CmpOp, ColumnRefName, ScalarBinaryOp, ScalarExpr, ScalarFunc,
    ScalarUnaryOp, Value, fe,
};
use pg_query::NodeEnum;
use pg_query::protobuf::{
    AArrayExpr, AExpr, AExprKind, BoolExprType, CoalesceExpr, ColumnRef, FuncCall, Node,
    NullTestType, ParamRef, SqlValueFunction, SqlValueFunctionOp,
};
use pgwire::error::PgWireResult;

use super::tokens::{const_to_value, parse_type_name, try_parse_literal};

pub fn is_aggregate_func_name(name: &str) -> bool {
    matches!(
        name.to_ascii_lowercase().as_str(),
        "count" | "sum" | "avg" | "min" | "max"
    )
}

pub fn parse_bool_expr(node: &NodeEnum) -> PgWireResult<BoolExpr> {
    parse_bool_expr_internal(node, None)
}

pub fn parse_bool_expr_with_aggregates(
    node: &NodeEnum,
    collector: &mut AggregateExprCollector,
) -> PgWireResult<BoolExpr> {
    parse_bool_expr_internal(node, Some(collector))
}

fn parse_bool_expr_internal(
    node: &NodeEnum,
    mut agg_ctx: Option<&mut AggregateExprCollector>,
) -> PgWireResult<BoolExpr> {
    match node {
        NodeEnum::BoolExpr(be) => {
            let op = BoolExprType::try_from(be.boolop).map_err(|_| fe("bad bool expr op"))?;
            let mut args = Vec::new();
            for a in &be.args {
                let n = a.node.as_ref().ok_or_else(|| fe("bad bool arg"))?;
                args.push(parse_bool_expr_internal(n, agg_ctx.as_deref_mut())?);
            }
            match op {
                BoolExprType::AndExpr => Ok(BoolExpr::And(args)),
                BoolExprType::OrExpr => Ok(BoolExpr::Or(args)),
                BoolExprType::NotExpr => {
                    if args.len() != 1 {
                        return Err(fe("NOT expects single operand"));
                    }
                    Ok(BoolExpr::Not(Box::new(args.into_iter().next().unwrap())))
                }
                BoolExprType::Undefined => Err(fe("unsupported bool op")),
            }
        }
        NodeEnum::AExpr(ax) => {
            let kind = AExprKind::try_from(ax.kind).map_err(|_| fe("unknown expression kind"))?;
            if kind == AExprKind::AexprIn {
                return parse_in_expr(ax, agg_ctx.as_deref_mut());
            }
            if kind == AExprKind::AexprOpAny {
                return parse_any_expr(ax, agg_ctx.as_deref_mut());
            }
            if ax.name.is_empty() {
                if let Some(inner) = ax.lexpr.as_ref().and_then(|n| n.node.as_ref()) {
                    return parse_bool_expr_internal(inner, agg_ctx);
                }
                return Err(fe("bad parenthesized expression"));
            }
            let op = parse_cmp_op(&ax.name)?;
            let lexpr = ax
                .lexpr
                .as_ref()
                .and_then(|n| n.node.as_ref())
                .ok_or_else(|| fe("bad where lhs"))?;
            let rexpr = ax
                .rexpr
                .as_ref()
                .and_then(|n| n.node.as_ref())
                .ok_or_else(|| fe("bad where rhs"))?;
            let lhs = parse_scalar_expr_internal(lexpr, agg_ctx.as_deref_mut())?;
            let rhs = parse_scalar_expr_internal(rexpr, agg_ctx.as_deref_mut())?;
            Ok(BoolExpr::Comparison { lhs, op, rhs })
        }
        NodeEnum::NullTest(nt) => {
            let nt_type =
                NullTestType::try_from(nt.nulltesttype).map_err(|_| fe("bad nulltest"))?;
            let arg = nt
                .arg
                .as_ref()
                .and_then(|n| n.node.as_ref())
                .ok_or_else(|| fe("bad nulltest arg"))?;
            let expr = parse_scalar_expr_internal(arg, agg_ctx)?;
            Ok(BoolExpr::IsNull {
                expr,
                negated: matches!(nt_type, NullTestType::IsNotNull),
            })
        }
        NodeEnum::SubLink(sl) => {
            let testexpr = sl
                .testexpr
                .as_ref()
                .and_then(|n| n.node.as_ref())
                .ok_or_else(|| fe("subquery requires test expression"))?;
            let lhs = parse_scalar_expr_internal(testexpr, agg_ctx.as_deref_mut())?;
            let subselect = sl
                .subselect
                .as_ref()
                .and_then(|n| n.node.as_ref())
                .ok_or_else(|| fe("subquery missing SELECT"))?;
            let NodeEnum::SelectStmt(sel) = subselect else {
                return Err(fe("only SELECT supported in subquery"));
            };
            let plan = crate::sql::dml::plan_select(*sel.clone())?;
            Ok(BoolExpr::InSubquery {
                expr: lhs,
                subplan: Box::new(plan),
            })
        }
        NodeEnum::AConst(c) => match const_to_value(c)? {
            Value::Bool(b) => Ok(BoolExpr::Literal(b)),
            _ => Err(fe("boolean literal expected")),
        },
        NodeEnum::ColumnRef(_)
        | NodeEnum::FuncCall(_)
        | NodeEnum::CoalesceExpr(_)
        | NodeEnum::TypeCast(_) => {
            let expr = parse_scalar_expr_internal(node, agg_ctx.as_deref_mut())?;
            Ok(BoolExpr::Comparison {
                lhs: expr,
                op: CmpOp::Eq,
                rhs: ScalarExpr::Literal(Value::Bool(true)),
            })
        }
        _ => Err(fe("unsupported WHERE expression")),
    }
}

pub fn parse_scalar_expr(node: &NodeEnum) -> PgWireResult<ScalarExpr> {
    parse_scalar_expr_internal(node, None)
}

fn parse_scalar_expr_internal(
    node: &NodeEnum,
    mut agg_ctx: Option<&mut AggregateExprCollector>,
) -> PgWireResult<ScalarExpr> {
    match node {
        NodeEnum::ColumnRef(cr) => Ok(ScalarExpr::Column(parse_column_ref(cr)?)),
        NodeEnum::ParamRef(pr) => parse_param_ref(pr),
        NodeEnum::AExpr(ax) => parse_arithmetic_expr(ax, agg_ctx.as_deref_mut()),
        NodeEnum::FuncCall(fc) => parse_function_call(fc, agg_ctx.as_deref_mut()),
        NodeEnum::CoalesceExpr(ce) => parse_coalesce_expr(ce, agg_ctx.as_deref_mut()),
        NodeEnum::SqlvalueFunction(svf) => parse_sql_value_function(svf),
        NodeEnum::TypeCast(tc) => {
            let inner = tc
                .arg
                .as_ref()
                .and_then(|n| n.node.as_ref())
                .ok_or_else(|| fe("bad type cast"))?;
            let expr = parse_scalar_expr_internal(inner, agg_ctx.as_deref_mut())?;
            let target = tc
                .type_name
                .as_ref()
                .ok_or_else(|| fe("missing cast target"))?;
            let dt = parse_type_name(target)?;
            Ok(ScalarExpr::Cast {
                expr: Box::new(expr),
                ty: dt,
            })
        }
        NodeEnum::MinMaxExpr(mm) => {
            let op = pg_query::protobuf::MinMaxOp::try_from(mm.op)
                .map_err(|_| fe("unsupported minmax op"))?;
            if op != pg_query::protobuf::MinMaxOp::IsGreatest {
                return Err(fe("only GREATEST is supported"));
            }
            if mm.args.is_empty() {
                return Err(fe("greatest() requires arguments"));
            }
            let mut args = Vec::new();
            for arg in &mm.args {
                let node = arg
                    .node
                    .as_ref()
                    .ok_or_else(|| fe("bad greatest argument"))?;
                args.push(parse_scalar_expr_internal(node, agg_ctx.as_deref_mut())?);
            }
            Ok(ScalarExpr::Func {
                func: ScalarFunc::Greatest,
                args,
            })
        }
        _ => {
            if let Some(v) = try_parse_literal(node)? {
                Ok(ScalarExpr::Literal(v))
            } else {
                Err(fe("unsupported scalar expression"))
            }
        }
    }
}

fn parse_coalesce_expr(
    ce: &CoalesceExpr,
    mut agg_ctx: Option<&mut AggregateExprCollector>,
) -> PgWireResult<ScalarExpr> {
    let mut args = Vec::new();
    for arg in &ce.args {
        let node = arg
            .node
            .as_ref()
            .ok_or_else(|| fe("bad coalesce argument"))?;
        args.push(parse_scalar_expr_internal(node, agg_ctx.as_deref_mut())?);
    }
    if args.is_empty() {
        return Err(fe("coalesce requires at least one argument"));
    }
    Ok(ScalarExpr::Func {
        func: ScalarFunc::Coalesce,
        args,
    })
}

fn parse_in_expr(
    ax: &AExpr,
    mut agg_ctx: Option<&mut AggregateExprCollector>,
) -> PgWireResult<BoolExpr> {
    let lexpr = ax
        .lexpr
        .as_ref()
        .and_then(|n| n.node.as_ref())
        .ok_or_else(|| fe("IN expression missing lhs"))?;
    let lhs = parse_scalar_expr_internal(lexpr, agg_ctx.as_deref_mut())?;
    let rexpr_node = ax
        .rexpr
        .as_ref()
        .and_then(|n| n.node.as_ref())
        .ok_or_else(|| fe("IN expression missing rhs"))?;
    match rexpr_node {
        NodeEnum::List(list) => {
            if list.items.is_empty() {
                return Err(fe("IN list must have at least one element"));
            }
            let mut comparisons = Vec::with_capacity(list.items.len());
            for item in &list.items {
                let node = item
                    .node
                    .as_ref()
                    .ok_or_else(|| fe("bad IN list element"))?;
                let rhs = parse_scalar_expr_internal(node, agg_ctx.as_deref_mut())?;
                comparisons.push(BoolExpr::Comparison {
                    lhs: lhs.clone(),
                    op: CmpOp::Eq,
                    rhs,
                });
            }
            if comparisons.len() == 1 {
                Ok(comparisons.pop().unwrap())
            } else {
                Ok(BoolExpr::Or(comparisons))
            }
        }
        NodeEnum::SubLink(sl) => {
            let subselect = sl
                .subselect
                .as_ref()
                .and_then(|n| n.node.as_ref())
                .ok_or_else(|| fe("IN subquery missing subselect"))?;
            let NodeEnum::SelectStmt(sel) = subselect else {
                return Err(fe("only SELECT supported in IN subquery"));
            };
            let plan = crate::sql::dml::plan_select(*sel.clone())?;
            Ok(BoolExpr::InSubquery {
                expr: lhs,
                subplan: Box::new(plan),
            })
        }
        _ => Err(fe("IN expression expects list or subquery")),
    }
}

fn parse_any_expr(
    ax: &AExpr,
    mut agg_ctx: Option<&mut AggregateExprCollector>,
) -> PgWireResult<BoolExpr> {
    let op = parse_cmp_op(&ax.name)?;
    if op != CmpOp::Eq {
        return Err(fe("only = ANY is supported"));
    }
    let lexpr = ax
        .lexpr
        .as_ref()
        .and_then(|n| n.node.as_ref())
        .ok_or_else(|| fe("ANY expression missing lhs"))?;
    let lhs = parse_scalar_expr_internal(lexpr, agg_ctx.as_deref_mut())?;
    let rexpr = ax
        .rexpr
        .as_ref()
        .and_then(|n| n.node.as_ref())
        .ok_or_else(|| fe("ANY expression missing rhs"))?;

    let elements: &[pg_query::protobuf::Node] = match rexpr {
        NodeEnum::AArrayExpr(AArrayExpr { elements, .. }) => elements.as_slice(),
        NodeEnum::ArrayExpr(arr) => arr.elements.as_slice(),
        _ => return Err(fe("ANY expects array expression rhs")),
    };
    if elements.is_empty() {
        return Err(fe("ANY array must have elements"));
    }
    let mut comparisons = Vec::with_capacity(elements.len());
    for elem in elements {
        let node = elem.node.as_ref().ok_or_else(|| fe("bad ANY element"))?;
        let rhs = parse_scalar_expr_internal(node, agg_ctx.as_deref_mut())?;
        comparisons.push(BoolExpr::Comparison {
            lhs: lhs.clone(),
            op: CmpOp::Eq,
            rhs,
        });
    }
    if comparisons.len() == 1 {
        Ok(comparisons.pop().unwrap())
    } else {
        Ok(BoolExpr::Or(comparisons))
    }
}

fn parse_sql_value_function(svf: &SqlValueFunction) -> PgWireResult<ScalarExpr> {
    let op = SqlValueFunctionOp::try_from(svf.op).map_err(|_| fe("unknown SQL value function"))?;
    let func = match op {
        SqlValueFunctionOp::SvfopCurrentTimestamp | SqlValueFunctionOp::SvfopCurrentTimestampN => {
            ScalarFunc::CurrentTimestamp
        }
        SqlValueFunctionOp::SvfopCurrentDate => ScalarFunc::CurrentDate,
        _ => return Err(fe("unsupported SQL value function")),
    };
    Ok(ScalarExpr::Func {
        func,
        args: Vec::new(),
    })
}

fn parse_param_ref(pr: &ParamRef) -> PgWireResult<ScalarExpr> {
    if pr.number <= 0 {
        return Err(fe("parameter numbers start at 1"));
    }
    Ok(ScalarExpr::Param {
        idx: (pr.number as usize) - 1,
        ty: None,
    })
}

pub fn agg_func_from_name(name: &str) -> Option<AggFunc> {
    match name.to_ascii_lowercase().as_str() {
        "count" => Some(AggFunc::Count),
        "sum" => Some(AggFunc::Sum),
        "avg" => Some(AggFunc::Avg),
        "min" => Some(AggFunc::Min),
        "max" => Some(AggFunc::Max),
        _ => None,
    }
}

pub struct AggregateExprCollector {
    prefix: String,
    counter: usize,
    aggs: Vec<(AggCall, String)>,
}

impl AggregateExprCollector {
    pub fn new(prefix: &str) -> Self {
        Self {
            prefix: prefix.to_string(),
            counter: 0,
            aggs: Vec::new(),
        }
    }

    pub fn register_aggregate_call(
        &mut self,
        name: &str,
        fc: &FuncCall,
    ) -> PgWireResult<ScalarExpr> {
        let Some(func) = agg_func_from_name(name) else {
            return Err(fe("unsupported function"));
        };
        if fc.agg_distinct {
            return Err(fe("DISTINCT aggregates are not supported"));
        }
        let agg_call = if func == AggFunc::Count && fc.agg_star {
            AggCall { func, expr: None }
        } else {
            if fc.args.len() != 1 {
                return Err(fe("aggregate functions require exactly one argument"));
            }
            let arg_node = fc.args[0]
                .node
                .as_ref()
                .ok_or_else(|| fe("bad aggregate argument"))?;
            let expr = parse_scalar_expr_internal(arg_node, None)?;
            AggCall {
                func,
                expr: Some(expr),
            }
        };
        let alias = format!("{}{}", self.prefix, self.counter);
        self.counter += 1;
        self.aggs.push((agg_call, alias.clone()));
        Ok(ScalarExpr::Column(ColumnRefName {
            schema: None,
            relation: None,
            column: alias,
        }))
    }

    pub fn into_aggs(self) -> Vec<(AggCall, String)> {
        self.aggs
    }
}

pub fn parse_arithmetic_expr(
    ax: &AExpr,
    mut agg_ctx: Option<&mut AggregateExprCollector>,
) -> PgWireResult<ScalarExpr> {
    let op = ax
        .name
        .iter()
        .find_map(|n| {
            n.node.as_ref().and_then(|nn| {
                if let NodeEnum::String(s) = nn {
                    Some(s.sval.clone())
                } else {
                    None
                }
            })
        })
        .ok_or_else(|| fe("missing operator"))?;
    if ax.lexpr.is_none() && op == "-" {
        let rhs = ax
            .rexpr
            .as_ref()
            .and_then(|n| n.node.as_ref())
            .ok_or_else(|| fe("bad unary minus"))?;
        let expr = parse_scalar_expr_internal(rhs, agg_ctx.as_deref_mut())?;
        return Ok(ScalarExpr::UnaryOp {
            op: ScalarUnaryOp::Negate,
            expr: Box::new(expr),
        });
    }
    let lexpr = ax
        .lexpr
        .as_ref()
        .and_then(|n| n.node.as_ref())
        .ok_or_else(|| fe("bad lhs"))?;
    let rexpr = ax
        .rexpr
        .as_ref()
        .and_then(|n| n.node.as_ref())
        .ok_or_else(|| fe("bad rhs"))?;
    let left = parse_scalar_expr_internal(lexpr, agg_ctx.as_deref_mut())?;
    let right = parse_scalar_expr_internal(rexpr, agg_ctx.as_deref_mut())?;
    let bin_op = match op.as_str() {
        "+" => ScalarBinaryOp::Add,
        "-" => ScalarBinaryOp::Sub,
        "*" => ScalarBinaryOp::Mul,
        "/" => ScalarBinaryOp::Div,
        "||" => ScalarBinaryOp::Concat,
        other => return Err(fe(format!("unsupported operator: {other}"))),
    };
    Ok(ScalarExpr::BinaryOp {
        op: bin_op,
        left: Box::new(left),
        right: Box::new(right),
    })
}

fn parse_function_call(
    fc: &FuncCall,
    mut agg_ctx: Option<&mut AggregateExprCollector>,
) -> PgWireResult<ScalarExpr> {
    let name = fc
        .funcname
        .iter()
        .filter_map(|n| {
            n.node.as_ref().and_then(|nn| {
                if let NodeEnum::String(s) = nn {
                    Some(s.sval.to_ascii_lowercase())
                } else {
                    None
                }
            })
        })
        .last()
        .ok_or_else(|| fe("bad function name"))?;
    if is_aggregate_func_name(&name) {
        if let Some(ctx) = agg_ctx.as_deref_mut() {
            return ctx.register_aggregate_call(name.as_str(), fc);
        } else {
            return Err(fe(
                "aggregate functions are handled in planner, not as scalar functions",
            ));
        }
    }
    let mut args = Vec::new();
    for arg in &fc.args {
        let node = arg
            .node
            .as_ref()
            .ok_or_else(|| fe("bad function argument"))?;
        args.push(parse_scalar_expr_internal(node, agg_ctx.as_deref_mut())?);
    }
    if name == "extract" || name == "date_part" {
        if args.len() != 2 {
            return Err(fe("extract() requires field and source expression"));
        }
        let field = &args[0];
        let field_name = match field {
            ScalarExpr::Literal(Value::Text(s)) => s.to_ascii_lowercase(),
            ScalarExpr::Column(col) => col.column.to_ascii_lowercase(),
            _ => return Err(fe("extract(field FROM expr) requires literal field name")),
        };
        if field_name != "epoch" {
            return Err(fe(format!("unsupported extract field: {field_name}")));
        }
        return Ok(ScalarExpr::Func {
            func: ScalarFunc::ExtractEpoch,
            args: vec![args.remove(1)],
        });
    }

    let func = match name.as_str() {
        "coalesce" => ScalarFunc::Coalesce,
        "upper" => ScalarFunc::Upper,
        "lower" => ScalarFunc::Lower,
        "length" | "char_length" => ScalarFunc::Length,
        "current_schema" => ScalarFunc::CurrentSchema,
        "current_schemas" => ScalarFunc::CurrentSchemas,
        "current_database" => ScalarFunc::CurrentDatabase,
        "now" => ScalarFunc::Now,
        "current_timestamp" => ScalarFunc::CurrentTimestamp,
        "statement_timestamp" => ScalarFunc::StatementTimestamp,
        "transaction_timestamp" => ScalarFunc::TransactionTimestamp,
        "clock_timestamp" => ScalarFunc::ClockTimestamp,
        "current_date" => ScalarFunc::CurrentDate,
        "abs" => ScalarFunc::Abs,
        "ln" => ScalarFunc::Ln,
        "log" => ScalarFunc::Log,
        "greatest" => ScalarFunc::Greatest,
        "version" => ScalarFunc::Version,
        "pg_table_is_visible" => ScalarFunc::PgTableIsVisible,
        other => return Err(fe(format!("unsupported function: {other}"))),
    };
    match func {
        ScalarFunc::Coalesce => {
            if args.is_empty() {
                return Err(fe("coalesce requires at least one argument"));
            }
        }
        ScalarFunc::Upper | ScalarFunc::Lower | ScalarFunc::Length => {
            if args.len() != 1 {
                return Err(fe("function expects exactly one argument"));
            }
        }
        ScalarFunc::CurrentSchema => {
            if !args.is_empty() {
                return Err(fe("current_schema() takes no arguments"));
            }
        }
        ScalarFunc::CurrentSchemas => {
            if args.len() != 1 {
                return Err(fe("current_schemas(boolean) requires one argument"));
            }
        }
        ScalarFunc::CurrentDatabase => {
            if !args.is_empty() {
                return Err(fe("current_database() takes no arguments"));
            }
        }
        ScalarFunc::Abs | ScalarFunc::Ln | ScalarFunc::Log => {
            if !(args.len() == 1 || (matches!(func, ScalarFunc::Log) && args.len() == 2)) {
                return Err(fe("invalid number of arguments"));
            }
        }
        ScalarFunc::Greatest => {
            if args.len() < 2 {
                return Err(fe("greatest() requires at least two arguments"));
            }
        }
        ScalarFunc::PgTableIsVisible => {
            if args.len() != 1 {
                return Err(fe("pg_table_is_visible(oid) requires one argument"));
            }
        }
        ScalarFunc::Now
        | ScalarFunc::CurrentTimestamp
        | ScalarFunc::StatementTimestamp
        | ScalarFunc::TransactionTimestamp
        | ScalarFunc::ClockTimestamp
        | ScalarFunc::CurrentDate
        | ScalarFunc::Version => {
            if !args.is_empty() {
                return Err(fe("function takes no arguments"));
            }
        }
        ScalarFunc::ExtractEpoch => {
            if args.len() != 1 {
                return Err(fe("extract(epoch from x) requires one source expression"));
            }
        }
    }
    Ok(ScalarExpr::Func { func, args })
}

pub fn collect_columns_from_scalar_expr(expr: &ScalarExpr, out: &mut Vec<String>) {
    match expr {
        ScalarExpr::Column(col) => out.push(col.column.clone()),
        ScalarExpr::ColumnIdx(_) | ScalarExpr::ExcludedIdx(_) | ScalarExpr::Literal(_) => {}
        ScalarExpr::Param { .. } => {}
        ScalarExpr::BinaryOp { left, right, .. } => {
            collect_columns_from_scalar_expr(left, out);
            collect_columns_from_scalar_expr(right, out);
        }
        ScalarExpr::UnaryOp { expr, .. } => collect_columns_from_scalar_expr(expr, out),
        ScalarExpr::Cast { expr, .. } => collect_columns_from_scalar_expr(expr, out),
        ScalarExpr::Func { args, .. } => {
            for arg in args {
                collect_columns_from_scalar_expr(arg, out);
            }
        }
    }
}

pub fn collect_columns_from_bool_expr(expr: &BoolExpr, out: &mut Vec<String>) {
    match expr {
        BoolExpr::Literal(_) => {}
        BoolExpr::Comparison { lhs, rhs, .. } => {
            collect_columns_from_scalar_expr(lhs, out);
            collect_columns_from_scalar_expr(rhs, out);
        }
        BoolExpr::And(parts) | BoolExpr::Or(parts) => {
            for p in parts {
                collect_columns_from_bool_expr(p, out);
            }
        }
        BoolExpr::Not(inner) => collect_columns_from_bool_expr(inner, out),
        BoolExpr::IsNull { expr, .. } => collect_columns_from_scalar_expr(expr, out),
        BoolExpr::InSubquery { expr, .. } => collect_columns_from_scalar_expr(expr, out),
        BoolExpr::InListValues { expr, .. } => collect_columns_from_scalar_expr(expr, out),
    }
}

pub fn derive_expr_name(expr: &ScalarExpr) -> String {
    match expr {
        ScalarExpr::Column(col) => col.column.clone(),
        ScalarExpr::ColumnIdx(idx) => format!("?column{}?", idx + 1),
        ScalarExpr::ExcludedIdx(idx) => format!("?column{}?", idx + 1),
        ScalarExpr::Param { idx, .. } => format!("param{}", idx + 1),
        ScalarExpr::Literal(_) => "?column?".into(),
        ScalarExpr::BinaryOp { .. } => "?column?".into(),
        ScalarExpr::UnaryOp { .. } => "?column?".into(),
        ScalarExpr::Cast { expr, .. } => derive_expr_name(expr),
        ScalarExpr::Func { .. } => "?column?".into(),
    }
}

pub fn parse_column_ref(cr: &ColumnRef) -> PgWireResult<ColumnRefName> {
    let mut parts = Vec::new();
    for field in &cr.fields {
        let node = field.node.as_ref().ok_or_else(|| fe("bad colref"))?;
        match node {
            NodeEnum::String(s) => parts.push(s.sval.clone()),
            _ => return Err(fe("unsupported column reference")),
        }
    }
    if parts.is_empty() {
        return Err(fe("bad colref"));
    }
    let column = parts.pop().unwrap();
    let (schema, relation) = match parts.len() {
        0 => (None, None),
        1 => (None, Some(parts.remove(0))),
        2 => (Some(parts.remove(0)), Some(parts.remove(0))),
        _ => return Err(fe("column reference has too many qualifiers")),
    };
    Ok(ColumnRefName {
        schema,
        relation,
        column,
    })
}

fn parse_cmp_op(nodes: &[Node]) -> PgWireResult<CmpOp> {
    for n in nodes {
        if let Some(NodeEnum::String(s)) = n.node.as_ref() {
            return Ok(match s.sval.as_str() {
                "=" => CmpOp::Eq,
                "!=" | "<>" => CmpOp::Neq,
                "<" => CmpOp::Lt,
                "<=" => CmpOp::Lte,
                ">" => CmpOp::Gt,
                ">=" => CmpOp::Gte,
                other => return Err(fe(format!("unsupported where operator: {other}"))),
            });
        }
    }
    Err(fe("missing operator"))
}
