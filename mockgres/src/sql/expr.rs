use crate::engine::{
    BoolExpr, CmpOp, ColumnRefName, ScalarBinaryOp, ScalarExpr, ScalarFunc, ScalarUnaryOp, Value,
    fe,
};
use pg_query::NodeEnum;
use pg_query::protobuf::{
    AExpr, BoolExprType, CoalesceExpr, ColumnRef, FuncCall, Node, NullTestType, ParamRef,
};
use pgwire::error::PgWireResult;

use super::tokens::{const_to_value, parse_type_name, try_parse_literal};

pub fn parse_bool_expr(node: &NodeEnum) -> PgWireResult<BoolExpr> {
    match node {
        NodeEnum::BoolExpr(be) => {
            let op = BoolExprType::try_from(be.boolop).map_err(|_| fe("bad bool expr op"))?;
            let mut args = Vec::new();
            for a in &be.args {
                let n = a.node.as_ref().ok_or_else(|| fe("bad bool arg"))?;
                args.push(parse_bool_expr(n)?);
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
            if ax.name.is_empty() {
                if let Some(inner) = ax.lexpr.as_ref().and_then(|n| n.node.as_ref()) {
                    return parse_bool_expr(inner);
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
            let lhs = parse_scalar_expr(lexpr)?;
            let rhs = parse_scalar_expr(rexpr)?;
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
            let expr = parse_scalar_expr(arg)?;
            Ok(BoolExpr::IsNull {
                expr,
                negated: matches!(nt_type, NullTestType::IsNotNull),
            })
        }
        NodeEnum::AConst(c) => match const_to_value(c)? {
            Value::Bool(b) => Ok(BoolExpr::Literal(b)),
            _ => Err(fe("boolean literal expected")),
        },
        NodeEnum::ColumnRef(_) => {
            let col = parse_scalar_expr(node)?;
            Ok(BoolExpr::Comparison {
                lhs: col,
                op: CmpOp::Eq,
                rhs: ScalarExpr::Literal(Value::Bool(true)),
            })
        }
        _ => Err(fe("unsupported WHERE expression")),
    }
}

pub fn parse_scalar_expr(node: &NodeEnum) -> PgWireResult<ScalarExpr> {
    match node {
        NodeEnum::ColumnRef(cr) => Ok(ScalarExpr::Column(parse_column_ref(cr)?)),
        NodeEnum::ParamRef(pr) => parse_param_ref(pr),
        NodeEnum::AExpr(ax) => parse_arithmetic_expr(ax),
        NodeEnum::FuncCall(fc) => parse_function_call(fc),
        NodeEnum::CoalesceExpr(ce) => parse_coalesce_expr(ce),
        NodeEnum::TypeCast(tc) => {
            let inner = tc
                .arg
                .as_ref()
                .and_then(|n| n.node.as_ref())
                .ok_or_else(|| fe("bad type cast"))?;
            let expr = parse_scalar_expr(inner)?;
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
        _ => {
            if let Some(v) = try_parse_literal(node)? {
                Ok(ScalarExpr::Literal(v))
            } else {
                Err(fe("unsupported scalar expression"))
            }
        }
    }
}

fn parse_coalesce_expr(ce: &CoalesceExpr) -> PgWireResult<ScalarExpr> {
    let mut args = Vec::new();
    for arg in &ce.args {
        let node = arg
            .node
            .as_ref()
            .ok_or_else(|| fe("bad coalesce argument"))?;
        args.push(parse_scalar_expr(node)?);
    }
    if args.is_empty() {
        return Err(fe("coalesce requires at least one argument"));
    }
    Ok(ScalarExpr::Func {
        func: ScalarFunc::Coalesce,
        args,
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

pub fn parse_arithmetic_expr(ax: &AExpr) -> PgWireResult<ScalarExpr> {
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
    if ax.lexpr.is_none() {
        if op == "-" {
            let rhs = ax
                .rexpr
                .as_ref()
                .and_then(|n| n.node.as_ref())
                .ok_or_else(|| fe("bad unary minus"))?;
            let expr = parse_scalar_expr(rhs)?;
            return Ok(ScalarExpr::UnaryOp {
                op: ScalarUnaryOp::Negate,
                expr: Box::new(expr),
            });
        }
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
    let left = parse_scalar_expr(lexpr)?;
    let right = parse_scalar_expr(rexpr)?;
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

pub fn parse_function_call(fc: &FuncCall) -> PgWireResult<ScalarExpr> {
    let name = fc
        .funcname
        .iter()
        .find_map(|n| {
            n.node.as_ref().and_then(|nn| {
                if let NodeEnum::String(s) = nn {
                    Some(s.sval.to_ascii_lowercase())
                } else {
                    None
                }
            })
        })
        .ok_or_else(|| fe("bad function name"))?;
    let mut args = Vec::new();
    for arg in &fc.args {
        let node = arg
            .node
            .as_ref()
            .ok_or_else(|| fe("bad function argument"))?;
        args.push(parse_scalar_expr(node)?);
    }
    let func = match name.as_str() {
        "coalesce" => ScalarFunc::Coalesce,
        "upper" => ScalarFunc::Upper,
        "lower" => ScalarFunc::Lower,
        "length" | "char_length" => ScalarFunc::Length,
        "current_schema" => ScalarFunc::CurrentSchema,
        "current_schemas" => ScalarFunc::CurrentSchemas,
        "current_database" => ScalarFunc::CurrentDatabase,
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
    }
    Ok(ScalarExpr::Func { func, args })
}

pub fn collect_columns_from_scalar_expr(expr: &ScalarExpr, out: &mut Vec<String>) {
    match expr {
        ScalarExpr::Column(col) => out.push(col.column.clone()),
        ScalarExpr::ColumnIdx(_) | ScalarExpr::Literal(_) => {}
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
    }
}

pub fn derive_expr_name(expr: &ScalarExpr) -> String {
    match expr {
        ScalarExpr::Column(col) => col.column.clone(),
        ScalarExpr::ColumnIdx(idx) => format!("?column{}?", idx + 1),
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
