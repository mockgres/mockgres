use super::expr::parse_scalar_expr;
use crate::catalog::SchemaName;
use crate::engine::{DataType, ObjName, ScalarExpr, Value, fe};
use pg_query::NodeEnum;
use pg_query::protobuf::a_const::Val;
use pg_query::protobuf::{AConst, ColumnDef, TypeName};
use pgwire::error::PgWireResult;

pub(super) fn const_to_value(c: &AConst) -> PgWireResult<Value> {
    if c.val.is_none() {
        return Ok(Value::Null);
    }
    let v = c.val.as_ref().unwrap();
    match v {
        Val::Ival(i) => Ok(Value::Int64(i.ival as i64)),
        Val::Fval(f) => {
            Ok(Value::from_f64(f.fval.parse::<f64>().map_err(|e| {
                pgwire::error::PgWireError::ApiError(Box::new(e))
            })?))
        }
        Val::Boolval(b) => Ok(Value::Bool(b.boolval)),
        Val::Sval(s) => Ok(Value::Text(s.sval.clone())),
        Val::Bsval(_) => Err(fe("bitstring const not yet supported")),
    }
}

pub(super) fn map_type(cd: &ColumnDef) -> PgWireResult<DataType> {
    let typ = cd.type_name.as_ref().ok_or_else(|| fe("missing type"))?;
    parse_type_name(typ)
}

pub(super) fn parse_type_name(typ: &TypeName) -> PgWireResult<DataType> {
    let mut tokens: Vec<String> = typ
        .names
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
        .collect();
    tokens.retain(|t| t != "pg_catalog" && t != "public");
    if tokens.is_empty() {
        return Err(fe("bad type name"));
    }
    let last = tokens.last().unwrap().as_str();
    let dt = if tokens.len() >= 2
        && tokens[tokens.len() - 2] == "double"
        && tokens[tokens.len() - 1] == "precision"
    {
        DataType::Float8
    } else if tokens.len() >= 4
        && tokens[tokens.len() - 4] == "timestamp"
        && tokens[tokens.len() - 3] == "without"
        && tokens[tokens.len() - 2] == "time"
        && tokens[tokens.len() - 1] == "zone"
    {
        DataType::Timestamp
    } else {
        match last {
            "int" | "int4" | "integer" => DataType::Int4,
            "bigint" | "int8" => DataType::Int8,
            "float8" | "double" => DataType::Float8,
            "text" | "varchar" => DataType::Text,
            "bool" | "boolean" => DataType::Bool,
            "date" => DataType::Date,
            "timestamp" => DataType::Timestamp,
            "bytea" => DataType::Bytea,
            other => return Err(fe(format!("unsupported type: {other}"))),
        }
    };
    Ok(dt)
}

pub(super) fn parse_column_def(
    cd: &ColumnDef,
) -> PgWireResult<(String, DataType, bool, Option<ScalarExpr>)> {
    let dt = map_type(cd)?;
    let default_node = cd
        .raw_default
        .as_ref()
        .and_then(|n| n.node.as_ref())
        .or_else(|| cd.cooked_default.as_ref().and_then(|n| n.node.as_ref()))
        .or_else(|| {
            cd.constraints.iter().find_map(|c| {
                let Some(NodeEnum::Constraint(cons)) = c.node.as_ref() else {
                    return None;
                };
                if cons.contype == pg_query::protobuf::ConstrType::ConstrDefault as i32 {
                    cons.raw_expr.as_ref().and_then(|n| n.node.as_ref())
                } else {
                    None
                }
            })
        });
    let nullable = !cd
        .constraints
        .iter()
        .any(|c| matches!(c.node.as_ref(), Some(NodeEnum::Constraint(cons)) if cons.contype == pg_query::protobuf::ConstrType::ConstrNotnull as i32));
    let default = match default_node {
        Some(node) => {
            let expr = parse_scalar_expr(node)?;
            ensure_default_expr_is_const(&expr)?;
            Some(expr)
        }
        None => None,
    };
    let name = cd.colname.clone();
    if name.is_empty() {
        return Err(fe("column must have a name"));
    }
    Ok((name, dt, nullable, default))
}

fn ensure_default_expr_is_const(expr: &ScalarExpr) -> PgWireResult<()> {
    match expr {
        ScalarExpr::Literal(_) => Ok(()),
        ScalarExpr::Column(..) | ScalarExpr::ColumnIdx(_) => {
            Err(fe("DEFAULT expressions cannot reference columns"))
        }
        ScalarExpr::Param { .. } => Err(fe("DEFAULT expressions cannot reference parameters")),
        ScalarExpr::BinaryOp { left, right, .. } => {
            ensure_default_expr_is_const(left)?;
            ensure_default_expr_is_const(right)
        }
        ScalarExpr::UnaryOp { expr, .. } | ScalarExpr::Cast { expr, .. } => {
            ensure_default_expr_is_const(expr)
        }
        ScalarExpr::Func { args, .. } => {
            for arg in args {
                ensure_default_expr_is_const(arg)?;
            }
            Ok(())
        }
    }
}

pub(super) fn parse_index_columns(params: &[pg_query::Node]) -> PgWireResult<Vec<String>> {
    if params.is_empty() {
        return Err(fe("index requires at least one column"));
    }
    let mut cols = Vec::with_capacity(params.len());
    for p in params {
        let node = p.node.as_ref().ok_or_else(|| fe("bad index column"))?;
        let NodeEnum::IndexElem(elem) = node else {
            return Err(fe("index expressions not supported"));
        };
        if elem.expr.is_some() {
            return Err(fe("expression indexes not supported"));
        }
        if elem.name.is_empty() {
            return Err(fe("index column name required"));
        }
        cols.push(elem.name.clone());
    }
    Ok(cols)
}

pub(super) fn parse_obj_name_from_list(node: &NodeEnum) -> PgWireResult<ObjName> {
    let mut parts = Vec::new();
    match node {
        NodeEnum::List(list) => {
            for item in &list.items {
                let Some(NodeEnum::String(s)) = item.node.as_ref() else {
                    return Err(fe("bad qualified name component"));
                };
                parts.push(s.sval.clone());
            }
        }
        NodeEnum::String(s) => parts.push(s.sval.clone()),
        _ => return Err(fe("bad qualified name")),
    }
    if parts.is_empty() {
        return Err(fe("empty name"));
    }
    let name = parts.pop().unwrap();
    let schema = if parts.is_empty() {
        None
    } else {
        Some(SchemaName::new(parts.join(".")))
    };
    Ok(ObjName { schema, name })
}

pub(super) fn parse_set_value(args: &[pg_query::Node]) -> PgWireResult<Vec<String>> {
    if args.is_empty() {
        return Err(fe("SET requires value"));
    }
    let mut values = Vec::with_capacity(args.len());
    for arg in args {
        let node = arg.node.as_ref().ok_or_else(|| fe("bad SET value"))?;
        let Some(v) = try_parse_literal(node)? else {
            return Err(fe("unsupported SET value"));
        };
        values.push(literal_value_to_string(v)?);
    }
    Ok(values)
}

pub(super) fn literal_value_to_string(value: Value) -> PgWireResult<String> {
    Ok(match value {
        Value::Text(s) => s,
        Value::Int64(i) => i.to_string(),
        Value::Bool(b) => {
            if b {
                "true".into()
            } else {
                "false".into()
            }
        }
        _ => return Err(fe("SET literal type not supported")),
    })
}

pub(super) fn try_parse_literal(node: &NodeEnum) -> PgWireResult<Option<Value>> {
    match node {
        NodeEnum::AConst(c) => Ok(Some(const_to_value(c)?)),
        NodeEnum::AExpr(ax) => {
            let is_minus = ax.name.iter().any(|nn| {
                matches!(
                    nn.node.as_ref(),
                    Some(NodeEnum::String(s)) if s.sval == "-"
                )
            });
            if is_minus {
                let rhs = ax
                    .rexpr
                    .as_ref()
                    .and_then(|n| n.node.as_ref())
                    .ok_or_else(|| fe("bad unary minus"))?;
                match rhs {
                    NodeEnum::AConst(c) => match const_to_value(c)? {
                        Value::Int64(i) => Ok(Some(Value::Int64(-i))),
                        Value::Float64Bits(b) => Ok(Some(Value::from_f64(-f64::from_bits(b)))),
                        Value::Null => Err(fe("minus over null")),
                        Value::Text(_)
                        | Value::Bool(_)
                        | Value::Date(_)
                        | Value::TimestampMicros(_)
                        | Value::Bytes(_) => Err(fe("minus over non-numeric literal")),
                    },
                    _ => Err(fe("minus over non-const")),
                }
            } else {
                Ok(None)
            }
        }
        _ => Ok(None),
    }
}
