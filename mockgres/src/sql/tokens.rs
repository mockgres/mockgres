use super::expr::parse_scalar_expr;
use crate::catalog::SchemaName;
use crate::engine::{DataType, IdentitySpec, ObjName, ScalarExpr, Value, fe};
use pg_query::protobuf::a_const::Val;
use pg_query::protobuf::{AConst, ColumnDef, TypeName};
use pg_query::{Node, NodeEnum};
use pgwire::error::PgWireResult;

type ColumnDefSpec = (
    String,
    DataType,
    bool,
    Option<ScalarExpr>,
    Option<IdentitySpec>,
);

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
    } else if tokens.len() >= 4
        && tokens[tokens.len() - 4] == "timestamp"
        && tokens[tokens.len() - 3] == "with"
        && tokens[tokens.len() - 2] == "time"
        && tokens[tokens.len() - 1] == "zone"
    {
        DataType::Timestamptz
    } else {
        match last {
            "int" | "int4" | "integer" => DataType::Int4,
            "bigint" | "int8" => DataType::Int8,
            "float8" | "double" => DataType::Float8,
            "text" | "varchar" => DataType::Text,
            "json" => DataType::Json,
            "bool" | "boolean" => DataType::Bool,
            "oid" => DataType::Int8,
            "date" => DataType::Date,
            "timestamp" => DataType::Timestamp,
            "timestamptz" => DataType::Timestamptz,
            "bytea" => DataType::Bytea,
            "interval" => DataType::Interval,
            "regtype" => DataType::Text,
            "void" => DataType::Void,
            other => return Err(fe(format!("unsupported type: {other}"))),
        }
    };
    Ok(dt)
}

pub(super) fn parse_column_def(cd: &ColumnDef) -> PgWireResult<ColumnDefSpec> {
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
    let mut nullable = !cd
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
    let identity = parse_identity_spec(cd)?;
    if let Some(spec) = &identity {
        if !matches!(dt, DataType::Int4 | DataType::Int8) {
            return Err(fe("IDENTITY columns must be INT or BIGINT"));
        }
        if spec.increment_by == 0 {
            return Err(fe("IDENTITY INCREMENT BY cannot be zero"));
        }
    }
    let name = cd.colname.clone();
    if name.is_empty() {
        return Err(fe("column must have a name"));
    }
    if identity.is_some() && default.is_some() {
        return Err(fe(format!(
            "identity column {name} cannot have an explicit DEFAULT"
        )));
    }
    if identity.is_some() {
        nullable = false;
    }
    Ok((name, dt, nullable, default, identity))
}

fn ensure_default_expr_is_const(expr: &ScalarExpr) -> PgWireResult<()> {
    match expr {
        ScalarExpr::Literal(_) => Ok(()),
        ScalarExpr::Column(..) | ScalarExpr::ColumnIdx(_) | ScalarExpr::ExcludedIdx(_) => {
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

fn parse_identity_spec(cd: &ColumnDef) -> PgWireResult<Option<IdentitySpec>> {
    let mut spec: Option<IdentitySpec> = None;
    for constraint in &cd.constraints {
        let Some(NodeEnum::Constraint(cons)) = constraint.node.as_ref() else {
            continue;
        };
        if cons.contype != pg_query::protobuf::ConstrType::ConstrIdentity as i32 {
            continue;
        }
        if spec.is_some() {
            return Err(fe(format!(
                "column {} specifies IDENTITY more than once",
                cd.colname
            )));
        }
        let always = match cons.generated_when.as_str() {
            "a" | "A" => true,
            "" | "d" | "D" => false,
            other => {
                return Err(fe(format!(
                    "unsupported IDENTITY generation mode {other:?}"
                )));
            }
        };
        let mut start_with = None;
        let mut increment_by = None;
        for opt in &cons.options {
            let Some(NodeEnum::DefElem(def)) = opt.node.as_ref() else {
                continue;
            };
            match def.defname.as_str() {
                "start" => start_with = Some(parse_identity_option_value(&def.arg)?),
                "increment" => increment_by = Some(parse_identity_option_value(&def.arg)?),
                _ => {}
            }
        }
        spec = Some(IdentitySpec {
            always,
            start_with: start_with.unwrap_or(1),
            increment_by: increment_by.unwrap_or(1),
        });
    }
    Ok(spec)
}

fn parse_identity_option_value(arg: &Option<Box<Node>>) -> PgWireResult<i128> {
    let node = arg
        .as_ref()
        .and_then(|n| n.node.as_ref())
        .ok_or_else(|| fe("IDENTITY option requires a value"))?;
    match node {
        NodeEnum::Integer(i) => Ok(i.ival as i128),
        NodeEnum::AConst(c) => match const_to_value(c)? {
            Value::Int64(v) => Ok(v as i128),
            Value::Text(s) => s
                .parse::<i128>()
                .map_err(|_| fe("IDENTITY option requires integer")),
            _ => Err(fe("IDENTITY option requires integer literal")),
        },
        _ => Err(fe("IDENTITY option requires integer literal")),
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
                        | Value::TimestamptzMicros(_)
                        | Value::Bytes(_)
                        | Value::IntervalMicros(_) => Err(fe("minus over non-numeric literal")),
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
