#![allow(clippy::needless_option_as_deref)]

use crate::engine::{
    AggCall, AggFunc, BoolExpr, CmpOp, ColumnRefName, ScalarBinaryOp, ScalarExpr, ScalarFunc,
    ScalarUnaryOp, SortKey, Value, WindowSpec, fe, fe_code,
};
use pg_query::NodeEnum;
use pg_query::protobuf::{
    AArrayExpr, AExpr, AExprKind, BoolExprType, CaseExpr, CoalesceExpr, ColumnRef, FuncCall, Node,
    NullTestType, ParamRef, ResTarget, SelectStmt, SqlValueFunction, SqlValueFunctionOp, WindowDef,
};
use pgwire::error::{ErrorInfo, PgWireError, PgWireResult};

use super::tokens::{const_to_value, parse_type_name, try_parse_literal};

mod functions;
mod parse;

pub use functions::parse_arithmetic_expr;
use functions::parse_function_call;
use functions::unsupported_xml_feature;
use parse::parse_scalar_expr_internal;
pub use parse::{
    parse_bool_expr, parse_bool_expr_with_aggregates, parse_scalar_expr,
    parse_scalar_expr_with_aggregates,
};

pub fn is_aggregate_func_name(name: &str) -> bool {
    matches!(
        name.to_ascii_lowercase().as_str(),
        "count" | "sum" | "avg" | "min" | "max" | "bool_and"
    )
}

pub fn agg_func_from_name(name: &str) -> Option<AggFunc> {
    match name.to_ascii_lowercase().as_str() {
        "count" => Some(AggFunc::Count),
        "sum" => Some(AggFunc::Sum),
        "avg" => Some(AggFunc::Avg),
        "min" => Some(AggFunc::Min),
        "max" => Some(AggFunc::Max),
        "bool_and" => Some(AggFunc::BoolAnd),
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
        let agg_call = if func == AggFunc::Count && fc.agg_star {
            if fc.agg_distinct {
                return Err(fe("COUNT(DISTINCT *) is not supported"));
            }
            AggCall {
                func,
                expr: None,
                distinct: false,
            }
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
                distinct: fc.agg_distinct,
            }
        };
        let alias = format!("{}{}", self.prefix, self.counter);
        self.counter += 1;
        self.aggs.push((agg_call, alias.clone()));
        Ok(ScalarExpr::Column(ColumnRefName {
            schema: None,
            relation: None,
            column: alias,
            location: None,
        }))
    }

    pub fn into_aggs(self) -> Vec<(AggCall, String)> {
        self.aggs
    }

    pub fn agg_count(&self) -> usize {
        self.aggs.len()
    }
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
        ScalarExpr::WindowRowNumber(spec) => {
            for expr in &spec.partition_by {
                collect_columns_from_scalar_expr(expr, out);
            }
            for key in &spec.order_by {
                match key {
                    SortKey::ByName { col, .. } => out.push(col.clone()),
                    SortKey::Expr { expr, .. } => collect_columns_from_scalar_expr(expr, out),
                    SortKey::ByIndex { .. } => {}
                }
            }
        }
        ScalarExpr::Predicate(expr) => collect_columns_from_bool_expr(expr, out),
        ScalarExpr::Subquery(_) => {}
        ScalarExpr::Case {
            when_then,
            else_expr,
        } => {
            for (cond, result) in when_then {
                collect_columns_from_bool_expr(cond, out);
                collect_columns_from_scalar_expr(result, out);
            }
            if let Some(expr) = else_expr {
                collect_columns_from_scalar_expr(expr, out);
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
        ScalarExpr::Param { .. } => "?column?".into(),
        ScalarExpr::Literal(_) => "?column?".into(),
        ScalarExpr::BinaryOp { .. } => "?column?".into(),
        ScalarExpr::UnaryOp { .. } => "?column?".into(),
        ScalarExpr::Cast { expr, ty } => match ty {
            crate::engine::DataType::PgChar | crate::engine::DataType::BpChar(_) => "char".into(),
            crate::engine::DataType::Text => "text".into(),
            crate::engine::DataType::Time(_) => "time".into(),
            crate::engine::DataType::MacAddr => "macaddr".into(),
            crate::engine::DataType::MacAddr8 => "macaddr8".into(),
            _ => derive_expr_name(expr),
        },
        ScalarExpr::Func { func, .. } => match func {
            ScalarFunc::Coalesce => "coalesce",
            ScalarFunc::Upper => "upper",
            ScalarFunc::Lower => "lower",
            ScalarFunc::Trunc => "trunc",
            ScalarFunc::MacAddr8Set7Bit => "macaddr8_set7bit",
            ScalarFunc::Substring => "substring",
            ScalarFunc::IndirectToastRow => "indirect_toast_row",
            ScalarFunc::Length => "length",
            ScalarFunc::CharLength => "char_length",
            ScalarFunc::Position => "position",
            ScalarFunc::Repeat => "repeat",
            ScalarFunc::Decode => "decode",
            ScalarFunc::TestPglzCompress => "test_pglz_compress",
            ScalarFunc::TestPglzDecompress => "test_pglz_decompress",
            ScalarFunc::UnicodeVersion => "unicode_version",
            ScalarFunc::UnicodeAssigned => "unicode_assigned",
            ScalarFunc::Normalize => "normalize",
            ScalarFunc::IsNormalized => "is_normalized",
            ScalarFunc::ParseIdent | ScalarFunc::ParseIdentNameArray => "parse_ident",
            ScalarFunc::SatisfiesHashPartition => "satisfies_hash_partition",
            ScalarFunc::IsOpen => "isopen",
            ScalarFunc::IsClosed => "isclosed",
            ScalarFunc::PClose => "pclose",
            ScalarFunc::POpen => "popen",
            ScalarFunc::Point => "point",
            ScalarFunc::Lseg => "lseg",
            ScalarFunc::Line => "line",
            ScalarFunc::Center => "center",
            ScalarFunc::Radius => "radius",
            ScalarFunc::Diameter => "diameter",
            ScalarFunc::Area => "area",
            ScalarFunc::Box => "box",
            ScalarFunc::PgInputIsValid => "pg_input_is_valid",
            ScalarFunc::CurrentSchema => "current_schema",
            ScalarFunc::CurrentSchemas => "current_schemas",
            ScalarFunc::CurrentDatabase => "current_database",
            ScalarFunc::Now => "now",
            ScalarFunc::CurrentTimestamp => "current_timestamp",
            ScalarFunc::StatementTimestamp => "statement_timestamp",
            ScalarFunc::TransactionTimestamp => "transaction_timestamp",
            ScalarFunc::ClockTimestamp => "clock_timestamp",
            ScalarFunc::CurrentDate => "current_date",
            ScalarFunc::Abs => "abs",
            ScalarFunc::Ln => "ln",
            ScalarFunc::Log => "log",
            ScalarFunc::Greatest => "greatest",
            ScalarFunc::ExtractEpoch => "extract",
            ScalarFunc::ExtractMicrosecond
            | ScalarFunc::ExtractMillisecond
            | ScalarFunc::ExtractSecond
            | ScalarFunc::ExtractMinute
            | ScalarFunc::ExtractHour => "extract",
            ScalarFunc::DatePartEpoch
            | ScalarFunc::DatePartMicrosecond
            | ScalarFunc::DatePartMillisecond
            | ScalarFunc::DatePartSecond => "date_part",
            ScalarFunc::Version => "version",
            ScalarFunc::CurrentSetting => "current_setting",
            ScalarFunc::PgNumaAvailable => "pg_numa_available",
            ScalarFunc::GetDatabaseEncoding => "getdatabaseencoding",
            ScalarFunc::PgCharToEncoding => "pg_char_to_encoding",
            ScalarFunc::PgNotify => "pg_notify",
            ScalarFunc::PgNotificationQueueUsage => "pg_notification_queue_usage",
            ScalarFunc::Md5 => "md5",
            ScalarFunc::RegexpReplace => "regexp_replace",
            ScalarFunc::InfiniteRecurse => "infinite_recurse",
            ScalarFunc::PgRelationSize => "pg_relation_size",
            ScalarFunc::PgSizePretty => "pg_size_pretty",
            ScalarFunc::PgSizeBytes => "pg_size_bytes",
            ScalarFunc::PgTableIsVisible => "pg_table_is_visible",
            ScalarFunc::PgAdvisoryLock => "pg_advisory_lock",
            ScalarFunc::PgAdvisoryUnlock => "pg_advisory_unlock",
        }
        .into(),
        ScalarExpr::WindowRowNumber(_) => "row_number".into(),
        ScalarExpr::Predicate(_) | ScalarExpr::Subquery(_) => "?column?".into(),
        ScalarExpr::Case { .. } => "case".into(),
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
        location: (cr.location >= 0).then_some(cr.location),
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
                "~" => CmpOp::Regex,
                "!~" => CmpOp::NotRegex,
                "~*" => CmpOp::RegexInsensitive,
                "!~*" => CmpOp::NotRegexInsensitive,
                other => return Err(fe(format!("unsupported where operator: {other}"))),
            });
        }
    }
    Err(fe("missing operator"))
}
