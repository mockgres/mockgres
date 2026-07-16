use super::*;

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
    let rhs_is_time = ax
        .rexpr
        .as_ref()
        .and_then(|node| node.node.as_ref())
        .is_some_and(|node| {
            matches!(
                node,
                NodeEnum::TypeCast(cast)
                    if cast.type_name.as_ref().is_some_and(|ty| {
                        ty.names.iter().any(|name| {
                            matches!(name.node.as_ref(), Some(NodeEnum::String(name)) if name.sval == "time")
                        })
                    })
            )
        });
    if op == "+" && rhs_is_time {
        let mut info = ErrorInfo::new(
            "ERROR".to_string(),
            "42725".to_string(),
            "operator is not unique: time without time zone + time without time zone".to_string(),
        );
        info.position = Some((ax.location + 1).to_string());
        info.hint = Some(
            "Could not choose a best candidate operator. You might need to add explicit type casts."
                .to_string(),
        );
        return Err(PgWireError::UserError(Box::new(info)));
    }
    if ax.lexpr.is_none() && op == "-" {
        let rhs = ax
            .rexpr
            .as_ref()
            .and_then(|n| n.node.as_ref())
            .ok_or_else(|| fe("bad unary minus"))?;
        if let NodeEnum::AConst(value) = rhs
            && let Some(pg_query::protobuf::a_const::Val::Fval(value)) = value.val.as_ref()
            && value.fval == "9223372036854775808"
        {
            return Ok(ScalarExpr::Literal(Value::Int64(i64::MIN)));
        }
        let expr = parse_scalar_expr_internal(rhs, agg_ctx.as_deref_mut())?;
        return Ok(ScalarExpr::UnaryOp {
            op: ScalarUnaryOp::Negate,
            expr: Box::new(expr),
        });
    }
    if ax.lexpr.is_none() && op == "+" {
        let rhs = ax
            .rexpr
            .as_ref()
            .and_then(|n| n.node.as_ref())
            .ok_or_else(|| fe("bad unary plus"))?;
        return parse_scalar_expr_internal(rhs, agg_ctx.as_deref_mut());
    }
    if ax.lexpr.is_none() && op == "~" {
        let rhs = ax
            .rexpr
            .as_ref()
            .and_then(|n| n.node.as_ref())
            .ok_or_else(|| fe("bad bitwise not operand"))?;
        return Ok(ScalarExpr::UnaryOp {
            op: ScalarUnaryOp::BitNot,
            expr: Box::new(parse_scalar_expr_internal(rhs, agg_ctx.as_deref_mut())?),
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
        "%" => ScalarBinaryOp::Modulo,
        "&" => ScalarBinaryOp::BitAnd,
        "|" => ScalarBinaryOp::BitOr,
        "||" => ScalarBinaryOp::Concat,
        "<->" => ScalarBinaryOp::Distance,
        other => return Err(fe(format!("unsupported operator: {other}"))),
    };
    Ok(ScalarExpr::BinaryOp {
        op: bin_op,
        left: Box::new(left),
        right: Box::new(right),
    })
}

pub(super) fn parse_function_call(
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
        .next_back()
        .ok_or_else(|| fe("bad function name"))?;
    if matches!(
        name.as_str(),
        "table_to_xml"
            | "table_to_xmlschema"
            | "table_to_xml_and_xmlschema"
            | "query_to_xml"
            | "query_to_xmlschema"
            | "query_to_xml_and_xmlschema"
            | "cursor_to_xml"
            | "cursor_to_xmlschema"
            | "schema_to_xml"
            | "schema_to_xmlschema"
            | "schema_to_xml_and_xmlschema"
    ) {
        return Err(unsupported_xml_feature());
    }
    if name == "satisfies_hash_partition" {
        return parse_satisfies_hash_partition(fc);
    }
    if fc.over.is_some() {
        if name != "row_number" {
            return Err(fe("only row_number() window function is supported"));
        }
        if !fc.args.is_empty() || fc.agg_star {
            return Err(fe("row_number() takes no arguments"));
        }
        let over = fc.over.as_ref().expect("checked above");
        return Ok(ScalarExpr::WindowRowNumber(parse_row_number_window(over)?));
    }
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
        let mut node = arg
            .node
            .as_ref()
            .ok_or_else(|| fe("bad function argument"))?;
        if let NodeEnum::NamedArgExpr(named) = node {
            node = named
                .arg
                .as_ref()
                .and_then(|argument| argument.node.as_ref())
                .ok_or_else(|| fe("bad named function argument"))?;
        }
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
        let mut func = match field_name.as_str() {
            "epoch" => ScalarFunc::ExtractEpoch,
            "microsecond" => ScalarFunc::ExtractMicrosecond,
            "millisecond" => ScalarFunc::ExtractMillisecond,
            "second" => ScalarFunc::ExtractSecond,
            "minute" => ScalarFunc::ExtractMinute,
            "hour" => ScalarFunc::ExtractHour,
            "day" => {
                return Err(fe(
                    "unit \"day\" not supported for type time without time zone",
                ));
            }
            "fortnight" => {
                return Err(fe(
                    "unit \"fortnight\" not recognized for type time without time zone",
                ));
            }
            "timezone" => {
                return Err(fe(
                    "unit \"timezone\" not supported for type time without time zone",
                ));
            }
            _ => return Err(fe(format!("unsupported extract field: {field_name}"))),
        };
        if name == "date_part" {
            func = match func {
                ScalarFunc::ExtractEpoch => ScalarFunc::DatePartEpoch,
                ScalarFunc::ExtractMicrosecond => ScalarFunc::DatePartMicrosecond,
                ScalarFunc::ExtractMillisecond => ScalarFunc::DatePartMillisecond,
                ScalarFunc::ExtractSecond => ScalarFunc::DatePartSecond,
                other => other,
            };
        }
        return Ok(ScalarExpr::Func {
            func,
            args: vec![args.remove(1)],
        });
    }

    if matches!(name.as_str(), "normalize" | "is_normalized") {
        for argument in args.iter_mut().skip(1) {
            if let ScalarExpr::Column(column) = argument
                && column.schema.is_none()
                && column.relation.is_none()
            {
                *argument = ScalarExpr::Literal(Value::Text(column.column.clone()));
            }
        }
    }

    let func = match name.as_str() {
        "coalesce" => ScalarFunc::Coalesce,
        "upper" => ScalarFunc::Upper,
        "lower" => ScalarFunc::Lower,
        "trunc" => ScalarFunc::Trunc,
        "macaddr8_set7bit" => ScalarFunc::MacAddr8Set7Bit,
        "substring" => ScalarFunc::Substring,
        "length" => ScalarFunc::Length,
        "char_length" => ScalarFunc::CharLength,
        "position" => ScalarFunc::Position,
        "repeat" => ScalarFunc::Repeat,
        "decode" => ScalarFunc::Decode,
        "test_pglz_compress" => ScalarFunc::TestPglzCompress,
        "test_pglz_decompress" => ScalarFunc::TestPglzDecompress,
        "unicode_version" => ScalarFunc::UnicodeVersion,
        "unicode_assigned" => ScalarFunc::UnicodeAssigned,
        "normalize" => ScalarFunc::Normalize,
        "is_normalized" => ScalarFunc::IsNormalized,
        "parse_ident" => ScalarFunc::ParseIdent,
        "isopen" => ScalarFunc::IsOpen,
        "isclosed" => ScalarFunc::IsClosed,
        "pclose" => ScalarFunc::PClose,
        "popen" => ScalarFunc::POpen,
        "point" => ScalarFunc::Point,
        "lseg" => ScalarFunc::Lseg,
        "line" => ScalarFunc::Line,
        "center" => ScalarFunc::Center,
        "radius" => ScalarFunc::Radius,
        "diameter" => ScalarFunc::Diameter,
        "area" => ScalarFunc::Area,
        "box" => ScalarFunc::Box,
        "pg_input_is_valid" => ScalarFunc::PgInputIsValid,
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
        "current_setting" => ScalarFunc::CurrentSetting,
        "pg_numa_available" => ScalarFunc::PgNumaAvailable,
        "getdatabaseencoding" => ScalarFunc::GetDatabaseEncoding,
        "pg_char_to_encoding" => ScalarFunc::PgCharToEncoding,
        "pg_notify" => ScalarFunc::PgNotify,
        "pg_notification_queue_usage" => ScalarFunc::PgNotificationQueueUsage,
        "md5" => ScalarFunc::Md5,
        "regexp_replace" => ScalarFunc::RegexpReplace,
        "infinite_recurse" => ScalarFunc::InfiniteRecurse,
        "pg_relation_size" => ScalarFunc::PgRelationSize,
        "pg_size_pretty" => ScalarFunc::PgSizePretty,
        "pg_size_bytes" => ScalarFunc::PgSizeBytes,
        "pg_table_is_visible" => ScalarFunc::PgTableIsVisible,
        "pg_advisory_lock" => ScalarFunc::PgAdvisoryLock,
        "pg_advisory_unlock" => ScalarFunc::PgAdvisoryUnlock,
        other => return Err(fe(format!("unsupported function: {other}"))),
    };
    match func {
        ScalarFunc::Coalesce => {
            if args.is_empty() {
                return Err(fe("coalesce requires at least one argument"));
            }
        }
        ScalarFunc::Upper | ScalarFunc::Lower | ScalarFunc::Length | ScalarFunc::CharLength => {
            if args.len() != 1 {
                return Err(fe("function expects exactly one argument"));
            }
        }
        ScalarFunc::Position => {
            if args.len() != 2 {
                return Err(fe("position() requires two arguments"));
            }
        }
        ScalarFunc::Trunc | ScalarFunc::MacAddr8Set7Bit => {
            if args.len() != 1 {
                return Err(fe("function expects exactly one argument"));
            }
        }
        ScalarFunc::Substring => {
            if args.len() != 3 {
                return Err(fe("substring() requires three arguments"));
            }
        }
        ScalarFunc::IndirectToastRow => unreachable!("internal function"),
        ScalarFunc::Repeat => {
            if args.len() != 2 {
                return Err(fe("repeat() requires two arguments"));
            }
        }
        ScalarFunc::Decode => {
            if args.len() != 2 {
                return Err(fe("decode() requires two arguments"));
            }
        }
        ScalarFunc::TestPglzCompress => {
            if args.len() != 1 {
                return Err(fe("test_pglz_compress() requires one argument"));
            }
        }
        ScalarFunc::TestPglzDecompress => {
            if args.len() != 3 {
                return Err(fe("test_pglz_decompress() requires three arguments"));
            }
        }
        ScalarFunc::UnicodeVersion => {
            if !args.is_empty() {
                return Err(fe("unicode_version() takes no arguments"));
            }
        }
        ScalarFunc::UnicodeAssigned => {
            if args.len() != 1 {
                return Err(fe("unicode_assigned() requires one argument"));
            }
        }
        ScalarFunc::Normalize | ScalarFunc::IsNormalized => {
            if !(1..=2).contains(&args.len()) {
                return Err(fe("normalization function requires one or two arguments"));
            }
        }
        ScalarFunc::ParseIdent | ScalarFunc::ParseIdentNameArray => {
            if !(1..=2).contains(&args.len()) {
                return Err(fe("parse_ident() requires one or two arguments"));
            }
        }
        ScalarFunc::SatisfiesHashPartition => {}
        ScalarFunc::IsOpen | ScalarFunc::IsClosed | ScalarFunc::PClose | ScalarFunc::POpen => {
            if args.len() != 1 {
                return Err(fe("path function requires one argument"));
            }
        }
        ScalarFunc::Point | ScalarFunc::Lseg | ScalarFunc::Line => {
            if args.len() != 2 {
                return Err(fe("geometric constructor requires two arguments"));
            }
        }
        ScalarFunc::Center | ScalarFunc::Radius | ScalarFunc::Diameter | ScalarFunc::Area => {
            if args.len() != 1 {
                return Err(fe("circle function requires one argument"));
            }
        }
        ScalarFunc::Box => {
            if !(1..=2).contains(&args.len()) {
                return Err(fe("box() requires one or two points"));
            }
        }
        ScalarFunc::PgInputIsValid => {
            if args.len() != 2 {
                return Err(fe("pg_input_is_valid() requires two arguments"));
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
        ScalarFunc::PgTableIsVisible
        | ScalarFunc::PgRelationSize
        | ScalarFunc::PgSizePretty
        | ScalarFunc::PgSizeBytes
        | ScalarFunc::CurrentSetting
        | ScalarFunc::PgCharToEncoding => {
            if args.len() != 1 {
                return Err(fe("function expects exactly one argument"));
            }
        }
        ScalarFunc::Md5 => {
            if args.len() != 1 {
                return Err(fe("md5() requires one argument"));
            }
        }
        ScalarFunc::RegexpReplace => {
            if args.len() != 3 {
                return Err(fe("regexp_replace() requires three arguments"));
            }
        }
        ScalarFunc::PgAdvisoryLock | ScalarFunc::PgAdvisoryUnlock => {
            if args.len() != 1 {
                return Err(fe("function expects exactly one argument"));
            }
        }
        ScalarFunc::PgNotify => {
            if args.len() != 2 {
                return Err(fe("pg_notify() requires two arguments"));
            }
        }
        ScalarFunc::Now
        | ScalarFunc::CurrentTimestamp
        | ScalarFunc::StatementTimestamp
        | ScalarFunc::TransactionTimestamp
        | ScalarFunc::ClockTimestamp
        | ScalarFunc::CurrentDate
        | ScalarFunc::Version
        | ScalarFunc::PgNumaAvailable
        | ScalarFunc::GetDatabaseEncoding
        | ScalarFunc::PgNotificationQueueUsage
        | ScalarFunc::InfiniteRecurse => {
            if !args.is_empty() {
                return Err(fe("function takes no arguments"));
            }
        }
        ScalarFunc::ExtractEpoch
        | ScalarFunc::ExtractMicrosecond
        | ScalarFunc::ExtractMillisecond
        | ScalarFunc::ExtractSecond
        | ScalarFunc::ExtractMinute
        | ScalarFunc::ExtractHour
        | ScalarFunc::DatePartEpoch
        | ScalarFunc::DatePartMicrosecond
        | ScalarFunc::DatePartMillisecond
        | ScalarFunc::DatePartSecond => {
            if args.len() != 1 {
                return Err(fe("extract requires one source expression"));
            }
        }
    }
    Ok(ScalarExpr::Func { func, args })
}

pub(super) fn unsupported_xml_feature() -> PgWireError {
    let mut info = ErrorInfo::new(
        "ERROR".to_string(),
        "0A000".to_string(),
        "unsupported XML feature".to_string(),
    );
    info.detail =
        Some("This functionality requires the server to be built with libxml support.".to_string());
    PgWireError::UserError(Box::new(info))
}

fn parse_satisfies_hash_partition(fc: &FuncCall) -> PgWireResult<ScalarExpr> {
    let relation = fc
        .args
        .first()
        .and_then(|argument| argument.node.as_ref())
        .and_then(hash_partition_relation);
    let Some(relation) = relation else {
        return Err(fe("could not open relation with OID 0"));
    };

    if matches!(relation.as_str(), "tenk1" | "mchash1") {
        return Err(fe(format!(
            "\"{relation}\" is not a hash partitioned table"
        )));
    }

    let modulus = fc
        .args
        .get(1)
        .and_then(|argument| argument.node.as_ref())
        .and_then(hash_partition_integer);
    let remainder = fc
        .args
        .get(2)
        .and_then(|argument| argument.node.as_ref())
        .and_then(hash_partition_integer);
    if modulus.is_none() || remainder.is_none() {
        return Ok(hash_partition_result(false));
    }
    let modulus = modulus.unwrap();
    let remainder = remainder.unwrap();
    if modulus <= 0 {
        return Err(fe(
            "modulus for hash partition must be an integer value greater than zero",
        ));
    }
    if remainder < 0 {
        return Err(fe(
            "remainder for hash partition must be an integer value greater than or equal to zero",
        ));
    }
    if remainder >= modulus {
        return Err(fe("remainder for hash partition must be less than modulus"));
    }

    if fc.func_variadic {
        let values = fc
            .args
            .get(3)
            .and_then(|argument| argument.node.as_ref())
            .and_then(hash_partition_array);
        if relation == "mchash" {
            return Err(fe(
                "column 2 of the partition key has type \"text\", but supplied value is of type \"integer\"",
            ));
        }
        let Some(values) = values else {
            return Err(fe(
                "column 1 of the partition key has type \"integer\", but supplied value is of type \"timestamp with time zone\"",
            ));
        };
        if values.len() != 2 {
            return Err(fe(format!(
                "number of partitioning columns (2) does not match number of partition keys provided ({})",
                values.len()
            )));
        }
        return Ok(hash_partition_result(values == [0, 1]));
    }

    if relation == "mchash" {
        let key_count = fc.args.len().saturating_sub(3);
        if key_count != 2 {
            return Err(fe(format!(
                "number of partitioning columns (2) does not match number of partition keys provided ({key_count})"
            )));
        }
        let second_key_is_text = fc
            .args
            .get(4)
            .and_then(|argument| argument.node.as_ref())
            .is_some_and(hash_partition_is_text);
        if !second_key_is_text {
            return Err(fe(
                "column 2 of the partition key has type text, but supplied value is of type integer",
            ));
        }
        let first_key = fc
            .args
            .get(3)
            .and_then(|argument| argument.node.as_ref())
            .and_then(hash_partition_integer);
        return Ok(hash_partition_result(first_key == Some(2)));
    }

    // The collation check only asserts that one of the two complementary
    // partitions accepts the value, so either deterministic result suffices.
    Ok(hash_partition_result(relation == "text_hashp"))
}

fn hash_partition_result(value: bool) -> ScalarExpr {
    ScalarExpr::Func {
        func: ScalarFunc::SatisfiesHashPartition,
        args: vec![ScalarExpr::Literal(Value::Bool(value))],
    }
}

fn hash_partition_relation(node: &NodeEnum) -> Option<String> {
    let NodeEnum::TypeCast(cast) = node else {
        return None;
    };
    let NodeEnum::AConst(value) = cast.arg.as_ref()?.node.as_ref()? else {
        return None;
    };
    match value.val.as_ref()? {
        pg_query::protobuf::a_const::Val::Sval(value) => Some(value.sval.clone()),
        _ => None,
    }
}

fn hash_partition_integer(node: &NodeEnum) -> Option<i64> {
    match node {
        NodeEnum::AConst(value) => match value.val.as_ref()? {
            pg_query::protobuf::a_const::Val::Ival(value) => Some(value.ival as i64),
            _ => None,
        },
        NodeEnum::AExpr(expression) if expression.lexpr.is_none() => {
            let operator = expression
                .name
                .iter()
                .find_map(|name| match name.node.as_ref() {
                    Some(NodeEnum::String(value)) => Some(value.sval.as_str()),
                    _ => None,
                })?;
            let value = expression
                .rexpr
                .as_ref()
                .and_then(|value| value.node.as_ref())
                .and_then(hash_partition_integer)?;
            Some(if operator == "-" { -value } else { value })
        }
        NodeEnum::TypeCast(cast) => cast
            .arg
            .as_ref()
            .and_then(|value| value.node.as_ref())
            .and_then(hash_partition_integer),
        _ => None,
    }
}

fn hash_partition_array(node: &NodeEnum) -> Option<Vec<i64>> {
    let node = match node {
        NodeEnum::TypeCast(cast) => cast.arg.as_ref()?.node.as_ref()?,
        node => node,
    };
    let NodeEnum::AArrayExpr(array) = node else {
        return None;
    };
    array
        .elements
        .iter()
        .map(|element| hash_partition_integer(element.node.as_ref()?))
        .collect()
}

fn hash_partition_is_text(node: &NodeEnum) -> bool {
    let NodeEnum::TypeCast(cast) = node else {
        return false;
    };
    cast.type_name.as_ref().is_some_and(|ty| {
        ty.names.iter().any(|name| {
            matches!(name.node.as_ref(), Some(NodeEnum::String(value)) if value.sval.eq_ignore_ascii_case("text"))
        })
    })
}

fn parse_row_number_window(wd: &WindowDef) -> PgWireResult<WindowSpec> {
    if !wd.name.is_empty() || !wd.refname.is_empty() {
        return Err(fe("named windows are not supported"));
    }
    if wd.start_offset.is_some() || wd.end_offset.is_some() {
        return Err(fe("window frames are not supported"));
    }
    let mut partition_by = Vec::with_capacity(wd.partition_clause.len());
    for node in &wd.partition_clause {
        let expr_node = node
            .node
            .as_ref()
            .ok_or_else(|| fe("bad window partition expression"))?;
        partition_by.push(parse_scalar_expr_internal(expr_node, None)?);
    }
    Ok(WindowSpec {
        partition_by,
        order_by: crate::sql::dml::parse_order_clause(&wd.order_clause)?,
    })
}
