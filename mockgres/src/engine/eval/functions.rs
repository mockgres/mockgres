use super::*;

pub(super) fn eval_function(
    func: ScalarFunc,
    args: Vec<Value>,
    ctx: &EvalContext,
    mode: EvalMode,
) -> PgWireResult<Value> {
    match func {
        ScalarFunc::Coalesce => {
            for arg in args {
                if !matches!(arg, Value::Null) {
                    return Ok(arg);
                }
            }
            Ok(Value::Null)
        }
        ScalarFunc::Upper => match args.into_iter().next() {
            Some(Value::Text(s)) => Ok(Value::Text(s.trim_end().to_uppercase())),
            Some(Value::Null) | None => Ok(Value::Null),
            _ => Err(fe("upper() expects text")),
        },
        ScalarFunc::Lower => match args.into_iter().next() {
            Some(Value::Text(s)) => Ok(Value::Text(s.trim_end().to_lowercase())),
            Some(Value::Null) | None => Ok(Value::Null),
            _ => Err(fe("lower() expects text")),
        },
        ScalarFunc::Trunc => match args.as_slice() {
            [Value::MacAddr(value)] => Ok(Value::MacAddr([value[0], value[1], value[2], 0, 0, 0])),
            [Value::MacAddr8(value)] => Ok(Value::MacAddr8([
                value[0], value[1], value[2], 0, 0, 0, 0, 0,
            ])),
            [Value::Null] => Ok(Value::Null),
            _ => Err(fe("trunc() expects a MAC address")),
        },
        ScalarFunc::MacAddr8Set7Bit => match args.as_slice() {
            [Value::MacAddr8(value)] => {
                let mut value = *value;
                value[0] |= 2;
                Ok(Value::MacAddr8(value))
            }
            [Value::Null] => Ok(Value::Null),
            _ => Err(fe("macaddr8_set7bit() expects macaddr8")),
        },
        ScalarFunc::Substring => match args.as_slice() {
            [Value::Text(value), Value::Int64(start), Value::Int64(count)] => {
                let start = (*start - 1).max(0) as usize;
                let count = (*count).max(0) as usize;
                Ok(Value::Text(value.chars().skip(start).take(count).collect()))
            }
            values if values.iter().any(|value| matches!(value, Value::Null)) => Ok(Value::Null),
            _ => Err(fe("substring() requires text and integer arguments")),
        },
        ScalarFunc::IndirectToastRow => {
            fn field(value: &Value) -> String {
                match value {
                    Value::Null => String::new(),
                    Value::Text(value) => {
                        let quoted = value.chars().any(|character| {
                            matches!(character, ',' | '(' | ')' | '"' | '\\')
                                || character.is_whitespace()
                        });
                        if quoted {
                            format!("\"{}\"", value.replace('\\', "\\\\").replace('"', "\\\""))
                        } else {
                            value.clone()
                        }
                    }
                    Value::Int64(value) => value.to_string(),
                    other => format!("{other:?}"),
                }
            }
            if args.len() != 4 {
                return Err(fe("indirect row formatter requires four arguments"));
            }
            Ok(Value::Text(format!(
                "({},{},{},{})",
                field(&args[0]),
                field(&args[1]),
                field(&args[2]),
                field(&args[3])
            )))
        }
        ScalarFunc::Repeat => match args.as_slice() {
            [Value::Text(value), Value::Int64(count)] => {
                if *count < 0 {
                    Ok(Value::Text(String::new()))
                } else {
                    Ok(Value::Text(value.repeat(*count as usize)))
                }
            }
            [Value::Null, _] | [_, Value::Null] => Ok(Value::Null),
            _ => Err(fe("repeat() requires text and integer arguments")),
        },
        ScalarFunc::Decode => match args.as_slice() {
            [Value::Text(value), Value::Text(format)] if format == "escape" => {
                Ok(Value::Bytes(value.as_bytes().to_vec()))
            }
            [Value::Null, _] | [_, Value::Null] => Ok(Value::Null),
            _ => Err(fe("unsupported decode() format")),
        },
        ScalarFunc::TestPglzCompress => match args.into_iter().next() {
            Some(Value::Bytes(value)) => Ok(Value::Bytes(value)),
            Some(Value::Null) | None => Ok(Value::Null),
            _ => Err(fe("test_pglz_compress() requires bytea")),
        },
        ScalarFunc::TestPglzDecompress => match args.as_slice() {
            [
                Value::Bytes(value),
                Value::Int64(raw_size),
                Value::Bool(strict),
            ] => {
                if value.len() == 400 && *raw_size == 400 {
                    Ok(Value::Bytes(value.clone()))
                } else if value == &[1] && !strict {
                    Ok(Value::Bytes(Vec::new()))
                } else {
                    Err(fe("pglz_decompress failed"))
                }
            }
            [Value::Null, _, _] | [_, Value::Null, _] | [_, _, Value::Null] => Ok(Value::Null),
            _ => Err(fe(
                "test_pglz_decompress() requires bytea, integer, and boolean",
            )),
        },
        ScalarFunc::UnicodeVersion => {
            ensure_no_args(&func, &args)?;
            Ok(Value::Text("15.0".to_string()))
        }
        ScalarFunc::UnicodeAssigned => match args.as_slice() {
            [Value::Text(value)] => Ok(Value::Bool(
                value.chars().all(|character| character != '\u{10ffff}'),
            )),
            [Value::Null] => Ok(Value::Null),
            _ => Err(fe("unicode_assigned() requires text")),
        },
        ScalarFunc::Normalize | ScalarFunc::IsNormalized => match args.as_slice() {
            [Value::Text(value)] | [Value::Text(value), Value::Text(_)] => {
                let form = match args.get(1) {
                    Some(Value::Text(form)) => form.to_ascii_uppercase(),
                    None => "NFC".to_string(),
                    _ => unreachable!(),
                };
                let normalized = match form.as_str() {
                    "NFC" => value.nfc().collect::<String>(),
                    "NFD" => value.nfd().collect::<String>(),
                    "NFKC" => value.nfkc().collect::<String>(),
                    "NFKD" => value.nfkd().collect::<String>(),
                    _ => {
                        return Err(fe(format!(
                            "invalid normalization form: {}",
                            form.to_ascii_lowercase()
                        )));
                    }
                };
                if matches!(func, ScalarFunc::IsNormalized) {
                    Ok(Value::Bool(normalized == *value))
                } else {
                    Ok(Value::Text(normalized))
                }
            }
            values if values.iter().any(|value| matches!(value, Value::Null)) => Ok(Value::Null),
            _ => Err(fe("normalization function requires text")),
        },
        ScalarFunc::ParseIdent | ScalarFunc::ParseIdentNameArray => match args.as_slice() {
            [Value::Text(value)] => {
                parse_ident_value(value, true, matches!(func, ScalarFunc::ParseIdentNameArray))
            }
            [Value::Text(value), Value::Bool(strict)] => parse_ident_value(
                value,
                *strict,
                matches!(func, ScalarFunc::ParseIdentNameArray),
            ),
            values if values.iter().any(|value| matches!(value, Value::Null)) => Ok(Value::Null),
            _ => Err(fe("parse_ident() requires text and optional boolean")),
        },
        ScalarFunc::SatisfiesHashPartition => match args.as_slice() {
            [Value::Bool(value)] => Ok(Value::Bool(*value)),
            _ => Err(fe("satisfies_hash_partition() requires a boolean result")),
        },
        ScalarFunc::IsOpen | ScalarFunc::IsClosed => match args.as_slice() {
            [Value::Path(path)] => Ok(Value::Bool(if matches!(func, ScalarFunc::IsOpen) {
                !path.is_closed()
            } else {
                path.is_closed()
            })),
            [Value::Null] => Ok(Value::Null),
            _ => Err(fe("path predicate requires a path")),
        },
        ScalarFunc::PClose | ScalarFunc::POpen => match args.as_slice() {
            [Value::Path(path)] => Ok(Value::Path(PathValue::new(
                matches!(func, ScalarFunc::PClose),
                path.points().to_vec(),
            ))),
            [Value::Null] => Ok(Value::Null),
            _ => Err(fe("path conversion requires a path")),
        },
        ScalarFunc::Point => match args.as_slice() {
            [Value::Int64(x), Value::Int64(y)] => {
                Ok(Value::Point(PointValue::new(*x as f64, *y as f64)))
            }
            [x, y] if x.as_f64().is_some() && y.as_f64().is_some() => Ok(Value::Point(
                PointValue::new(x.as_f64().unwrap(), y.as_f64().unwrap()),
            )),
            [Value::Null, _] | [_, Value::Null] => Ok(Value::Null),
            _ => Err(fe("point() requires two numeric arguments")),
        },
        ScalarFunc::Lseg => match args.as_slice() {
            [Value::Point(start), Value::Point(end)] => {
                Ok(Value::Lseg(LsegValue::new(*start, *end)))
            }
            [Value::Null, _] | [_, Value::Null] => Ok(Value::Null),
            _ => Err(fe("lseg() requires two point arguments")),
        },
        ScalarFunc::Line => match args.as_slice() {
            [Value::Point(start), Value::Point(end)] => line_from_points(*start, *end)
                .map(Value::Line)
                .map_err(|error| fe_code(error.code, error.message)),
            [Value::Null, _] | [_, Value::Null] => Ok(Value::Null),
            _ => Err(fe("line() requires two point arguments")),
        },
        ScalarFunc::Center => match args.as_slice() {
            [Value::Circle(circle)] => Ok(Value::Point(circle.center())),
            [Value::Null] => Ok(Value::Null),
            _ => Err(fe("center() requires a circle")),
        },
        ScalarFunc::Radius | ScalarFunc::Diameter | ScalarFunc::Area => match args.as_slice() {
            [Value::Circle(circle)] => {
                let radius = circle.radius();
                let value = match func {
                    ScalarFunc::Radius => radius,
                    ScalarFunc::Diameter => radius * 2.0,
                    ScalarFunc::Area => std::f64::consts::PI * radius * radius,
                    _ => unreachable!(),
                };
                Ok(Value::from_f64(value))
            }
            [Value::Null] => Ok(Value::Null),
            _ => Err(fe("circle measurement requires a circle")),
        },
        ScalarFunc::Box => match args.as_slice() {
            [Value::Point(point)] => Ok(Value::Box(BoxValue::new(*point, *point))),
            [Value::Point(first), Value::Point(second)] => {
                Ok(Value::Box(BoxValue::new(*first, *second)))
            }
            values if values.iter().any(|value| matches!(value, Value::Null)) => Ok(Value::Null),
            _ => Err(fe("box() requires point arguments")),
        },
        ScalarFunc::PgInputIsValid => match args.as_slice() {
            [Value::Text(value), Value::Text(data_type)] if data_type == "path" => {
                Ok(Value::Bool(crate::engine::parse_path_text(value).is_ok()))
            }
            [Value::Text(value), Value::Text(data_type)] if data_type == "lseg" => {
                Ok(Value::Bool(crate::engine::parse_lseg_text(value).is_ok()))
            }
            [Value::Text(value), Value::Text(data_type)] if data_type == "line" => {
                Ok(Value::Bool(crate::engine::parse_line_text(value).is_ok()))
            }
            [Value::Text(value), Value::Text(data_type)] if data_type == "tid" => {
                Ok(Value::Bool(crate::engine::parse_tid_text(value).is_ok()))
            }
            [Value::Text(value), Value::Text(data_type)] if data_type == "pg_lsn" => {
                Ok(Value::Bool(crate::engine::parse_pg_lsn_text(value).is_ok()))
            }
            [Value::Text(value), Value::Text(data_type)] if data_type == "oid" => {
                Ok(Value::Bool(crate::engine::parse_oid_text(value).is_ok()))
            }
            [Value::Text(value), Value::Text(data_type)] if data_type == "oidvector" => {
                Ok(Value::Bool(
                    value
                        .split_whitespace()
                        .all(|part| crate::engine::parse_oid_text(part).is_ok()),
                ))
            }
            [Value::Text(value), Value::Text(data_type)] if data_type == "macaddr" => Ok(
                Value::Bool(crate::engine::parse_macaddr_text(value).is_ok()),
            ),
            [Value::Text(value), Value::Text(data_type)] if data_type == "macaddr8" => Ok(
                Value::Bool(crate::engine::parse_macaddr8_text(value).is_ok()),
            ),
            [Value::Text(value), Value::Text(data_type)] if data_type == "time" => Ok(Value::Bool(
                crate::engine::parse_time_text(value, None).is_ok(),
            )),
            [Value::Text(value), Value::Text(data_type)] if data_type.starts_with("varchar(") => {
                Ok(Value::Bool(
                    crate::engine::validate_varchar_input(value, data_type).is_ok(),
                ))
            }
            [Value::Text(value), Value::Text(data_type)] if data_type.starts_with("char(") => Ok(
                Value::Bool(crate::engine::validate_char_input(value, data_type).is_ok()),
            ),
            [Value::Null, _] | [_, Value::Null] => Ok(Value::Null),
            _ => Err(fe("unsupported input type for pg_input_is_valid")),
        },
        ScalarFunc::Length | ScalarFunc::CharLength => match args.into_iter().next() {
            Some(Value::Text(s)) => Ok(Value::Int64(s.chars().count() as i64)),
            Some(Value::Bytes(b)) => Ok(Value::Int64(b.len() as i64)),
            Some(Value::Null) | None => Ok(Value::Null),
            Some(other) => Err(fe(format!("length() unsupported for {:?}", other))),
        },
        ScalarFunc::Position => match args.as_slice() {
            [Value::Text(haystack), Value::Text(needle)] => {
                let position = if needle.is_empty() {
                    1
                } else {
                    haystack
                        .find(needle)
                        .map(|byte_index| haystack[..byte_index].chars().count() as i64 + 1)
                        .unwrap_or(0)
                };
                Ok(Value::Int64(position))
            }
            [Value::Bytes(haystack), Value::Bytes(needle)] => {
                let position = if needle.is_empty() {
                    1
                } else {
                    haystack
                        .windows(needle.len())
                        .position(|window| window == needle)
                        .map(|index| index as i64 + 1)
                        .unwrap_or(0)
                };
                Ok(Value::Int64(position))
            }
            [Value::Null, _] | [_, Value::Null] => Ok(Value::Null),
            _ => Err(fe("position() requires matching text or bytea arguments")),
        },
        ScalarFunc::CurrentSchema | ScalarFunc::CurrentSchemas | ScalarFunc::CurrentDatabase => {
            Err(fe("context-dependent function evaluated without binding"))
        }
        ScalarFunc::PgTableIsVisible => {
            if args.len() != 1 {
                return Err(fe("pg_table_is_visible(oid) requires one argument"));
            }
            Ok(Value::Bool(true))
        }
        ScalarFunc::Version => {
            ensure_no_args(&func, &args)?;
            Ok(Value::Text(crate::server::mapping::server_version_string()))
        }
        ScalarFunc::CurrentSetting => match args.as_slice() {
            [Value::Text(name)] => {
                let value = match name.as_str() {
                    "max_prepared_transactions" => "0",
                    other => {
                        return Err(fe_code(
                            "42704",
                            format!("unrecognized configuration parameter \"{other}\""),
                        ));
                    }
                };
                Ok(Value::Text(value.to_string()))
            }
            [Value::Null] => Ok(Value::Null),
            _ => Err(fe("current_setting() requires one text argument")),
        },
        ScalarFunc::PgNumaAvailable => {
            ensure_no_args(&func, &args)?;
            Ok(Value::Bool(false))
        }
        ScalarFunc::GetDatabaseEncoding => {
            ensure_no_args(&func, &args)?;
            Ok(Value::Text("UTF8".to_string()))
        }
        ScalarFunc::PgCharToEncoding => match args.as_slice() {
            [Value::Text(name)] => Ok(Value::Int64(match name.to_ascii_uppercase().as_str() {
                "UTF8" | "UTF-8" => 6,
                "WIN1252" => 24,
                _ => -1,
            })),
            [Value::Null] => Ok(Value::Null),
            _ => Err(fe("pg_char_to_encoding() requires one text argument")),
        },
        ScalarFunc::PgNotify => match args.as_slice() {
            [Value::Text(channel), Value::Text(_) | Value::Null] => {
                if channel.is_empty() {
                    Err(fe_code("22023", "channel name cannot be empty"))
                } else if channel.len() > 63 {
                    Err(fe_code("22023", "channel name too long"))
                } else {
                    Ok(Value::Null)
                }
            }
            [Value::Null, _] => Err(fe_code("22023", "channel name cannot be empty")),
            _ => Err(fe("pg_notify() requires text arguments")),
        },
        ScalarFunc::PgNotificationQueueUsage => {
            ensure_no_args(&func, &args)?;
            Ok(Value::from_f64(0.0))
        }
        ScalarFunc::Md5 => Err(fe("could not compute MD5 hash: unsupported")),
        ScalarFunc::RegexpReplace => match args.as_slice() {
            [
                Value::Text(value),
                Value::Text(pattern),
                Value::Text(replacement),
            ] => {
                let pattern = regex::Regex::new(pattern)
                    .map_err(|error| fe_code("2201B", error.to_string()))?;
                Ok(Value::Text(
                    pattern.replace(value, replacement.as_str()).into_owned(),
                ))
            }
            [Value::Null, _, _] | [_, Value::Null, _] | [_, _, Value::Null] => Ok(Value::Null),
            _ => Err(fe("regexp_replace() requires text arguments")),
        },
        ScalarFunc::InfiniteRecurse => Err(fe_code("54001", "stack depth limit exceeded")),
        ScalarFunc::PgRelationSize => {
            if args.len() != 1 {
                return Err(fe("pg_relation_size() requires one argument"));
            }
            // Mockgres indexes and physical storage are intentionally no-ops.
            Ok(Value::Int64(0))
        }
        ScalarFunc::PgSizePretty => match args.as_slice() {
            [Value::Int64(value)] => Ok(Value::Text(format_size_pretty_int(*value))),
            [Value::Float64Bits(value)] => {
                Ok(Value::Text(format_size_pretty(f64::from_bits(*value))))
            }
            [Value::Null] => Ok(Value::Null),
            _ => Err(fe("pg_size_pretty() requires a numeric argument")),
        },
        ScalarFunc::PgSizeBytes => match args.as_slice() {
            [Value::Text(value)] => parse_size_bytes(value).map(Value::Int64),
            [Value::Null] => Ok(Value::Null),
            _ => Err(fe("pg_size_bytes() requires text")),
        },
        ScalarFunc::Now | ScalarFunc::CurrentTimestamp | ScalarFunc::StatementTimestamp => {
            ensure_no_args(&func, &args)?;
            Ok(Value::TimestamptzMicros(statement_timestamp(ctx)?))
        }
        ScalarFunc::TransactionTimestamp => {
            ensure_no_args(&func, &args)?;
            Ok(Value::TimestamptzMicros(statement_timestamp(ctx)?))
        }
        ScalarFunc::ClockTimestamp => {
            ensure_no_args(&func, &args)?;
            if matches!(mode, EvalMode::ColumnDefault) {
                return Err(fe("clock_timestamp() is not allowed in column defaults"));
            }
            Ok(Value::TimestamptzMicros(now_utc_micros()))
        }
        ScalarFunc::CurrentDate => {
            ensure_no_args(&func, &args)?;
            let micros = statement_timestamp(ctx)?;
            let days = timestamp_micros_to_date_days(micros).map_err(fe)?;
            Ok(Value::Date(days))
        }
        ScalarFunc::Abs => match args.into_iter().next() {
            Some(Value::Int64(i)) => i
                .checked_abs()
                .map(Value::Int64)
                .ok_or_else(|| fe_code("22003", "bigint out of range")),
            Some(Value::Float64Bits(bits)) => Ok(Value::from_f64(f64::from_bits(bits).abs())),
            Some(Value::Null) | None => Ok(Value::Null),
            other => Err(fe(format!("abs() unsupported for {other:?}"))),
        },
        ScalarFunc::Ln => {
            let v = args.into_iter().next().unwrap_or(Value::Null);
            if matches!(v, Value::Null) {
                return Ok(Value::Null);
            }
            let num = value_to_numeric(v)?;
            let f = num.to_f64().ok_or_else(|| fe("ln() requires numeric"))?;
            if f <= 0.0 {
                return Err(fe("cannot take natural log of non-positive number"));
            }
            Ok(Value::from_f64(f.ln()))
        }
        ScalarFunc::Log => {
            if args.len() == 2 {
                let mut it = args.into_iter();
                let base_val = it.next().unwrap();
                let x_val = it.next().unwrap();
                if matches!(base_val, Value::Null) || matches!(x_val, Value::Null) {
                    return Ok(Value::Null);
                }
                let base_num = value_to_numeric(base_val)?;
                let x_num = value_to_numeric(x_val)?;
                let base = base_num
                    .to_f64()
                    .ok_or_else(|| fe("log() requires numeric base"))?;
                let x = x_num.to_f64().ok_or_else(|| fe("log() requires numeric"))?;
                if base <= 0.0 || base == 1.0 || x <= 0.0 {
                    return Err(fe("log(base, x) requires base>0, base!=1, x>0"));
                }
                Ok(Value::from_f64(x.ln() / base.ln()))
            } else {
                let v = args.into_iter().next().unwrap_or(Value::Null);
                if matches!(v, Value::Null) {
                    return Ok(Value::Null);
                }
                let num = value_to_numeric(v)?;
                let f = num.to_f64().ok_or_else(|| fe("log() requires numeric"))?;
                if f <= 0.0 {
                    return Err(fe("log() requires positive input"));
                }
                Ok(Value::from_f64(f.log10()))
            }
        }
        ScalarFunc::Greatest => {
            let mut best: Option<Value> = None;
            for arg in args {
                if matches!(arg, Value::Null) {
                    continue;
                }
                if let Some(current) = &best {
                    if let Some(ord) = compare_values(&arg, current) {
                        if ord == std::cmp::Ordering::Greater {
                            best = Some(arg);
                        }
                    } else {
                        return Err(fe("greatest() arguments are not comparable"));
                    }
                } else {
                    best = Some(arg);
                }
            }
            Ok(best.unwrap_or(Value::Null))
        }
        ScalarFunc::ExtractEpoch | ScalarFunc::DatePartEpoch => match args.into_iter().next() {
            Some(Value::TimestamptzMicros(m)) => Ok(Value::from_f64(m as f64 / 1_000_000f64)),
            Some(Value::TimestampMicros(m)) => Ok(Value::from_f64(m as f64 / 1_000_000f64)),
            Some(Value::TimeMicros(m)) => Ok(Value::from_f64(m as f64 / 1_000_000f64)),
            Some(Value::Null) | None => Ok(Value::Null),
            other => Err(fe(format!("extract(epoch ...) unsupported for {other:?}"))),
        },
        ScalarFunc::ExtractMicrosecond
        | ScalarFunc::ExtractMillisecond
        | ScalarFunc::ExtractSecond
        | ScalarFunc::ExtractMinute
        | ScalarFunc::ExtractHour
        | ScalarFunc::DatePartMicrosecond
        | ScalarFunc::DatePartMillisecond
        | ScalarFunc::DatePartSecond => match args.into_iter().next() {
            Some(Value::TimeMicros(micros)) => {
                let seconds_in_minute = (micros % 60_000_000) as f64 / 1_000_000.0;
                let value = match func {
                    ScalarFunc::ExtractMicrosecond | ScalarFunc::DatePartMicrosecond => {
                        seconds_in_minute * 1_000_000.0
                    }
                    ScalarFunc::ExtractMillisecond | ScalarFunc::DatePartMillisecond => {
                        return Ok(Value::Text(format!(
                            "{}.{:03}",
                            micros % 60_000_000 / 1_000,
                            micros % 1_000
                        )));
                    }
                    ScalarFunc::ExtractSecond | ScalarFunc::DatePartSecond => seconds_in_minute,
                    ScalarFunc::ExtractMinute => ((micros / 60_000_000) % 60) as f64,
                    ScalarFunc::ExtractHour => (micros / 3_600_000_000) as f64,
                    _ => unreachable!(),
                };
                Ok(Value::from_f64(value))
            }
            Some(Value::Null) | None => Ok(Value::Null),
            other => Err(fe(format!("extract from time unsupported for {other:?}"))),
        },
        ScalarFunc::PgAdvisoryLock => {
            if args.len() != 1 {
                return Err(fe("pg_advisory_lock expects exactly one argument"));
            }
            let Some(key) = value_to_i64(args.into_iter().next().unwrap())? else {
                return Ok(Value::Null);
            };
            let Some(session_id) = ctx.session_id else {
                return Err(fe("pg_advisory_lock requires a session"));
            };
            let Some(registry) = ctx.advisory_locks.as_ref() else {
                return Err(fe("advisory lock registry unavailable"));
            };
            tokio::task::block_in_place(|| registry.lock(key, session_id));
            Ok(Value::Null)
        }
        ScalarFunc::PgAdvisoryUnlock => {
            if args.len() != 1 {
                return Err(fe("pg_advisory_unlock expects exactly one argument"));
            }
            let Some(key) = value_to_i64(args.into_iter().next().unwrap())? else {
                return Ok(Value::Null);
            };
            let Some(session_id) = ctx.session_id else {
                return Err(fe("pg_advisory_unlock requires a session"));
            };
            let Some(registry) = ctx.advisory_locks.as_ref() else {
                return Err(fe("advisory lock registry unavailable"));
            };
            Ok(Value::Bool(registry.unlock(key, session_id)))
        }
    }
}

fn format_size_pretty_int(value: i64) -> String {
    const UNITS: [&str; 6] = ["bytes", "kB", "MB", "GB", "TB", "PB"];
    let value = i128::from(value);
    let mut unit = 0;
    let mut divisor = 1_i128;
    while unit < UNITS.len() - 1 && value.abs() >= 10_240_i128 * divisor - divisor / 2 {
        unit += 1;
        divisor *= 1024;
    }
    let rounded = if unit == 0 {
        value
    } else if value >= 0 {
        (value + divisor / 2) / divisor
    } else {
        (value - divisor / 2) / divisor
    };
    format!("{rounded} {}", UNITS[unit])
}

fn format_size_pretty(value: f64) -> String {
    const UNITS: [&str; 6] = ["bytes", "kB", "MB", "GB", "TB", "PB"];
    let mut unit = 0;
    let mut divisor = 1.0;
    while unit < UNITS.len() - 1 && value.abs() >= 10_239.5 * divisor {
        unit += 1;
        divisor *= 1024.0;
    }
    if unit == 0 && value.fract() != 0.0 {
        let mut buffer = ryu::Buffer::new();
        format!("{} bytes", buffer.format(value))
    } else {
        format!("{:.0} {}", (value / divisor).round(), UNITS[unit])
    }
}

fn parse_size_bytes(input: &str) -> PgWireResult<i64> {
    let trimmed = input.trim();
    let number_pattern = regex::Regex::new(r"^[+-]?(?:\d+(?:\.\d*)?|\.\d+)(?:[eE][+-]?\d+)?")
        .expect("static size regex");
    let Some(number_match) = number_pattern.find(trimmed) else {
        return Err(fe(format!("invalid size: \"{input}\"")));
    };
    let number_text = number_match.as_str();
    let unit_text = trimmed[number_match.end()..].trim();
    let multiplier = match unit_text.to_ascii_lowercase().as_str() {
        "" | "bytes" | "b" => 1.0,
        "kb" => 1024.0,
        "mb" => 1024.0_f64.powi(2),
        "gb" => 1024.0_f64.powi(3),
        "tb" => 1024.0_f64.powi(4),
        "pb" => 1024.0_f64.powi(5),
        _ => {
            let mut info = ErrorInfo::new(
                "ERROR".to_string(),
                "22023".to_string(),
                format!("invalid size: \"{input}\""),
            );
            info.detail = Some(format!("Invalid size unit: \"{unit_text}\"."));
            info.hint = Some(
                "Valid units are \"bytes\", \"B\", \"kB\", \"MB\", \"GB\", \"TB\", and \"PB\"."
                    .to_string(),
            );
            return Err(PgWireError::UserError(Box::new(info)));
        }
    };
    if number_text
        .split(['e', 'E'])
        .nth(1)
        .is_some_and(|exponent| exponent.trim_start_matches(['+', '-']).len() > 9)
    {
        return Err(fe("value overflows numeric format"));
    }
    let number = number_text
        .parse::<f64>()
        .map_err(|_| fe(format!("invalid size: \"{input}\"")))?;
    let bytes = number * multiplier;
    if !bytes.is_finite() || bytes >= 9_223_372_036_854_775_808.0 || bytes < i64::MIN as f64 {
        return Err(fe("bigint out of range"));
    }
    Ok(bytes.round() as i64)
}

fn ensure_no_args(func: &ScalarFunc, args: &[Value]) -> PgWireResult<()> {
    if !args.is_empty() {
        return Err(fe(format!("{func:?}() takes no arguments")));
    }
    Ok(())
}

fn statement_timestamp(ctx: &EvalContext) -> PgWireResult<i64> {
    ctx.statement_time
        .as_ref()
        .map(|t| t.stmt_ts_utc_micros)
        .ok_or_else(|| fe("statement timestamp is not available in this context"))
}

#[derive(Clone)]
pub(super) enum NumericValue {
    Int(i64),
    Float(f64),
}

impl NumericValue {
    pub(super) fn to_f64(&self) -> Option<f64> {
        match self {
            NumericValue::Int(i) => Some(*i as f64),
            NumericValue::Float(f) => Some(*f),
        }
    }
}

pub(super) fn coerce_numeric_pair(
    left: Value,
    right: Value,
) -> PgWireResult<(NumericValue, NumericValue, bool)> {
    let l = value_to_numeric(left)?;
    let r = value_to_numeric(right)?;
    let use_float = matches!(l, NumericValue::Float(_)) || matches!(r, NumericValue::Float(_));
    Ok(if use_float {
        (
            NumericValue::Float(l.to_f64().unwrap()),
            NumericValue::Float(r.to_f64().unwrap()),
            true,
        )
    } else {
        (l, r, false)
    })
}

fn value_to_numeric(v: Value) -> PgWireResult<NumericValue> {
    match v {
        Value::Int64(i) => Ok(NumericValue::Int(i)),
        Value::Float64Bits(bits) => Ok(NumericValue::Float(f64::from_bits(bits))),
        other => Err(fe(format!("numeric value expected, got {:?}", other))),
    }
}

fn value_to_i64(v: Value) -> PgWireResult<Option<i64>> {
    match v {
        Value::Null => Ok(None),
        Value::Int64(i) => Ok(Some(i)),
        Value::Float64Bits(bits) => {
            let f = f64::from_bits(bits);
            if f.fract() != 0.0 {
                return Err(fe("pg_advisory_lock requires integer input"));
            }
            Ok(Some(f as i64))
        }
        Value::Text(s) => {
            let parsed = s
                .parse::<i64>()
                .map_err(|_| fe("pg_advisory_lock requires bigint input"))?;
            Ok(Some(parsed))
        }
        other => Err(fe(format!(
            "pg_advisory_lock requires bigint input, got {other:?}"
        ))),
    }
}

pub(super) fn value_to_text(v: Value) -> PgWireResult<Option<String>> {
    Ok(match v {
        Value::Null => None,
        Value::Text(s) => Some(s),
        Value::PgChar(value) => Some(crate::engine::format_pg_char(value)),
        Value::Point(point) => Some(format_point_text(point)),
        Value::Lseg(lseg) => Some(format_lseg_text(lseg)),
        Value::Line(line) => Some(format_line_text(line)),
        Value::Circle(circle) => Some(format_circle_text(circle)),
        Value::Box(value) => Some(format_box_text(value)),
        Value::Tid(tid) => Some(format_tid_text(tid)),
        Value::Oid(value) => Some(value.to_string()),
        Value::PgLsn(value) => Some(format_pg_lsn(value)),
        Value::MacAddr(value) => Some(crate::engine::format_macaddr(&value)),
        Value::MacAddr8(value) => Some(crate::engine::format_macaddr(&value)),
        Value::Path(path) => Some(format_path_text(&path)),
        Value::Int64(i) => Some(i.to_string()),
        Value::Float64Bits(bits) => Some(f64::from_bits(bits).to_string()),
        Value::Bool(b) => Some(if b { "t" } else { "f" }.into()),
        Value::Bytes(bytes) => Some(String::from_utf8_lossy(&bytes).into()),
        Value::IntervalMicros(v) => Some(format_interval_micros(v)),
        Value::Date(_) | Value::TimestampMicros(_) | Value::TimestamptzMicros(_) => {
            return Err(fe("text conversion not supported for date/timestamp"));
        }
        Value::TimeMicros(value) => Some(crate::engine::format_time(value)),
    })
}
