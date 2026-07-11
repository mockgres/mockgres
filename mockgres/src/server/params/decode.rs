use super::*;

pub fn decode_param_value(
    raw: Option<&[u8]>,
    fmt: FieldFormat,
    ty: Option<DataType>,
    tz: &SessionTimeZone,
) -> PgWireResult<Value> {
    if raw.is_none() {
        return Ok(Value::Null);
    }
    let bytes = raw.unwrap();
    let ty = ty.unwrap_or(DataType::Text);
    match fmt {
        FieldFormat::Text => parse_text_value(bytes, &ty, tz),
        FieldFormat::Binary => parse_binary_value(bytes, &ty, tz),
    }
}

fn parse_text_value(bytes: &[u8], ty: &DataType, tz: &SessionTimeZone) -> PgWireResult<Value> {
    let s = std::str::from_utf8(bytes).map_err(|e| fe(format!("invalid utf8 parameter: {e}")))?;
    match ty {
        DataType::Int2 => {
            let v: i16 = s
                .trim()
                .parse()
                .map_err(|e| fe(format!("bad int2 param: {e}")))?;
            Ok(Value::Int64(v as i64))
        }
        DataType::Int4 => {
            let v: i32 = s
                .trim()
                .parse()
                .map_err(|e| fe(format!("bad int4 param: {e}")))?;
            Ok(Value::Int64(v as i64))
        }
        DataType::Int8 => {
            let v: i64 = s
                .trim()
                .parse()
                .map_err(|e| fe(format!("bad int8 param: {e}")))?;
            Ok(Value::Int64(v))
        }
        DataType::Float8 => {
            let v: f64 = s
                .parse()
                .map_err(|e| fe(format!("bad float8 param: {e}")))?;
            Ok(Value::from_f64(v))
        }
        DataType::Text => Ok(Value::Text(s.to_string())),
        DataType::Varchar(length) => crate::engine::coerce_value_to_type(
            Value::Text(s.to_string()),
            &DataType::Varchar(*length),
            tz,
        )
        .map_err(|error| fe_code(error.code, error.message)),
        DataType::Name => {
            crate::engine::coerce_value_to_type(Value::Text(s.to_string()), &DataType::Name, tz)
                .map_err(|e| fe_code(e.code, e.message))
        }
        DataType::BpChar(length) => {
            let value = crate::engine::coerce_value_to_type(
                Value::Text(s.to_string()),
                &DataType::BpChar(*length),
                tz,
            )
            .map_err(|e| fe_code(e.code, e.message))?;
            Ok(value)
        }
        DataType::PgChar => {
            crate::engine::coerce_value_to_type(Value::Text(s.to_string()), &DataType::PgChar, tz)
                .map_err(|error| fe_code(error.code, error.message))
        }
        DataType::Point => crate::engine::parse_point_text(s)
            .map(Value::Point)
            .map_err(|error| fe_code(error.code, error.message)),
        DataType::Lseg => crate::engine::parse_lseg_text(s)
            .map(Value::Lseg)
            .map_err(|error| fe_code(error.code, error.message)),
        DataType::Line => crate::engine::parse_line_text(s)
            .map(Value::Line)
            .map_err(|error| fe_code(error.code, error.message)),
        DataType::Circle => crate::engine::parse_circle_text(s)
            .map(Value::Circle)
            .map_err(|error| fe_code(error.code, error.message)),
        DataType::Box => crate::engine::parse_box_text(s)
            .map(Value::Box)
            .map_err(|error| fe_code(error.code, error.message)),
        DataType::Tid => crate::engine::parse_tid_text(s)
            .map(Value::Tid)
            .map_err(|error| fe_code(error.code, error.message)),
        DataType::Oid => crate::engine::parse_oid_text(s)
            .map(Value::Oid)
            .map_err(|error| fe_code(error.code, error.message)),
        DataType::PgLsn => crate::engine::parse_pg_lsn_text(s)
            .map(Value::PgLsn)
            .map_err(|error| fe_code(error.code, error.message)),
        DataType::MacAddr => crate::engine::parse_macaddr_text(s)
            .map(Value::MacAddr)
            .map_err(|error| fe_code(error.code, error.message)),
        DataType::MacAddr8 => crate::engine::parse_macaddr8_text(s)
            .map(Value::MacAddr8)
            .map_err(|error| fe_code(error.code, error.message)),
        DataType::Path => crate::engine::parse_path_text(s)
            .map(Value::Path)
            .map_err(|error| fe_code(error.code, error.message)),
        DataType::Json => Ok(Value::Text(s.to_string())),
        DataType::Jsonb => Ok(Value::Text(s.to_string())),
        DataType::Bool => {
            let lowered = s.to_ascii_lowercase();
            match lowered.as_str() {
                "t" | "true" => Ok(Value::Bool(true)),
                "f" | "false" => Ok(Value::Bool(false)),
                other => Err(fe(format!("bad bool param: {other}"))),
            }
        }
        DataType::Date => {
            let days = parse_date_str(s).map_err(fe)?;
            Ok(Value::Date(days))
        }
        DataType::Time(precision) => crate::engine::parse_time_text(s, *precision)
            .map(Value::TimeMicros)
            .map_err(|error| fe_code(error.code, error.message)),
        DataType::Timestamp => {
            let micros = parse_timestamp_str(s).map_err(fe)?;
            Ok(Value::TimestampMicros(micros))
        }
        DataType::Timestamptz => {
            let micros = parse_timestamptz_str(s, tz).map_err(fe)?;
            Ok(Value::TimestamptzMicros(micros))
        }
        DataType::Bytea => {
            let bytes = parse_bytea_text(s).map_err(fe)?;
            Ok(Value::Bytes(bytes))
        }
        DataType::Interval => {
            let micros =
                parse_interval_literal(s).map_err(|e| fe(format!("bad interval param: {e}")))?;
            Ok(Value::IntervalMicros(micros))
        }
        DataType::Void => Ok(Value::Null),
    }
}

fn parse_binary_value(bytes: &[u8], ty: &DataType, tz: &SessionTimeZone) -> PgWireResult<Value> {
    match ty {
        DataType::Int2 => {
            let arr: [u8; 2] = bytes
                .try_into()
                .map_err(|_| fe("binary int2 must be 2 bytes"))?;
            Ok(Value::Int64(i16::from_be_bytes(arr) as i64))
        }
        DataType::Int4 => {
            let arr: [u8; 4] = bytes
                .try_into()
                .map_err(|_| fe("binary int4 must be 4 bytes"))?;
            Ok(Value::Int64(i32::from_be_bytes(arr) as i64))
        }
        DataType::Int8 => {
            let arr: [u8; 8] = bytes
                .try_into()
                .map_err(|_| fe("binary int8 must be 8 bytes"))?;
            Ok(Value::Int64(i64::from_be_bytes(arr)))
        }
        DataType::Float8 => {
            let arr: [u8; 8] = bytes
                .try_into()
                .map_err(|_| fe("binary float8 must be 8 bytes"))?;
            Ok(Value::Float64Bits(u64::from_be_bytes(arr)))
        }
        DataType::Bool => {
            if bytes.len() != 1 {
                return Err(fe("binary bool must be 1 byte"));
            }
            Ok(Value::Bool(bytes[0] != 0))
        }
        DataType::Text => {
            let s = std::str::from_utf8(bytes)
                .map_err(|e| fe(format!("invalid utf8 parameter: {e}")))?;
            Ok(Value::Text(s.to_string()))
        }
        DataType::Varchar(length) => {
            let s = std::str::from_utf8(bytes)
                .map_err(|error| fe(format!("invalid utf8 parameter: {error}")))?;
            crate::engine::coerce_value_to_type(
                Value::Text(s.to_string()),
                &DataType::Varchar(*length),
                tz,
            )
            .map_err(|error| fe_code(error.code, error.message))
        }
        DataType::Name => {
            let s = std::str::from_utf8(bytes)
                .map_err(|e| fe(format!("invalid utf8 parameter: {e}")))?;
            crate::engine::coerce_value_to_type(Value::Text(s.to_string()), &DataType::Name, tz)
                .map_err(|e| fe_code(e.code, e.message))
        }
        DataType::BpChar(length) => {
            let s = std::str::from_utf8(bytes)
                .map_err(|e| fe(format!("invalid utf8 parameter: {e}")))?;
            crate::engine::coerce_value_to_type(
                Value::Text(s.to_string()),
                &DataType::BpChar(*length),
                tz,
            )
            .map_err(|e| fe_code(e.code, e.message))
        }
        DataType::PgChar => {
            if bytes.len() != 1 {
                return Err(fe("binary char must be 1 byte"));
            }
            Ok(Value::PgChar(bytes[0]))
        }
        DataType::Point => {
            if bytes.len() != 16 {
                return Err(fe("binary point must be 16 bytes"));
            }
            let x = f64::from_be_bytes(bytes[..8].try_into().expect("point x width checked"));
            let y = f64::from_be_bytes(bytes[8..].try_into().expect("point y width checked"));
            Ok(Value::Point(crate::engine::PointValue::new(x, y)))
        }
        DataType::Lseg => {
            if bytes.len() != 32 {
                return Err(fe("binary lseg must be 32 bytes"));
            }
            let coordinate = |offset: usize| {
                f64::from_be_bytes(
                    bytes[offset..offset + 8]
                        .try_into()
                        .expect("binary lseg coordinate width checked"),
                )
            };
            Ok(Value::Lseg(crate::engine::LsegValue::new(
                crate::engine::PointValue::new(coordinate(0), coordinate(8)),
                crate::engine::PointValue::new(coordinate(16), coordinate(24)),
            )))
        }
        DataType::Line => {
            if bytes.len() != 24 {
                return Err(fe("binary line must be 24 bytes"));
            }
            let coordinate = |offset: usize| {
                f64::from_be_bytes(
                    bytes[offset..offset + 8]
                        .try_into()
                        .expect("binary line coordinate width checked"),
                )
            };
            Ok(Value::Line(crate::engine::LineValue::new(
                coordinate(0),
                coordinate(8),
                coordinate(16),
            )))
        }
        DataType::Circle => {
            if bytes.len() != 24 {
                return Err(fe("binary circle must be 24 bytes"));
            }
            let coordinate = |offset: usize| {
                f64::from_be_bytes(
                    bytes[offset..offset + 8]
                        .try_into()
                        .expect("binary circle coordinate width checked"),
                )
            };
            Ok(Value::Circle(crate::engine::CircleValue::new(
                crate::engine::PointValue::new(coordinate(0), coordinate(8)),
                coordinate(16),
            )))
        }
        DataType::Box => {
            if bytes.len() != 32 {
                return Err(fe("binary box must be 32 bytes"));
            }
            let coordinate = |offset: usize| {
                f64::from_be_bytes(
                    bytes[offset..offset + 8]
                        .try_into()
                        .expect("binary box coordinate width checked"),
                )
            };
            Ok(Value::Box(crate::engine::BoxValue::new(
                crate::engine::PointValue::new(coordinate(0), coordinate(8)),
                crate::engine::PointValue::new(coordinate(16), coordinate(24)),
            )))
        }
        DataType::Tid => {
            if bytes.len() != 6 {
                return Err(fe("binary tid must be 6 bytes"));
            }
            Ok(Value::Tid(crate::engine::TidValue::new(
                u32::from_be_bytes(
                    bytes[..4]
                        .try_into()
                        .expect("binary tid block width checked"),
                ),
                u16::from_be_bytes(
                    bytes[4..]
                        .try_into()
                        .expect("binary tid offset width checked"),
                ),
            )))
        }
        DataType::Oid => {
            if bytes.len() != 4 {
                return Err(fe("binary oid must be 4 bytes"));
            }
            Ok(Value::Oid(u32::from_be_bytes(
                bytes.try_into().expect("binary oid width checked"),
            )))
        }
        DataType::PgLsn => {
            if bytes.len() != 8 {
                return Err(fe("binary pg_lsn must be 8 bytes"));
            }
            Ok(Value::PgLsn(u64::from_be_bytes(
                bytes.try_into().expect("binary pg_lsn width checked"),
            )))
        }
        DataType::MacAddr => {
            if bytes.len() != 6 {
                return Err(fe("binary macaddr must be 6 bytes"));
            }
            Ok(Value::MacAddr(
                bytes.try_into().expect("binary macaddr width checked"),
            ))
        }
        DataType::MacAddr8 => {
            if bytes.len() != 8 {
                return Err(fe("binary macaddr8 must be 8 bytes"));
            }
            Ok(Value::MacAddr8(
                bytes.try_into().expect("binary macaddr8 width checked"),
            ))
        }
        DataType::Path => {
            if bytes.len() < 5 {
                return Err(fe("binary path must contain a header"));
            }
            let closed = match bytes[0] {
                0 => false,
                1 => true,
                _ => return Err(fe("binary path has an invalid closed flag")),
            };
            let point_count = i32::from_be_bytes(
                bytes[1..5]
                    .try_into()
                    .expect("binary path count width checked"),
            );
            if point_count <= 0 {
                return Err(fe("binary path must contain at least one point"));
            }
            let point_count = point_count as usize;
            let expected_len = point_count
                .checked_mul(16)
                .and_then(|coordinate_bytes| coordinate_bytes.checked_add(5))
                .ok_or_else(|| fe("binary path point count is too large"))?;
            if bytes.len() != expected_len {
                return Err(fe("binary path length does not match its point count"));
            }
            let points = bytes[5..]
                .chunks_exact(16)
                .map(|point| {
                    let x = f64::from_be_bytes(
                        point[..8].try_into().expect("binary path x width checked"),
                    );
                    let y = f64::from_be_bytes(
                        point[8..].try_into().expect("binary path y width checked"),
                    );
                    crate::engine::PointValue::new(x, y)
                })
                .collect();
            Ok(Value::Path(crate::engine::PathValue::new(closed, points)))
        }
        DataType::Json => {
            let s = std::str::from_utf8(bytes)
                .map_err(|e| fe(format!("invalid utf8 parameter: {e}")))?;
            Ok(Value::Text(s.to_string()))
        }
        DataType::Jsonb => {
            let s = std::str::from_utf8(bytes)
                .map_err(|e| fe(format!("invalid utf8 parameter: {e}")))?;
            Ok(Value::Text(s.to_string()))
        }
        DataType::Bytea => Ok(Value::Bytes(bytes.to_vec())),
        DataType::Date => {
            let arr: [u8; 4] = bytes
                .try_into()
                .map_err(|_| fe("binary date must be 4 bytes"))?;
            let pg_days = i32::from_be_bytes(arr);
            let days = postgres_days_to_date(pg_days);
            Ok(Value::Date(days))
        }
        DataType::Time(_) => {
            if bytes.len() != 8 {
                return Err(fe("binary time must be 8 bytes"));
            }
            Ok(Value::TimeMicros(u64::from_be_bytes(
                bytes.try_into().expect("binary time width checked"),
            )))
        }
        DataType::Timestamp => {
            let arr: [u8; 8] = bytes
                .try_into()
                .map_err(|_| fe("binary timestamp must be 8 bytes"))?;
            let pg_micros = i64::from_be_bytes(arr);
            let micros = postgres_micros_to_timestamp(pg_micros);
            Ok(Value::TimestampMicros(micros))
        }
        DataType::Timestamptz => {
            let arr: [u8; 8] = bytes
                .try_into()
                .map_err(|_| fe("binary timestamptz must be 8 bytes"))?;
            let pg_micros = i64::from_be_bytes(arr);
            let micros = postgres_micros_to_timestamp(pg_micros);
            Ok(Value::TimestamptzMicros(micros))
        }
        DataType::Interval => {
            let s = std::str::from_utf8(bytes)
                .map_err(|e| fe(format!("invalid utf8 parameter: {e}")))?;
            let micros =
                parse_interval_literal(s).map_err(|e| fe(format!("bad interval param: {e}")))?;
            Ok(Value::IntervalMicros(micros))
        }
        DataType::Void => Ok(Value::Null),
    }
}
