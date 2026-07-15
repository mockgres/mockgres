use super::*;

pub fn cast_value_to_type(
    val: Value,
    target: &DataType,
    tz: &SessionTimeZone,
) -> Result<Value, SqlError> {
    convert_value_to_type(val, target, tz, false)
}

fn parse_pg_char(input: &str) -> Value {
    let byte = input
        .strip_prefix('\\')
        .filter(|digits| {
            digits.len() == 3 && digits.bytes().all(|byte| matches!(byte, b'0'..=b'7'))
        })
        .and_then(|digits| u8::from_str_radix(digits, 8).ok())
        .unwrap_or_else(|| input.as_bytes().first().copied().unwrap_or(0));
    Value::PgChar(byte)
}

pub fn coerce_value_to_type(
    val: Value,
    target: &DataType,
    tz: &SessionTimeZone,
) -> Result<Value, SqlError> {
    convert_value_to_type(val, target, tz, true)
}

fn convert_value_to_type(
    val: Value,
    target: &DataType,
    tz: &SessionTimeZone,
    assignment: bool,
) -> Result<Value, SqlError> {
    fn validate_json(input: &str, type_name: &str) -> Result<(), SqlError> {
        serde_json::from_str::<JsonValue>(input).map_err(|e| {
            SqlError::new(
                "22P02",
                format!("invalid input syntax for type {type_name}: {e}"),
            )
        })?;
        Ok(())
    }

    fn parse_integer_input(
        input: &str,
        min: i128,
        max: i128,
        type_name: &str,
    ) -> Result<i64, SqlError> {
        let parsed = input.trim().parse::<i128>().map_err(|_| {
            SqlError::new(
                "22P02",
                format!("invalid input syntax for type {type_name}: \"{input}\""),
            )
        })?;
        if parsed < min || parsed > max {
            return Err(SqlError::new(
                "22003",
                format!("value \"{input}\" is out of range for type {type_name}"),
            ));
        }
        Ok(parsed as i64)
    }

    fn coerce_bpchar(
        input: String,
        length: Option<usize>,
        assignment: bool,
    ) -> Result<Value, SqlError> {
        let Some(length) = length else {
            return Ok(Value::Text(input));
        };
        let mut chars = input.chars();
        let mut output: String = chars.by_ref().take(length).collect();
        if assignment && chars.any(|c| c != ' ') {
            return Err(SqlError::new(
                "22001",
                format!("value too long for type character({length})"),
            ));
        }
        let padding = length.saturating_sub(output.chars().count());
        output.extend(std::iter::repeat_n(' ', padding));
        Ok(Value::Text(output))
    }

    fn coerce_varchar(
        input: String,
        length: Option<usize>,
        assignment: bool,
    ) -> Result<Value, SqlError> {
        let Some(length) = length else {
            return Ok(Value::Text(input));
        };
        let mut chars = input.chars();
        let output: String = chars.by_ref().take(length).collect();
        if assignment && chars.any(|character| character != ' ') {
            return Err(SqlError::new(
                "22001",
                format!("value too long for type character varying({length})"),
            ));
        }
        Ok(Value::Text(output))
    }

    fn coerce_name(mut input: String) -> Value {
        const MAX_NAME_BYTES: usize = 63;

        if input.len() > MAX_NAME_BYTES {
            let mut end = MAX_NAME_BYTES;
            while !input.is_char_boundary(end) {
                end -= 1;
            }
            input.truncate(end);
        }
        Value::Text(input)
    }

    match (target, val) {
        (DataType::Int2, Value::Int64(v)) => {
            if v < i16::MIN as i64 || v > i16::MAX as i64 {
                return Err(SqlError::new("22003", "smallint out of range"));
            }
            Ok(Value::Int64(v))
        }
        (DataType::Int2, Value::Text(s)) => {
            parse_integer_input(&s, i16::MIN as i128, i16::MAX as i128, "smallint")
                .map(Value::Int64)
        }
        (DataType::Int4, Value::Int64(v)) => {
            if v < i32::MIN as i64 || v > i32::MAX as i64 {
                return Err(SqlError::new("22003", "value out of range for int4"));
            }
            Ok(Value::Int64(v))
        }
        (DataType::Int4, Value::Bool(value)) if !assignment => Ok(Value::Int64(i64::from(value))),
        (DataType::Int4, Value::Text(s)) => {
            parse_integer_input(&s, i32::MIN as i128, i32::MAX as i128, "integer").map(Value::Int64)
        }
        (DataType::Int8, Value::Int64(v)) => Ok(Value::Int64(v)),
        (DataType::Int8, Value::Text(s)) => {
            parse_integer_input(&s, i64::MIN as i128, i64::MAX as i128, "bigint").map(Value::Int64)
        }
        (DataType::Float8, Value::Float64Bits(bits)) => Ok(Value::Float64Bits(bits)),
        (DataType::Float8, Value::Int64(v)) => Ok(Value::from_f64(v as f64)),
        (DataType::Float8, Value::Text(s)) => {
            let parsed: f64 = s
                .parse()
                .map_err(|e| SqlError::new("22P02", format!("invalid input for float8: {e}")))?;
            Ok(Value::from_f64(parsed))
        }
        (DataType::Interval, Value::IntervalMicros(v)) => Ok(Value::IntervalMicros(v)),
        (DataType::Interval, Value::Text(s)) => parse_interval_literal(&s)
            .map(Value::IntervalMicros)
            .map_err(|e| {
                SqlError::new(
                    "22007",
                    format!("invalid input syntax for type interval: {e}"),
                )
            }),
        (DataType::Text, Value::Text(s)) => Ok(Value::Text(s)),
        (DataType::Text, Value::PgChar(value)) => Ok(Value::Text(format_pg_char(value))),
        (DataType::Varchar(length), Value::Text(s)) => coerce_varchar(s, *length, assignment),
        (DataType::Varchar(length), Value::Int64(value)) => {
            coerce_varchar(value.to_string(), *length, assignment)
        }
        (DataType::Text, Value::Bool(b)) => Ok(Value::Text(if b { "t" } else { "f" }.into())),
        (DataType::Text, Value::Int64(i)) => Ok(Value::Text(i.to_string())),
        (DataType::Text, Value::Float64Bits(bits)) => {
            let f = f64::from_bits(bits);
            Ok(Value::Text(f.to_string()))
        }
        (DataType::Text, Value::IntervalMicros(m)) => Ok(Value::Text(format_interval_micros(m))),
        (DataType::Text, Value::TimestampMicros(m)) => {
            let text = format_timestamp(m).map_err(|e| SqlError::new("22008", e))?;
            Ok(Value::Text(text))
        }
        (DataType::Text, Value::TimestamptzMicros(m)) => {
            let text = format_timestamptz(m, tz).map_err(|e| SqlError::new("22008", e))?;
            Ok(Value::Text(text))
        }
        (DataType::Name, Value::Text(s)) => Ok(coerce_name(s)),
        (DataType::BpChar(length), Value::Text(s)) => coerce_bpchar(s, *length, assignment),
        (DataType::BpChar(length), Value::Int64(value)) => {
            coerce_bpchar(value.to_string(), *length, assignment)
        }
        (DataType::PgChar, Value::PgChar(value)) => Ok(Value::PgChar(value)),
        (DataType::PgChar, Value::Text(value)) => Ok(parse_pg_char(&value)),
        (DataType::Point, Value::Point(point)) => Ok(Value::Point(point)),
        (DataType::Point, Value::Text(value)) => parse_point_text(&value).map(Value::Point),
        (DataType::Text, Value::Point(point)) => Ok(Value::Text(format_point_text(point))),
        (DataType::Lseg, Value::Lseg(lseg)) => Ok(Value::Lseg(lseg)),
        (DataType::Lseg, Value::Text(value)) => parse_lseg_text(&value).map(Value::Lseg),
        (DataType::Text, Value::Lseg(lseg)) => Ok(Value::Text(format_lseg_text(lseg))),
        (DataType::Line, Value::Line(line)) => Ok(Value::Line(line)),
        (DataType::Line, Value::Text(value)) => parse_line_text(&value).map(Value::Line),
        (DataType::Text, Value::Line(line)) => Ok(Value::Text(format_line_text(line))),
        (DataType::Circle, Value::Circle(circle)) => Ok(Value::Circle(circle)),
        (DataType::Circle, Value::Text(value)) => parse_circle_text(&value).map(Value::Circle),
        (DataType::Text, Value::Circle(circle)) => Ok(Value::Text(format_circle_text(circle))),
        (DataType::Box, Value::Box(value)) => Ok(Value::Box(value)),
        (DataType::Box, Value::Text(value)) => parse_box_text(&value).map(Value::Box),
        (DataType::Text, Value::Box(value)) => Ok(Value::Text(format_box_text(value))),
        (DataType::Tid, Value::Tid(tid)) => Ok(Value::Tid(tid)),
        (DataType::Tid, Value::Text(value)) => parse_tid_text(&value).map(Value::Tid),
        (DataType::Text, Value::Tid(tid)) => Ok(Value::Text(format_tid_text(tid))),
        (DataType::Oid, Value::Oid(value)) => Ok(Value::Oid(value)),
        (DataType::Oid, Value::Int64(value)) if value >= i32::MIN as i64 => {
            Ok(Value::Oid(value as u32))
        }
        (DataType::Oid, Value::Text(value)) => parse_oid_text(&value).map(Value::Oid),
        (DataType::Text, Value::Oid(value)) => Ok(Value::Text(value.to_string())),
        (DataType::PgLsn, Value::PgLsn(value)) => Ok(Value::PgLsn(value)),
        (DataType::PgLsn, Value::Text(value)) => parse_pg_lsn_text(&value).map(Value::PgLsn),
        (DataType::Text, Value::PgLsn(value)) => Ok(Value::Text(format_pg_lsn(value))),
        (DataType::MacAddr, Value::MacAddr(value)) => Ok(Value::MacAddr(value)),
        (DataType::MacAddr, Value::Text(value)) => parse_macaddr_text(&value).map(Value::MacAddr),
        (DataType::MacAddr, Value::MacAddr8(value)) if value[3..5] == [0xff, 0xfe] => {
            Ok(Value::MacAddr([
                value[0], value[1], value[2], value[5], value[6], value[7],
            ]))
        }
        (DataType::Text, Value::MacAddr(value)) => Ok(Value::Text(format_macaddr(&value))),
        (DataType::MacAddr8, Value::MacAddr8(value)) => Ok(Value::MacAddr8(value)),
        (DataType::MacAddr8, Value::MacAddr(value)) => Ok(Value::MacAddr8([
            value[0], value[1], value[2], 0xff, 0xfe, value[3], value[4], value[5],
        ])),
        (DataType::MacAddr8, Value::Text(value)) => {
            parse_macaddr8_text(&value).map(Value::MacAddr8)
        }
        (DataType::Text, Value::MacAddr8(value)) => Ok(Value::Text(format_macaddr(&value))),
        (DataType::Time(precision), Value::TimeMicros(value)) => {
            if let Some(precision) = precision {
                let quantum = 10_u64.pow((6 - (*precision).min(6)) as u32);
                Ok(Value::TimeMicros(
                    ((value + quantum / 2) / quantum) * quantum,
                ))
            } else {
                Ok(Value::TimeMicros(value))
            }
        }
        (DataType::Time(precision), Value::Text(value)) => {
            parse_time_text(&value, *precision).map(Value::TimeMicros)
        }
        (DataType::Text, Value::TimeMicros(value)) => Ok(Value::Text(format_time(value))),
        (DataType::Path, Value::Path(path)) => Ok(Value::Path(path)),
        (DataType::Path, Value::Text(value)) => parse_path_text(&value).map(Value::Path),
        (DataType::Text, Value::Path(path)) => Ok(Value::Text(format_path_text(&path))),
        (DataType::Json, Value::Text(s)) => {
            validate_json(&s, "json")?;
            Ok(Value::Text(s))
        }
        (DataType::Json, Value::Bytes(b)) => {
            let s = String::from_utf8(b).map_err(|e| {
                SqlError::new("22P02", format!("invalid input syntax for type json: {e}"))
            })?;
            validate_json(&s, "json")?;
            Ok(Value::Text(s))
        }
        (DataType::Json, Value::Bool(b)) => {
            let text = if b { "true" } else { "false" }.to_string();
            Ok(Value::Text(text))
        }
        (DataType::Json, Value::Int64(i)) => {
            let text = i.to_string();
            validate_json(&text, "json")?;
            Ok(Value::Text(text))
        }
        (DataType::Json, Value::Float64Bits(bits)) => {
            let f = f64::from_bits(bits);
            let text = f.to_string();
            validate_json(&text, "json")?;
            Ok(Value::Text(text))
        }
        (DataType::Jsonb, Value::Text(s)) => {
            validate_json(&s, "jsonb")?;
            Ok(Value::Text(s))
        }
        (DataType::Jsonb, Value::Bytes(b)) => {
            let s = String::from_utf8(b).map_err(|e| {
                SqlError::new("22P02", format!("invalid input syntax for type jsonb: {e}"))
            })?;
            validate_json(&s, "jsonb")?;
            Ok(Value::Text(s))
        }
        (DataType::Jsonb, Value::Bool(b)) => {
            let text = if b { "true" } else { "false" }.to_string();
            Ok(Value::Text(text))
        }
        (DataType::Jsonb, Value::Int64(i)) => {
            let text = i.to_string();
            validate_json(&text, "jsonb")?;
            Ok(Value::Text(text))
        }
        (DataType::Jsonb, Value::Float64Bits(bits)) => {
            let f = f64::from_bits(bits);
            let text = f.to_string();
            validate_json(&text, "jsonb")?;
            Ok(Value::Text(text))
        }
        (DataType::Bool, Value::Bool(b)) => Ok(Value::Bool(b)),
        (DataType::Bool, Value::Text(s)) => {
            let lowered = s.to_ascii_lowercase();
            match lowered.as_str() {
                "t" | "true" => Ok(Value::Bool(true)),
                "f" | "false" => Ok(Value::Bool(false)),
                other => Err(SqlError::new(
                    "22P02",
                    format!("invalid input for bool: {other}"),
                )),
            }
        }
        (DataType::Date, Value::Date(d)) => Ok(Value::Date(d)),
        (DataType::Date, Value::Text(s)) => {
            let days = parse_date_str(&s).map_err(|e| SqlError::new("22007", e))?;
            Ok(Value::Date(days))
        }
        (DataType::Date, Value::TimestampMicros(m)) => {
            let days = timestamp_micros_to_date_days(m).map_err(|e| SqlError::new("22008", e))?;
            Ok(Value::Date(days))
        }
        (DataType::Date, Value::TimestamptzMicros(m)) => {
            let days = timestamptz_to_date_days(m, tz).map_err(|e| SqlError::new("22008", e))?;
            Ok(Value::Date(days))
        }
        (DataType::Timestamp, Value::TimestampMicros(m)) => Ok(Value::TimestampMicros(m)),
        (DataType::Timestamp, Value::Text(s)) => {
            let micros = parse_timestamp_str(&s).map_err(|e| SqlError::new("22007", e))?;
            Ok(Value::TimestampMicros(micros))
        }
        (DataType::Timestamp, Value::TimestamptzMicros(m)) => {
            let local = timestamptz_to_timestamp(m, tz).map_err(|e| SqlError::new("22008", e))?;
            Ok(Value::TimestampMicros(local))
        }
        (DataType::Bytea, Value::Bytes(bytes)) => Ok(Value::Bytes(bytes)),
        (DataType::Bytea, Value::Text(s)) => {
            let bytes = parse_bytea_text(&s).map_err(|e| SqlError::new("22001", e))?;
            Ok(Value::Bytes(bytes))
        }
        (DataType::Timestamptz, Value::TimestamptzMicros(m)) => Ok(Value::TimestamptzMicros(m)),
        (DataType::Timestamptz, Value::TimestampMicros(m)) => {
            let utc = timestamp_to_timestamptz(m, tz).map_err(|e| SqlError::new("22008", e))?;
            Ok(Value::TimestamptzMicros(utc))
        }
        (DataType::Timestamptz, Value::Date(days)) => {
            let utc = date_to_timestamptz(days, tz).map_err(|e| SqlError::new("22008", e))?;
            Ok(Value::TimestamptzMicros(utc))
        }
        (DataType::Timestamptz, Value::Text(s)) => {
            let micros = parse_timestamptz_str(&s, tz).map_err(|e| SqlError::new("22007", e))?;
            Ok(Value::TimestamptzMicros(micros))
        }
        (_, Value::Null) => Ok(Value::Null),
        (dt, got) => Err(SqlError::new(
            "42804",
            format!("type mismatch: expected {dt:?}, got {got:?}"),
        )),
    }
}
