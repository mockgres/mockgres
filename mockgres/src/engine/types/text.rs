use super::*;

pub fn parse_pg_lsn_text(input: &str) -> Result<u64, SqlError> {
    fn invalid(input: &str) -> SqlError {
        SqlError::new(
            "22P02",
            format!("invalid input syntax for type pg_lsn: \"{input}\""),
        )
    }
    if input.trim() != input {
        return Err(invalid(input));
    }
    let (high, low) = input.split_once('/').ok_or_else(|| invalid(input))?;
    if high.is_empty() || low.is_empty() || high.contains('/') || low.contains('/') {
        return Err(invalid(input));
    }
    let high = u32::from_str_radix(high, 16).map_err(|_| invalid(input))?;
    let low = u32::from_str_radix(low, 16).map_err(|_| invalid(input))?;
    Ok((u64::from(high) << 32) | u64::from(low))
}

pub fn parse_oid_text(input: &str) -> Result<u32, SqlError> {
    let trimmed = input.trim();
    let parsed = trimmed.parse::<i128>().map_err(|_| {
        SqlError::new(
            "22P02",
            format!("invalid input syntax for type oid: \"{input}\""),
        )
    })?;
    if parsed < i32::MIN as i128 || parsed > u32::MAX as i128 {
        return Err(SqlError::new(
            "22003",
            format!("value \"{input}\" is out of range for type oid"),
        ));
    }
    Ok(parsed as u32)
}

pub fn format_pg_lsn(value: u64) -> String {
    format!("{:X}/{:X}", value >> 32, value & u64::from(u32::MAX))
}

pub fn parse_macaddr_text(input: &str) -> Result<[u8; 6], SqlError> {
    let trimmed = input.trim();
    let compact = if trimmed.len() == 12 && trimmed.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        trimmed.to_string()
    } else {
        let separator = if trimmed.contains(':') {
            ':'
        } else if trimmed.contains('-') {
            '-'
        } else if trimmed.contains('.') {
            '.'
        } else {
            '\0'
        };
        let groups = trimmed.split(separator).collect::<Vec<_>>();
        let valid = (matches!(separator, ':' | '-')
            && ((groups.len() == 6 && groups.iter().all(|group| group.len() == 2))
                || (groups.len() == 2 && groups.iter().all(|group| group.len() == 6))))
            || (matches!(separator, '.' | '-')
                && groups.len() == 3
                && groups.iter().all(|group| group.len() == 4));
        if !valid {
            return Err(invalid_macaddr(input, "macaddr"));
        }
        groups.concat()
    };
    parse_mac_bytes::<6>(&compact).map_err(|_| invalid_macaddr(input, "macaddr"))
}

pub fn parse_macaddr8_text(input: &str) -> Result<[u8; 8], SqlError> {
    let trimmed = input.trim();
    if let Ok(mac) = parse_macaddr_text(trimmed) {
        return Ok([mac[0], mac[1], mac[2], 0xff, 0xfe, mac[3], mac[4], mac[5]]);
    }
    {
        let compact = trimmed.replace([':', '-', '.'], "");
        if compact.len() == 12
            && compact.bytes().all(|byte| byte.is_ascii_hexdigit())
            && ((trimmed.matches(':').count() == 2
                && trimmed.split(':').all(|group| group.len() == 4))
                || (trimmed.matches('-').count() == 2
                    && trimmed.split('-').all(|group| group.len() == 4)))
        {
            let mac =
                parse_mac_bytes::<6>(&compact).map_err(|_| invalid_macaddr(input, "macaddr8"))?;
            return Ok([mac[0], mac[1], mac[2], 0xff, 0xfe, mac[3], mac[4], mac[5]]);
        }
    }
    let compact = trimmed.replace([':', '-', '.'], "");
    let shape_valid = (trimmed.len() == 23
        && (trimmed.matches(':').count() == 7 || trimmed.matches('-').count() == 7))
        || (trimmed.len() == 19 && trimmed.matches('.').count() == 3)
        || (trimmed.len() == 17 && trimmed.matches(':').count() == 1)
        || (trimmed.len() == 17 && trimmed.matches('-').count() == 1)
        || (trimmed.len() == 16 && !trimmed.contains([':', '-', '.']));
    if !shape_valid || compact.len() != 16 {
        return Err(invalid_macaddr(input, "macaddr8"));
    }
    parse_mac_bytes::<8>(&compact).map_err(|_| invalid_macaddr(input, "macaddr8"))
}

fn parse_mac_bytes<const N: usize>(input: &str) -> Result<[u8; N], ()> {
    if input.len() != N * 2 {
        return Err(());
    }
    let mut bytes = [0; N];
    for (index, byte) in bytes.iter_mut().enumerate() {
        *byte = u8::from_str_radix(&input[index * 2..index * 2 + 2], 16).map_err(|_| ())?;
    }
    Ok(bytes)
}

fn invalid_macaddr(input: &str, type_name: &str) -> SqlError {
    SqlError::new(
        "22P02",
        format!("invalid input syntax for type {type_name}: \"{input}\""),
    )
}

pub fn format_macaddr<const N: usize>(value: &[u8; N]) -> String {
    value
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect::<Vec<_>>()
        .join(":")
}

pub fn parse_time_text(input: &str, precision: Option<usize>) -> Result<u64, SqlError> {
    let tokens = input.split_whitespace().collect::<Vec<_>>();
    if tokens.is_empty() {
        return Err(invalid_time(input, "22007"));
    }
    let has_date = tokens.first().is_some_and(|token| token.contains('-'));
    let time_index = usize::from(has_date);
    let Some(time) = tokens.get(time_index) else {
        return Err(invalid_time(input, "22007"));
    };
    if tokens
        .iter()
        .skip(time_index + 1)
        .any(|token| token.contains('/') && !has_date)
    {
        return Err(invalid_time(input, "22007"));
    }
    let meridiem = tokens
        .iter()
        .skip(time_index + 1)
        .find(|token| token.eq_ignore_ascii_case("am") || token.eq_ignore_ascii_case("pm"));
    let parts = time.split(':').collect::<Vec<_>>();
    if !(2..=3).contains(&parts.len()) {
        return Err(invalid_time(input, "22007"));
    }
    let mut hour = parts[0]
        .parse::<u64>()
        .map_err(|_| invalid_time(input, "22007"))?;
    let minute = parts[1]
        .parse::<u64>()
        .map_err(|_| invalid_time(input, "22007"))?;
    let seconds = parts
        .get(2)
        .copied()
        .unwrap_or("0")
        .parse::<f64>()
        .map_err(|_| invalid_time(input, "22007"))?;
    if let Some(meridiem) = meridiem {
        if !(1..=12).contains(&hour) {
            return Err(range_time(input));
        }
        if meridiem.eq_ignore_ascii_case("pm") {
            if hour != 12 {
                hour += 12;
            }
        } else if hour == 12 {
            hour = 0;
        }
    }
    if minute >= 60 || !(0.0..60.01).contains(&seconds) || hour > 24 {
        return Err(range_time(input));
    }
    let mut micros =
        ((hour * 3600 + minute * 60) as f64 * 1_000_000.0 + seconds * 1_000_000.0).round() as u64;
    if let Some(precision) = precision {
        let precision = precision.min(6);
        let quantum = 10_u64.pow((6 - precision) as u32);
        micros = ((micros + quantum / 2) / quantum) * quantum;
    }
    const DAY: u64 = 86_400_000_000;
    if micros > DAY || (hour == 24 && (minute != 0 || seconds != 0.0)) {
        return Err(range_time(input));
    }
    Ok(micros)
}

fn invalid_time(input: &str, code: &'static str) -> SqlError {
    SqlError::new(
        code,
        format!("invalid input syntax for type time: \"{input}\""),
    )
}

fn range_time(input: &str) -> SqlError {
    SqlError::new(
        "22008",
        format!("date/time field value out of range: \"{input}\""),
    )
}

pub fn format_time(value: u64) -> String {
    if value == 86_400_000_000 {
        return "24:00:00".to_string();
    }
    let hours = value / 3_600_000_000;
    let minutes = (value / 60_000_000) % 60;
    let seconds = (value / 1_000_000) % 60;
    let micros = value % 1_000_000;
    if micros == 0 {
        format!("{hours:02}:{minutes:02}:{seconds:02}")
    } else {
        let fraction = format!("{micros:06}").trim_end_matches('0').to_string();
        format!("{hours:02}:{minutes:02}:{seconds:02}.{fraction}")
    }
}

pub fn parse_point_text(input: &str) -> Result<PointValue, SqlError> {
    fn invalid(input: &str) -> SqlError {
        SqlError::new(
            "22P02",
            format!("invalid input syntax for type point: \"{input}\""),
        )
    }

    let trimmed = input.trim();
    let coordinates = if let Some(inner) = trimmed
        .strip_prefix('(')
        .and_then(|value| value.strip_suffix(')'))
    {
        inner
    } else if trimmed.starts_with('(') || trimmed.ends_with(')') {
        return Err(invalid(input));
    } else {
        trimmed
    };
    let mut parts = coordinates.split(',');
    let x = parts.next().ok_or_else(|| invalid(input))?;
    let y = parts.next().ok_or_else(|| invalid(input))?;
    if parts.next().is_some() || x.trim().is_empty() || y.trim().is_empty() {
        return Err(invalid(input));
    }
    Ok(PointValue::new(
        parse_geometry_coordinate(x, input, "point")?,
        parse_geometry_coordinate(y, input, "point")?,
    ))
}

pub fn format_point_text(point: PointValue) -> String {
    format!(
        "({},{})",
        format_geometry_coordinate(point.x()),
        format_geometry_coordinate(point.y())
    )
}

pub fn parse_lseg_text(input: &str) -> Result<LsegValue, SqlError> {
    let path = parse_path_text(input).map_err(|error| {
        SqlError::new(
            error.code,
            error.message.replacen("type path", "type lseg", 1),
        )
    })?;
    let [start, end] = path.points() else {
        return Err(SqlError::new(
            "22P02",
            format!("invalid input syntax for type lseg: \"{input}\""),
        ));
    };
    Ok(LsegValue::new(*start, *end))
}

pub fn format_lseg_text(lseg: LsegValue) -> String {
    format!(
        "[{},{}]",
        format_point_text(lseg.start()),
        format_point_text(lseg.end())
    )
}

pub fn line_from_points(start: PointValue, end: PointValue) -> Result<LineValue, SqlError> {
    if start == end {
        return Err(SqlError::new(
            "22P02",
            "invalid line specification: must be two distinct points",
        ));
    }
    let dx = end.x() - start.x();
    let (mut a, b, mut c) = if dx != 0.0 {
        let a = (end.y() - start.y()) / dx;
        (a, -1.0, start.y() - a * start.x())
    } else {
        (-1.0, 0.0, start.x())
    };
    if a == -0.0 {
        a = 0.0;
    }
    if c == -0.0 {
        c = 0.0;
    }
    Ok(LineValue::new(a, b, c))
}

pub fn parse_line_text(input: &str) -> Result<LineValue, SqlError> {
    fn invalid(input: &str) -> SqlError {
        SqlError::new(
            "22P02",
            format!("invalid input syntax for type line: \"{input}\""),
        )
    }

    let trimmed = input.trim();
    if let Some(inner) = trimmed
        .strip_prefix('{')
        .and_then(|value| value.strip_suffix('}'))
    {
        let values = inner.split(',').collect::<Vec<_>>();
        if values.len() != 3 || values.iter().any(|value| value.trim().is_empty()) {
            return Err(invalid(input));
        }
        let a = parse_geometry_coordinate(values[0], input, "line")?;
        let b = parse_geometry_coordinate(values[1], input, "line")?;
        let c = parse_geometry_coordinate(values[2], input, "line")?;
        if a == 0.0 && b == 0.0 {
            return Err(SqlError::new(
                "22P02",
                "invalid line specification: A and B cannot both be zero",
            ));
        }
        return Ok(LineValue::new(a, b, c));
    }
    if trimmed.starts_with('{') || trimmed.ends_with('}') {
        return Err(invalid(input));
    }
    let lseg = parse_lseg_text(input).map_err(|error| {
        SqlError::new(
            error.code,
            error.message.replacen("type lseg", "type line", 1),
        )
    })?;
    line_from_points(lseg.start(), lseg.end())
}

pub fn format_line_text(line: LineValue) -> String {
    format!(
        "{{{},{},{}}}",
        format_geometry_coordinate(line.a()),
        format_geometry_coordinate(line.b()),
        format_geometry_coordinate(line.c())
    )
}

pub fn parse_circle_text(input: &str) -> Result<CircleValue, SqlError> {
    fn invalid(input: &str) -> SqlError {
        SqlError::new(
            "22P02",
            format!("invalid input syntax for type circle: \"{input}\""),
        )
    }

    let compact = input
        .chars()
        .filter(|character| !character.is_whitespace())
        .collect::<String>();
    let bracketless = if let Some(inner) = compact
        .strip_prefix('<')
        .and_then(|value| value.strip_suffix('>'))
    {
        inner
    } else if compact.starts_with("((") && compact.ends_with(')') {
        &compact[1..compact.len() - 1]
    } else {
        compact.as_str()
    };
    let (point, radius) = if let Some(inner) = bracketless.strip_prefix('(') {
        inner.split_once("),").ok_or_else(|| invalid(input))?
    } else if bracketless.contains(['(', ')', '<', '>']) {
        return Err(invalid(input));
    } else {
        let mut parts = bracketless.split(',');
        let x = parts.next().ok_or_else(|| invalid(input))?;
        let y = parts.next().ok_or_else(|| invalid(input))?;
        let radius = parts.next().ok_or_else(|| invalid(input))?;
        if parts.next().is_some() {
            return Err(invalid(input));
        }
        return parse_circle_components(&format!("{x},{y}"), radius, input);
    };
    parse_circle_components(point, radius, input)
}

fn parse_circle_components(
    point: &str,
    radius: &str,
    input: &str,
) -> Result<CircleValue, SqlError> {
    let invalid = || {
        SqlError::new(
            "22P02",
            format!("invalid input syntax for type circle: \"{input}\""),
        )
    };
    let center = parse_point_text(point).map_err(|_| invalid())?;
    let radius = parse_geometry_coordinate(radius, input, "circle")?;
    if radius < 0.0 {
        return Err(invalid());
    }
    Ok(CircleValue::new(center, radius))
}

pub fn format_circle_text(circle: CircleValue) -> String {
    format!(
        "<{},{}>",
        format_point_text(circle.center()),
        format_geometry_coordinate(circle.radius())
    )
}

pub fn parse_box_text(input: &str) -> Result<BoxValue, SqlError> {
    let lseg = parse_lseg_text(input).map_err(|error| {
        SqlError::new(
            error.code,
            error.message.replacen("type lseg", "type box", 1),
        )
    })?;
    Ok(BoxValue::new(lseg.start(), lseg.end()))
}

pub fn format_box_text(value: BoxValue) -> String {
    format!(
        "{},{}",
        format_point_text(value.high()),
        format_point_text(value.low())
    )
}

pub fn parse_tid_text(input: &str) -> Result<TidValue, SqlError> {
    let invalid = || {
        SqlError::new(
            "22P02",
            format!("invalid input syntax for type tid: \"{input}\""),
        )
    };
    let inner = input
        .trim()
        .strip_prefix('(')
        .and_then(|value| value.strip_suffix(')'))
        .ok_or_else(invalid)?;
    let mut parts = inner.split(',');
    let block = parts
        .next()
        .ok_or_else(invalid)?
        .trim()
        .parse::<i64>()
        .map_err(|_| invalid())?;
    let offset = parts
        .next()
        .ok_or_else(invalid)?
        .trim()
        .parse::<i64>()
        .map_err(|_| invalid())?;
    if parts.next().is_some()
        || !(-1..=u32::MAX as i64).contains(&block)
        || !(0..=u16::MAX as i64).contains(&offset)
    {
        return Err(invalid());
    }
    Ok(TidValue::new(block as u32, offset as u16))
}

pub fn format_tid_text(tid: TidValue) -> String {
    format!("({},{})", tid.block(), tid.offset())
}

pub fn validate_varchar_input(input: &str, type_spec: &str) -> Result<(), SqlError> {
    let length = type_spec
        .strip_prefix("varchar(")
        .and_then(|value| value.strip_suffix(')'))
        .and_then(|value| value.parse::<usize>().ok())
        .ok_or_else(|| SqlError::new("42704", format!("type \"{type_spec}\" does not exist")))?;
    coerce_value_to_type(
        Value::Text(input.to_string()),
        &DataType::Varchar(Some(length)),
        &SessionTimeZone::Utc,
    )
    .map(|_| ())
}

pub fn validate_char_input(input: &str, type_spec: &str) -> Result<(), SqlError> {
    let length = type_spec
        .strip_prefix("char(")
        .and_then(|value| value.strip_suffix(')'))
        .and_then(|value| value.parse::<usize>().ok())
        .ok_or_else(|| SqlError::new("42704", format!("type \"{type_spec}\" does not exist")))?;
    coerce_value_to_type(
        Value::Text(input.to_string()),
        &DataType::BpChar(Some(length)),
        &SessionTimeZone::Utc,
    )
    .map(|_| ())
}

pub fn format_pg_char(byte: u8) -> String {
    match byte {
        0 => String::new(),
        0x20..=0x7e => (byte as char).to_string(),
        _ => format!("\\{byte:03o}"),
    }
}

pub fn parse_path_text(input: &str) -> Result<PathValue, SqlError> {
    fn invalid(input: &str) -> SqlError {
        SqlError::new(
            "22P02",
            format!("invalid input syntax for type path: \"{input}\""),
        )
    }

    fn matching_close(input: &str) -> Option<usize> {
        let mut depth = 0_usize;
        for (index, ch) in input.char_indices() {
            match ch {
                '(' => depth += 1,
                ')' => {
                    depth = depth.checked_sub(1)?;
                    if depth == 0 {
                        return Some(index);
                    }
                }
                _ => {}
            }
        }
        None
    }

    fn parse_points(mut inner: &str, input: &str) -> Result<Vec<PointValue>, SqlError> {
        let mut points = Vec::new();
        loop {
            inner = inner.trim_start();
            let (x, y, rest, needs_separator, allows_end) =
                if let Some(rest) = inner.strip_prefix('(') {
                    let Some(close) = rest.find(')') else {
                        return Err(invalid(input));
                    };
                    let coordinates = &rest[..close];
                    if coordinates.contains(['(', ')', '[', ']']) {
                        return Err(invalid(input));
                    }
                    let mut pair = coordinates.split(',');
                    let x = pair.next().ok_or_else(|| invalid(input))?;
                    let y = pair.next().ok_or_else(|| invalid(input))?;
                    if pair.next().is_some() {
                        return Err(invalid(input));
                    }
                    (x, y, &rest[close + 1..], true, true)
                } else {
                    let Some((x, rest)) = inner.split_once(',') else {
                        return Err(invalid(input));
                    };
                    if let Some((y, rest)) = rest.split_once(',') {
                        (x, y, rest, false, false)
                    } else {
                        (x, rest, "", false, true)
                    }
                };
            if x.trim().is_empty() || y.trim().is_empty() {
                return Err(invalid(input));
            }
            points.push(PointValue::new(
                parse_geometry_coordinate(x, input, "path")?,
                parse_geometry_coordinate(y, input, "path")?,
            ));

            inner = rest.trim_start();
            if inner.is_empty() {
                return if allows_end {
                    Ok(points)
                } else {
                    Err(invalid(input))
                };
            }
            if needs_separator {
                let Some(rest) = inner.strip_prefix(',') else {
                    return Err(invalid(input));
                };
                inner = rest;
            }
            if inner.trim().is_empty() {
                return Err(invalid(input));
            }
        }
    }

    let trimmed = input.trim();
    if trimmed.is_empty() {
        return Err(invalid(input));
    }

    let (closed, inner) = if let Some(rest) = trimmed.strip_prefix('[') {
        let Some(inner) = rest.strip_suffix(']') else {
            return Err(invalid(input));
        };
        (false, inner)
    } else if trimmed.starts_with('(') {
        let Some(close) = matching_close(trimmed) else {
            return Err(invalid(input));
        };
        if close == trimmed.len() - 1 {
            (true, &trimmed[1..trimmed.len() - 1])
        } else {
            (true, trimmed)
        }
    } else {
        (true, trimmed)
    };

    let inner = inner.trim();
    if inner.is_empty() || inner.contains(['[', ']']) {
        return Err(invalid(input));
    }
    let points = parse_points(inner, input)?;
    Ok(PathValue::new(closed, points))
}

pub fn format_path_text(path: &PathValue) -> String {
    let points = path
        .points()
        .iter()
        .copied()
        .map(format_point_text)
        .collect::<Vec<_>>()
        .join(",");
    if path.is_closed() {
        format!("({points})")
    } else {
        format!("[{points}]")
    }
}

fn parse_geometry_coordinate(value: &str, input: &str, type_name: &str) -> Result<f64, SqlError> {
    let trimmed = value.trim();
    let normalized = match trimmed.to_ascii_lowercase().as_str() {
        "inf" | "+inf" | "infinity" | "+infinity" => "inf",
        "-inf" | "-infinity" => "-inf",
        "nan" | "+nan" | "-nan" => "NaN",
        _ => trimmed,
    };
    let coordinate = normalized.parse::<f64>().map_err(|_| {
        SqlError::new(
            "22P02",
            format!("invalid input syntax for type {type_name}: \"{input}\""),
        )
    })?;
    let explicitly_infinite = matches!(
        trimmed.to_ascii_lowercase().as_str(),
        "inf" | "+inf" | "infinity" | "+infinity" | "-inf" | "-infinity"
    );
    if coordinate.is_infinite() && !explicitly_infinite {
        return Err(SqlError::new(
            "22003",
            format!("\"{trimmed}\" is out of range for type double precision"),
        ));
    }
    Ok(coordinate)
}

fn format_geometry_coordinate(value: f64) -> String {
    if value.is_nan() {
        return "NaN".to_string();
    }
    if value == f64::INFINITY {
        return "Infinity".to_string();
    }
    if value == f64::NEG_INFINITY {
        return "-Infinity".to_string();
    }
    let mut buffer = ryu::Buffer::new();
    let mut formatted = buffer.format(value).to_string();
    if formatted.ends_with(".0") {
        formatted.truncate(formatted.len() - 2);
    }
    if let Some(exponent) = formatted.find('e')
        && !matches!(formatted.as_bytes().get(exponent + 1), Some(b'+' | b'-'))
    {
        formatted.insert(exponent + 1, '+');
    }
    formatted
}
