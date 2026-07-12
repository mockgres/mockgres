use super::*;

#[derive(Debug)]
struct PointOutput(PointValue);

#[derive(Debug)]
struct LsegOutput(LsegValue);

#[derive(Debug)]
struct LineOutput(LineValue);

#[derive(Debug)]
struct CircleOutput(CircleValue);

#[derive(Debug)]
struct BoxOutput(BoxValue);

#[derive(Debug)]
struct TidOutput(TidValue);

#[derive(Debug)]
struct OidOutput(u32);

#[derive(Debug)]
struct PgLsnOutput(u64);

#[derive(Debug)]
struct MacAddrOutput([u8; 6]);

#[derive(Debug)]
struct MacAddr8Output([u8; 8]);

#[derive(Debug)]
struct TimeOutput(u64);

impl ToSql for TimeOutput {
    fn to_sql(
        &self,
        _ty: &Type,
        out: &mut BytesMut,
    ) -> Result<IsNull, Box<dyn Error + Sync + Send>> {
        out.put_i64(self.0 as i64);
        Ok(IsNull::No)
    }

    fn accepts(ty: &Type) -> bool {
        *ty == Type::TIME
    }

    postgres_types::to_sql_checked!();
}

impl ToSqlText for TimeOutput {
    fn to_sql_text(
        &self,
        _ty: &Type,
        out: &mut BytesMut,
        _format_options: &FormatOptions,
    ) -> Result<IsNull, Box<dyn Error + Sync + Send>> {
        out.put_slice(crate::engine::format_time(self.0).as_bytes());
        Ok(IsNull::No)
    }
}

macro_rules! impl_mac_output {
    ($type:ty, $pg_type:expr) => {
        impl ToSql for $type {
            fn to_sql(
                &self,
                _ty: &Type,
                out: &mut BytesMut,
            ) -> Result<IsNull, Box<dyn Error + Sync + Send>> {
                out.put_slice(&self.0);
                Ok(IsNull::No)
            }

            fn accepts(ty: &Type) -> bool {
                *ty == $pg_type
            }

            postgres_types::to_sql_checked!();
        }

        impl ToSqlText for $type {
            fn to_sql_text(
                &self,
                _ty: &Type,
                out: &mut BytesMut,
                _format_options: &FormatOptions,
            ) -> Result<IsNull, Box<dyn Error + Sync + Send>> {
                out.put_slice(crate::engine::format_macaddr(&self.0).as_bytes());
                Ok(IsNull::No)
            }
        }
    };
}

impl_mac_output!(MacAddrOutput, Type::MACADDR);
impl_mac_output!(MacAddr8Output, Type::MACADDR8);

impl ToSql for OidOutput {
    fn to_sql(
        &self,
        _ty: &Type,
        out: &mut BytesMut,
    ) -> Result<IsNull, Box<dyn Error + Sync + Send>> {
        out.put_u32(self.0);
        Ok(IsNull::No)
    }

    fn accepts(ty: &Type) -> bool {
        *ty == Type::OID
    }

    postgres_types::to_sql_checked!();
}

impl ToSqlText for OidOutput {
    fn to_sql_text(
        &self,
        _ty: &Type,
        out: &mut BytesMut,
        _format_options: &FormatOptions,
    ) -> Result<IsNull, Box<dyn Error + Sync + Send>> {
        out.put_slice(self.0.to_string().as_bytes());
        Ok(IsNull::No)
    }
}

impl ToSql for PgLsnOutput {
    fn to_sql(
        &self,
        _ty: &Type,
        out: &mut BytesMut,
    ) -> Result<IsNull, Box<dyn Error + Sync + Send>> {
        out.put_u64(self.0);
        Ok(IsNull::No)
    }

    fn accepts(ty: &Type) -> bool {
        *ty == Type::PG_LSN
    }

    postgres_types::to_sql_checked!();
}

impl ToSqlText for PgLsnOutput {
    fn to_sql_text(
        &self,
        _ty: &Type,
        out: &mut BytesMut,
        _format_options: &FormatOptions,
    ) -> Result<IsNull, Box<dyn Error + Sync + Send>> {
        out.put_slice(format_pg_lsn(self.0).as_bytes());
        Ok(IsNull::No)
    }
}

#[derive(Debug)]
struct PgCharOutput(u8);

#[derive(Debug)]
struct FloatOutput {
    value: f64,
    extra_float_digits: i32,
}

#[derive(Debug)]
struct FloatTextOutput<'a>(&'a str);

#[derive(Debug)]
struct Int8TextOutput<'a>(&'a str);

impl ToSql for Int8TextOutput<'_> {
    fn to_sql(
        &self,
        _ty: &Type,
        out: &mut BytesMut,
    ) -> Result<IsNull, Box<dyn Error + Sync + Send>> {
        out.put_i64(self.0.parse::<i64>()?);
        Ok(IsNull::No)
    }

    fn accepts(ty: &Type) -> bool {
        *ty == Type::INT8
    }

    postgres_types::to_sql_checked!();
}

impl ToSqlText for Int8TextOutput<'_> {
    fn to_sql_text(
        &self,
        _ty: &Type,
        out: &mut BytesMut,
        _format_options: &FormatOptions,
    ) -> Result<IsNull, Box<dyn Error + Sync + Send>> {
        out.put_slice(self.0.as_bytes());
        Ok(IsNull::No)
    }
}

impl ToSql for FloatTextOutput<'_> {
    fn to_sql(
        &self,
        _ty: &Type,
        out: &mut BytesMut,
    ) -> Result<IsNull, Box<dyn Error + Sync + Send>> {
        out.put_f64(self.0.parse::<f64>()?);
        Ok(IsNull::No)
    }

    fn accepts(ty: &Type) -> bool {
        *ty == Type::FLOAT8
    }

    postgres_types::to_sql_checked!();
}

impl ToSqlText for FloatTextOutput<'_> {
    fn to_sql_text(
        &self,
        _ty: &Type,
        out: &mut BytesMut,
        _format_options: &FormatOptions,
    ) -> Result<IsNull, Box<dyn Error + Sync + Send>> {
        out.put_slice(self.0.as_bytes());
        Ok(IsNull::No)
    }
}

impl ToSql for FloatOutput {
    fn to_sql(
        &self,
        _ty: &Type,
        out: &mut BytesMut,
    ) -> Result<IsNull, Box<dyn Error + Sync + Send>> {
        out.put_f64(self.value);
        Ok(IsNull::No)
    }

    fn accepts(ty: &Type) -> bool {
        *ty == Type::FLOAT8
    }

    postgres_types::to_sql_checked!();
}

impl ToSqlText for FloatOutput {
    fn to_sql_text(
        &self,
        _ty: &Type,
        out: &mut BytesMut,
        _format_options: &FormatOptions,
    ) -> Result<IsNull, Box<dyn Error + Sync + Send>> {
        let mut value =
            if self.extra_float_digits <= 0 && self.value.is_finite() && self.value != 0.0 {
                let exponent = self.value.abs().log10().floor() as i32;
                let precision = if self.extra_float_digits < 0 { 13 } else { 14 };
                if !(-4..15).contains(&exponent) {
                    let formatted = format!("{:.*e}", precision, self.value);
                    let (mantissa, exponent) =
                        formatted.split_once('e').unwrap_or((&formatted, "0"));
                    let mantissa = mantissa.trim_end_matches('0').trim_end_matches('.');
                    let exponent = exponent.parse::<i32>().unwrap_or(0);
                    format!("{mantissa}e{exponent:+}")
                } else {
                    let decimals = (precision as i32 - exponent).max(0) as usize;
                    let formatted = format!("{:.*}", decimals, self.value);
                    formatted
                        .trim_end_matches('0')
                        .trim_end_matches('.')
                        .to_string()
                }
            } else {
                self.value.to_string()
            };
        value = match value.as_str() {
            "inf" => "Infinity".to_string(),
            "-inf" => "-Infinity".to_string(),
            _ => value,
        };
        if value.ends_with(".0") {
            value.truncate(value.len() - 2);
        }
        out.put_slice(value.as_bytes());
        Ok(IsNull::No)
    }
}

impl ToSql for PointOutput {
    fn to_sql(
        &self,
        _ty: &Type,
        out: &mut BytesMut,
    ) -> Result<IsNull, Box<dyn Error + Sync + Send>> {
        out.put_f64(self.0.x());
        out.put_f64(self.0.y());
        Ok(IsNull::No)
    }

    fn accepts(ty: &Type) -> bool {
        *ty == Type::POINT
    }

    postgres_types::to_sql_checked!();
}

impl ToSqlText for PointOutput {
    fn to_sql_text(
        &self,
        _ty: &Type,
        out: &mut BytesMut,
        _format_options: &FormatOptions,
    ) -> Result<IsNull, Box<dyn Error + Sync + Send>> {
        out.put_slice(format_point_text(self.0).as_bytes());
        Ok(IsNull::No)
    }
}

impl ToSql for LsegOutput {
    fn to_sql(
        &self,
        _ty: &Type,
        out: &mut BytesMut,
    ) -> Result<IsNull, Box<dyn Error + Sync + Send>> {
        out.put_f64(self.0.start().x());
        out.put_f64(self.0.start().y());
        out.put_f64(self.0.end().x());
        out.put_f64(self.0.end().y());
        Ok(IsNull::No)
    }

    fn accepts(ty: &Type) -> bool {
        *ty == Type::LSEG
    }

    postgres_types::to_sql_checked!();
}

impl ToSqlText for LsegOutput {
    fn to_sql_text(
        &self,
        _ty: &Type,
        out: &mut BytesMut,
        _format_options: &FormatOptions,
    ) -> Result<IsNull, Box<dyn Error + Sync + Send>> {
        out.put_slice(format_lseg_text(self.0).as_bytes());
        Ok(IsNull::No)
    }
}

impl ToSql for LineOutput {
    fn to_sql(
        &self,
        _ty: &Type,
        out: &mut BytesMut,
    ) -> Result<IsNull, Box<dyn Error + Sync + Send>> {
        out.put_f64(self.0.a());
        out.put_f64(self.0.b());
        out.put_f64(self.0.c());
        Ok(IsNull::No)
    }

    fn accepts(ty: &Type) -> bool {
        *ty == Type::LINE
    }

    postgres_types::to_sql_checked!();
}

impl ToSqlText for LineOutput {
    fn to_sql_text(
        &self,
        _ty: &Type,
        out: &mut BytesMut,
        _format_options: &FormatOptions,
    ) -> Result<IsNull, Box<dyn Error + Sync + Send>> {
        out.put_slice(format_line_text(self.0).as_bytes());
        Ok(IsNull::No)
    }
}

impl ToSql for CircleOutput {
    fn to_sql(
        &self,
        _ty: &Type,
        out: &mut BytesMut,
    ) -> Result<IsNull, Box<dyn Error + Sync + Send>> {
        out.put_f64(self.0.center().x());
        out.put_f64(self.0.center().y());
        out.put_f64(self.0.radius());
        Ok(IsNull::No)
    }

    fn accepts(ty: &Type) -> bool {
        *ty == Type::CIRCLE
    }

    postgres_types::to_sql_checked!();
}

impl ToSqlText for CircleOutput {
    fn to_sql_text(
        &self,
        _ty: &Type,
        out: &mut BytesMut,
        _format_options: &FormatOptions,
    ) -> Result<IsNull, Box<dyn Error + Sync + Send>> {
        out.put_slice(format_circle_text(self.0).as_bytes());
        Ok(IsNull::No)
    }
}

impl ToSql for BoxOutput {
    fn to_sql(
        &self,
        _ty: &Type,
        out: &mut BytesMut,
    ) -> Result<IsNull, Box<dyn Error + Sync + Send>> {
        out.put_f64(self.0.high().x());
        out.put_f64(self.0.high().y());
        out.put_f64(self.0.low().x());
        out.put_f64(self.0.low().y());
        Ok(IsNull::No)
    }

    fn accepts(ty: &Type) -> bool {
        *ty == Type::BOX
    }

    postgres_types::to_sql_checked!();
}

impl ToSqlText for BoxOutput {
    fn to_sql_text(
        &self,
        _ty: &Type,
        out: &mut BytesMut,
        _format_options: &FormatOptions,
    ) -> Result<IsNull, Box<dyn Error + Sync + Send>> {
        out.put_slice(format_box_text(self.0).as_bytes());
        Ok(IsNull::No)
    }
}

impl ToSql for TidOutput {
    fn to_sql(
        &self,
        _ty: &Type,
        out: &mut BytesMut,
    ) -> Result<IsNull, Box<dyn Error + Sync + Send>> {
        out.put_u32(self.0.block());
        out.put_u16(self.0.offset());
        Ok(IsNull::No)
    }

    fn accepts(ty: &Type) -> bool {
        *ty == Type::TID
    }

    postgres_types::to_sql_checked!();
}

impl ToSqlText for TidOutput {
    fn to_sql_text(
        &self,
        _ty: &Type,
        out: &mut BytesMut,
        _format_options: &FormatOptions,
    ) -> Result<IsNull, Box<dyn Error + Sync + Send>> {
        out.put_slice(format_tid_text(self.0).as_bytes());
        Ok(IsNull::No)
    }
}

impl ToSql for PgCharOutput {
    fn to_sql(
        &self,
        _ty: &Type,
        out: &mut BytesMut,
    ) -> Result<IsNull, Box<dyn Error + Sync + Send>> {
        out.put_u8(self.0);
        Ok(IsNull::No)
    }

    fn accepts(ty: &Type) -> bool {
        *ty == Type::CHAR
    }

    postgres_types::to_sql_checked!();
}

impl ToSqlText for PgCharOutput {
    fn to_sql_text(
        &self,
        _ty: &Type,
        out: &mut BytesMut,
        _format_options: &FormatOptions,
    ) -> Result<IsNull, Box<dyn Error + Sync + Send>> {
        out.put_slice(crate::engine::format_pg_char(self.0).as_bytes());
        Ok(IsNull::No)
    }
}

#[derive(Debug)]
struct PathOutput(PathValue);

impl ToSql for PathOutput {
    fn to_sql(
        &self,
        _ty: &Type,
        out: &mut BytesMut,
    ) -> Result<IsNull, Box<dyn Error + Sync + Send>> {
        out.put_u8(u8::from(self.0.is_closed()));
        out.put_i32(self.0.points().len() as i32);
        for point in self.0.points() {
            out.put_f64(point.x());
            out.put_f64(point.y());
        }
        Ok(IsNull::No)
    }

    fn accepts(ty: &Type) -> bool {
        *ty == Type::PATH
    }

    postgres_types::to_sql_checked!();
}

impl ToSqlText for PathOutput {
    fn to_sql_text(
        &self,
        _ty: &Type,
        out: &mut BytesMut,
        _format_options: &FormatOptions,
    ) -> Result<IsNull, Box<dyn Error + Sync + Send>> {
        out.put_slice(format_path_text(&self.0).as_bytes());
        Ok(IsNull::No)
    }
}

pub async fn to_pgwire_stream(
    mut node: Box<dyn ExecNode>,
    fmt: FieldFormat,
    ctx: EvalContext,
) -> PgWireResult<(
    Arc<Vec<FieldInfo>>,
    impl Stream<Item = PgWireResult<DataRow>> + Send + 'static,
)> {
    let ctx = Arc::new(ctx);
    node.open().await?;
    let schema = node.schema().clone();
    let fields = Arc::new(
        schema
            .fields
            .iter()
            .map(|f| FieldInfo::new(f.name.clone(), None, None, f.data_type.to_pg(), fmt))
            .collect::<Vec<_>>(),
    );
    let ctx_stream = ctx.clone();
    let s = stream::unfold(
        (node, fields.clone(), schema),
        move |(mut node, fields, schema)| {
            let ctx = ctx_stream.clone();
            async move {
                match node.next().await {
                    Ok(Some(vals)) => {
                        let mut enc = DataRowEncoder::new(fields.clone());
                        for (i, v) in vals.into_iter().enumerate() {
                            let dt = &schema.field(i).data_type;
                            let res = match (v, dt) {
                                (Value::Null, DataType::Interval) => {
                                    enc.encode_field(&Option::<String>::None)
                                }
                                (Value::Null, DataType::Void) => {
                                    enc.encode_field(&Option::<String>::None)
                                }
                                (Value::Null, DataType::Int2) => {
                                    enc.encode_field(&Option::<i16>::None)
                                }
                                (Value::Null, DataType::Int4) => {
                                    enc.encode_field(&Option::<i32>::None)
                                }
                                (Value::Null, DataType::Int8) => {
                                    enc.encode_field(&Option::<i64>::None)
                                }
                                (Value::Null, DataType::Float8) => {
                                    enc.encode_field(&Option::<f64>::None)
                                }
                                (Value::Null, DataType::Text) => {
                                    enc.encode_field(&Option::<String>::None)
                                }
                                (Value::Null, DataType::Varchar(_)) => {
                                    enc.encode_field(&Option::<String>::None)
                                }
                                (Value::Null, DataType::Name) => {
                                    enc.encode_field(&Option::<String>::None)
                                }
                                (Value::Null, DataType::BpChar(_)) => {
                                    enc.encode_field(&Option::<String>::None)
                                }
                                (Value::Null, DataType::PgChar) => {
                                    enc.encode_field(&Option::<PgCharOutput>::None)
                                }
                                (Value::Null, DataType::Point) => {
                                    enc.encode_field(&Option::<PointOutput>::None)
                                }
                                (Value::Null, DataType::Lseg) => {
                                    enc.encode_field(&Option::<LsegOutput>::None)
                                }
                                (Value::Null, DataType::Line) => {
                                    enc.encode_field(&Option::<LineOutput>::None)
                                }
                                (Value::Null, DataType::Circle) => {
                                    enc.encode_field(&Option::<CircleOutput>::None)
                                }
                                (Value::Null, DataType::Box) => {
                                    enc.encode_field(&Option::<BoxOutput>::None)
                                }
                                (Value::Null, DataType::Tid) => {
                                    enc.encode_field(&Option::<TidOutput>::None)
                                }
                                (Value::Null, DataType::Oid) => {
                                    enc.encode_field(&Option::<OidOutput>::None)
                                }
                                (Value::Null, DataType::PgLsn) => {
                                    enc.encode_field(&Option::<PgLsnOutput>::None)
                                }
                                (Value::Null, DataType::MacAddr) => {
                                    enc.encode_field(&Option::<MacAddrOutput>::None)
                                }
                                (Value::Null, DataType::MacAddr8) => {
                                    enc.encode_field(&Option::<MacAddr8Output>::None)
                                }
                                (Value::Null, DataType::Path) => {
                                    enc.encode_field(&Option::<PathOutput>::None)
                                }
                                (Value::Null, DataType::Json) => {
                                    enc.encode_field(&Option::<String>::None)
                                }
                                (Value::Null, DataType::Jsonb) => {
                                    enc.encode_field(&Option::<String>::None)
                                }
                                (Value::Null, DataType::Bool) => {
                                    enc.encode_field(&Option::<bool>::None)
                                }
                                (Value::Null, DataType::Date) => {
                                    enc.encode_field(&Option::<String>::None)
                                }
                                (Value::Null, DataType::Time(_)) => {
                                    enc.encode_field(&Option::<TimeOutput>::None)
                                }
                                (Value::Null, DataType::Timestamp) => {
                                    enc.encode_field(&Option::<String>::None)
                                }
                                (Value::Null, DataType::Timestamptz) => {
                                    enc.encode_field(&Option::<String>::None)
                                }
                                (Value::Null, DataType::Bytea) => {
                                    enc.encode_field(&Option::<Vec<u8>>::None)
                                }
                                (Value::Int64(i), DataType::Int2) => enc.encode_field(&(i as i16)),
                                (Value::Int64(i), DataType::Int4) => enc.encode_field(&(i as i32)),
                                (Value::Int64(i), DataType::Int8) => enc.encode_field(&i),
                                (Value::Int64(i), DataType::Float8) => {
                                    enc.encode_field(&FloatOutput {
                                        value: i as f64,
                                        extra_float_digits: ctx.extra_float_digits,
                                    })
                                }
                                (Value::Float64Bits(b), DataType::Float8) => {
                                    enc.encode_field(&FloatOutput {
                                        value: f64::from_bits(b),
                                        extra_float_digits: ctx.extra_float_digits,
                                    })
                                }
                                (Value::Text(s), DataType::Float8) => {
                                    enc.encode_field(&FloatTextOutput(&s))
                                }
                                (Value::Text(s), DataType::Int8) => {
                                    enc.encode_field(&Int8TextOutput(&s))
                                }
                                (Value::Text(s), DataType::Text) => enc.encode_field(&s),
                                (Value::Text(s), DataType::Varchar(_)) => enc.encode_field(&s),
                                (Value::Text(s), DataType::Name) => enc.encode_field(&s),
                                (Value::Text(s), DataType::BpChar(_)) => enc.encode_field(&s),
                                (Value::PgChar(value), DataType::PgChar) => {
                                    enc.encode_field(&PgCharOutput(value))
                                }
                                (Value::Point(point), DataType::Point) => {
                                    enc.encode_field(&PointOutput(point))
                                }
                                (Value::Lseg(lseg), DataType::Lseg) => {
                                    enc.encode_field(&LsegOutput(lseg))
                                }
                                (Value::Line(line), DataType::Line) => {
                                    enc.encode_field(&LineOutput(line))
                                }
                                (Value::Circle(circle), DataType::Circle) => {
                                    enc.encode_field(&CircleOutput(circle))
                                }
                                (Value::Box(value), DataType::Box) => {
                                    enc.encode_field(&BoxOutput(value))
                                }
                                (Value::Tid(tid), DataType::Tid) => {
                                    enc.encode_field(&TidOutput(tid))
                                }
                                (Value::Oid(value), DataType::Oid) => {
                                    enc.encode_field(&OidOutput(value))
                                }
                                (Value::PgLsn(value), DataType::PgLsn) => {
                                    enc.encode_field(&PgLsnOutput(value))
                                }
                                (Value::MacAddr(value), DataType::MacAddr) => {
                                    enc.encode_field(&MacAddrOutput(value))
                                }
                                (Value::MacAddr8(value), DataType::MacAddr8) => {
                                    enc.encode_field(&MacAddr8Output(value))
                                }
                                (Value::Path(path), DataType::Path) => {
                                    enc.encode_field(&PathOutput(path))
                                }
                                (Value::Text(s), DataType::Json) => {
                                    let parsed: JsonValue = match serde_json::from_str(&s) {
                                        Ok(v) => v,
                                        Err(e) => {
                                            return Some((
                                                Err(fe(format!("invalid json output value: {e}"))),
                                                (node, fields, schema),
                                            ));
                                        }
                                    };
                                    enc.encode_field(&Json(parsed))
                                }
                                (Value::Text(s), DataType::Jsonb) => {
                                    let parsed: JsonValue = match serde_json::from_str(&s) {
                                        Ok(v) => v,
                                        Err(e) => {
                                            return Some((
                                                Err(fe(format!("invalid jsonb output value: {e}"))),
                                                (node, fields, schema),
                                            ));
                                        }
                                    };
                                    enc.encode_field(&Json(parsed))
                                }
                                (Value::Bool(b), DataType::Bool) => enc.encode_field(&b),
                                (Value::Date(days), DataType::Date) => {
                                    if fmt == FieldFormat::Binary {
                                        let pg_days = date_days_to_postgres(days);
                                        enc.encode_field(&pg_days)
                                    } else {
                                        let text = match format_date(days) {
                                            Ok(t) => t,
                                            Err(e) => {
                                                return Some((Err(fe(e)), (node, fields, schema)));
                                            }
                                        };
                                        enc.encode_field(&text)
                                    }
                                }
                                (Value::TimeMicros(value), DataType::Time(_)) => {
                                    enc.encode_field(&TimeOutput(value))
                                }
                                (Value::TimestampMicros(micros), DataType::Timestamp) => {
                                    if fmt == FieldFormat::Binary {
                                        let pg_micros = timestamp_to_postgres_micros(micros);
                                        enc.encode_field(&pg_micros)
                                    } else {
                                        let text = match format_timestamp(micros) {
                                            Ok(t) => t,
                                            Err(e) => {
                                                return Some((Err(fe(e)), (node, fields, schema)));
                                            }
                                        };
                                        enc.encode_field(&text)
                                    }
                                }
                                (Value::TimestamptzMicros(micros), DataType::Timestamptz) => {
                                    if fmt == FieldFormat::Binary {
                                        let pg_micros = timestamp_to_postgres_micros(micros);
                                        enc.encode_field(&pg_micros)
                                    } else {
                                        let text = match format_timestamptz(micros, &ctx.time_zone)
                                        {
                                            Ok(t) => t,
                                            Err(e) => {
                                                return Some((Err(fe(e)), (node, fields, schema)));
                                            }
                                        };
                                        enc.encode_field(&text)
                                    }
                                }
                                (Value::IntervalMicros(micros), DataType::Interval) => {
                                    let text = format_interval_micros(micros);
                                    enc.encode_field(&text)
                                }
                                (Value::Bytes(bytes), DataType::Bytea) => {
                                    if fmt == FieldFormat::Binary {
                                        enc.encode_field_with_type_and_format(
                                            &bytes,
                                            &Type::BYTEA,
                                            FieldFormat::Binary,
                                            &FormatOptions::default(),
                                        )
                                    } else {
                                        let text = format_bytea(bytes.as_slice());
                                        enc.encode_field(&text)
                                    }
                                }
                                _ => Err(PgWireError::ApiError("type mismatch".into())),
                            };
                            if let Err(e) = res {
                                return Some((Err(e), (node, fields, schema)));
                            }
                        }
                        let dr = enc.take_row();
                        Some((Ok(dr), (node, fields, schema)))
                    }
                    Ok(None) => match node.close().await {
                        Ok(()) => None,
                        Err(e) => Some((Err(e), (node, fields, schema))),
                    },
                    Err(e) => Some((Err(e), (node, fields, schema))),
                }
            }
        },
    )
    .boxed();

    Ok((fields, s))
}
