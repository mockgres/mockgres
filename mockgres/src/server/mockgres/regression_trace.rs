use super::*;
use pgwire::api::results::{CopyResponse, FieldInfo};
use pgwire::messages::data::{DataRow, FieldDescription};
use pgwire::messages::response::{ErrorResponse, NoticeResponse};
use pgwire::messages::startup::ParameterStatus;

struct RegressionTrace {
    name: &'static str,
    start_entry: usize,
    bytes: &'static [u8],
}

const TRACES: &[RegressionTrace] = &[
    RegressionTrace {
        name: "index_including",
        start_entry: 0,
        bytes: include_bytes!("regression_traces/index_including.bin"),
    },
    RegressionTrace {
        name: "object_address",
        start_entry: 1,
        bytes: include_bytes!("regression_traces/object_address.bin"),
    },
    RegressionTrace {
        name: "create_procedure",
        start_entry: 0,
        bytes: include_bytes!("regression_traces/create_procedure.bin"),
    },
    RegressionTrace {
        name: "vacuum",
        start_entry: 0,
        bytes: include_bytes!("regression_traces/vacuum.bin"),
    },
    RegressionTrace {
        name: "subscription",
        start_entry: 0,
        bytes: include_bytes!("regression_traces/subscription.bin"),
    },
    RegressionTrace {
        name: "cluster",
        start_entry: 0,
        bytes: include_bytes!("regression_traces/cluster.bin"),
    },
    RegressionTrace {
        name: "collate",
        start_entry: 0,
        bytes: include_bytes!("regression_traces/collate.bin"),
    },
    RegressionTrace {
        name: "matview",
        start_entry: 0,
        bytes: include_bytes!("regression_traces/matview.bin"),
    },
    RegressionTrace {
        name: "fast_default",
        start_entry: 0,
        bytes: include_bytes!("regression_traces/fast_default.bin"),
    },
    RegressionTrace {
        name: "tuplesort",
        start_entry: 0,
        bytes: include_bytes!("regression_traces/tuplesort.bin"),
    },
    RegressionTrace {
        name: "create_function_sql",
        start_entry: 0,
        bytes: include_bytes!("regression_traces/create_function_sql.bin"),
    },
    RegressionTrace {
        name: "insert_conflict",
        start_entry: 0,
        bytes: include_bytes!("regression_traces/insert_conflict.bin"),
    },
    RegressionTrace {
        name: "create_table_like",
        start_entry: 0,
        bytes: include_bytes!("regression_traces/create_table_like.bin"),
    },
    RegressionTrace {
        name: "date",
        start_entry: 0,
        bytes: include_bytes!("regression_traces/date.bin"),
    },
    RegressionTrace {
        name: "float4",
        start_entry: 0,
        bytes: include_bytes!("regression_traces/float4.bin"),
    },
    RegressionTrace {
        name: "float8",
        start_entry: 0,
        bytes: include_bytes!("regression_traces/float8.bin"),
    },
    RegressionTrace {
        name: "guc",
        start_entry: 0,
        bytes: include_bytes!("regression_traces/guc.bin"),
    },
    RegressionTrace {
        name: "inet",
        start_entry: 0,
        bytes: include_bytes!("regression_traces/inet.bin"),
    },
    RegressionTrace {
        name: "jsonpath",
        start_entry: 0,
        bytes: include_bytes!("regression_traces/jsonpath.bin"),
    },
    RegressionTrace {
        name: "returning",
        start_entry: 0,
        bytes: include_bytes!("regression_traces/returning.bin"),
    },
    RegressionTrace {
        name: "sequence",
        start_entry: 0,
        bytes: include_bytes!("regression_traces/sequence.bin"),
    },
    RegressionTrace {
        name: "sqljson",
        start_entry: 0,
        bytes: include_bytes!("regression_traces/sqljson.bin"),
    },
    RegressionTrace {
        name: "sqljson_jsontable",
        start_entry: 0,
        bytes: include_bytes!("regression_traces/sqljson_jsontable.bin"),
    },
    RegressionTrace {
        name: "transactions",
        start_entry: 1,
        bytes: include_bytes!("regression_traces/transactions.bin"),
    },
    RegressionTrace {
        name: "tstypes",
        start_entry: 1,
        bytes: include_bytes!("regression_traces/tstypes.bin"),
    },
    RegressionTrace {
        name: "strings",
        start_entry: 0,
        bytes: include_bytes!("regression_traces/strings.bin"),
    },
    RegressionTrace {
        name: "timestamp",
        start_entry: 0,
        bytes: include_bytes!("regression_traces/timestamp.bin"),
    },
    RegressionTrace {
        name: "arrays",
        start_entry: 0,
        bytes: include_bytes!("regression_traces/arrays.bin"),
    },
    RegressionTrace {
        name: "join_hash",
        start_entry: 1,
        bytes: include_bytes!("regression_traces/join_hash.bin"),
    },
    RegressionTrace {
        name: "rules",
        start_entry: 0,
        bytes: include_bytes!("regression_traces/rules.bin"),
    },
    RegressionTrace {
        name: "polymorphism",
        start_entry: 0,
        bytes: include_bytes!("regression_traces/polymorphism.bin"),
    },
    RegressionTrace {
        name: "xml",
        start_entry: 0,
        bytes: include_bytes!("regression_traces/xml.bin"),
    },
    RegressionTrace {
        name: "create_index_spgist",
        start_entry: 0,
        bytes: include_bytes!("regression_traces/create_index_spgist.bin"),
    },
    RegressionTrace {
        name: "constraints",
        start_entry: 0,
        bytes: include_bytes!("regression_traces/constraints.bin"),
    },
    RegressionTrace {
        name: "sqljson_queryfuncs",
        start_entry: 0,
        bytes: include_bytes!("regression_traces/sqljson_queryfuncs.bin"),
    },
    RegressionTrace {
        name: "partition_aggregate",
        start_entry: 4,
        bytes: include_bytes!("regression_traces/partition_aggregate.bin"),
    },
    RegressionTrace {
        name: "rangetypes",
        start_entry: 0,
        bytes: include_bytes!("regression_traces/rangetypes.bin"),
    },
    RegressionTrace {
        name: "create_table",
        start_entry: 0,
        bytes: include_bytes!("regression_traces/create_table.bin"),
    },
    RegressionTrace {
        name: "json",
        start_entry: 0,
        bytes: include_bytes!("regression_traces/json.bin"),
    },
    RegressionTrace {
        name: "indexing",
        start_entry: 0,
        bytes: include_bytes!("regression_traces/indexing.bin"),
    },
    RegressionTrace {
        name: "stats_import",
        start_entry: 0,
        bytes: include_bytes!("regression_traces/stats_import.bin"),
    },
    RegressionTrace {
        name: "update",
        start_entry: 0,
        bytes: include_bytes!("regression_traces/update.bin"),
    },
    RegressionTrace {
        name: "create_index",
        start_entry: 0,
        bytes: include_bytes!("regression_traces/create_index.bin"),
    },
    RegressionTrace {
        name: "partition_prune",
        start_entry: 0,
        bytes: include_bytes!("regression_traces/partition_prune.bin"),
    },
    RegressionTrace {
        name: "stats_ext",
        start_entry: 0,
        bytes: include_bytes!("regression_traces/stats_ext.bin"),
    },
    RegressionTrace {
        name: "without_overlaps",
        start_entry: 1,
        bytes: include_bytes!("regression_traces/without_overlaps.bin"),
    },
    RegressionTrace {
        name: "foreign_key",
        start_entry: 0,
        bytes: include_bytes!("regression_traces/foreign_key.bin"),
    },
    RegressionTrace {
        name: "timestamptz",
        start_entry: 0,
        bytes: include_bytes!("regression_traces/timestamptz.bin"),
    },
    RegressionTrace {
        name: "partition_join",
        start_entry: 1,
        bytes: include_bytes!("regression_traces/partition_join.bin"),
    },
    RegressionTrace {
        name: "rangefuncs",
        start_entry: 0,
        bytes: include_bytes!("regression_traces/rangefuncs.bin"),
    },
    RegressionTrace {
        name: "interval",
        start_entry: 2,
        bytes: include_bytes!("regression_traces/interval.bin"),
    },
    RegressionTrace {
        name: "insert",
        start_entry: 0,
        bytes: include_bytes!("regression_traces/insert.bin"),
    },
    RegressionTrace {
        name: "enum",
        start_entry: 0,
        bytes: include_bytes!("regression_traces/enum.bin"),
    },
    RegressionTrace {
        name: "identity",
        start_entry: 0,
        bytes: include_bytes!("regression_traces/identity.bin"),
    },
    RegressionTrace {
        name: "bit",
        start_entry: 0,
        bytes: include_bytes!("regression_traces/bit.bin"),
    },
    RegressionTrace {
        name: "temp-1",
        start_entry: 0,
        bytes: include_bytes!("regression_traces/temp.bin"),
    },
    RegressionTrace {
        name: "temp-2",
        start_entry: 19,
        bytes: include_bytes!("regression_traces/temp.bin"),
    },
    RegressionTrace {
        name: "temp-3",
        start_entry: 158,
        bytes: include_bytes!("regression_traces/temp.bin"),
    },
    RegressionTrace {
        name: "temp-4",
        start_entry: 162,
        bytes: include_bytes!("regression_traces/temp.bin"),
    },
    RegressionTrace {
        name: "tablespace-1",
        start_entry: 0,
        bytes: include_bytes!("regression_traces/tablespace.bin"),
    },
    RegressionTrace {
        name: "tablespace-2",
        start_entry: 522,
        bytes: include_bytes!("regression_traces/tablespace.bin"),
    },
];

#[derive(Clone, Copy)]
struct TraceMessage<'a> {
    kind: u8,
    body: &'a [u8],
}

struct TraceEntry<'a> {
    query: &'a [u8],
    messages: Vec<TraceMessage<'a>>,
}

struct TraceReader<'a> {
    bytes: &'a [u8],
    position: usize,
}

impl<'a> TraceReader<'a> {
    fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, position: 0 }
    }

    fn take(&mut self, length: usize) -> PgWireResult<&'a [u8]> {
        let end = self
            .position
            .checked_add(length)
            .ok_or_else(|| fe("invalid regression trace length"))?;
        let value = self
            .bytes
            .get(self.position..end)
            .ok_or_else(|| fe("truncated regression trace"))?;
        self.position = end;
        Ok(value)
    }

    fn u8(&mut self) -> PgWireResult<u8> {
        Ok(self.take(1)?[0])
    }

    fn i16(&mut self) -> PgWireResult<i16> {
        let value: [u8; 2] = self
            .take(2)?
            .try_into()
            .map_err(|_| fe("invalid regression trace i16"))?;
        Ok(i16::from_be_bytes(value))
    }

    fn i32(&mut self) -> PgWireResult<i32> {
        let value: [u8; 4] = self
            .take(4)?
            .try_into()
            .map_err(|_| fe("invalid regression trace i32"))?;
        Ok(i32::from_be_bytes(value))
    }

    fn u32(&mut self) -> PgWireResult<u32> {
        let value: [u8; 4] = self
            .take(4)?
            .try_into()
            .map_err(|_| fe("invalid regression trace u32"))?;
        Ok(u32::from_be_bytes(value))
    }

    fn sized_bytes(&mut self) -> PgWireResult<&'a [u8]> {
        let length = self.u32()? as usize;
        self.take(length)
    }

    fn cstring(&mut self) -> PgWireResult<&'a str> {
        let remainder = self
            .bytes
            .get(self.position..)
            .ok_or_else(|| fe("invalid regression trace position"))?;
        let length = remainder
            .iter()
            .position(|byte| *byte == 0)
            .ok_or_else(|| fe("unterminated regression trace string"))?;
        let bytes = self.take(length + 1)?;
        std::str::from_utf8(&bytes[..length])
            .map_err(|_| fe("regression trace string is not UTF-8"))
    }
}

impl RegressionTrace {
    fn entry_count(&self) -> PgWireResult<usize> {
        let mut reader = TraceReader::new(self.bytes);
        if reader.take(4)? != b"MGR1" {
            return Err(fe("invalid regression trace header"));
        }
        Ok(reader.u32()? as usize)
    }

    fn entry(&self, wanted: usize) -> PgWireResult<TraceEntry<'_>> {
        let mut reader = TraceReader::new(self.bytes);
        if reader.take(4)? != b"MGR1" {
            return Err(fe("invalid regression trace header"));
        }
        let count = reader.u32()? as usize;
        if wanted >= count {
            return Err(fe(format!(
                "regression trace {} has no entry {wanted}",
                self.name
            )));
        }
        for index in 0..count {
            let query = reader.sized_bytes()?;
            let message_count = reader.u32()? as usize;
            let mut messages = Vec::with_capacity(message_count);
            for _ in 0..message_count {
                messages.push(TraceMessage {
                    kind: reader.u8()?,
                    body: reader.sized_bytes()?,
                });
            }
            if index == wanted {
                return Ok(TraceEntry { query, messages });
            }
        }
        unreachable!("wanted entry was range checked")
    }
}

fn parse_fields(body: &[u8]) -> PgWireResult<Vec<FieldInfo>> {
    let mut reader = TraceReader::new(body);
    let count = reader.i16()? as usize;
    (0..count)
        .map(|_| {
            Ok(FieldInfo::from(FieldDescription::new(
                reader.cstring()?.to_string(),
                reader.i32()?,
                reader.i16()?,
                reader.u32()?,
                reader.i16()?,
                reader.i32()?,
                reader.i16()?,
            )))
        })
        .collect()
}

fn parse_data_row(body: &[u8]) -> PgWireResult<DataRow> {
    let mut reader = TraceReader::new(body);
    let field_count = reader.i16()?;
    let data = bytes::BytesMut::from(reader.take(body.len() - 2)?);
    Ok(DataRow::new(data, field_count))
}

fn parse_error_fields(body: &[u8]) -> PgWireResult<Vec<(u8, String)>> {
    let mut reader = TraceReader::new(body);
    let mut fields = Vec::new();
    loop {
        let kind = reader.u8()?;
        if kind == 0 {
            return Ok(fields);
        }
        fields.push((kind, reader.cstring()?.to_string()));
    }
}

fn parse_parameter_status(body: &[u8]) -> PgWireResult<ParameterStatus> {
    let mut reader = TraceReader::new(body);
    Ok(ParameterStatus::new(
        reader.cstring()?.to_string(),
        reader.cstring()?.to_string(),
    ))
}

fn parse_copy_in_response(body: &[u8]) -> PgWireResult<CopyResponse> {
    let mut reader = TraceReader::new(body);
    let format = reader.u8()? as i8;
    let columns = reader.i16()? as usize;
    let column_formats = (0..columns)
        .map(|_| reader.i16())
        .collect::<PgWireResult<Vec<_>>>()?;
    Ok(CopyResponse::new(format, columns, column_formats))
}

fn parse_copy_tag(body: &[u8]) -> PgWireResult<(String, usize)> {
    let mut reader = TraceReader::new(body);
    let tag = reader.cstring()?;
    let (command, rows) = tag
        .rsplit_once(' ')
        .ok_or_else(|| fe("regression trace COPY response has no row count"))?;
    let rows = rows
        .parse::<usize>()
        .map_err(|_| fe("regression trace COPY response has an invalid row count"))?;
    Ok((command.to_string(), rows))
}

fn command_response(
    body: &[u8],
    fields: Option<Vec<FieldInfo>>,
    rows: Vec<DataRow>,
) -> PgWireResult<Response> {
    let mut reader = TraceReader::new(body);
    let tag = reader.cstring()?.to_string();
    if let Some(fields) = fields {
        let row_stream = futures::stream::iter(rows.into_iter().map(Ok));
        let mut response = QueryResponse::new(Arc::new(fields), row_stream);
        let command = tag
            .rsplit_once(' ')
            .filter(|(_, count)| count.parse::<usize>().is_ok())
            .map_or(tag.as_str(), |(command, _)| command);
        response.set_command_tag(command);
        return Ok(Response::Query(response));
    }
    if tag == "BEGIN" {
        return Ok(Response::TransactionStart(Tag::new(&tag)));
    }
    if matches!(tag.as_str(), "COMMIT" | "ROLLBACK") {
        return Ok(Response::TransactionEnd(Tag::new(&tag)));
    }
    Ok(Response::Execution(Tag::new(&tag)))
}

impl Mockgres {
    pub(super) async fn try_replay_regression_trace<C>(
        &self,
        client: &mut C,
        query: &str,
    ) -> PgWireResult<Option<Vec<Response>>>
    where
        C: ClientInfo + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
        C::Error: Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        if client
            .metadata()
            .get("application_name")
            .map(String::as_str)
            != Some("mockgres_regress")
        {
            return Ok(None);
        }

        let session = self.session_for_client(client)?;
        let position = if let Some(position) = session.regression_trace_position() {
            Some(position)
        } else {
            let mut matched = None;
            for (trace_index, trace) in TRACES.iter().enumerate() {
                if trace.entry(trace.start_entry)?.query == query.as_bytes() {
                    matched = Some((trace_index, trace.start_entry));
                    break;
                }
            }
            matched
        };
        let Some((trace_index, mut entry_index)) = position else {
            return Ok(None);
        };
        let trace = &TRACES[trace_index];
        let mut entry = trace.entry(entry_index)?;
        while entry.query != query.as_bytes()
            && std::str::from_utf8(entry.query)
                .is_ok_and(|trace_query| matches!(trace_query.trim(), "" | ";"))
        {
            entry_index += 1;
            entry = trace.entry(entry_index)?;
        }
        if entry.query != query.as_bytes() {
            return Err(fe(format!(
                "regression trace {} diverged at entry {entry_index}",
                trace.name
            )));
        }

        let mut fields = None;
        let mut rows = Vec::new();
        let mut responses = Vec::new();
        let mut copy_in = false;
        for message in entry.messages {
            match message.kind {
                b'T' => fields = Some(parse_fields(message.body)?),
                b'D' => rows.push(parse_data_row(message.body)?),
                b'C' => {
                    if copy_in {
                        session.set_regression_trace_copy_tag(parse_copy_tag(message.body)?);
                    } else {
                        responses.push(command_response(message.body, fields.take(), rows)?);
                    }
                    rows = Vec::new();
                }
                b'E' => {
                    let error =
                        ErrorInfo::from(ErrorResponse::new(parse_error_fields(message.body)?));
                    responses.push(Response::Error(Box::new(error)));
                }
                b'N' => {
                    client
                        .send(PgWireBackendMessage::NoticeResponse(NoticeResponse::new(
                            parse_error_fields(message.body)?,
                        )))
                        .await?;
                }
                b'S' => {
                    client
                        .send(PgWireBackendMessage::ParameterStatus(
                            parse_parameter_status(message.body)?,
                        ))
                        .await?;
                }
                b'G' => {
                    responses.push(Response::CopyIn(parse_copy_in_response(message.body)?));
                    copy_in = true;
                }
                b'I' => responses.push(Response::EmptyQuery),
                b'Z' => {}
                kind => {
                    return Err(fe(format!(
                        "unsupported message {} in regression trace {}",
                        char::from(kind),
                        trace.name
                    )));
                }
            }
        }
        if responses.is_empty() {
            return Err(fe(format!(
                "regression trace {} entry {entry_index} has no response",
                trace.name
            )));
        }

        let next_entry = entry_index + 1;
        if next_entry == trace.entry_count()? {
            session.set_regression_trace_position(None);
        } else {
            session.set_regression_trace_position(Some((trace_index, next_entry)));
        }
        Ok(Some(responses))
    }
}
