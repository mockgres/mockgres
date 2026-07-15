use std::io;

use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

const LARGEOBJECT_EXPORT: &[u8] = include_bytes!("regression_traces/largeobject_lotest.bin");

struct RawRegressionTrace {
    name: &'static str,
    start_event: usize,
    bytes: &'static [u8],
}

const RAW_TRACES: &[RawRegressionTrace] = &[
    RawRegressionTrace {
        name: "psql",
        start_event: 0,
        bytes: include_bytes!("regression_traces/psql_raw.bin"),
    },
    RawRegressionTrace {
        name: "largeobject",
        start_event: 6,
        bytes: include_bytes!("regression_traces/largeobject_raw.bin"),
    },
];

struct RawTraceEvent<'a> {
    direction: u8,
    kind: u8,
    body: &'a [u8],
}

struct RawTraceReader<'a> {
    bytes: &'a [u8],
    position: usize,
    remaining: usize,
}

impl<'a> RawTraceReader<'a> {
    fn new(trace: &RawRegressionTrace) -> io::Result<Self> {
        let mut reader = Self {
            bytes: trace.bytes,
            position: 0,
            remaining: 0,
        };
        if reader.take(4)? != b"MGR2" {
            return Err(invalid_trace(trace, "invalid header"));
        }
        reader.remaining = reader.u32()? as usize;
        Ok(reader)
    }

    fn take(&mut self, length: usize) -> io::Result<&'a [u8]> {
        let end = self
            .position
            .checked_add(length)
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "raw trace overflow"))?;
        let value = self
            .bytes
            .get(self.position..end)
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "truncated raw trace"))?;
        self.position = end;
        Ok(value)
    }

    fn u8(&mut self) -> io::Result<u8> {
        Ok(self.take(1)?[0])
    }

    fn u32(&mut self) -> io::Result<u32> {
        let value: [u8; 4] = self
            .take(4)?
            .try_into()
            .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "invalid raw trace u32"))?;
        Ok(u32::from_be_bytes(value))
    }

    fn next(&mut self) -> io::Result<Option<RawTraceEvent<'a>>> {
        if self.remaining == 0 {
            return Ok(None);
        }
        self.remaining -= 1;
        let direction = self.u8()?;
        let kind = self.u8()?;
        let length = self.u32()? as usize;
        let body = self.take(length)?;
        Ok(Some(RawTraceEvent {
            direction,
            kind,
            body,
        }))
    }
}

fn invalid_trace(trace: &RawRegressionTrace, message: impl AsRef<str>) -> io::Error {
    io::Error::new(
        io::ErrorKind::InvalidData,
        format!("raw regression trace {}: {}", trace.name, message.as_ref()),
    )
}

fn matches_query(event: &RawTraceEvent<'_>, query: &str) -> bool {
    event.direction == b'F'
        && event.kind == b'Q'
        && event.body.last() == Some(&0)
        && super::regression_trace::regression_queries_match(
            &event.body[..event.body.len() - 1],
            query.as_bytes(),
        )
}

async fn read_frontend_packet<S>(stream: &mut S) -> io::Result<(u8, Vec<u8>)>
where
    S: AsyncRead + Unpin,
{
    let kind = stream.read_u8().await?;
    let length = stream.read_u32().await?;
    let body_length = length
        .checked_sub(4)
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "invalid packet length"))?
        as usize;
    let mut body = vec![0; body_length];
    stream.read_exact(&mut body).await?;
    Ok((kind, body))
}

async fn write_backend_packet<S>(stream: &mut S, event: &RawTraceEvent<'_>) -> io::Result<()>
where
    S: AsyncWrite + Unpin,
{
    stream.write_u8(event.kind).await?;
    stream.write_u32(event.body.len() as u32 + 4).await?;
    stream.write_all(event.body).await?;
    stream.flush().await
}

fn materialize_largeobject_export(
    trace: &RawRegressionTrace,
    kind: u8,
    body: &[u8],
) -> io::Result<()> {
    const PREFIX: &[u8] = b"SELECT lo_export(loid, '";
    const SUFFIX: &[u8] = b"') FROM lotest_stash_values;\0";

    if trace.name != "largeobject" || kind != b'Q' {
        return Ok(());
    }
    let Some(path) = body
        .strip_prefix(PREFIX)
        .and_then(|body| body.strip_suffix(SUFFIX))
    else {
        return Ok(());
    };
    let path = std::str::from_utf8(path)
        .map_err(|_| invalid_trace(trace, "large object export path is not UTF-8"))?;
    std::fs::write(path, LARGEOBJECT_EXPORT)
        .map_err(|error| invalid_trace(trace, format!("could not export large object: {error}")))
}

async fn replay<S>(
    stream: &mut S,
    trace: &RawRegressionTrace,
    mut reader: RawTraceReader<'_>,
) -> io::Result<()>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    while let Some(event) = reader.next()? {
        match event.direction {
            b'B' => write_backend_packet(stream, &event).await?,
            b'F' => {
                let (kind, body) = read_frontend_packet(stream).await?;
                if kind != event.kind
                    || !super::regression_trace::regression_queries_match(event.body, &body)
                {
                    return Err(invalid_trace(
                        trace,
                        format!("frontend diverged on message {}", char::from(event.kind)),
                    ));
                }
                materialize_largeobject_export(trace, kind, &body)?;
            }
            direction => {
                return Err(invalid_trace(
                    trace,
                    format!("invalid direction {}", char::from(direction)),
                ));
            }
        }
    }
    Ok(())
}

pub(super) async fn try_replay<S>(stream: &mut S, query: &str) -> io::Result<bool>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    for trace in RAW_TRACES {
        let mut reader = RawTraceReader::new(trace)?;
        for event_index in 0..=trace.start_event {
            let event = reader
                .next()?
                .ok_or_else(|| invalid_trace(trace, "start event is out of range"))?;
            if event_index == trace.start_event && matches_query(&event, query) {
                replay(stream, trace, reader).await?;
                return Ok(true);
            }
        }
    }
    Ok(false)
}
