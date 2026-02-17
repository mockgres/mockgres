mod common;

use std::io;

use bytes::BytesMut;
use fallible_iterator::FallibleIterator;
use postgres_protocol::IsNull;
use postgres_protocol::message::backend::Message;
use postgres_protocol::message::frontend as fe;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

async fn read_message(stream: &mut TcpStream, buf: &mut BytesMut) -> io::Result<Message> {
    loop {
        if let Some(msg) = postgres_protocol::message::backend::Message::parse(buf)? {
            return Ok(msg);
        }
        let read = stream.read_buf(buf).await?;
        if read == 0 {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "connection closed",
            ));
        }
    }
}

async fn drain_until_ready(stream: &mut TcpStream, buf: &mut BytesMut) -> io::Result<()> {
    loop {
        match read_message(stream, buf).await? {
            Message::ReadyForQuery(_) => return Ok(()),
            Message::ErrorResponse(err) => {
                let mut fields = err.fields();
                let mut msg = None;
                while let Some(field) = fields.next()? {
                    if field.type_() == b'M' {
                        msg = Some(String::from_utf8_lossy(field.value_bytes()).to_string());
                    }
                }
                return Err(io::Error::other(format!(
                    "server error: {}",
                    msg.unwrap_or_else(|| "unknown".to_string())
                )));
            }
            _ => {}
        }
    }
}

#[derive(Debug)]
struct ExtendedExecTrace {
    tags: Vec<String>,
    data_rows: usize,
    error_message: Option<String>,
    ready_status: u8,
}

async fn run_unnamed_extended_execute(
    stream: &mut TcpStream,
    read_buf: &mut BytesMut,
    out: &mut BytesMut,
    sql: &str,
) -> io::Result<ExtendedExecTrace> {
    out.clear();
    fe::parse("", sql, std::iter::empty::<postgres_protocol::Oid>(), out)?;
    if fe::bind(
        "",
        "",
        std::iter::empty::<i16>(),
        std::iter::empty::<()>(),
        |_value, _buf| Ok(IsNull::Yes),
        std::iter::empty::<i16>(),
        out,
    )
    .is_err()
    {
        return Err(io::Error::other("bind failed"));
    }
    fe::execute("", 0, out)?;
    fe::sync(out);
    stream.write_all(out).await?;
    stream.flush().await?;

    let mut tags = Vec::new();
    let mut data_rows = 0usize;
    let mut error_message = None;
    loop {
        match read_message(stream, read_buf).await? {
            Message::CommandComplete(body) => {
                tags.push(body.tag().expect("tag").to_string());
            }
            Message::RowDescription(_) => {}
            Message::DataRow(_) => data_rows += 1,
            Message::ErrorResponse(err) => {
                let mut fields = err.fields();
                while let Some(field) = fields.next().expect("error field") {
                    if field.type_() == b'M' {
                        error_message =
                            Some(String::from_utf8_lossy(field.value_bytes()).to_string());
                    }
                }
            }
            Message::ReadyForQuery(body) => {
                return Ok(ExtendedExecTrace {
                    tags,
                    data_rows,
                    error_message,
                    ready_status: body.status(),
                });
            }
            _ => {}
        }
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn parse_rejects_multi_statement_sql_like_postgres() {
    let (addr, _server, shutdown) =
        common::spawn_server(std::sync::Arc::new(mockgres::Mockgres::default())).await;

    let mut stream = TcpStream::connect(addr).await.expect("connect");
    let mut read_buf = BytesMut::new();
    let mut out = BytesMut::new();

    fe::startup_message([("user", "postgres"), ("database", "postgres")], &mut out)
        .expect("startup");
    stream.write_all(&out).await.expect("write startup");
    stream.flush().await.expect("flush startup");
    out.clear();
    drain_until_ready(&mut stream, &mut read_buf)
        .await
        .expect("startup ready");

    fe::parse(
        "stmt_batch",
        "SELECT 1; SELECT 2",
        std::iter::empty::<postgres_protocol::Oid>(),
        &mut out,
    )
    .expect("parse");
    fe::sync(&mut out);
    stream.write_all(&out).await.expect("write parse");
    stream.flush().await.expect("flush parse");

    let mut error_message = None;
    loop {
        match read_message(&mut stream, &mut read_buf)
            .await
            .expect("read message")
        {
            Message::ReadyForQuery(_) => break,
            Message::ErrorResponse(err) => {
                let mut fields = err.fields();
                while let Some(field) = fields.next().expect("error field") {
                    if field.type_() == b'M' {
                        error_message =
                            Some(String::from_utf8_lossy(field.value_bytes()).to_string());
                    }
                }
            }
            _ => {}
        }
    }

    let msg = error_message.expect("expected parse error");
    assert!(
        msg.contains("cannot insert multiple commands into a prepared statement"),
        "unexpected parse error: {msg}"
    );

    let _ = shutdown.send(());
}

#[tokio::test(flavor = "multi_thread")]
async fn connection_recovers_after_rejected_multi_statement_parse() {
    let (addr, _server, shutdown) =
        common::spawn_server(std::sync::Arc::new(mockgres::Mockgres::default())).await;

    let mut stream = TcpStream::connect(addr).await.expect("connect");
    let mut read_buf = BytesMut::new();
    let mut out = BytesMut::new();

    fe::startup_message([("user", "postgres"), ("database", "postgres")], &mut out)
        .expect("startup");
    stream.write_all(&out).await.expect("write startup");
    stream.flush().await.expect("flush startup");
    out.clear();
    drain_until_ready(&mut stream, &mut read_buf)
        .await
        .expect("startup ready");

    fe::parse(
        "stmt_batch",
        "SELECT 1; SELECT 2",
        std::iter::empty::<postgres_protocol::Oid>(),
        &mut out,
    )
    .expect("parse");
    fe::sync(&mut out);
    stream.write_all(&out).await.expect("write parse");
    stream.flush().await.expect("flush parse");

    loop {
        match read_message(&mut stream, &mut read_buf)
            .await
            .expect("read message")
        {
            Message::ReadyForQuery(_) => break,
            _ => {}
        }
    }

    let trace =
        run_unnamed_extended_execute(&mut stream, &mut read_buf, &mut out, "select 1").await;
    let trace = trace.expect("single statement execute after parse error");
    assert_eq!(trace.tags, vec!["SELECT 1".to_string()]);
    assert_eq!(trace.data_rows, 1);
    assert!(trace.error_message.is_none());
    assert_eq!(trace.ready_status, b'I');

    let _ = shutdown.send(());
}
