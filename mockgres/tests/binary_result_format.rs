mod common;

use std::io;

use bytes::BytesMut;
use fallible_iterator::FallibleIterator;
use postgres_protocol::IsNull;
use postgres_protocol::message::backend::{DataRowBody, Message, RowDescriptionBody};
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
                return Err(io::Error::new(
                    io::ErrorKind::Other,
                    format!(
                        "server error: {}",
                        msg.unwrap_or_else(|| "unknown".to_string())
                    ),
                ));
            }
            _ => {}
        }
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn extended_query_respects_binary_result_format() {
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
        "",
        "SELECT 1",
        std::iter::empty::<postgres_protocol::Oid>(),
        &mut out,
    )
    .expect("parse");
    if fe::bind(
        "",
        "",
        std::iter::empty::<i16>(),
        std::iter::empty::<()>(),
        |_value, _buf| Ok(IsNull::Yes),
        std::iter::once(1_i16),
        &mut out,
    )
    .is_err()
    {
        panic!("bind");
    }
    fe::describe(b'P', "", &mut out).expect("describe");
    fe::execute("", 0, &mut out).expect("execute");
    fe::sync(&mut out);
    stream.write_all(&out).await.expect("write query");
    stream.flush().await.expect("flush query");

    let mut row_description: Option<RowDescriptionBody> = None;
    let mut data_row: Option<DataRowBody> = None;
    loop {
        match read_message(&mut stream, &mut read_buf)
            .await
            .expect("read message")
        {
            Message::RowDescription(body) => row_description = Some(body),
            Message::DataRow(body) => data_row = Some(body),
            Message::ReadyForQuery(_) => break,
            Message::ErrorResponse(err) => {
                let mut fields = err.fields();
                let mut msg = None;
                while let Some(field) = fields.next().expect("error field") {
                    if field.type_() == b'M' {
                        msg = Some(String::from_utf8_lossy(field.value_bytes()).to_string());
                    }
                }
                panic!(
                    "server error: {}",
                    msg.unwrap_or_else(|| "unknown".to_string())
                );
            }
            _ => {}
        }
    }

    let row_description = row_description.expect("row description");
    let mut fields = row_description.fields();
    let field = fields.next().expect("field iter").expect("field");
    assert_eq!(field.format(), 1, "expected binary result format");
    assert!(fields.next().expect("field iter").is_none());

    let data_row = data_row.expect("data row");
    let mut ranges = data_row.ranges();
    let range = ranges
        .next()
        .expect("range iter")
        .expect("range")
        .expect("value");
    let len = range.end - range.start;
    assert_eq!(len, 4, "expected int4 binary length");
    let bytes = &data_row.buffer()[range];
    let value = i32::from_be_bytes(bytes.try_into().expect("int4 bytes"));
    assert_eq!(value, 1);

    let _ = shutdown.send(());
}
