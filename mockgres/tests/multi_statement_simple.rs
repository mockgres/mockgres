mod common;

use std::io;

use bytes::BytesMut;
use fallible_iterator::FallibleIterator;
use postgres_protocol::message::backend::Message;
use postgres_protocol::message::frontend as fe;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio_postgres::SimpleQueryMessage;

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

async fn run_simple_query(
    stream: &mut TcpStream,
    read_buf: &mut BytesMut,
    out: &mut BytesMut,
    sql: &str,
) -> io::Result<(Vec<String>, bool, u8)> {
    out.clear();
    fe::query(sql, out)?;
    stream.write_all(out).await?;
    stream.flush().await?;

    let mut tags = Vec::new();
    let mut saw_error = false;
    loop {
        match read_message(stream, read_buf).await? {
            Message::CommandComplete(body) => tags.push(body.tag()?.to_string()),
            Message::ErrorResponse(_) => saw_error = true,
            Message::ReadyForQuery(body) => return Ok((tags, saw_error, body.status())),
            _ => {}
        }
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn simple_query_executes_full_batch_in_order() {
    let ctx = common::start().await;

    let msgs = ctx
        .client
        .simple_query(
            "create table ms_simple_order(id int primary key, note text); \
             insert into ms_simple_order values (1, 'a'), (2, 'b'); \
             select id, note from ms_simple_order order by id",
        )
        .await
        .expect("simple query");

    let mut command_completes = Vec::new();
    let mut rows = Vec::new();
    for msg in msgs {
        match msg {
            SimpleQueryMessage::CommandComplete(count) => command_completes.push(count),
            SimpleQueryMessage::Row(row) => {
                let id = row.get(0).expect("id").to_string();
                let note = row.get(1).expect("note").to_string();
                rows.push((id, note));
            }
            SimpleQueryMessage::RowDescription(_) => {}
            _ => {}
        }
    }

    assert_eq!(command_completes, vec![0, 2, 2]);
    assert_eq!(
        rows,
        vec![
            ("1".to_string(), "a".to_string()),
            ("2".to_string(), "b".to_string())
        ]
    );

    let _ = ctx.shutdown.send(());
}

#[tokio::test(flavor = "multi_thread")]
async fn later_statements_in_batch_see_earlier_statement_effects() {
    let ctx = common::start().await;

    ctx.client
        .execute("create table ms_simple_visibility(id int primary key)", &[])
        .await
        .expect("create table");

    let msgs = ctx
        .client
        .simple_query(
            "insert into ms_simple_visibility values (1); \
             select count(*) from ms_simple_visibility",
        )
        .await
        .expect("multi statement query");

    let count = msgs
        .into_iter()
        .find_map(|msg| match msg {
            SimpleQueryMessage::Row(row) => row.get(0).map(|s| s.to_string()),
            _ => None,
        })
        .expect("count row");
    assert_eq!(
        count, "1",
        "later statement should observe effects from earlier statement in same batch"
    );

    let _ = ctx.shutdown.send(());
}

#[tokio::test(flavor = "multi_thread")]
async fn simple_multi_statement_uses_implicit_tx_and_rolls_back_on_error() {
    let ctx = common::start().await;

    ctx.client
        .execute("create table ms_simple_implicit(id int primary key)", &[])
        .await
        .expect("create table");

    let err = ctx
        .client
        .simple_query("insert into ms_simple_implicit values (1); insert into ms_simple_implicit values ('bad')")
        .await
        .expect_err("batch should fail");
    let msg = err
        .as_db_error()
        .expect("db error")
        .message()
        .to_ascii_lowercase();
    assert!(
        msg.contains("invalid"),
        "unexpected error for failing insert: {msg}"
    );

    let count = ctx
        .client
        .query_one("select count(*) from ms_simple_implicit", &[])
        .await
        .expect("count rows")
        .get::<_, i64>(0);
    assert_eq!(
        count, 0,
        "implicit transaction should rollback all statements"
    );

    let _ = ctx.shutdown.send(());
}

#[tokio::test(flavor = "multi_thread")]
async fn explicit_transaction_sequence_behavior_is_preserved() {
    let ctx = common::start().await;

    ctx.client
        .execute("create table ms_simple_explicit(id int primary key)", &[])
        .await
        .expect("create table");

    let _ = ctx
        .client
        .simple_query(
            "begin; \
             insert into ms_simple_explicit values (1); \
             insert into ms_simple_explicit values ('bad'); \
             rollback",
        )
        .await
        .expect_err("batch should fail before rollback statement");

    ctx.client
        .execute("rollback", &[])
        .await
        .expect("transaction should still be open and rollback-able");

    let count = ctx
        .client
        .query_one("select count(*) from ms_simple_explicit", &[])
        .await
        .expect("count rows")
        .get::<_, i64>(0);
    assert_eq!(count, 0);

    let _ = ctx.shutdown.send(());
}

#[tokio::test(flavor = "multi_thread")]
async fn simple_protocol_command_complete_tags_are_ordered() {
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

    fe::query(
        "create table ms_simple_tags(id int primary key); \
         insert into ms_simple_tags values (1), (2); \
         select id from ms_simple_tags order by id",
        &mut out,
    )
    .expect("query");
    stream.write_all(&out).await.expect("write query");
    stream.flush().await.expect("flush query");

    let mut tags = Vec::new();
    loop {
        match read_message(&mut stream, &mut read_buf)
            .await
            .expect("read message")
        {
            Message::CommandComplete(body) => tags.push(body.tag().expect("tag").to_string()),
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

    assert_eq!(
        tags,
        vec![
            "CREATE TABLE".to_string(),
            "INSERT 0 2".to_string(),
            "SELECT 2".to_string()
        ]
    );

    let _ = shutdown.send(());
}

#[tokio::test(flavor = "multi_thread")]
async fn simple_protocol_transaction_status_transitions_are_correct() {
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

    let (tags, saw_error, status) =
        run_simple_query(&mut stream, &mut read_buf, &mut out, "begin; select 1")
            .await
            .expect("run begin batch");
    assert_eq!(tags, vec!["BEGIN".to_string(), "SELECT 1".to_string()]);
    assert!(!saw_error);
    assert_eq!(status, b'T', "should stay in explicit transaction");

    let (_tags, saw_error, status) =
        run_simple_query(&mut stream, &mut read_buf, &mut out, "rollback")
            .await
            .expect("rollback");
    assert!(!saw_error);
    assert_eq!(status, b'I', "rollback should return to idle");

    let _ = shutdown.send(());
}

#[tokio::test(flavor = "multi_thread")]
async fn implicit_batch_failure_returns_idle_ready_for_query_status() {
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

    let (_tags, saw_error, status) = run_simple_query(
        &mut stream,
        &mut read_buf,
        &mut out,
        "create table ms_simple_status(id int primary key)",
    )
    .await
    .expect("create table");
    assert!(!saw_error);
    assert_eq!(status, b'I');

    let (tags, saw_error, status) = run_simple_query(
        &mut stream,
        &mut read_buf,
        &mut out,
        "insert into ms_simple_status values (1); insert into ms_simple_status values ('bad')",
    )
    .await
    .expect("run failing implicit batch");
    assert_eq!(tags, vec!["INSERT 0 1".to_string()]);
    assert!(saw_error, "expected error response for second statement");
    assert_eq!(
        status, b'I',
        "implicit transaction failure should end in idle status"
    );

    let _ = shutdown.send(());
}

#[tokio::test(flavor = "multi_thread")]
async fn simple_multi_statement_supports_builtin_calls() {
    let ctx = common::start().await;

    ctx.client
        .execute("create table ms_simple_builtin(id int primary key)", &[])
        .await
        .expect("create table");
    ctx.client
        .execute("insert into ms_simple_builtin values (1)", &[])
        .await
        .expect("seed row");

    let msgs = ctx
        .client
        .simple_query(
            "select mockgres_freeze(); \
             insert into ms_simple_builtin values (2); \
             select mockgres_reset(); \
             select count(*) from ms_simple_builtin",
        )
        .await
        .expect("multi statement builtins");

    let rows: Vec<String> = msgs
        .into_iter()
        .filter_map(|msg| match msg {
            SimpleQueryMessage::Row(row) => row.get(0).map(|v| v.to_string()),
            _ => None,
        })
        .collect();

    assert_eq!(
        rows,
        vec!["t".to_string(), "t".to_string(), "1".to_string()]
    );

    let _ = ctx.shutdown.send(());
}

#[tokio::test(flavor = "multi_thread")]
async fn implicit_batch_failure_rolls_back_unique_index_state() {
    let ctx = common::start().await;

    ctx.client
        .execute(
            "create table ms_simple_unique_rollback(id int primary key, code int unique)",
            &[],
        )
        .await
        .expect("create table");

    let _ = ctx
        .client
        .simple_query(
            "insert into ms_simple_unique_rollback values (1, 10); \
             insert into ms_simple_unique_rollback values (2, 10)",
        )
        .await
        .expect_err("second statement should fail on unique index");

    let count_after_failure = ctx
        .client
        .query_one("select count(*) from ms_simple_unique_rollback", &[])
        .await
        .expect("count after failure")
        .get::<_, i64>(0);
    assert_eq!(
        count_after_failure, 0,
        "implicit rollback should remove inserted rows"
    );

    ctx.client
        .execute("insert into ms_simple_unique_rollback values (1, 10)", &[])
        .await
        .expect("unique index bookkeeping should be clean after rollback");

    let _ = ctx.shutdown.send(());
}

#[tokio::test(flavor = "multi_thread")]
async fn implicit_batch_failure_rolls_back_foreign_key_state() {
    let ctx = common::start().await;

    ctx.client
        .execute("create table ms_parent(id int primary key)", &[])
        .await
        .expect("create parent");
    ctx.client
        .execute(
            "create table ms_child(id int primary key, parent_id int references ms_parent(id))",
            &[],
        )
        .await
        .expect("create child");

    let _ = ctx
        .client
        .simple_query(
            "insert into ms_parent values (1); \
             insert into ms_child values (1, 999)",
        )
        .await
        .expect_err("child insert should fail on missing parent");

    let parent_count = ctx
        .client
        .query_one("select count(*) from ms_parent", &[])
        .await
        .expect("parent count")
        .get::<_, i64>(0);
    assert_eq!(
        parent_count, 0,
        "failed batch should rollback parent row insert"
    );

    ctx.client
        .execute("insert into ms_parent values (1)", &[])
        .await
        .expect("parent insert after rollback");
    ctx.client
        .execute("insert into ms_child values (1, 1)", &[])
        .await
        .expect("child insert after rollback");

    let _ = ctx.shutdown.send(());
}
