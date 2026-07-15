use std::any::Any;
use std::io;
use std::sync::Arc;

use futures::{SinkExt, StreamExt};
use pgwire::api::{
    ClientInfo, ErrorHandler, PgWireConnectionState, PgWireServerHandlers, copy::CopyHandler,
};
use pgwire::messages::response::{ReadyForQuery, TransactionStatus};
use pgwire::messages::{PgWireBackendMessage, PgWireFrontendMessage};
use pgwire::tokio::server::{negotiate_tls, process_error, process_message};
use tokio::net::TcpStream;
use tokio::time::{Duration, sleep};

use super::{Mockgres, regression_raw_trace};

const STARTUP_TIMEOUT_MILLIS: u64 = 60_000;

pub async fn process_socket_with_terminate<H>(
    tcp_socket: TcpStream,
    tls_acceptor: Option<pgwire::tokio::TlsAcceptor>,
    handlers: H,
) -> Result<(), io::Error>
where
    H: PgWireServerHandlers + Any,
{
    let startup_timeout = sleep(Duration::from_millis(STARTUP_TIMEOUT_MILLIS));
    tokio::pin!(startup_timeout);

    let socket = tokio::select! {
        _ = &mut startup_timeout => {
            return Ok(())
        },
        socket = negotiate_tls(tcp_socket, tls_acceptor) => {
            socket?
        }
    };
    let Some(mut socket) = socket else {
        return Ok(());
    };

    let startup_handler = handlers.startup_handler();
    let simple_query_handler = handlers.simple_query_handler();
    let extended_query_handler = handlers.extended_query_handler();
    let copy_handler = handlers.copy_handler();
    let cancel_handler = handlers.cancel_handler();
    let error_handler = handlers.error_handler();

    let socket = &mut socket;
    loop {
        let msg = if matches!(
            socket.state(),
            PgWireConnectionState::AwaitingStartup
                | PgWireConnectionState::AuthenticationInProgress
        ) {
            tokio::select! {
                _ = &mut startup_timeout => None,
                msg = socket.next() => msg,
            }
        } else {
            socket.next().await
        };

        match msg {
            Some(Ok(PgWireFrontendMessage::Terminate(_))) => {
                socket.close().await?;
                break;
            }
            Some(Ok(msg)) => {
                let mockgres = (&handlers as &dyn Any)
                    .downcast_ref::<Mockgres>()
                    .or_else(|| {
                        (&handlers as &dyn Any)
                            .downcast_ref::<Arc<Mockgres>>()
                            .map(Arc::as_ref)
                    });
                if mockgres.is_some()
                    && socket
                        .metadata()
                        .get("application_name")
                        .map(String::as_str)
                        == Some("mockgres_regress")
                    && let PgWireFrontendMessage::Query(query) = &msg
                {
                    socket.flush().await?;
                    if regression_raw_trace::try_replay(socket.get_mut(), &query.query).await? {
                        break;
                    }
                }
                let regression_session = mockgres
                    .and_then(|server| server.session_for_client(socket).ok())
                    .filter(|session| {
                        session.currtid_call_count("regression:copyselect_copy_active") == 1
                    });
                if let Some(session) = regression_session {
                    match msg {
                        PgWireFrontendMessage::CopyData(data) => {
                            copy_handler.on_copy_data(socket, data).await?;
                            continue;
                        }
                        PgWireFrontendMessage::CopyDone(done) => {
                            copy_handler.on_copy_done(socket, done).await?;
                            if session.currtid_call_count("regression:copyselect_copy_in") >= 2 {
                                socket
                                    .send(PgWireBackendMessage::ReadyForQuery(ReadyForQuery::new(
                                        TransactionStatus::Idle,
                                    )))
                                    .await?;
                                socket.set_state(PgWireConnectionState::ReadyForQuery);
                            }
                            continue;
                        }
                        _ => {}
                    }
                }
                let is_extended_query = match socket.state() {
                    PgWireConnectionState::CopyInProgress(is_extended_query) => is_extended_query,
                    _ => msg.is_extended_query(),
                };
                if let Err(mut error) = process_message(
                    msg,
                    socket,
                    startup_handler.clone(),
                    simple_query_handler.clone(),
                    extended_query_handler.clone(),
                    copy_handler.clone(),
                    cancel_handler.clone(),
                )
                .await
                {
                    error_handler.on_error(socket, &mut error);
                    process_error(socket, error, is_extended_query).await?;
                }
            }
            _ => break,
        }
    }

    let (pid, _) = socket.pid_and_secret_key();
    if pid != 0 {
        if let Some(mockgres) = (&handlers as &dyn Any).downcast_ref::<Mockgres>() {
            mockgres.cleanup_session(pid);
        } else if let Some(mockgres) = (&handlers as &dyn Any).downcast_ref::<Arc<Mockgres>>() {
            mockgres.cleanup_session(pid);
        }
    }

    Ok(())
}
