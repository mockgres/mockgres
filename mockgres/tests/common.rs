use std::sync::Arc;

use tokio::net::TcpListener;
use tokio::task::JoinHandle;
use tokio_postgres::{Client, NoTls, SimpleQueryMessage};

pub struct TestCtx {
    pub client: Client,
    pub _bg: JoinHandle<()>,
    pub _server: JoinHandle<()>,
    pub shutdown: tokio::sync::oneshot::Sender<()>,
}

pub async fn start() -> TestCtx {
    let handler = Arc::new(mockgres::Mockgres::default());
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let addr = listener.local_addr().expect("local addr");
    let (shutdown, shutdown_rx) = tokio::sync::oneshot::channel::<()>();
    let server_task = tokio::spawn(run_server(listener, handler.clone(), shutdown_rx));
    let conn_str = format!("host={} port={} user=postgres", addr.ip(), addr.port());
    let (client, connection) = tokio_postgres::connect(&conn_str, NoTls)
        .await
        .expect("connect");
    let conn_task = tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("connection error: {e}");
        }
    });

    TestCtx {
        client,
        _bg: conn_task,
        _server: server_task,
        shutdown,
    }
}

async fn run_server(
    listener: TcpListener,
    handler: Arc<mockgres::Mockgres>,
    mut shutdown: tokio::sync::oneshot::Receiver<()>,
) {
    loop {
        tokio::select! {
            _ = &mut shutdown => break,
            accept_result = listener.accept() => {
                match accept_result {
                    Ok((socket, _peer)) => {
                        let h2 = handler.clone();
                        tokio::spawn(async move {
                            let _ = pgwire::tokio::process_socket(socket, None, h2).await;
                        });
                    }
                    Err(e) => {
                        eprintln!("listener accept error: {e}");
                        break;
                    }
                }
            }
        }
    }
}

// run a simple query and return the first cell as string
pub async fn simple_first_cell(client: &Client, sql: &str) -> String {
    let msgs = client.simple_query(sql).await.expect("simple query");
    let row = msgs
        .iter()
        .find_map(|m| match m {
            SimpleQueryMessage::Row(r) => Some(r),
            _ => None,
        })
        .expect("expected one row");
    row.get(0).expect("one column").to_string()
}
