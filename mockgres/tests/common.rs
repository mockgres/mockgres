use std::net::SocketAddr;
use std::sync::Arc;

use tokio::net::TcpListener;
use tokio::sync::Mutex;
use tokio::task::JoinHandle;
use tokio_postgres::{Client, NoTls, SimpleQueryMessage};

pub struct TestCtx {
    pub client: Client,
    #[allow(dead_code)]
    conn_str: String,
    pub _bg: JoinHandle<()>,
    pub _server: JoinHandle<()>,
    pub shutdown: tokio::sync::oneshot::Sender<()>,
    #[allow(dead_code)]
    extra_connections: Arc<Mutex<Vec<JoinHandle<()>>>>,
}

pub async fn start() -> TestCtx {
    let handler = Arc::new(mockgres::Mockgres::default());
    start_with_handler(handler).await
}

pub async fn start_with_handler(handler: Arc<mockgres::Mockgres>) -> TestCtx {
    let (addr, server_task, shutdown) = spawn_server(handler).await;
    let conn_str = format!("host={} port={} user=postgres", addr.ip(), addr.port());
    let (client, connection) = tokio_postgres::connect(&conn_str, NoTls)
        .await
        .expect("connect");
    let conn_task = tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("connection error: {e}");
        }
    });
    let extras = Arc::new(Mutex::new(Vec::new()));

    TestCtx {
        client,
        conn_str,
        _bg: conn_task,
        _server: server_task,
        shutdown,
        extra_connections: extras,
    }
}

pub async fn spawn_server(
    handler: Arc<mockgres::Mockgres>,
) -> (SocketAddr, JoinHandle<()>, tokio::sync::oneshot::Sender<()>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let addr = listener.local_addr().expect("local addr");
    let (shutdown, shutdown_rx) = tokio::sync::oneshot::channel::<()>();
    let server_task = tokio::spawn(run_server(listener, handler, shutdown_rx));
    (addr, server_task, shutdown)
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
                            let _ = mockgres::process_socket_with_terminate(socket, None, h2).await;
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

impl TestCtx {
    #[allow(dead_code)]
    pub async fn new_client(&self) -> Client {
        let (client, connection) = tokio_postgres::connect(&self.conn_str, NoTls)
            .await
            .expect("connect extra client");
        let handle = tokio::spawn(async move {
            if let Err(e) = connection.await {
                eprintln!("connection error: {e}");
            }
        });
        self.extra_connections.lock().await.push(handle);
        client
    }
}

// run a simple query and return the first cell as string
#[allow(dead_code)]
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

pub fn assert_db_error_contains(err: &tokio_postgres::Error, needle: &str) {
    let db_err = err.as_db_error().expect("expected db error");
    assert!(
        db_err.message().contains(needle),
        "unexpected error: {:?}",
        db_err.message()
    );
}
