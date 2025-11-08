use std::sync::Arc;

use tokio::net::TcpListener;
use tokio::sync::OnceCell;
use tokio::task::JoinHandle;
use tokio_postgres::{Client, NoTls, SimpleQueryMessage};

pub struct TestCtx {
    pub client: Client,
    pub _bg: JoinHandle<()>,
    pub shutdown: tokio::sync::oneshot::Sender<()>,
}

pub async fn start() -> TestCtx {
    let server = ensure_server().await;
    let conn_str = format!(
        "host={} port={} user=postgres",
        server.addr.ip(),
        server.addr.port()
    );
    let (client, connection) = tokio_postgres::connect(&conn_str, NoTls)
        .await
        .expect("connect");
    let conn_task = tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("connection error: {e}");
        }
    });
    let (shutdown, _) = tokio::sync::oneshot::channel();

    TestCtx {
        client,
        _bg: conn_task,
        shutdown,
    }
}

struct ServerHandle {
    addr: std::net::SocketAddr,
    _bg: JoinHandle<()>,
    _shutdown: tokio::sync::oneshot::Sender<()>,
}

static SERVER: OnceCell<ServerHandle> = OnceCell::const_new();

async fn ensure_server() -> &'static ServerHandle {
    SERVER
        .get_or_init(|| async {
            let handler = Arc::new(mockgres::Mockgres::default());
            let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
            let addr = listener.local_addr().expect("local addr");
            let (shutdown, mut rx) = tokio::sync::oneshot::channel::<()>();
            let h = handler.clone();
            let bg = tokio::spawn(async move {
                loop {
                    tokio::select! {
                        _ = &mut rx => break,
                        Ok((socket, _peer)) = listener.accept() => {
                            let h2 = h.clone();
                            tokio::spawn(async move {
                                let _ = pgwire::tokio::process_socket(socket, None, h2).await;
                            });
                        }
                    }
                }
            });
            ServerHandle {
                addr,
                _bg: bg,
                _shutdown: shutdown,
            }
        })
        .await
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
