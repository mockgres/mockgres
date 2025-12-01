use std::{net::SocketAddr, sync::Arc};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let addr_str = std::env::args()
        .nth(1)
        .or_else(|| std::env::var("MOCKGRES_ADDR").ok())
        .unwrap_or_else(|| "127.0.0.1:6543".to_string());

    let addr: SocketAddr = addr_str.parse().expect("invalid listen address");

    println!("mockgres listening on {addr}");
    let handler = Arc::new(mockgres::Mockgres::default());

    handler.serve(addr).await?;

    Ok(())
}
