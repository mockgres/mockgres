use std::{net::SocketAddr, sync::Arc};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = parse_args()?;
    let addr = resolve_addr(args)?;

    println!("mockgres listening on {addr}");
    let handler = Arc::new(mockgres::Mockgres::default());

    handler.serve(addr).await?;

    Ok(())
}

#[derive(Default)]
struct CliArgs {
    addr: Option<String>,
    host: Option<String>,
    port: Option<u16>,
}

fn parse_args() -> anyhow::Result<CliArgs> {
    let mut parsed = CliArgs::default();
    let mut args = std::env::args().skip(1);

    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--addr" => {
                parsed.addr = Some(next_arg_value("--addr", &mut args)?);
            }
            "--host" => {
                parsed.host = Some(next_arg_value("--host", &mut args)?);
            }
            "--port" => {
                let raw = next_arg_value("--port", &mut args)?;
                let port = raw
                    .parse::<u16>()
                    .map_err(|_| anyhow::anyhow!("invalid port: {raw}"))?;
                parsed.port = Some(port);
            }
            _ => {
                if arg.starts_with("--") {
                    return Err(anyhow::anyhow!("unknown argument: {arg}"));
                }
                if parsed.addr.is_some() {
                    return Err(anyhow::anyhow!("unexpected extra argument: {arg}"));
                }
                parsed.addr = Some(arg);
            }
        }
    }

    if parsed.addr.is_some() && (parsed.host.is_some() || parsed.port.is_some()) {
        return Err(anyhow::anyhow!(
            "--addr cannot be used together with --host or --port"
        ));
    }

    Ok(parsed)
}

fn next_arg_value(
    flag: &'static str,
    args: &mut impl Iterator<Item = String>,
) -> anyhow::Result<String> {
    args.next()
        .ok_or_else(|| anyhow::anyhow!("missing value for {flag}"))
}

fn resolve_addr(args: CliArgs) -> anyhow::Result<SocketAddr> {
    let addr_str = if let Some(addr) = args.addr {
        addr
    } else if args.host.is_some() || args.port.is_some() {
        let host = args.host.unwrap_or_else(|| "127.0.0.1".to_string());
        let port = args.port.unwrap_or(6543);
        format!("{host}:{port}")
    } else if let Ok(addr) = std::env::var("MOCKGRES_ADDR") {
        addr
    } else {
        "127.0.0.1:6543".to_string()
    };

    addr_str
        .parse()
        .map_err(|_| anyhow::anyhow!("invalid listen address: {addr_str}"))
}
