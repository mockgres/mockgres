use std::{net::SocketAddr, sync::Arc};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = match parse_args()? {
        CliCommand::Run(args) => args,
        CliCommand::Help => {
            print_help();
            return Ok(());
        }
        CliCommand::Version => {
            print_version();
            return Ok(());
        }
    };
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

enum CliCommand {
    Run(CliArgs),
    Help,
    Version,
}

fn parse_args() -> anyhow::Result<CliCommand> {
    parse_args_from(std::env::args().skip(1))
}

fn parse_args_from(args: impl IntoIterator<Item = String>) -> anyhow::Result<CliCommand> {
    let raw: Vec<String> = args.into_iter().collect();
    if raw.iter().any(|arg| arg == "--help" || arg == "-h") {
        return Ok(CliCommand::Help);
    }
    if raw.iter().any(|arg| arg == "--version" || arg == "-V") {
        return Ok(CliCommand::Version);
    }

    let mut parsed = CliArgs::default();
    let mut args = raw.into_iter();

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

    Ok(CliCommand::Run(parsed))
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

fn print_help() {
    println!(
        "\
mockgres {}

Usage:
  mockgres [--addr <host:port>]
  mockgres [--host <host>] [--port <port>]
  mockgres <host:port>

Options:
  --addr <host:port>  Listen address (same as positional host:port)
  --host <host>       Listen host (default: 127.0.0.1)
  --port <port>       Listen port (default: 6543)
  -h, --help          Show this help and exit
  -V, --version       Show version and exit

Environment:
  MOCKGRES_ADDR       Listen address when no CLI address options are provided",
        env!("CARGO_PKG_VERSION")
    );
}

fn print_version() {
    println!("mockgres {}", env!("CARGO_PKG_VERSION"));
}

#[cfg(test)]
mod tests {
    use super::{CliCommand, parse_args_from, resolve_addr};

    #[test]
    fn parse_help_flags() {
        assert!(matches!(
            parse_args_from(vec!["--help".to_string()]).expect("parse args"),
            CliCommand::Help
        ));
        assert!(matches!(
            parse_args_from(vec!["-h".to_string()]).expect("parse args"),
            CliCommand::Help
        ));
    }

    #[test]
    fn parse_version_flags() {
        assert!(matches!(
            parse_args_from(vec!["--version".to_string()]).expect("parse args"),
            CliCommand::Version
        ));
        assert!(matches!(
            parse_args_from(vec!["-V".to_string()]).expect("parse args"),
            CliCommand::Version
        ));
    }

    #[test]
    fn parse_and_resolve_host_and_port() {
        let cmd = parse_args_from(vec![
            "--host".to_string(),
            "127.0.0.1".to_string(),
            "--port".to_string(),
            "6543".to_string(),
        ])
        .expect("parse args");
        let CliCommand::Run(args) = cmd else {
            panic!("expected run command");
        };
        let addr = resolve_addr(args).expect("resolve addr");
        assert_eq!(addr.to_string(), "127.0.0.1:6543");
    }
}
