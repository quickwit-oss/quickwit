use std::net::SocketAddr;

use anyhow::Context;
use clap::Parser;
use quickwit_remote_api::{make_client_tls_config, run_server};
use tracing_subscriber::EnvFilter;
use tracing_subscriber::prelude::*;

#[derive(Parser)]
#[command(version, about, long_about = None)]
struct Args {
    /// Service to connect to
    target: String,

    /// Optional port-forwarding proxy to go through
    #[arg(short, long)]
    proxy_addr: Option<SocketAddr>,

    /// mTLS certificate to use
    #[arg(short, long)]
    cert: Option<String>,
    /// mTLS private key to use
    #[arg(short, long)]
    key: Option<String>,
}

#[tokio::main]
pub async fn main() -> anyhow::Result<()> {
    let args = Args::parse();

    let env_filter = std::env::var("RUST_LOG")
        .map(|_| EnvFilter::from_default_env())
        .or_else(|_| EnvFilter::try_new("INFO"))
        .context("failed to set up tracing env filter")?;

    tracing_subscriber::registry()
        .with(env_filter)
        .with(tracing_subscriber::fmt::layer())
        .try_init()
        .context("failed to register tracing subscriber")?;

    let tls_config = match (args.cert, args.key) {
        (Some(cert), Some(key)) => Some(make_client_tls_config(&cert, &key, &args.target)?),
        (None, None) => None,
        _ => anyhow::bail!("either both --cert and --key must be set, or neither"),
    };

    run_server(&args.target, args.proxy_addr, tls_config).await
}
