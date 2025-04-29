mod grpc_client;
mod rest;

use std::net::SocketAddr;
use std::sync::Arc;

use grpc_client::CloudPremRootSearchService;
use rest::rest_server;
use tokio::net::TcpListener;
use tonic::transport::{Certificate, ClientTlsConfig, Identity};

pub fn make_client_tls_config(
    cert_path: &str,
    key_path: &str,
    expected_name: &str,
) -> anyhow::Result<ClientTlsConfig> {
    let pem = std::fs::read_to_string("/opt/homebrew/etc/ca-certificates/cert.pem")?;
    let ca = Certificate::from_pem(pem);

    let cert = std::fs::read_to_string(cert_path)?;
    let key = std::fs::read_to_string(key_path)?;
    let identity = Identity::from_pem(cert, key);

    let expected_name_no_port = expected_name.split(':').next().unwrap();

    let tls = ClientTlsConfig::new()
        // tonic 0.12 comes with .with_native_roots()
        .ca_certificate(ca)
        .identity(identity)
        .domain_name(expected_name_no_port);

    Ok(tls)
}

pub async fn run_server(
    target: &str,
    proxy_addr: Option<SocketAddr>,
    tls_config: Option<ClientTlsConfig>,
) -> anyhow::Result<()> {
    let listener = TcpListener::bind("127.0.0.1:7380").await?;

    let search_service = CloudPremRootSearchService::new(target, proxy_addr, tls_config).await?;

    rest_server(Arc::new(search_service), listener).await
}
