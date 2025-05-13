mod grpc_client;
mod grpc_server;
mod rest;

use std::net::SocketAddr;
use std::sync::Arc;

use grpc_client::CloudPremRootSearchService;
use grpc_server::grpc_server;
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

pub fn get_mtls_header(cert_path: &str) -> anyhow::Result<String> {
    let cert = std::fs::read_to_string(cert_path)?;

    Ok(urlencoding::encode(&cert).to_string())
}

pub async fn run_server(
    target: &str,
    proxy_addr: Option<SocketAddr>,
    tls_config: Option<ClientTlsConfig>,
    mtls_header: Option<String>,
) -> anyhow::Result<()> {
    let rest_listener = TcpListener::bind("127.0.0.1:7380").await?;
    let grpc_listener = TcpListener::bind("127.0.0.1:7381").await?;

    let search_service = Arc::new(
        CloudPremRootSearchService::new(target, proxy_addr, tls_config, mtls_header).await?,
    );

    tracing::info!("client ready, server listening on 127.0.0.1:7380 and 127.0.0.1:7381");

    tokio::try_join!(
        rest_server(search_service.clone(), rest_listener),
        grpc_server(search_service, grpc_listener),
    )
    .map(|_| ())
}
