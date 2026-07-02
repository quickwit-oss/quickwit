// Copyright 2021-Present Datadog, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::net::SocketAddr;
use std::time::Duration;

use http_body_util::Empty;
use hyper::body::Bytes;
use hyper::client::conn::http2::SendRequest;
use hyper_util::rt::{TokioExecutor, TokioIo};
use quickwit_common::test_utils::wait_until_predicate;
use quickwit_config::service::QuickwitService;
use quickwit_config::{HumanDuration, TlsConfig};
use quickwit_serve::SearchRequestQueryString;

use crate::test_utils::ClusterSandboxBuilder;

const TLS_FIXTURES_DIR: &str = concat!(env!("CARGO_MANIFEST_DIR"), "/../resources/tests/tls");

/// The standard TLS config pointing at the shared fixtures: one-way TLS, no client-certificate
/// verification, 5-minute reload interval. Tests override individual fields as needed.
fn fixture_tls_config() -> TlsConfig {
    TlsConfig {
        cert_path: format!("{TLS_FIXTURES_DIR}/server.crt"),
        key_path: format!("{TLS_FIXTURES_DIR}/server.key"),
        ca_path: format!("{TLS_FIXTURES_DIR}/ca.crt"),
        expected_name: None,
        verify_client_cert: false,
        cert_reload_interval: HumanDuration::try_from("5m".to_string()).unwrap(),
    }
}

/// Reads a PEM certificate file and returns the DER bytes of its first (leaf) certificate.
fn leaf_cert_der(cert_path: &str) -> Vec<u8> {
    let cert_file = std::fs::File::open(cert_path).unwrap();
    let mut reader = std::io::BufReader::new(cert_file);
    let leaf = rustls_pemfile::certs(&mut reader)
        .next()
        .expect("PEM file should contain a certificate")
        .unwrap();
    leaf.as_ref().to_vec()
}

/// Opens a fresh TLS connection to `addr` (trusting `ca_path`), completes the handshake, and
/// returns the DER of the leaf certificate the server presented. A new connection is used on every
/// call so each observes the certificate currently served by the resolver.
async fn fetch_served_leaf_cert(addr: SocketAddr, ca_path: &str) -> Vec<u8> {
    // Build a no-client-auth client config trusting `ca_path`, reusing the production TLS builder.
    // Uses the process-wide default crypto provider, which the node installs (ring) at startup.
    let tls_config = TlsConfig {
        ca_path: ca_path.to_string(),
        ..fixture_tls_config()
    };
    let client_config = quickwit_transport::make_tls_client_config(&tls_config).unwrap();
    let tls_connector = tokio_rustls::TlsConnector::from(client_config);

    let tcp_stream = tokio::net::TcpStream::connect(addr).await.unwrap();
    // The server certificate's SAN includes `127.0.0.1`, so validate against the peer IP.
    let server_name = rustls::pki_types::ServerName::IpAddress(addr.ip().into());
    let tls_stream = tls_connector
        .connect(server_name, tcp_stream)
        .await
        .unwrap();
    let (_tcp_stream, connection) = tls_stream.get_ref();
    let peer_certs = connection
        .peer_certificates()
        .expect("the server must present a certificate");
    peer_certs[0].as_ref().to_vec()
}

#[tokio::test]
async fn test_tls_rest() {
    quickwit_common::setup_logging_for_tests();
    let mut sandbox_config = ClusterSandboxBuilder::default()
        .add_node(QuickwitService::supported_services())
        .build_config()
        .await;
    sandbox_config.node_configs[0].0.rest_config.tls_config = Some(fixture_tls_config());
    let sandbox = sandbox_config.start().await;
    let node_config = sandbox.node_configs.first().unwrap();
    let client = hyper_util::client::legacy::Client::builder(TokioExecutor::new())
        .pool_idle_timeout(Duration::from_secs(30))
        .http2_only(true)
        .build_http::<String>();
    let root_uri = format!("http://{}/", node_config.0.rest_config.listen_addr)
        .parse::<hyper::Uri>()
        .unwrap();
    client
        .get(root_uri.clone())
        .await
        .expect_err("non tls connection should fail");

    assert_eq!(
        sandbox
            .rest_client(QuickwitService::Indexer)
            .indexes()
            .list()
            .await
            .unwrap()
            .len(),
        0
    );

    sandbox.shutdown().await.unwrap();
}

#[tokio::test]
async fn test_tls_grpc() {
    quickwit_common::setup_logging_for_tests();
    let mut sandbox_config = ClusterSandboxBuilder::default()
        .add_node([QuickwitService::Searcher])
        .add_node([QuickwitService::Metastore])
        .add_node([QuickwitService::Indexer])
        .add_node([QuickwitService::ControlPlane])
        .add_node([QuickwitService::Janitor])
        .build_config()
        .await;

    for node in &mut sandbox_config.node_configs {
        // Enable mTLS on the node-to-node gRPC transport. Each node reuses the server cert as its
        // own client identity (the cert carries the `clientAuth` EKU), so this exercises both the
        // hot-reloadable server resolver and the hot-reloadable client identity introduced for
        // gRPC.
        node.0.grpc_config.tls_config = Some(TlsConfig {
            expected_name: Some("quickwit.local".to_string()),
            verify_client_cert: true,
            ..fixture_tls_config()
        });
    }
    let sandbox = sandbox_config.start().await;

    // TODO connect to grpc port and verify it refuses non-tls connection

    sandbox
        .rest_client(QuickwitService::Indexer)
        .indexes()
        .create(
            r#"
            version: 0.8
            index_id: my-new-multi-node-index
            doc_mapping:
              field_mappings:
              - name: body
                type: text
            indexing_settings:
              commit_timeout_secs: 1
            "#,
            quickwit_config::ConfigFormat::Yaml,
            false,
        )
        .await
        .unwrap();

    assert!(
        sandbox
            .rest_client(QuickwitService::Indexer)
            .node_health()
            .is_live()
            .await
            .unwrap()
    );

    // Assert that at least 1 indexing pipelines is successfully started
    sandbox.wait_for_indexing_pipelines(1).await.unwrap();

    // Check that search is working
    let search_response_empty = sandbox
        .rest_client(QuickwitService::Searcher)
        .search(
            "my-new-multi-node-index",
            SearchRequestQueryString {
                query: "body:bar".to_string(),
                ..Default::default()
            },
        )
        .await
        .unwrap();
    assert_eq!(search_response_empty.num_hits, 0);

    sandbox.shutdown().await.unwrap();
}

#[tokio::test]
async fn test_mtls_rest() {
    quickwit_common::setup_logging_for_tests();
    let mut sandbox_config = ClusterSandboxBuilder::default()
        .add_node(QuickwitService::supported_services())
        .build_config()
        .await;
    // Reuse the server cert/key as the client identity — it is signed by the same CA, so the
    // server's WebPkiClientVerifier will accept it.
    sandbox_config.node_configs[0].0.rest_config.tls_config = Some(TlsConfig {
        verify_client_cert: true,
        ..fixture_tls_config()
    });
    let sandbox = sandbox_config.start().await;
    let listen_addr = sandbox.node_configs[0].0.rest_config.listen_addr;

    let ca_bytes = std::fs::read(format!("{TLS_FIXTURES_DIR}/ca.crt")).unwrap();
    let ca_cert = reqwest::tls::Certificate::from_pem(&ca_bytes).unwrap();

    // reqwest::Identity::from_pem expects a single buffer with key + cert concatenated.
    let mut identity_pem = std::fs::read(format!("{TLS_FIXTURES_DIR}/server.key")).unwrap();
    identity_pem.extend(std::fs::read(format!("{TLS_FIXTURES_DIR}/server.crt")).unwrap());
    let client_identity = reqwest::tls::Identity::from_pem(&identity_pem).unwrap();

    // A client with no certificate should be rejected during the TLS handshake.
    let client_no_cert = reqwest::Client::builder()
        .add_root_certificate(ca_cert.clone())
        .build()
        .unwrap();
    let url = format!("https://{listen_addr}/api/v1/indexes");
    client_no_cert
        .get(&url)
        .send()
        .await
        .expect_err("connection without client certificate should fail");

    // A client presenting the client certificate signed by the trusted CA should succeed.
    let client_with_cert = reqwest::Client::builder()
        .add_root_certificate(ca_cert)
        .identity(client_identity)
        .build()
        .unwrap();
    let resp = client_with_cert
        .get(&url)
        .send()
        .await
        .expect("connection with valid client certificate should succeed");
    assert!(resp.status().is_success());

    sandbox.shutdown().await.unwrap();
}

#[tokio::test]
async fn test_health_check_server_plaintext_with_mtls_rest() {
    quickwit_common::setup_logging_for_tests();
    let mut sandbox_config = ClusterSandboxBuilder::default()
        .add_node(QuickwitService::supported_services())
        .enable_health_check()
        .build_config()
        .await;
    // Put the main REST API behind mTLS, while keeping the health server plaintext.
    sandbox_config.node_configs[0].0.rest_config.tls_config = Some(TlsConfig {
        verify_client_cert: true,
        ..fixture_tls_config()
    });
    let sandbox = sandbox_config.start().await;

    let rest_addr = sandbox.node_configs[0].0.rest_config.listen_addr;
    let health_addr = sandbox.node_configs[0]
        .0
        .health_config
        .as_ref()
        .expect("health config should be set")
        .listen_addr;

    // The plaintext health server answers liveness and readiness probes without any TLS.
    let livez_response = reqwest::get(format!("http://{health_addr}/health/livez"))
        .await
        .expect("plaintext liveness probe should connect");
    assert!(livez_response.status().is_success());
    let readyz_response = reqwest::get(format!("http://{health_addr}/health/readyz"))
        .await
        .expect("plaintext readiness probe should connect");
    assert!(readyz_response.status().is_success());

    // The same plaintext probe against the mTLS-protected REST port must fail: that port speaks
    // TLS and requires a client certificate, so a plaintext request cannot succeed.
    reqwest::get(format!("http://{rest_addr}/health/livez"))
        .await
        .expect_err("plaintext request to the mTLS REST port should fail");

    sandbox.shutdown().await.unwrap();
}

/// Opens a long-lived HTTP/2 connection to `addr` over TLS, trusting `ca_path`. Returns the DER of
/// the leaf certificate the server presented, a `SendRequest` handle that must be kept alive to
/// keep the connection open, and a handle to the driving task that resolves once the server closes
/// the connection.
async fn open_http2_connection(
    addr: SocketAddr,
    ca_path: &str,
) -> (
    Vec<u8>,
    SendRequest<Empty<Bytes>>,
    tokio::task::JoinHandle<()>,
) {
    let tls_config = TlsConfig {
        ca_path: ca_path.to_string(),
        ..fixture_tls_config()
    };
    let client_config = quickwit_transport::make_tls_client_config(&tls_config).unwrap();
    let tls_connector = tokio_rustls::TlsConnector::from(client_config);
    let tcp_stream = tokio::net::TcpStream::connect(addr).await.unwrap();
    let server_name = rustls::pki_types::ServerName::IpAddress(addr.ip().into());
    let tls_stream = tls_connector
        .connect(server_name, tcp_stream)
        .await
        .unwrap();

    let leaf_cert_der = {
        let (_tcp_stream, connection) = tls_stream.get_ref();
        let peer_certs = connection
            .peer_certificates()
            .expect("the server must present a certificate");
        peer_certs[0].as_ref().to_vec()
    };

    let (send_request, connection) = hyper::client::conn::http2::handshake::<_, _, Empty<Bytes>>(
        TokioExecutor::new(),
        TokioIo::new(tls_stream),
    )
    .await
    .expect("HTTP/2 handshake should succeed");
    // The connection future resolves when the server closes the connection (e.g. after the max
    // connection age elapses), even though this client never initiates a disconnect.
    let connection_handle = tokio::spawn(async move {
        let _ = connection.await;
    });
    (leaf_cert_der, send_request, connection_handle)
}

#[tokio::test]
async fn test_tls_rest_max_connection_age() {
    quickwit_common::setup_logging_for_tests();
    // The node reads its certificate from a temp directory we can mutate to simulate a rotation.
    let temp_dir = tempfile::tempdir().unwrap();
    let cert_path = temp_dir.path().join("server.crt");
    let key_path = temp_dir.path().join("server.key");
    std::fs::copy(format!("{TLS_FIXTURES_DIR}/server.crt"), &cert_path).unwrap();
    std::fs::copy(format!("{TLS_FIXTURES_DIR}/server.key"), &key_path).unwrap();
    let ca_path = format!("{TLS_FIXTURES_DIR}/ca.crt");

    let mut sandbox_config = ClusterSandboxBuilder::default()
        .add_node(QuickwitService::supported_services())
        .build_config()
        .await;
    sandbox_config.node_configs[0].0.rest_config.tls_config = Some(TlsConfig {
        cert_path: cert_path.to_str().unwrap().to_string(),
        key_path: key_path.to_str().unwrap().to_string(),
        ca_path: ca_path.clone(),
        // Long interval: only `reload_tls_cert()` reloads the cert here, never the periodic poll.
        cert_reload_interval: HumanDuration::try_from("1h".to_string()).unwrap(),
        ..fixture_tls_config()
    });
    // Short max connection age: the server must send a GOAWAY and close a long-lived connection
    // within this window, forcing the client to reconnect and re-handshake with the current cert.
    sandbox_config.node_configs[0]
        .0
        .rest_config
        .max_connection_age = Some(HumanDuration::try_from("2s".to_string()).unwrap());
    let sandbox = sandbox_config.start().await;
    let rest_addr = sandbox.node_configs[0].0.rest_config.listen_addr;

    // Open a long-lived connection and record the certificate it negotiated.
    let (served_before, _send_request, connection_handle) =
        open_http2_connection(rest_addr, &ca_path).await;
    let server1_der = leaf_cert_der(&format!("{TLS_FIXTURES_DIR}/server.crt"));
    assert_eq!(served_before, server1_der);

    // Rotate the certificate on disk and reload; the long-lived connection is still on the old one.
    let server2_der = leaf_cert_der(&format!("{TLS_FIXTURES_DIR}/server2.crt"));
    assert_ne!(server1_der, server2_der);
    std::fs::copy(format!("{TLS_FIXTURES_DIR}/server2.crt"), &cert_path).unwrap();
    std::fs::copy(format!("{TLS_FIXTURES_DIR}/server2.key"), &key_path).unwrap();
    quickwit_serve::reload_tls_cert();

    // The server closes the long-lived connection on its own once the max connection age elapses.
    // An idle HTTP/2 connection without a max age would otherwise stay open indefinitely, so a
    // bounded close demonstrates the feature.
    tokio::time::timeout(Duration::from_secs(10), connection_handle)
        .await
        .expect("server should close the connection within the max connection age")
        .expect("connection task should not panic");

    // After reconnecting, the server presents the rotated certificate.
    let (served_after, _send_request, _connection_handle) =
        open_http2_connection(rest_addr, &ca_path).await;
    assert_eq!(served_after, server2_der);

    sandbox.shutdown().await.unwrap();
}

#[tokio::test]
async fn test_tls_grpc_max_connection_age() {
    quickwit_common::setup_logging_for_tests();
    let ca_path = format!("{TLS_FIXTURES_DIR}/ca.crt");

    let mut sandbox_config = ClusterSandboxBuilder::default()
        .add_node(QuickwitService::supported_services())
        .build_config()
        .await;
    // One-way TLS (no client-cert verification) so the bare HTTP/2 probe below, which presents no
    // client identity, can complete the handshake. The max connection age behavior is independent
    // of mTLS.
    sandbox_config.node_configs[0].0.grpc_config.tls_config = Some(fixture_tls_config());
    sandbox_config.node_configs[0]
        .0
        .grpc_config
        .max_connection_age = Some(HumanDuration::try_from("2s".to_string()).unwrap());
    let sandbox = sandbox_config.start().await;
    let grpc_addr = sandbox.node_configs[0].0.grpc_listen_addr;

    // The gRPC server (tonic, `h2` only) must close a long-lived connection once the max connection
    // age elapses.
    let (_served, _send_request, connection_handle) =
        open_http2_connection(grpc_addr, &ca_path).await;
    tokio::time::timeout(Duration::from_secs(10), connection_handle)
        .await
        .expect("gRPC server should close the connection within the max connection age")
        .expect("connection task should not panic");

    sandbox.shutdown().await.unwrap();
}

#[tokio::test]
async fn test_tls_rest_cert_hot_reload() {
    quickwit_common::setup_logging_for_tests();
    // The node reads its certificate from a temp directory we can mutate to simulate a rotation.
    let temp_dir = tempfile::tempdir().unwrap();
    let cert_path = temp_dir.path().join("server.crt");
    let key_path = temp_dir.path().join("server.key");
    std::fs::copy(format!("{TLS_FIXTURES_DIR}/server.crt"), &cert_path).unwrap();
    std::fs::copy(format!("{TLS_FIXTURES_DIR}/server.key"), &key_path).unwrap();
    let ca_path = format!("{TLS_FIXTURES_DIR}/ca.crt");

    let mut sandbox_config = ClusterSandboxBuilder::default()
        .add_node(QuickwitService::supported_services())
        .build_config()
        .await;
    sandbox_config.node_configs[0].0.rest_config.tls_config = Some(TlsConfig {
        cert_path: cert_path.to_str().unwrap().to_string(),
        key_path: key_path.to_str().unwrap().to_string(),
        ca_path: ca_path.clone(),
        // Long interval: the only reload trigger exercised here is `reload_tls_cert()`, so the
        // assertion below cannot be satisfied by the periodic poll instead.
        cert_reload_interval: HumanDuration::try_from("1h".to_string()).unwrap(),
        ..fixture_tls_config()
    });
    let sandbox = sandbox_config.start().await;
    let rest_addr = sandbox.node_configs[0].0.rest_config.listen_addr;

    // Initially the server presents the first certificate.
    let served_before = fetch_served_leaf_cert(rest_addr, &ca_path).await;
    let server1_der = leaf_cert_der(&format!("{TLS_FIXTURES_DIR}/server.crt"));
    assert_eq!(served_before, server1_der);

    // Rotate the certificate on disk to a second, distinct certificate signed by the same CA.
    let server2_der = leaf_cert_der(&format!("{TLS_FIXTURES_DIR}/server2.crt"));
    assert_ne!(server1_der, server2_der);
    std::fs::copy(format!("{TLS_FIXTURES_DIR}/server2.crt"), &cert_path).unwrap();
    std::fs::copy(format!("{TLS_FIXTURES_DIR}/server2.key"), &key_path).unwrap();

    // Trigger an immediate reload (as `SIGHUP` would) and wait for new connections to pick up the
    // rotated certificate. In-flight reload is asynchronous, hence the bounded poll.
    quickwit_serve::reload_tls_cert();

    wait_until_predicate(
        || async {
            let served = fetch_served_leaf_cert(rest_addr, &ca_path).await;
            assert!(
                served == server1_der || served == server2_der,
                "server should serve either the old or the new certificate, never anything else"
            );
            served == server2_der
        },
        Duration::from_secs(10),
        Duration::from_millis(100),
    )
    .await
    .expect("timed out waiting for the rotated certificate to be served");

    sandbox.shutdown().await.unwrap();
}
