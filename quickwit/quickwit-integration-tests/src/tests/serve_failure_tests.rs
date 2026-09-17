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

use std::collections::HashSet;
use std::net::SocketAddr;
use std::str::FromStr;
use std::time::Duration;

use quickwit_common::new_coolid;
use quickwit_common::runtimes::RuntimesConfig;
use quickwit_common::uri::Uri as QuickwitUri;
use quickwit_config::service::QuickwitService;
use quickwit_config::{HealthConfig, HumanDuration, NodeConfig, TlsConfig};
use quickwit_metastore::MetastoreResolver;
use quickwit_serve::serve_quickwit;
use quickwit_serve::tcp_listener::for_tests::TestTcpListenerResolver;
use quickwit_storage::StorageResolver;
use tokio::net::TcpListener;

async fn assert_listener_is_released(addr: SocketAddr) {
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            match TcpListener::bind(addr).await {
                Ok(listener) => {
                    drop(listener);
                    return;
                }
                Err(error) if error.kind() == std::io::ErrorKind::AddrInUse => {
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
                Err(error) => panic!("failed to bind listener on {addr}: {error}"),
            }
        }
    })
    .await
    .unwrap_or_else(|_| panic!("listener on {addr} was not released after server failure"));
}

#[derive(Debug, Clone, Copy)]
enum FailingServer {
    Grpc,
    Rest,
    HealthCheck,
}

#[tokio::test]
async fn test_serve_quickwit_returns_error_when_a_server_fails_to_start() {
    for failing_server in [
        FailingServer::Grpc,
        FailingServer::Rest,
        FailingServer::HealthCheck,
    ] {
        let temp_dir = tempfile::tempdir().unwrap();
        let unique_dir_name = new_coolid("test-dir");

        let any_port: SocketAddr = ([127, 0, 0, 1], 0u16).into();
        let grpc_listener = TcpListener::bind(any_port).await.unwrap();
        let rest_listener = TcpListener::bind(any_port).await.unwrap();
        let health_listener = TcpListener::bind(any_port).await.unwrap();
        let grpc_addr = grpc_listener.local_addr().unwrap();
        let rest_addr = rest_listener.local_addr().unwrap();
        let health_addr = health_listener.local_addr().unwrap();

        let mut node_config = NodeConfig::for_test_from_ports(rest_addr.port(), grpc_addr.port());
        node_config.enabled_services = HashSet::from_iter([QuickwitService::Metastore]);
        node_config.health_config = Some(HealthConfig {
            listen_addr: health_addr,
        });
        node_config.cluster_id = new_coolid("test-cluster");
        node_config.data_dir_path = temp_dir.path().to_path_buf();
        node_config.metastore_uri =
            QuickwitUri::from_str(&format!("ram:///{unique_dir_name}/metastore")).unwrap();
        node_config.default_index_root_uri =
            QuickwitUri::from_str(&format!("ram:///{unique_dir_name}/indexes")).unwrap();

        let tcp_listener_resolver = TestTcpListenerResolver::default();
        let expected_addr = match failing_server {
            FailingServer::Grpc => {
                tcp_listener_resolver.add_listener(rest_listener).await;
                tcp_listener_resolver.add_listener(health_listener).await;
                grpc_addr
            }
            FailingServer::Rest => {
                tcp_listener_resolver.add_listener(grpc_listener).await;
                tcp_listener_resolver.add_listener(health_listener).await;
                rest_addr
            }
            FailingServer::HealthCheck => {
                tcp_listener_resolver.add_listener(grpc_listener).await;
                tcp_listener_resolver.add_listener(rest_listener).await;
                health_addr
            }
        };

        let error = serve_quickwit(
            node_config,
            RuntimesConfig::light_for_tests(),
            MetastoreResolver::unconfigured(),
            StorageResolver::unconfigured(),
            tcp_listener_resolver,
            Box::pin(std::future::pending()),
            quickwit_serve::do_nothing_env_filter_reload_fn(),
        )
        .await
        .unwrap_err();

        assert!(
            format!("{error:?}").contains(&expected_addr.to_string()),
            "{failing_server:?} scenario should have failed on {expected_addr}, got: {error:?}"
        );
    }
}

#[tokio::test]
async fn test_serve_quickwit_shuts_down_sibling_servers_after_server_failure() {
    let temp_dir = tempfile::tempdir().unwrap();
    let unique_dir_name = new_coolid("test-dir");

    let any_port: SocketAddr = ([127, 0, 0, 1], 0u16).into();
    let grpc_listener = TcpListener::bind(any_port).await.unwrap();
    let rest_listener = TcpListener::bind(any_port).await.unwrap();
    let health_listener = TcpListener::bind(any_port).await.unwrap();
    let grpc_addr = grpc_listener.local_addr().unwrap();
    let rest_addr = rest_listener.local_addr().unwrap();
    let health_addr = health_listener.local_addr().unwrap();

    let mut node_config = NodeConfig::for_test_from_ports(rest_addr.port(), grpc_addr.port());
    node_config.enabled_services = HashSet::from_iter([QuickwitService::Metastore]);
    node_config.health_config = Some(HealthConfig {
        listen_addr: health_addr,
    });
    node_config.rest_config.tls_config = Some(TlsConfig {
        cert_path: temp_dir
            .path()
            .join("missing-cert.pem")
            .to_string_lossy()
            .into_owned(),
        key_path: temp_dir
            .path()
            .join("missing-key.pem")
            .to_string_lossy()
            .into_owned(),
        ca_path: String::new(),
        expected_name: None,
        verify_client_cert: false,
        cert_poll_interval: HumanDuration::try_from("5m".to_string()).unwrap(),
    });
    node_config.cluster_id = new_coolid("test-cluster");
    node_config.data_dir_path = temp_dir.path().to_path_buf();
    node_config.metastore_uri =
        QuickwitUri::from_str(&format!("ram:///{unique_dir_name}/metastore")).unwrap();
    node_config.default_index_root_uri =
        QuickwitUri::from_str(&format!("ram:///{unique_dir_name}/indexes")).unwrap();

    let tcp_listener_resolver = TestTcpListenerResolver::default();
    tcp_listener_resolver.add_listener(grpc_listener).await;
    tcp_listener_resolver.add_listener(rest_listener).await;
    tcp_listener_resolver.add_listener(health_listener).await;

    let error = serve_quickwit(
        node_config,
        RuntimesConfig::light_for_tests(),
        MetastoreResolver::unconfigured(),
        StorageResolver::unconfigured(),
        tcp_listener_resolver,
        Box::pin(std::future::pending()),
        quickwit_serve::do_nothing_env_filter_reload_fn(),
    )
    .await
    .unwrap_err();

    assert!(
        format!("{error:?}").contains("REST server failed"),
        "expected REST server failure, got: {error:?}"
    );
    assert_listener_is_released(grpc_addr).await;
    assert_listener_is_released(rest_addr).await;
    assert_listener_is_released(health_addr).await;
}
