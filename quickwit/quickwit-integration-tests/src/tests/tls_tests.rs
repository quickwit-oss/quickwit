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

use std::time::Duration;

use hyper_util::rt::TokioExecutor;
use quickwit_config::service::QuickwitService;
use quickwit_serve::SearchRequestQueryString;

use crate::test_utils::ClusterSandboxBuilder;

#[tokio::test]
async fn test_tls_rest() {
    quickwit_common::setup_logging_for_tests();
    let mut sandbox_config = ClusterSandboxBuilder::default()
        .add_node(QuickwitService::supported_services())
        .build_config()
        .await;
    sandbox_config.node_configs[0].0.rest_config.tls = Some(quickwit_config::TlsConfig {
        cert_path: concat!(env!("CARGO_MANIFEST_DIR"), "/test_data/server.crt").to_string(),
        key_path: concat!(env!("CARGO_MANIFEST_DIR"), "/test_data/server.key").to_string(),
        ca_path: concat!(env!("CARGO_MANIFEST_DIR"), "/test_data/ca.crt").to_string(),
        expected_name: None,
        validate_client: false,
    });
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
        node.0.rest_config.tls = Some(quickwit_config::TlsConfig {
            cert_path: concat!(env!("CARGO_MANIFEST_DIR"), "/test_data/server.crt").to_string(),
            key_path: concat!(env!("CARGO_MANIFEST_DIR"), "/test_data/server.key").to_string(),
            ca_path: concat!(env!("CARGO_MANIFEST_DIR"), "/test_data/ca.crt").to_string(),
            expected_name: Some("quickwit.local".to_string()),
            validate_client: false,
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
    sandbox_config.node_configs[0].0.rest_config.tls = Some(quickwit_config::TlsConfig {
        cert_path: concat!(env!("CARGO_MANIFEST_DIR"), "/test_data/server.crt").to_string(),
        key_path: concat!(env!("CARGO_MANIFEST_DIR"), "/test_data/server.key").to_string(),
        ca_path: concat!(env!("CARGO_MANIFEST_DIR"), "/test_data/ca.crt").to_string(),
        expected_name: None,
        validate_client: true,
    });
    let sandbox = sandbox_config.start().await;
    let listen_addr = sandbox.node_configs[0].0.rest_config.listen_addr;

    let ca_bytes = std::fs::read(concat!(env!("CARGO_MANIFEST_DIR"), "/test_data/ca.crt")).unwrap();
    let ca_cert = reqwest::tls::Certificate::from_pem(&ca_bytes).unwrap();

    // reqwest::Identity::from_pem expects a single buffer with key + cert concatenated.
    let mut identity_pem =
        std::fs::read(concat!(env!("CARGO_MANIFEST_DIR"), "/test_data/server.key")).unwrap();
    identity_pem.extend(
        std::fs::read(concat!(env!("CARGO_MANIFEST_DIR"), "/test_data/server.crt")).unwrap(),
    );
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
