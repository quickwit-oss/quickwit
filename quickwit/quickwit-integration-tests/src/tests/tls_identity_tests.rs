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

use quickwit_config::service::QuickwitService;
use quickwit_config::{AllowedClientIdentities, HumanDuration, TlsConfig};
use quickwit_proto::metastore::{
    ListIndexesMetadataRequest, MetastoreService, MetastoreServiceClient,
};
use quickwit_transport::ChannelFactory;

use crate::test_utils::ClusterSandboxBuilder;

#[tokio::test]
async fn test_mtls_client_identity_allowlist_rest_and_grpc() {
    quickwit_common::setup_logging_for_tests();
    let fixtures = concat!(env!("CARGO_MANIFEST_DIR"), "/../resources/tests/tls");
    let mut config = ClusterSandboxBuilder::default()
        .add_node(QuickwitService::default_services())
        .build_config()
        .await;
    let tls_config = TlsConfig {
        cert_path: format!("{fixtures}/server.crt"),
        key_path: format!("{fixtures}/server.key"),
        ca_path: format!("{fixtures}/ca.crt"),
        expected_name: Some("quickwit.local".to_string()),
        verify_client_cert: true,
        allowed_client_identities: Some(AllowedClientIdentities {
            // This is the CN used in the generated server.crt test certificate.
            common_names: vec!["qw test certificate".to_string()],
            ..Default::default()
        }),
        cert_poll_interval: HumanDuration::try_from("5m".to_string()).unwrap(),
    };
    config.node_configs[0].0.rest_config.tls_config = Some(tls_config.clone());
    config.node_configs[0].0.grpc_config.tls_config = Some(tls_config);
    let sandbox = config.start().await;
    let node_config = &sandbox.node_configs[0].0;
    let ca = reqwest::Certificate::from_pem(&std::fs::read(format!("{fixtures}/ca.crt")).unwrap())
        .unwrap();

    // Both fixture certificates are valid and issued by the same CA.
    // server.crt has CN "qw test certificate"; server2.crt has CN "qw test certificate 2".
    // Only the first CN is allowed.
    for (certificate_name, allowed) in [("server", true), ("server2", false)] {
        let cert_path = format!("{fixtures}/{certificate_name}.crt");
        let key_path = format!("{fixtures}/{certificate_name}.key");
        let mut pem = std::fs::read(&key_path).unwrap();
        pem.extend(std::fs::read(&cert_path).unwrap());
        let client = reqwest::Client::builder()
            .add_root_certificate(ca.clone())
            .identity(reqwest::Identity::from_pem(&pem).unwrap())
            .timeout(Duration::from_secs(5))
            .build()
            .unwrap();
        let rest_response = client
            .get(format!(
                "https://{}/api/v1/indexes",
                node_config.rest_config.listen_addr
            ))
            .send()
            .await;
        if allowed {
            assert!(rest_response.unwrap().status().is_success());
        } else {
            assert!(
                rest_response.is_err(),
                "REST must reject the unauthorized identity at TLS"
            );
        }

        let mut grpc_config = node_config.grpc_config.clone();
        let client_tls = grpc_config.tls_config.as_mut().unwrap();
        client_tls.cert_path = cert_path;
        client_tls.key_path = key_path;
        let channel = ChannelFactory::for_grpc(&grpc_config)
            .unwrap()
            .make_channel(node_config.grpc_listen_addr)
            .await;
        let metastore = MetastoreServiceClient::from_channel(
            node_config.grpc_listen_addr,
            channel,
            grpc_config.max_message_size,
            None,
        );
        let grpc_response = tokio::time::timeout(
            Duration::from_secs(5),
            metastore.list_indexes_metadata(ListIndexesMetadataRequest::all()),
        )
        .await
        .expect("gRPC request should complete or fail without hanging");
        assert_eq!(
            grpc_response.is_ok(),
            allowed,
            "gRPC authorization for {certificate_name}"
        );
    }
    sandbox.shutdown().await.unwrap();
}
