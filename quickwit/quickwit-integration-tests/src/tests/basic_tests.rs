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

use hyper::{Method, Request, StatusCode};
use hyper_util::rt::TokioExecutor;
use quickwit_config::service::QuickwitService;
use quickwit_serve::SearchRequestQueryString;

use crate::test_utils::ClusterSandboxBuilder;

#[tokio::test]
async fn test_ui_redirect_on_get() {
    quickwit_common::setup_logging_for_tests();
    let sandbox = ClusterSandboxBuilder::build_and_start_standalone().await;
    let node_config = sandbox.node_configs.first().unwrap();
    let client = hyper_util::client::legacy::Client::builder(TokioExecutor::new())
        .pool_idle_timeout(Duration::from_secs(30))
        .http2_only(true)
        .build_http();
    let root_uri = format!("http://{}/", node_config.0.rest_config.listen_addr)
        .parse::<hyper::Uri>()
        .unwrap();
    let response = client.get(root_uri.clone()).await.unwrap();
    assert_eq!(response.status(), StatusCode::MOVED_PERMANENTLY);
    let post_request = Request::builder()
        .uri(root_uri)
        .method(Method::POST)
        .body("{}".to_string())
        .unwrap();
    let response = client.request(post_request).await.unwrap();
    assert_eq!(response.status(), StatusCode::METHOD_NOT_ALLOWED);
    sandbox.shutdown().await.unwrap();
}

#[tokio::test]
async fn test_standalone_server() {
    quickwit_common::setup_logging_for_tests();
    let sandbox = ClusterSandboxBuilder::build_and_start_standalone().await;
    {
        // The indexing service should be running.
        let counters = sandbox
            .rest_client(QuickwitService::Indexer)
            .node_stats()
            .indexing()
            .await
            .unwrap();
        assert_eq!(counters.num_running_pipelines, 0);
    }

    {
        // Create an dynamic index.
        sandbox
            .rest_client(QuickwitService::Indexer)
            .indexes()
            .create(
                r#"
                version: 0.8
                index_id: my-new-index
                doc_mapping:
                  field_mappings:
                  - name: body
                    type: text
                "#,
                quickwit_config::ConfigFormat::Yaml,
                false,
            )
            .await
            .unwrap();

        // Index should be searchable
        assert_eq!(
            sandbox
                .rest_client(QuickwitService::Indexer)
                .search(
                    "my-new-index",
                    SearchRequestQueryString {
                        query: "body:test".to_string(),
                        max_hits: 10,
                        ..Default::default()
                    },
                )
                .await
                .unwrap()
                .num_hits,
            0
        );
        sandbox.wait_for_indexing_pipelines(1).await.unwrap();
    }
    sandbox.shutdown().await.unwrap();
}

#[tokio::test]
async fn test_multi_nodes_cluster() {
    quickwit_common::setup_logging_for_tests();
    let sandbox = ClusterSandboxBuilder::default()
        .add_node([QuickwitService::Searcher])
        .add_node([QuickwitService::Metastore])
        .add_node([QuickwitService::Indexer])
        .add_node([QuickwitService::ControlPlane])
        .add_node([QuickwitService::Janitor])
        .build_and_start()
        .await;

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
