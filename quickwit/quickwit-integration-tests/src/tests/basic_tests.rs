// Copyright (C) 2024 Quickwit, Inc.
//
// Quickwit is offered under the AGPL v3.0 and as commercial software.
// For commercial licensing, contact us at hello@quickwit.io.
//
// AGPL:
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as
// published by the Free Software Foundation, either version 3 of the
// License, or (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Affero General Public License for more details.
//
// You should have received a copy of the GNU Affero General Public License
// along with this program. If not, see <http://www.gnu.org/licenses/>.

use std::time::Duration;

use hyper::{Body, Method, Request, StatusCode};
use quickwit_config::service::QuickwitService;
use quickwit_serve::SearchRequestQueryString;

use crate::test_utils::ClusterSandboxBuilder;

#[tokio::test]
async fn test_ui_redirect_on_get() {
    quickwit_common::setup_logging_for_tests();
    let sandbox = ClusterSandboxBuilder::build_and_start_standalone().await;
    let node_config = sandbox.node_configs.first().unwrap();
    let client = hyper::Client::builder()
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
        .body(Body::from("{}"))
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

    assert!(sandbox
        .rest_client(QuickwitService::Indexer)
        .node_health()
        .is_live()
        .await
        .unwrap());

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
