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

use std::collections::HashSet;
use std::time::Duration;

use hyper::body::Bytes;
use quickwit_config::service::QuickwitService;
use quickwit_config::ConfigFormat;
use quickwit_rest_client::rest_client::{CommitType, UpdateConfigField};
use quickwit_serve::SearchRequestQueryString;
use serde_json::json;

use crate::ingest_json;
use crate::test_utils::{ingest_with_retry, ClusterSandbox};

#[tokio::test]
async fn test_update_on_multi_nodes_cluster() {
    quickwit_common::setup_logging_for_tests();
    let nodes_services = vec![
        HashSet::from_iter([QuickwitService::Searcher]),
        HashSet::from_iter([QuickwitService::Metastore]),
        HashSet::from_iter([QuickwitService::Indexer]),
        HashSet::from_iter([QuickwitService::ControlPlane]),
        HashSet::from_iter([QuickwitService::Janitor]),
    ];
    let sandbox = ClusterSandbox::start_cluster_nodes(&nodes_services)
        .await
        .unwrap();
    sandbox.wait_for_cluster_num_ready_nodes(5).await.unwrap();

    {
        // Wait for indexer to fully start.
        // The starting time is a bit long for a cluster.
        tokio::time::sleep(Duration::from_secs(3)).await;
        let indexing_service_counters = sandbox
            .indexer_rest_client
            .node_stats()
            .indexing()
            .await
            .unwrap();
        assert_eq!(indexing_service_counters.num_running_pipelines, 0);
    }

    // Create index
    sandbox
        .indexer_rest_client
        .indexes()
        .create(
            r#"
            version: 0.8
            index_id: my-updatable-index
            doc_mapping:
              field_mappings:
              - name: title
                type: text
              - name: body
                type: text
            indexing_settings:
              commit_timeout_secs: 1
            search_settings:
              default_search_fields: [title]
            "#,
            quickwit_config::ConfigFormat::Yaml,
            false,
        )
        .await
        .unwrap();
    assert!(sandbox
        .indexer_rest_client
        .node_health()
        .is_live()
        .await
        .unwrap());

    // Wait until indexing pipelines are started.
    sandbox.wait_for_indexing_pipelines(1).await.unwrap();

    // Check that ingest request send to searcher is forwarded to indexer and thus indexed.
    ingest_with_retry(
        &sandbox.searcher_rest_client,
        "my-updatable-index",
        ingest_json!({"title": "first", "body": "first record"}),
        CommitType::Auto,
    )
    .await
    .unwrap();
    // Wait until split is commited and search.
    tokio::time::sleep(Duration::from_secs(4)).await;
    // No hit because default_search_fields covers "title" only
    let search_response_no_hit = sandbox
        .searcher_rest_client
        .search(
            "my-updatable-index",
            SearchRequestQueryString {
                query: "record".to_string(),
                ..Default::default()
            },
        )
        .await
        .unwrap();
    assert_eq!(search_response_no_hit.num_hits, 0);
    // Update index to also search "body" by default, search should now have 1 hit
    sandbox
        .searcher_rest_client
        .indexes()
        .update(
            "my-updatable-index",
            UpdateConfigField::SearchSettings,
            Bytes::from(r#"{"default_search_fields":["title", "body"]}"#),
            ConfigFormat::Json,
        )
        .await
        .unwrap();
    let search_response_no_hit = sandbox
        .searcher_rest_client
        .search(
            "my-updatable-index",
            SearchRequestQueryString {
                query: "record".to_string(),
                ..Default::default()
            },
        )
        .await
        .unwrap();
    assert_eq!(search_response_no_hit.num_hits, 1);
    sandbox.shutdown().await.unwrap();
}
