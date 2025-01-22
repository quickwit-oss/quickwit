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

use quickwit_config::service::QuickwitService;
use quickwit_rest_client::rest_client::CommitType;
use serde_json::json;

use super::assert_hits_unordered;
use crate::ingest_json;
use crate::test_utils::{ingest, ClusterSandboxBuilder};

#[tokio::test]
async fn test_update_search_settings_on_multi_nodes_cluster() {
    quickwit_common::setup_logging_for_tests();
    let sandbox = ClusterSandboxBuilder::default()
        .add_node([QuickwitService::Searcher])
        .add_node([QuickwitService::Metastore])
        .add_node([QuickwitService::Indexer])
        .add_node([QuickwitService::ControlPlane])
        .add_node([QuickwitService::Janitor])
        .build_and_start()
        .await;

    {
        // Wait for indexer to fully start.
        // The starting time is a bit long for a cluster.
        tokio::time::sleep(Duration::from_secs(3)).await;
        let indexing_service_counters = sandbox
            .rest_client(QuickwitService::Indexer)
            .node_stats()
            .indexing()
            .await
            .unwrap();
        assert_eq!(indexing_service_counters.num_running_pipelines, 0);
    }

    // Create an index
    sandbox
        .rest_client(QuickwitService::Indexer)
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
        .rest_client(QuickwitService::Indexer)
        .node_health()
        .is_live()
        .await
        .unwrap());

    // Wait until indexing pipelines are started
    sandbox.wait_for_indexing_pipelines(1).await.unwrap();

    ingest(
        &sandbox.rest_client(QuickwitService::Indexer),
        "my-updatable-index",
        ingest_json!({"title": "first", "body": "first record"}),
        CommitType::Auto,
    )
    .await
    .unwrap();

    // Wait until split is committed
    tokio::time::sleep(Duration::from_secs(4)).await;

    // No hit because `default_search_fields`` only covers the `title` field
    assert_hits_unordered(&sandbox, "my-updatable-index", "record", Ok(&[])).await;

    // Update the index to also search `body` by default, the same search should
    // now have 1 hit
    sandbox
        .rest_client(QuickwitService::Indexer)
        .indexes()
        .update(
            "my-updatable-index",
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
              default_search_fields: [title, body]
            "#,
            quickwit_config::ConfigFormat::Yaml,
        )
        .await
        .unwrap();

    assert_hits_unordered(
        &sandbox,
        "my-updatable-index",
        "record",
        Ok(&[json!({"title": "first", "body": "first record"})]),
    )
    .await;

    sandbox.shutdown().await.unwrap();
}
