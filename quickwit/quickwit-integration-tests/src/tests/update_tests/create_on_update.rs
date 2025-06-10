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
use quickwit_rest_client::rest_client::CommitType;
use serde_json::json;

use super::assert_hits_unordered;
use crate::ingest_json;
use crate::test_utils::{ClusterSandboxBuilder, ingest};

#[tokio::test]
async fn test_update_missing_no_create() {
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

    assert!(
        sandbox
            .rest_client(QuickwitService::Indexer)
            .node_health()
            .is_live()
            .await
            .unwrap()
    );

    let status_code = sandbox
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
            false,
        )
        .await
        .unwrap_err()
        .status_code()
        .unwrap();
    assert_eq!(status_code, 404);

    sandbox.shutdown().await.unwrap();
}

#[tokio::test]
async fn test_update_missing_create() {
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

    assert!(
        sandbox
            .rest_client(QuickwitService::Indexer)
            .node_health()
            .is_live()
            .await
            .unwrap()
    );

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
            true,
        )
        .await
        .unwrap();

    sandbox.shutdown().await.unwrap();
}

#[tokio::test]
async fn test_update_create_existing_doesnt_clear() {
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
    assert!(
        sandbox
            .rest_client(QuickwitService::Indexer)
            .node_health()
            .is_live()
            .await
            .unwrap()
    );

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

    // No hit because `default_search_fields` only covers the `title` field
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
            true,
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
