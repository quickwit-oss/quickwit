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

use quickwit_config::service::QuickwitService;
use quickwit_rest_client::rest_client::CommitType;
use quickwit_serve::SearchRequestQueryString;
use serde_json::{json, Value};

use crate::ingest_json;
use crate::test_utils::{ingest_with_retry, ClusterSandbox};

async fn assert_hit_count(
    sandbox: &ClusterSandbox,
    index_id: &str,
    query: &str,
    expected_hits: Result<u64, ()>,
) {
    let search_res = sandbox
        .searcher_rest_client
        .search(
            index_id,
            SearchRequestQueryString {
                query: query.to_string(),
                ..Default::default()
            },
        )
        .await;
    if let Ok(expected_hit_count) = expected_hits {
        let resp = search_res.unwrap();
        assert_eq!(resp.errors.len(), 0);
        assert_eq!(resp.num_hits, expected_hit_count);
    } else if let Ok(search_response) = search_res {
        assert!(search_response.errors.len() > 0);
    }
}

#[tokio::test]
async fn test_update_search_settings_on_multi_nodes_cluster() {
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
    // Wait until split is committed and search.
    tokio::time::sleep(Duration::from_secs(4)).await;
    // No hit because default_search_fields covers "title" only
    assert_hit_count(&sandbox, "my-updatable-index", "record", Ok(0)).await;
    // Update index to also search "body" by default, search should now have 1 hit
    sandbox
        .searcher_rest_client
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
    assert_hit_count(&sandbox, "my-updatable-index", "record", Ok(1)).await;
    sandbox.shutdown().await.unwrap();
}

/// Update the doc mapping between 2 local ingestion (separate indexing pipelines) and assert the
/// number of hits for the given query
async fn validate_search_across_doc_mapping_updates(
    index_id: &str,
    original_doc_mapping: Value,
    ingest_before_update: &[Value],
    updated_doc_mapping: Value,
    ingest_after_update: &[Value],
    query_and_expect: &[(&str, Result<u64, ()>)],
) {
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
            json!({
                "version": "0.8",
                "index_id": index_id,
                "doc_mapping": original_doc_mapping,
                "indexing_settings": {
                    "commit_timeout_secs": 1
                },
            })
            .to_string(),
            quickwit_config::ConfigFormat::Json,
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

    // We use local ingest to always pick up the latest doc mapping
    sandbox
        .local_ingest(index_id, ingest_before_update)
        .await
        .unwrap();

    // Update index to also search "body" by default, search should now have 1 hit
    sandbox
        .searcher_rest_client
        .indexes()
        .update(
            index_id,
            json!({
                "version": "0.8",
                "index_id": index_id,
                "doc_mapping": updated_doc_mapping,
                "indexing_settings": {
                    "commit_timeout_secs": 1,
                },
            })
            .to_string(),
            quickwit_config::ConfigFormat::Json,
        )
        .await
        .unwrap();

    sandbox
        .local_ingest(index_id, ingest_after_update)
        .await
        .unwrap();

    for (query, expected_hits) in query_and_expect {
        assert_hit_count(&sandbox, index_id, query, *expected_hits).await;
    }

    sandbox.shutdown().await.unwrap();
}

#[tokio::test]
async fn test_update_doc_mappings_text_to_u64() {
    let index_id = "update-text-to-u64";
    let original_doc_mappings = json!({
        "field_mappings": [
            {"name": "body", "type": "text"}
        ]
    });
    let ingest_before_update = &[json!({"body": "14"}), json!({"body": "15"})];
    let updated_doc_mappings = json!({
        "field_mappings": [
            {"name": "body", "type": "u64"}
        ]
    });
    let ingest_after_update = &[json!({"body": 16}), json!({"body": 17})];
    validate_search_across_doc_mapping_updates(
        index_id,
        original_doc_mappings,
        ingest_before_update,
        updated_doc_mappings,
        ingest_after_update,
        &[
            ("body:14", Ok(1)),
            ("body:16", Ok(1)),
            // error expected because the validation is performed
            // by latest doc mapping
            ("body:hello", Err(())),
        ],
    )
    .await;
}

#[tokio::test]
async fn test_update_doc_mappings_u64_to_text() {
    let index_id = "update-u64-to-text";
    let original_doc_mappings = json!({
        "field_mappings": [
            {"name": "body", "type": "u64"}
        ]
    });
    let ingest_before_update = &[json!({"body": 14}), json!({"body": 15})];
    let updated_doc_mappings = json!({
        "field_mappings": [
            {"name": "body", "type": "text"}
        ]
    });
    let ingest_after_update = &[json!({"body": "16"}), json!({"body": "hello world"})];
    validate_search_across_doc_mapping_updates(
        index_id,
        original_doc_mappings,
        ingest_before_update,
        updated_doc_mappings,
        ingest_after_update,
        &[
            ("body:14", Ok(1)),
            ("body:16", Ok(1)),
            ("body:hello", Ok(1)),
        ],
    )
    .await;
}

#[tokio::test]
async fn test_update_doc_mappings_json_to_text() {
    let index_id = "update-json-to-text";
    let original_doc_mappings = json!({
        "field_mappings": [
            {"name": "body", "type": "json"}
        ]
    });
    let ingest_before_update = &[
        json!({"body": {"field1": "hello"}}),
        json!({"body": {"field2": "world"}}),
    ];
    let updated_doc_mappings = json!({
        "field_mappings": [
            {"name": "body", "type": "text"}
        ]
    });
    let ingest_after_update = &[json!({"body": "hello world"})];
    validate_search_across_doc_mapping_updates(
        index_id,
        original_doc_mappings,
        ingest_before_update,
        updated_doc_mappings,
        ingest_after_update,
        &[
            ("body:hello", Ok(1)),
            // error expected because the validation is performed
            // by latest doc mapping
            ("body.field1:hello", Err(())),
        ],
    )
    .await;
}

#[tokio::test]
async fn test_update_doc_mappings_json_to_object() {
    let index_id = "update-json-to-object";
    let original_doc_mappings = json!({
        "field_mappings": [
            {"name": "body", "type": "json"}
        ]
    });
    let ingest_before_update = &[
        json!({"body": {"field1": "hello"}}),
        json!({"body": {"field2": "world"}}),
    ];
    let updated_doc_mappings = json!({
        "field_mappings": [
            {
                "name": "body",
                "type": "object",
                "field_mappings": [
                    {"name": "field1", "type": "text"},
                    {"name": "field2", "type": "text"},
                ]
            }
        ]
    });
    let ingest_after_update = &[
        json!({"body": {"field1": "hola"}}),
        json!({"body": {"field2": "mundo"}}),
    ];
    validate_search_across_doc_mapping_updates(
        index_id,
        original_doc_mappings,
        ingest_before_update,
        updated_doc_mappings,
        ingest_after_update,
        &[("body.field1:hello", Ok(1)), ("body.field1:hola", Ok(1))],
    )
    .await;
}
