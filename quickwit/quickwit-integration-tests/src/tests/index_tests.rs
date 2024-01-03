// Copyright (C) 2023 Quickwit, Inc.
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

use quickwit_common::test_utils::wait_until_predicate;
use quickwit_config::service::QuickwitService;
use quickwit_config::ConfigFormat;
use quickwit_indexing::actors::INDEXING_DIR_NAME;
use quickwit_janitor::actors::DELETE_SERVICE_TASK_DIR_NAME;
use quickwit_metastore::SplitState;
use quickwit_proto::opentelemetry::proto::collector::trace::v1::ExportTraceServiceRequest;
use quickwit_proto::opentelemetry::proto::trace::v1::{ResourceSpans, ScopeSpans, Span};
use quickwit_rest_client::error::{ApiError, Error};
use quickwit_rest_client::rest_client::CommitType;
use quickwit_serve::SearchRequestQueryString;
use serde_json::json;

use crate::ingest_json;
use crate::test_utils::{ingest_with_retry, ClusterSandbox};

#[tokio::test]
async fn test_restarting_standalone_server() {
    quickwit_common::setup_logging_for_tests();
    let sandbox = ClusterSandbox::start_standalone_node().await.unwrap();
    let index_id = "test-index-with-restarting";
    let index_config = format!(
        r#"
            version: 0.6
            index_id: {}
            doc_mapping:
                field_mappings:
                - name: body
                  type: text
            indexing_settings:
                commit_timeout_secs: 1
                merge_policy:
                    type: stable_log
                    merge_factor: 3
                    max_merge_factor: 3
            "#,
        index_id
    );

    // Create the index.
    sandbox
        .indexer_rest_client
        .indexes()
        .create(index_config.clone(), ConfigFormat::Yaml, false)
        .await
        .unwrap();

    // Wait fo the pipeline to start.
    // TODO: there should be a better way to do this.
    sandbox.wait_for_indexing_pipelines(1).await.unwrap();

    let old_uid = sandbox
        .indexer_rest_client
        .indexes()
        .get(index_id)
        .await
        .unwrap()
        .index_uid;

    // Index one record.
    ingest_with_retry(
        &sandbox.indexer_rest_client,
        index_id,
        ingest_json!({"body": "first record"}),
        CommitType::Force,
    )
    .await
    .unwrap();

    // Delete the index
    sandbox
        .indexer_rest_client
        .indexes()
        .delete(index_id, false)
        .await
        .unwrap();

    // Create the index again.
    sandbox
        .indexer_rest_client
        .indexes()
        .create(index_config, ConfigFormat::Yaml, false)
        .await
        .unwrap();

    sandbox.wait_for_indexing_pipelines(1).await.unwrap();

    let new_uid = sandbox
        .indexer_rest_client
        .indexes()
        .get(index_id)
        .await
        .unwrap()
        .index_uid;
    assert_ne!(old_uid.incarnation_id(), new_uid.incarnation_id());

    // Index a couple of records to create 2 additional splits.
    ingest_with_retry(
        &sandbox.indexer_rest_client,
        index_id,
        ingest_json!({"body": "second record"}),
        CommitType::Force,
    )
    .await
    .unwrap();
    sandbox
        .indexer_rest_client
        .ingest(
            index_id,
            ingest_json!({"body": "third record"}),
            None,
            None,
            CommitType::Force,
        )
        .await
        .unwrap();
    sandbox
        .indexer_rest_client
        .ingest(
            index_id,
            ingest_json!({"body": "fourth record"}),
            None,
            None,
            CommitType::Force,
        )
        .await
        .unwrap();

    let search_response_empty = sandbox
        .searcher_rest_client
        .search(
            index_id,
            SearchRequestQueryString {
                query: "body:record".to_string(),
                ..Default::default()
            },
        )
        .await
        .unwrap();

    assert_eq!(search_response_empty.num_hits, 3);

    // Wait for splits to merge, since we created 3 splits and merge factor is 3,
    // we should get 1 published split with no staged splits eventually.
    sandbox
        .wait_for_splits(
            index_id,
            Some(vec![SplitState::Published, SplitState::Staged]),
            1,
        )
        .await
        .unwrap();

    // Check that we have one directory
    let path = sandbox
        .node_configs
        .first()
        .unwrap()
        .node_config
        .data_dir_path
        .clone();
    let delete_service_path = path.join(DELETE_SERVICE_TASK_DIR_NAME);
    let indexing_path = path.join(INDEXING_DIR_NAME);

    assert_eq!(delete_service_path.read_dir().unwrap().count(), 1);
    assert_eq!(indexing_path.read_dir().unwrap().count(), 1);

    // Delete the index
    sandbox
        .indexer_rest_client
        .indexes()
        .delete(index_id, false)
        .await
        .unwrap();

    // Wait to make sure all directories are cleaned up
    wait_until_predicate(
        || {
            let delete_service_path = delete_service_path.clone();
            let indexing_path = indexing_path.clone();
            async move {
                delete_service_path.read_dir().unwrap().count() == 0
                    && indexing_path.read_dir().unwrap().count() == 0
            }
        },
        Duration::from_secs(10),
        Duration::from_millis(100),
    )
    .await
    .unwrap();

    sandbox.shutdown().await.unwrap();
}

const TEST_INDEX_CONFIG: &str = r#"
    version: 0.6
    index_id: test_index
    doc_mapping:
      field_mappings:
      - name: body
        type: text
    indexing_settings:
      commit_timeout_secs: 1
      merge_policy:
        type: stable_log
        merge_factor: 4
        max_merge_factor: 4
"#;

#[tokio::test]
async fn test_ingest_v2_index_not_found() {
    // This tests checks what happens when we try to ingest into a non-existing index.
    quickwit_common::setup_logging_for_tests();
    let nodes_services = &[
        HashSet::from_iter([QuickwitService::Indexer, QuickwitService::Janitor]),
        HashSet::from_iter([QuickwitService::Indexer, QuickwitService::Janitor]),
        HashSet::from_iter([
            QuickwitService::ControlPlane,
            QuickwitService::Metastore,
            QuickwitService::Searcher,
        ]),
    ];
    let mut sandbox = ClusterSandbox::start_cluster_nodes(&nodes_services[..])
        .await
        .unwrap();
    sandbox.enable_ingest_v2();
    sandbox.wait_for_cluster_num_ready_nodes(3).await.unwrap();
    let missing_index_err: Error = sandbox
        .indexer_rest_client
        .ingest(
            "missing_index",
            ingest_json!({"body": "doc1"}),
            None,
            None,
            CommitType::WaitFor,
        )
        .await
        .unwrap_err();
    let Error::Api(ApiError { message, code }) = missing_index_err else {
        panic!("Expected an API error.");
    };
    assert_eq!(code, 404u16);
    let error_message = message.unwrap();
    assert_eq!(error_message, "index `missing_index` not found");
    sandbox.shutdown().await.unwrap();
}

#[tokio::test]
async fn test_ingest_v2_happy_path() {
    // This tests checks our happy path for ingesting one doc.
    quickwit_common::setup_logging_for_tests();
    let nodes_services = &[
        HashSet::from_iter([QuickwitService::Indexer, QuickwitService::Janitor]),
        HashSet::from_iter([QuickwitService::Indexer, QuickwitService::Janitor]),
        HashSet::from_iter([
            QuickwitService::ControlPlane,
            QuickwitService::Metastore,
            QuickwitService::Searcher,
        ]),
    ];
    let mut sandbox = ClusterSandbox::start_cluster_nodes(&nodes_services[..])
        .await
        .unwrap();
    sandbox.enable_ingest_v2();
    sandbox.wait_for_cluster_num_ready_nodes(3).await.unwrap();
    sandbox
        .indexer_rest_client
        .indexes()
        .create(TEST_INDEX_CONFIG, ConfigFormat::Yaml, false)
        .await
        .unwrap();
    sandbox
        .indexer_rest_client
        .sources("test_index")
        .toggle("_ingest-source", true)
        .await
        .unwrap();
    sandbox
        .indexer_rest_client
        .ingest(
            "test_index",
            ingest_json!({"body": "doc1"}),
            None,
            None,
            CommitType::WaitFor,
        )
        .await
        .unwrap();
    let search_req = SearchRequestQueryString {
        query: "*".to_string(),
        ..Default::default()
    };
    let search_result = sandbox
        .indexer_rest_client
        .search("test_index", search_req)
        .await
        .unwrap();
    assert_eq!(search_result.num_hits, 1);
    sandbox.shutdown().await.unwrap();
}

#[tokio::test]
async fn test_commit_modes() {
    quickwit_common::setup_logging_for_tests();
    let sandbox = ClusterSandbox::start_standalone_node().await.unwrap();
    let index_id = "test_index";

    // Create index
    sandbox
        .indexer_rest_client
        .indexes()
        .create(TEST_INDEX_CONFIG, ConfigFormat::Yaml, false)
        .await
        .unwrap();

    // Test force commit
    ingest_with_retry(
        &sandbox.indexer_rest_client,
        index_id,
        ingest_json!({"body": "force"}),
        CommitType::Force,
    )
    .await
    .unwrap();

    assert_eq!(
        sandbox
            .searcher_rest_client
            .search(
                index_id,
                SearchRequestQueryString {
                    query: "body:force".to_string(),
                    ..Default::default()
                },
            )
            .await
            .unwrap()
            .num_hits,
        1
    );

    // Test wait_for commit
    sandbox
        .indexer_rest_client
        .ingest(
            index_id,
            ingest_json!({"body": "wait"}),
            None,
            None,
            CommitType::WaitFor,
        )
        .await
        .unwrap();

    assert_eq!(
        sandbox
            .searcher_rest_client
            .search(
                index_id,
                SearchRequestQueryString {
                    query: "body:wait".to_string(),
                    ..Default::default()
                },
            )
            .await
            .unwrap()
            .num_hits,
        1
    );

    // Test auto commit
    sandbox
        .indexer_rest_client
        .ingest(
            index_id,
            ingest_json!({"body": "auto"}),
            None,
            None,
            CommitType::Auto,
        )
        .await
        .unwrap();

    assert_eq!(
        sandbox
            .searcher_rest_client
            .search(
                index_id,
                SearchRequestQueryString {
                    query: "body:auto".to_string(),
                    ..Default::default()
                },
            )
            .await
            .unwrap()
            .num_hits,
        0
    );

    tokio::time::sleep(Duration::from_secs(3)).await;

    assert_eq!(
        sandbox
            .searcher_rest_client
            .search(
                index_id,
                SearchRequestQueryString {
                    query: "body:auto".to_string(),
                    ..Default::default()
                },
            )
            .await
            .unwrap()
            .num_hits,
        1
    );

    // Clean up
    sandbox.shutdown().await.unwrap();
}

#[tokio::test]
async fn test_very_large_index_name() {
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

    let index_id = "its_very_very_very_very_very_very_very_very_very_very_very_\
    very_very_very_very_very_very_very_very_very_very_very_very_very_very_very_\
    very_very_very_very_very_very_very_very_very_very_very_very_very_very_very_\
    very_very_very_very_very_very_index_large_name";
    assert_eq!(index_id.len(), 255);
    let oversized_index_id = format!("{index_id}1");
    // Create index
    sandbox
        .indexer_rest_client
        .indexes()
        .create(
            format!(
                r#"
                version: 0.6
                index_id: {index_id}
                doc_mapping:
                  field_mappings:
                    - name: body
                      type: text
                "#,
            ),
            ConfigFormat::Yaml,
            false,
        )
        .await
        .unwrap();

    // Test force commit
    ingest_with_retry(
        &sandbox.indexer_rest_client,
        index_id,
        ingest_json!({"body": "force"}),
        CommitType::Force,
    )
    .await
    .unwrap();

    assert_eq!(
        sandbox
            .searcher_rest_client
            .search(
                index_id,
                SearchRequestQueryString {
                    query: "body:force".to_string(),
                    ..Default::default()
                },
            )
            .await
            .unwrap()
            .num_hits,
        1
    );

    // Delete the index
    sandbox
        .indexer_rest_client
        .indexes()
        .delete(index_id, false)
        .await
        .unwrap();

    // Try to create an index with a very long name
    let error = sandbox
        .indexer_rest_client
        .indexes()
        .create(
            format!(
                r#"
                    version: 0.6
                    index_id: {oversized_index_id}
                    doc_mapping:
                      field_mappings:
                        - name: body
                          type: text
                    "#,
            ),
            ConfigFormat::Yaml,
            false,
        )
        .await
        .unwrap_err();

    assert!(error.to_string().ends_with(
        "is invalid. identifiers must match the following regular expression: \
         `^[a-zA-Z][a-zA-Z0-9-_\\.]{2,254}$`)"
    ));

    // Clean up
    sandbox.shutdown().await.unwrap();
}

#[tokio::test]
async fn test_shutdown() {
    quickwit_common::setup_logging_for_tests();
    let sandbox = ClusterSandbox::start_standalone_node().await.unwrap();
    let index_id = "test_commit_modes_index";

    // Create index
    sandbox
        .indexer_rest_client
        .indexes()
        .create(
            r#"
            version: 0.6
            index_id: test_commit_modes_index
            doc_mapping:
              field_mappings:
              - name: body
                type: text
            indexing_settings:
              commit_timeout_secs: 1
            "#,
            ConfigFormat::Yaml,
            false,
        )
        .await
        .unwrap();

    // Ensure that the index is ready to accept records.
    ingest_with_retry(
        &sandbox.indexer_rest_client,
        index_id,
        ingest_json!({"body": "one"}),
        CommitType::Force,
    )
    .await
    .unwrap();

    // Test force commit
    sandbox
        .indexer_rest_client
        .ingest(
            index_id,
            ingest_json!({"body": "two"}),
            None,
            None,
            CommitType::Force,
        )
        .await
        .unwrap();

    // The error we are trying to catch here is that the sandbox is getting stuck in
    // shutdown for 180 seconds if not all services exit cleanly, which in turn manifests
    // itself with a very slow test that passes. This timeout ensures that the test fails
    // if the sandbox gets stuck in shutdown.
    tokio::time::timeout(std::time::Duration::from_secs(10), sandbox.shutdown())
        .await
        .unwrap()
        .unwrap();
}

#[tokio::test]
async fn test_ingest_traces_with_otlp_grpc_api() {
    quickwit_common::setup_logging_for_tests();
    let sandbox = ClusterSandbox::start_standalone_with_otlp_service()
        .await
        .unwrap();
    // Wait fo the pipelines to start (one for logs and one for traces)
    sandbox.wait_for_indexing_pipelines(2).await.unwrap();

    let scope_spans = vec![ScopeSpans {
        spans: vec![
            Span {
                trace_id: vec![1; 16],
                span_id: vec![2; 8],
                start_time_unix_nano: 1_000_000_001,
                end_time_unix_nano: 1_000_000_002,
                ..Default::default()
            },
            Span {
                trace_id: vec![3; 16],
                span_id: vec![4; 8],
                start_time_unix_nano: 2_000_000_001,
                end_time_unix_nano: 2_000_000_002,
                ..Default::default()
            },
        ],
        ..Default::default()
    }];
    let resource_spans = vec![ResourceSpans {
        scope_spans,
        ..Default::default()
    }];
    let request = ExportTraceServiceRequest { resource_spans };

    // Send the spans on the default index.
    {
        let response = sandbox
            .trace_client
            .clone()
            .export(request.clone())
            .await
            .unwrap();
        assert_eq!(
            response
                .into_inner()
                .partial_success
                .unwrap()
                .rejected_spans,
            0
        );
    }

    // Send the spans on a non existing index, should return an error.
    {
        let mut tonic_request = tonic::Request::new(request);
        tonic_request.metadata_mut().insert(
            "qw-otel-traces-index",
            tonic::metadata::MetadataValue::try_from("non-existing-index").unwrap(),
        );
        let status = sandbox
            .trace_client
            .clone()
            .export(tonic_request)
            .await
            .unwrap_err();
        assert_eq!(status.code(), tonic::Code::NotFound);
    }

    sandbox.shutdown().await.unwrap();
}
