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

use futures_util::FutureExt;
use itertools::Itertools;
use quickwit_common::test_utils::wait_until_predicate;
use quickwit_config::ConfigFormat;
use quickwit_config::service::QuickwitService;
use quickwit_indexing::actors::INDEXING_DIR_NAME;
use quickwit_metastore::SplitState;
use quickwit_proto::ingest::ParseFailureReason;
use quickwit_rest_client::error::{ApiError, Error};
use quickwit_rest_client::models::IngestSource;
use quickwit_rest_client::rest_client::CommitType;
use quickwit_serve::{ListSplitsQueryParams, RestIngestResponse, RestParseFailure};
use serde_json::json;

use crate::ingest_json;
use crate::test_utils::{ClusterSandboxBuilder, ingest};

/// Ingesting on a freshly re-created index sometimes fails, see #5430
#[tokio::test]
#[ignore]
async fn test_ingest_recreated_index() {
    let sandbox = ClusterSandboxBuilder::build_and_start_standalone().await;
    let index_id = "test-ingest-recreated-index";
    let index_config = format!(
        r#"
            version: 0.8
            index_id: {index_id}
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
            "#
    );
    let current_index_metadata = sandbox
        .rest_client(QuickwitService::Indexer)
        .indexes()
        .create(index_config.clone(), ConfigFormat::Yaml, false)
        .await
        .unwrap();

    ingest(
        &sandbox.rest_client(QuickwitService::Indexer),
        index_id,
        ingest_json!({"body": "first record"}),
        CommitType::Force,
    )
    .await
    .unwrap();

    sandbox
        .wait_for_splits(index_id, Some(vec![SplitState::Published]), 1)
        .await
        .unwrap();

    sandbox
        .rest_client(QuickwitService::Indexer)
        .indexes()
        .delete(index_id, false)
        .await
        .unwrap();

    // Recreate the index and start ingesting into it again

    let new_index_metadata = sandbox
        .rest_client(QuickwitService::Indexer)
        .indexes()
        .create(index_config, ConfigFormat::Yaml, false)
        .await
        .unwrap();

    assert_ne!(
        current_index_metadata.index_uid.incarnation_id,
        new_index_metadata.index_uid.incarnation_id
    );

    ingest(
        &sandbox.rest_client(QuickwitService::Indexer),
        index_id,
        ingest_json!({"body": "second record"}),
        CommitType::Force,
    )
    .await
    .unwrap();

    sandbox
        .wait_for_splits(index_id, Some(vec![SplitState::Published]), 1)
        .await
        .unwrap();

    ingest(
        &sandbox.rest_client(QuickwitService::Indexer),
        index_id,
        ingest_json!({"body": "third record"}),
        CommitType::Force,
    )
    .await
    .unwrap();

    sandbox
        .wait_for_splits(index_id, Some(vec![SplitState::Published]), 2)
        .await
        .unwrap();

    ingest(
        &sandbox.rest_client(QuickwitService::Indexer),
        index_id,
        ingest_json!({"body": "fourth record"}),
        CommitType::Force,
    )
    .await
    .unwrap();

    sandbox
        .wait_for_splits(index_id, Some(vec![SplitState::Published]), 3)
        .await
        .unwrap();

    sandbox.assert_hit_count(index_id, "body:record", 3).await;

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

    // Delete the index to avoid potential hanging on shutdown #5068
    sandbox
        .rest_client(QuickwitService::Indexer)
        .indexes()
        .delete(index_id, false)
        .await
        .unwrap();

    sandbox.shutdown().await.unwrap();
}

/// Indexing directory is not cleaned up after deleting an index, see #5436
#[tokio::test]
#[ignore]
async fn test_indexing_directory_cleanup() {
    let sandbox = ClusterSandboxBuilder::build_and_start_standalone().await;
    let index_id = "test-ingest-directory-cleanup";
    let index_config = format!(
        r#"
            version: 0.8
            index_id: {index_id}
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
            "#
    );
    sandbox
        .rest_client(QuickwitService::Indexer)
        .indexes()
        .create(index_config.clone(), ConfigFormat::Yaml, false)
        .await
        .unwrap();

    ingest(
        &sandbox.rest_client(QuickwitService::Indexer),
        index_id,
        ingest_json!({"body": "first record"}),
        CommitType::Force,
    )
    .await
    .unwrap();

    sandbox
        .wait_for_splits(index_id, Some(vec![SplitState::Published]), 1)
        .await
        .unwrap();

    sandbox
        .rest_client(QuickwitService::Indexer)
        .indexes()
        .delete(index_id, false)
        .await
        .unwrap();

    // The index is deleted so the `indexing` directory should be cleaned up
    let data_dir_path = &sandbox.node_configs.first().unwrap().0.data_dir_path;
    let indexing_dir_path = data_dir_path.join(INDEXING_DIR_NAME);
    wait_until_predicate(
        || async {
            let indexing_dir_entries = indexing_dir_path.read_dir().unwrap().collect_vec();
            indexing_dir_entries.is_empty()
        },
        Duration::from_secs(100),
        Duration::from_millis(500),
    )
    .await
    .unwrap();

    sandbox.shutdown().await.unwrap();
}

/// This tests checks what happens when we try to ingest into a non-existing index.
#[tokio::test]
async fn test_ingest_v2_index_not_found() {
    let sandbox = ClusterSandboxBuilder::default()
        .add_node([QuickwitService::Indexer, QuickwitService::Janitor])
        .add_node([QuickwitService::Indexer, QuickwitService::Janitor])
        .add_node([
            QuickwitService::ControlPlane,
            QuickwitService::Metastore,
            QuickwitService::Searcher,
        ])
        .build_and_start()
        .await;
    let missing_index_err: Error = sandbox
        .rest_client(QuickwitService::Indexer)
        .ingest(
            "missing_index",
            ingest_json!({"body": "doc1"}),
            None,
            None,
            CommitType::Auto,
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

/// This tests checks our happy path for ingesting one doc.
#[tokio::test]
async fn test_ingest_v2_happy_path() {
    let sandbox = ClusterSandboxBuilder::default()
        .add_node([QuickwitService::Indexer, QuickwitService::Janitor])
        .add_node([QuickwitService::Indexer, QuickwitService::Janitor])
        .add_node([
            QuickwitService::ControlPlane,
            QuickwitService::Metastore,
            QuickwitService::Searcher,
        ])
        .build_and_start()
        .await;
    let index_id = "test_happy_path";
    let index_config = format!(
        r#"
        version: 0.8
        index_id: {index_id}
        doc_mapping:
            field_mappings:
            - name: body
              type: text
        indexing_settings:
            commit_timeout_secs: 1
        "#
    );
    sandbox
        .rest_client(QuickwitService::Indexer)
        .indexes()
        .create(index_config, ConfigFormat::Yaml, false)
        .await
        .unwrap();

    let ingest_resp = ingest(
        &sandbox.rest_client(QuickwitService::Indexer),
        index_id,
        ingest_json!({"body": "doc1"}),
        CommitType::Auto,
    )
    .await
    .unwrap();
    assert_eq!(
        ingest_resp,
        RestIngestResponse {
            num_docs_for_processing: 1,
            num_ingested_docs: Some(1),
            num_rejected_docs: Some(0),
            parse_failures: None,
        },
    );

    sandbox
        .wait_for_splits(index_id, Some(vec![SplitState::Published]), 1)
        .await
        .unwrap();

    sandbox.assert_hit_count(index_id, "*", 1).await;

    // Delete the index to avoid potential hanging on shutdown #5068
    sandbox
        .rest_client(QuickwitService::Indexer)
        .indexes()
        .delete(index_id, false)
        .await
        .unwrap();

    sandbox.shutdown().await.unwrap();
}

#[tokio::test]
async fn test_commit_force() {
    let sandbox = ClusterSandboxBuilder::build_and_start_standalone().await;
    let index_id = "test_commit_force";
    let index_config = format!(
        r#"
        version: 0.8
        index_id: {index_id}
        doc_mapping:
            field_mappings:
            - name: body
              type: text
        indexing_settings:
            commit_timeout_secs: 60
        "#
    );

    sandbox
        .rest_client(QuickwitService::Indexer)
        .indexes()
        .create(index_config, ConfigFormat::Yaml, false)
        .await
        .unwrap();

    // commit_timeout_secs is set to a large value, so this would timeout if
    // the commit isn't forced
    let ingest_resp = tokio::time::timeout(
        Duration::from_secs(20),
        ingest(
            &sandbox.rest_client(QuickwitService::Indexer),
            index_id,
            ingest_json!({"body": "force"}),
            CommitType::Force,
        ),
    )
    .await
    .unwrap()
    .unwrap();
    assert_eq!(
        ingest_resp,
        RestIngestResponse {
            num_docs_for_processing: 1,
            num_ingested_docs: Some(1),
            num_rejected_docs: Some(0),
            parse_failures: None,
        },
    );

    sandbox.assert_hit_count(index_id, "body:force", 1).await;

    // Delete the index to avoid waiting for the commit timeout on shutdown #5068
    sandbox
        .rest_client(QuickwitService::Indexer)
        .indexes()
        .delete(index_id, false)
        .await
        .unwrap();

    sandbox.shutdown().await.unwrap();
}

#[tokio::test]
async fn test_commit_wait_for() {
    let sandbox = ClusterSandboxBuilder::build_and_start_standalone().await;
    let index_id = "test_commit_wait_for";
    let index_config = format!(
        r#"
        version: 0.8
        index_id: {index_id}
        doc_mapping:
            field_mappings:
            - name: body 
              type: text
        indexing_settings:
            commit_timeout_secs: 3
        "#
    );

    // Create index
    sandbox
        .rest_client(QuickwitService::Indexer)
        .indexes()
        .create(index_config, ConfigFormat::Yaml, false)
        .await
        .unwrap();

    // run 2 ingest requests at the same time on the same index
    // wait_for shouldn't force the commit so expect only 1 published split
    let client = sandbox.rest_client(QuickwitService::Indexer);
    let ingest_1_fut = client
        .ingest(
            index_id,
            ingest_json!({"body": "wait for"}),
            None,
            None,
            CommitType::WaitFor,
        )
        .then(|res| async {
            let ingest_resp = res.unwrap();
            sandbox.assert_hit_count(index_id, "body:for", 1).await;
            ingest_resp
        });

    let ingest_2_fut = client
        .ingest(
            index_id,
            ingest_json!({"body": "wait again"}),
            None,
            None,
            CommitType::WaitFor,
        )
        .then(|res| async {
            let ingest_resp = res.unwrap();
            sandbox.assert_hit_count(index_id, "body:again", 1).await;
            ingest_resp
        });

    let (ingest_resp_1, ingest_resp_2) = tokio::join!(ingest_1_fut, ingest_2_fut);
    assert_eq!(
        ingest_resp_1,
        RestIngestResponse {
            num_docs_for_processing: 1,
            num_ingested_docs: Some(1),
            num_rejected_docs: Some(0),
            parse_failures: None,
        },
    );
    assert_eq!(
        ingest_resp_2,
        RestIngestResponse {
            num_docs_for_processing: 1,
            num_ingested_docs: Some(1),
            num_rejected_docs: Some(0),
            parse_failures: None,
        },
    );

    sandbox.assert_hit_count(index_id, "body:wait", 2).await;

    let splits_query_params = ListSplitsQueryParams {
        split_states: Some(vec![SplitState::Published]),
        ..Default::default()
    };
    let published_splits = sandbox
        .rest_client(QuickwitService::Indexer)
        .splits(index_id)
        .list(splits_query_params)
        .await
        .unwrap();
    assert_eq!(published_splits.len(), 1);

    sandbox.shutdown().await.unwrap();
}

#[tokio::test]
async fn test_commit_auto() {
    let sandbox = ClusterSandboxBuilder::build_and_start_standalone().await;
    let index_id = "test_commit_auto";
    let index_config = format!(
        r#"
        version: 0.8
        index_id: {index_id}
        doc_mapping:
            field_mappings:
            - name: body
              type: text
        indexing_settings:
            commit_timeout_secs: 2
        "#
    );

    sandbox
        .rest_client(QuickwitService::Indexer)
        .indexes()
        .create(index_config, ConfigFormat::Yaml, false)
        .await
        .unwrap();

    let ingest_resp = sandbox
        .rest_client(QuickwitService::Indexer)
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
        ingest_resp,
        RestIngestResponse {
            num_docs_for_processing: 1,
            num_ingested_docs: Some(1),
            num_rejected_docs: Some(0),
            parse_failures: None,
        },
    );

    sandbox.assert_hit_count(index_id, "body:auto", 0).await;

    sandbox
        .wait_for_splits(index_id, Some(vec![SplitState::Published]), 1)
        .await
        .unwrap();

    sandbox.assert_hit_count(index_id, "body:auto", 1).await;

    sandbox.shutdown().await.unwrap();
}

#[tokio::test]
async fn test_detailed_ingest_response() {
    let sandbox = ClusterSandboxBuilder::build_and_start_standalone().await;
    let index_id = "test_detailed_ingest_response";
    let index_config = format!(
        r#"
        version: 0.8
        index_id: {index_id}
        doc_mapping:
            field_mappings:
            - name: body
              type: text
        indexing_settings:
            commit_timeout_secs: 1
        "#
    );
    sandbox
        .rest_client(QuickwitService::Indexer)
        .indexes()
        .create(index_config, ConfigFormat::Yaml, false)
        .await
        .unwrap();

    let ingest_resp = ingest(
        &sandbox.detailed_ingest_client(),
        index_id,
        IngestSource::Str("{\"body\":\"hello\"}\naouch!".to_string()),
        CommitType::Auto,
    )
    .await
    .unwrap();

    assert_eq!(
        ingest_resp,
        RestIngestResponse {
            num_docs_for_processing: 2,
            num_ingested_docs: Some(1),
            num_rejected_docs: Some(1),
            parse_failures: Some(vec![RestParseFailure {
                document: "aouch!".to_string(),
                message: "failed to parse JSON document".to_string(),
                reason: ParseFailureReason::InvalidJson,
            }]),
        },
    );
    sandbox.shutdown().await.unwrap();
}

#[tokio::test]
async fn test_very_large_index_name() {
    let sandbox = ClusterSandboxBuilder::default()
        .add_node([QuickwitService::Searcher])
        .add_node([QuickwitService::Metastore])
        .add_node([QuickwitService::Indexer])
        .add_node([QuickwitService::ControlPlane])
        .add_node([QuickwitService::Janitor])
        .build_and_start()
        .await;

    let acceptable_index_id = "its_very_very_very_very_very_very_very_very_very_very_very_\
    very_very_very_very_very_very_very_very_very_very_very_very_very_very_very_\
    very_very_very_very_very_very_very_very_very_very_very_very_very_very_very_\
    very_very_very_very_very_very_index_large_name";
    assert_eq!(acceptable_index_id.len(), 255);
    let oversized_index_id = format!("{acceptable_index_id}1");

    sandbox
        .rest_client(QuickwitService::Indexer)
        .indexes()
        .create(
            format!(
                r#"
                version: 0.8
                index_id: {acceptable_index_id}
                doc_mapping:
                  field_mappings:
                    - name: body
                      type: text
                indexing_settings:
                    commit_timeout_secs: 1
                "#,
            ),
            ConfigFormat::Yaml,
            false,
        )
        .await
        .unwrap();

    ingest(
        &sandbox.rest_client(QuickwitService::Indexer),
        acceptable_index_id,
        ingest_json!({"body": "not too long"}),
        CommitType::Auto,
    )
    .await
    .unwrap();

    sandbox
        .wait_for_splits(acceptable_index_id, Some(vec![SplitState::Published]), 1)
        .await
        .unwrap();

    sandbox
        .assert_hit_count(acceptable_index_id, "body:long", 1)
        .await;

    // Delete the index to avoid potential hanging on shutdown #5068
    sandbox
        .rest_client(QuickwitService::Indexer)
        .indexes()
        .delete(acceptable_index_id, false)
        .await
        .unwrap();

    let error = sandbox
        .rest_client(QuickwitService::Indexer)
        .indexes()
        .create(
            format!(
                r#"
                version: 0.8
                index_id: {oversized_index_id}
                doc_mapping:
                    field_mappings:
                    - name: body
                      type: text
                indexing_settings:
                    commit_timeout_secs: 1
                "#,
            ),
            ConfigFormat::Yaml,
            false,
        )
        .await
        .unwrap_err();

    assert!(error.to_string().ends_with(
        "is invalid: identifiers must match the following regular expression: \
         `^[a-zA-Z][a-zA-Z0-9-_\\.]{2,254}$`)"
    ));

    sandbox.shutdown().await.unwrap();
}

#[tokio::test]
async fn test_shutdown_single_node() {
    let sandbox = ClusterSandboxBuilder::build_and_start_standalone().await;
    let index_id = "test_shutdown_single_node";

    sandbox
        .rest_client(QuickwitService::Indexer)
        .indexes()
        .create(
            format!(
                r#"
            version: 0.8
            index_id: {index_id}
            doc_mapping:
              field_mappings:
              - name: body
                type: text
            indexing_settings:
              commit_timeout_secs: 1
            "#
            ),
            ConfigFormat::Yaml,
            false,
        )
        .await
        .unwrap();

    ingest(
        &sandbox.rest_client(QuickwitService::Indexer),
        index_id,
        ingest_json!({"body": "one"}),
        CommitType::Force,
    )
    .await
    .unwrap();

    sandbox
        .rest_client(QuickwitService::Indexer)
        .ingest(
            index_id,
            ingest_json!({"body": "two"}),
            None,
            None,
            CommitType::Force,
        )
        .await
        .unwrap();

    tokio::time::timeout(Duration::from_secs(10), sandbox.shutdown())
        .await
        .unwrap()
        .unwrap();
}

#[tokio::test]
async fn test_shutdown_control_plane_first() {
    let mut sandbox = ClusterSandboxBuilder::default()
        .add_node([QuickwitService::Indexer])
        .add_node([
            QuickwitService::ControlPlane,
            QuickwitService::Searcher,
            QuickwitService::Metastore,
            QuickwitService::Janitor,
        ])
        .build_and_start()
        .await;
    let index_id = "test_shutdown_control_plane_first";

    // Create index
    sandbox
        .rest_client(QuickwitService::Indexer)
        .indexes()
        .create(
            format!(
                r#"
            version: 0.8
            index_id: {index_id}
            doc_mapping:
              field_mappings:
              - name: body
                type: text
            indexing_settings:
              commit_timeout_secs: 1
            "#
            ),
            ConfigFormat::Yaml,
            false,
        )
        .await
        .unwrap();

    ingest(
        &sandbox.rest_client(QuickwitService::Indexer),
        index_id,
        ingest_json!({"body": "one"}),
        CommitType::Force,
    )
    .await
    .unwrap();

    sandbox
        .shutdown_services([
            QuickwitService::ControlPlane,
            QuickwitService::Searcher,
            QuickwitService::Metastore,
            QuickwitService::Janitor,
        ])
        .await
        .unwrap();

    // The indexer hangs on shutdown because it cannot commit the shard EOF
    tokio::time::timeout(Duration::from_secs(5), sandbox.shutdown())
        .await
        .unwrap_err();
}

#[tokio::test]
async fn test_shutdown_indexer_first() {
    let mut sandbox = ClusterSandboxBuilder::default()
        .add_node([QuickwitService::Indexer])
        .add_node([
            QuickwitService::ControlPlane,
            QuickwitService::Searcher,
            QuickwitService::Metastore,
            QuickwitService::Janitor,
        ])
        .build_and_start()
        .await;
    let index_id = "test_shutdown_indexer_first";

    sandbox
        .rest_client(QuickwitService::Indexer)
        .indexes()
        .create(
            format!(
                r#"
            version: 0.8
            index_id: {index_id}
            doc_mapping:
              field_mappings:
              - name: body
                type: text
            indexing_settings:
              commit_timeout_secs: 1
            "#
            ),
            ConfigFormat::Yaml,
            false,
        )
        .await
        .unwrap();

    ingest(
        &sandbox.rest_client(QuickwitService::Indexer),
        index_id,
        ingest_json!({"body": "one"}),
        CommitType::Force,
    )
    .await
    .unwrap();

    sandbox
        .shutdown_services([QuickwitService::Indexer])
        .await
        .unwrap();

    tokio::time::timeout(Duration::from_secs(5), sandbox.shutdown())
        .await
        .unwrap()
        .unwrap();
}

/// Tests that the graceful shutdown sequence works correctly in a multi-indexer
/// cluster: shutting down one indexer does NOT cause 500 errors or data loss,
/// and the cluster eventually rebalances. see #6158
#[tokio::test]
async fn test_graceful_shutdown_no_data_loss() {
    let mut sandbox = ClusterSandboxBuilder::default()
        .add_node([QuickwitService::Indexer])
        .add_node([QuickwitService::Indexer])
        .add_node([
            QuickwitService::ControlPlane,
            QuickwitService::Searcher,
            QuickwitService::Metastore,
            QuickwitService::Janitor,
        ])
        .build_and_start()
        .await;
    let index_id = "test_graceful_shutdown_no_data_loss";

    // Create index with a long commit timeout so documents stay uncommitted
    // in the ingesters' WAL. The decommission sequence should commit
    // them before the indexer quits.
    sandbox
        .rest_client(QuickwitService::Indexer)
        .indexes()
        .create(
            format!(
                r#"
            version: 0.8
            index_id: {index_id}
            doc_mapping:
              field_mappings:
              - name: body
                type: text
            indexing_settings:
              commit_timeout_secs: 5
            "#
            ),
            ConfigFormat::Yaml,
            false,
        )
        .await
        .unwrap();

    // Ingest docs with auto-commit. With a 5s commit timeout, these documents
    // sit uncommitted in the ingesters' WAL - exactly the in-flight state we
    // want to exercise during draining.
    ingest(
        &sandbox.rest_client(QuickwitService::Indexer),
        index_id,
        ingest_json!({"body": "before-shutdown-1"}),
        CommitType::Auto,
    )
    .await
    .unwrap();

    ingest(
        &sandbox.rest_client(QuickwitService::Indexer),
        index_id,
        ingest_json!({"body": "before-shutdown-2"}),
        CommitType::Auto,
    )
    .await
    .unwrap();

    // Remove the first indexer node from the sandbox and get its shutdown handle.
    // After this call, rest_client(Indexer) returns the second (surviving) indexer.
    let shutdown_handle = sandbox.remove_node_with_service(QuickwitService::Indexer);

    // Concurrently: shut down the removed indexer AND ingest more data via the
    // surviving indexer. This verifies the cluster stays operational and the
    // router on the surviving node does not return 500 errors while one indexer
    // is decommissioning. The control plane excludes the decommissioning
    // ingester from shard allocation, so new shards go to the surviving one.
    let ingest_client = sandbox.rest_client(QuickwitService::Indexer);
    let (shutdown_result, ingest_result) = tokio::join!(
        async {
            tokio::time::timeout(Duration::from_secs(30), shutdown_handle.shutdown())
                .await
                .expect("indexer shutdown timed out — decommission may be stuck")
        },
        async {
            // Small delay so the decommission sequence has started before we ingest.
            tokio::time::sleep(Duration::from_millis(200)).await;
            ingest(
                &ingest_client,
                index_id,
                ingest_json!({"body": "during-shutdown"}),
                CommitType::Auto,
            )
            .await
        },
    );
    shutdown_result.expect("indexer shutdown failed");
    ingest_result.expect("ingest during shutdown should succeed (no 500 errors)");

    // All 3 documents should eventually be searchable. Documents 1 & 2 were
    // in-flight on the decommissioning indexer and should have been committed during
    // the decommission step. Document 3 was ingested to the surviving indexer.
    wait_until_predicate(
        || async {
            match sandbox
                .rest_client(QuickwitService::Searcher)
                .search(
                    index_id,
                    quickwit_serve::SearchRequestQueryString {
                        query: "*".to_string(),
                        max_hits: 10,
                        ..Default::default()
                    },
                )
                .await
            {
                Ok(resp) => resp.num_hits == 3,
                Err(_) => false,
            }
        },
        Duration::from_secs(30),
        Duration::from_millis(500),
    )
    .await
    .expect("expected 3 documents after decommission shutdown, some data may have been lost");

    // Verify the cluster sees 2 ready nodes (the surviving indexer + the
    // control-plane/searcher/metastore/janitor node).
    sandbox
        .wait_for_cluster_num_ready_nodes(2)
        .await
        .expect("cluster should see 2 ready nodes after indexer shutdown");

    // Clean shutdown of the remaining nodes.
    tokio::time::timeout(Duration::from_secs(15), sandbox.shutdown())
        .await
        .unwrap()
        .unwrap();
}
