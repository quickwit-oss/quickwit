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

use hyper::StatusCode;
use quickwit_config::service::QuickwitService;
use quickwit_config::ConfigFormat;
use quickwit_metastore::SplitState;
use quickwit_rest_client::error::{ApiError, Error};
use quickwit_rest_client::rest_client::CommitType;
use serde_json::json;

use crate::ingest_json;
use crate::test_utils::{ingest_with_retry, ClusterSandboxConfigBuilder};

fn initialize_tests() {
    quickwit_common::setup_logging_for_tests();
    std::env::set_var("QW_ENABLE_INGEST_V2", "true");
}

#[tokio::test]
async fn test_single_node_cluster() {
    initialize_tests();
    let mut sandbox = ClusterSandboxConfigBuilder::build_standalone()
        .await
        .start()
        .await;
    let index_id = "test-single-node-cluster";
    let index_config = format!(
        r#"
            version: 0.8
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
    sandbox.enable_ingest_v2();

    // Create the index.
    let current_index_metadata = sandbox
        .indexer_rest_client
        .indexes()
        .create(index_config.clone(), ConfigFormat::Yaml, false)
        .await
        .unwrap();

    // Index one record.
    ingest_with_retry(
        &sandbox.indexer_rest_client,
        index_id,
        ingest_json!({"body": "first record"}),
        CommitType::Force,
    )
    .await
    .unwrap();

    // Wait for the split to be published.
    sandbox
        .wait_for_splits(index_id, Some(vec![SplitState::Published]), 1)
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
    let new_index_metadata = sandbox
        .indexer_rest_client
        .indexes()
        .create(index_config, ConfigFormat::Yaml, false)
        .await
        .unwrap();

    assert_ne!(
        current_index_metadata.index_uid.incarnation_id,
        new_index_metadata.index_uid.incarnation_id
    );

    // TODO: The control plane schedules the old pipeline and this test fails. I don't know if it's
    // because the reschule takes too long to happen or it's a bug.

    // Index multiple records in different splits.
    // ingest_with_retry(
    //     &sandbox.indexer_rest_client,
    //     index_id,
    //     ingest_json!({"body": "second record"}),
    //     CommitType::Force,
    // )
    // .await
    // .unwrap();

    // sandbox
    //     .wait_for_splits(index_id, Some(vec![SplitState::Published]), 1)
    //     .await
    //     .unwrap();

    // ingest_with_retry(
    //     &sandbox.indexer_rest_client,
    //     index_id,
    //     ingest_json!({"body": "third record"}),
    //     CommitType::Force,
    // )
    // .await
    // .unwrap();

    // sandbox
    //     .wait_for_splits(index_id, Some(vec![SplitState::Published]), 2)
    //     .await
    //     .unwrap();

    // ingest_with_retry(
    //     &sandbox.indexer_rest_client,
    //     index_id,
    //     ingest_json!({"body": "fourd record"}),
    //     CommitType::Force,
    // )
    // .await
    // .unwrap();

    // sandbox
    //     .wait_for_splits(index_id, Some(vec![SplitState::Published]), 3)
    //     .await
    //     .unwrap();

    // assert_hit_count(&sandbox, index_id, "body:record", 3).await;

    // // Wait for splits to merge, since we created 3 splits and merge factor is 3,
    // // we should get 1 published split with no staged splits eventually.
    // sandbox
    //     .wait_for_splits(
    //         index_id,
    //         Some(vec![SplitState::Published, SplitState::Staged]),
    //         1,
    //     )
    //     .await
    //     .unwrap();

    // Delete the index
    sandbox
        .indexer_rest_client
        .indexes()
        .delete(index_id, false)
        .await
        .unwrap();

    // TODO: Those directories are no longer cleaned up properly.
    // let data_dir_path = &sandbox
    //     .node_configs
    //     .first()
    //     .unwrap()
    //     .node_config
    //     .data_dir_path;

    // let delete_tasks_dir_name = data_dir_path.join(DELETE_SERVICE_TASK_DIR_NAME);
    // let indexing_dir_path = data_dir_path.join(INDEXING_DIR_NAME);

    // // Wait to make sure all directories are cleaned up
    // wait_until_predicate(
    //     || async {
    //         delete_tasks_dir_name.read_dir().unwrap().count() == 0
    //             && indexing_dir_path.read_dir().unwrap().count() == 0
    //     },
    //     Duration::from_secs(10),
    //     Duration::from_millis(100),
    // )
    // .await
    // .unwrap();

    sandbox.shutdown().await.unwrap();
}

/// This tests checks what happens when we try to ingest into a non-existing index.
#[tokio::test]
async fn test_ingest_v2_index_not_found() {
    initialize_tests();
    let mut sandbox = ClusterSandboxConfigBuilder::default()
        .add_node([QuickwitService::Indexer, QuickwitService::Janitor])
        .add_node([QuickwitService::Indexer, QuickwitService::Janitor])
        .add_node([
            QuickwitService::ControlPlane,
            QuickwitService::Metastore,
            QuickwitService::Searcher,
        ])
        .build()
        .await
        .start()
        .await;
    sandbox.enable_ingest_v2();
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

/// This tests checks our happy path for ingesting one doc.
#[tokio::test]
async fn test_ingest_v2_happy_path() {
    initialize_tests();
    let mut sandbox = ClusterSandboxConfigBuilder::default()
        .add_node([QuickwitService::Indexer, QuickwitService::Janitor])
        .add_node([QuickwitService::Indexer, QuickwitService::Janitor])
        .add_node([
            QuickwitService::ControlPlane,
            QuickwitService::Metastore,
            QuickwitService::Searcher,
        ])
        .build()
        .await
        .start()
        .await;
    sandbox.enable_ingest_v2();
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
        .indexer_rest_client
        .indexes()
        .create(index_config, ConfigFormat::Yaml, false)
        .await
        .unwrap();

    // The server have been detected as ready. Unfortunately, they may not have been added
    // to the ingester pool yet.
    //
    // If we get an unavailable error, we retry up to 10 times.
    // See #4213
    const MAX_NUM_RETRIES: usize = 10;
    for i in 1..=MAX_NUM_RETRIES {
        let ingest_res = sandbox
            .indexer_rest_client
            .ingest(
                index_id,
                ingest_json!({"body": "doc1"}),
                None,
                None,
                CommitType::WaitFor,
            )
            .await;
        let Some(ingest_error) = ingest_res.err() else {
            // Success
            break;
        };
        assert_eq!(
            ingest_error.status_code(),
            Some(StatusCode::SERVICE_UNAVAILABLE)
        );
        assert!(
            i < MAX_NUM_RETRIES,
            "service not available after {MAX_NUM_RETRIES} tries"
        );
        tokio::time::sleep(Duration::from_millis(200)).await;
    }

    sandbox
        .wait_for_splits(index_id, Some(vec![SplitState::Published]), 1)
        .await
        .unwrap();

    sandbox.assert_hit_count(index_id, "*", 1).await;

    sandbox
        .indexer_rest_client
        .indexes()
        .delete(index_id, false)
        .await
        .unwrap();

    sandbox.shutdown().await.unwrap();
}

#[tokio::test]
async fn test_commit_modes() {
    initialize_tests();
    let sandbox = ClusterSandboxConfigBuilder::build_standalone()
        .await
        .start()
        .await;
    let index_id = "test_commit_modes";
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

    // Create index
    sandbox
        .indexer_rest_client
        .indexes()
        .create(index_config, ConfigFormat::Yaml, false)
        .await
        .unwrap();

    // TODO: make this test work with ingest v2 (#4438)
    // sandbox.enable_ingest_v2();

    // Test force commit
    ingest_with_retry(
        &sandbox.indexer_rest_client,
        index_id,
        ingest_json!({"body": "force"}),
        CommitType::Force,
    )
    .await
    .unwrap();

    sandbox.assert_hit_count(index_id, "body:force", 1).await;

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

    sandbox.assert_hit_count(index_id, "body:wait", 1).await;

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

    sandbox.assert_hit_count(index_id, "body:auto", 0).await;

    sandbox
        .wait_for_splits(index_id, Some(vec![SplitState::Published]), 3)
        .await
        .unwrap();

    sandbox.assert_hit_count(index_id, "body:auto", 1).await;

    // Clean up
    sandbox.shutdown().await.unwrap();
}

#[tokio::test]
async fn test_very_large_index_name() {
    initialize_tests();
    let mut sandbox = ClusterSandboxConfigBuilder::default()
        .add_node([QuickwitService::Searcher])
        .add_node([QuickwitService::Metastore])
        .add_node([QuickwitService::Indexer])
        .add_node([QuickwitService::ControlPlane])
        .add_node([QuickwitService::Janitor])
        .build()
        .await
        .start()
        .await;
    sandbox.enable_ingest_v2();

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
                version: 0.8
                index_id: {index_id}
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

    // Test force commit
    ingest_with_retry(
        &sandbox.indexer_rest_client,
        index_id,
        ingest_json!({"body": "not too long"}),
        CommitType::WaitFor,
    )
    .await
    .unwrap();

    sandbox
        .wait_for_splits(index_id, Some(vec![SplitState::Published]), 1)
        .await
        .unwrap();

    sandbox.assert_hit_count(index_id, "body:long", 1).await;

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

    // Clean up
    sandbox.shutdown().await.unwrap();
}

#[tokio::test]
async fn test_shutdown_single_node() {
    initialize_tests();
    let mut sandbox = ClusterSandboxConfigBuilder::build_standalone()
        .await
        .start()
        .await;
    let index_id = "test_shutdown_single_node";

    sandbox.enable_ingest_v2();

    // Create index
    sandbox
        .indexer_rest_client
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

    tokio::time::timeout(std::time::Duration::from_secs(10), sandbox.shutdown())
        .await
        .unwrap()
        .unwrap();
}

/// When the control plane is on a different node, it might be shutdown
/// before the ingest pipeline is scheduled on the indexer.
#[tokio::test]
async fn test_shutdown_control_plane_early_shutdown() {
    initialize_tests();
    let sandbox = ClusterSandboxConfigBuilder::default()
        .add_node([QuickwitService::Indexer])
        .add_node([
            QuickwitService::ControlPlane,
            QuickwitService::Searcher,
            QuickwitService::Metastore,
            QuickwitService::Janitor,
        ])
        .build()
        .await
        .start()
        .await;
    let index_id = "test_shutdown_separate_indexer";

    // TODO: make this test work with ingest v2 (#5068)
    // sandbox.enable_ingest_v2();

    // Create index
    sandbox
        .indexer_rest_client
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

    // Ensure that the index is ready to accept records.
    ingest_with_retry(
        &sandbox.indexer_rest_client,
        index_id,
        ingest_json!({"body": "one"}),
        CommitType::WaitFor,
    )
    .await
    .unwrap();

    tokio::time::timeout(std::time::Duration::from_secs(10), sandbox.shutdown())
        .await
        .unwrap()
        .unwrap();
}

/// When the control plane/metastore are shutdown before the indexer, the
/// indexer shutdown should not hang indefinitely
#[tokio::test]
async fn test_shutdown_separate_indexer() {
    initialize_tests();
    let sandbox = ClusterSandboxConfigBuilder::default()
        .add_node([QuickwitService::Indexer])
        .add_node([
            QuickwitService::ControlPlane,
            QuickwitService::Searcher,
            QuickwitService::Metastore,
            QuickwitService::Janitor,
        ])
        .build()
        .await
        .start()
        .await;
    let index_id = "test_shutdown_separate_indexer";

    // TODO: make this test work with ingest v2 (#5068)
    // sandbox.enable_ingest_v2();

    // Create index
    sandbox
        .indexer_rest_client
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

    // Ensure that the index is ready to accept records.
    ingest_with_retry(
        &sandbox.indexer_rest_client,
        index_id,
        ingest_json!({"body": "one"}),
        CommitType::WaitFor,
    )
    .await
    .unwrap();

    sandbox
        .wait_for_splits(index_id, Some(vec![SplitState::Published]), 1)
        .await
        .unwrap();

    tokio::time::timeout(std::time::Duration::from_secs(10), sandbox.shutdown())
        .await
        .unwrap()
        .unwrap();
}
