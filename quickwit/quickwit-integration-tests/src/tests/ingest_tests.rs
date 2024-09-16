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

use futures_util::FutureExt;
use quickwit_config::service::QuickwitService;
use quickwit_config::ConfigFormat;
use quickwit_metastore::SplitState;
use quickwit_rest_client::error::{ApiError, Error};
use quickwit_rest_client::rest_client::CommitType;
use quickwit_serve::ListSplitsQueryParams;
use serde_json::json;

use crate::ingest_json;
use crate::test_utils::{ingest, ClusterSandboxBuilder};

fn initialize_tests() {
    quickwit_common::setup_logging_for_tests();
    std::env::set_var("QW_ENABLE_INGEST_V2", "true");
}

// TODO: The control plane schedules the old pipeline and this test fails (not
// always). It might be because the reschedule takes too long to happen
// or another bug.
#[tokio::test]
async fn test_ingest_recreated_index() {
    initialize_tests();
    let mut sandbox = ClusterSandboxBuilder::build_and_start_standalone().await;
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

    let current_index_metadata = sandbox
        .indexer_rest_client
        .indexes()
        .create(index_config.clone(), ConfigFormat::Yaml, false)
        .await
        .unwrap();

    ingest(
        &sandbox.indexer_rest_client,
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
        .indexer_rest_client
        .indexes()
        .delete(index_id, false)
        .await
        .unwrap();

    // Recreate the index and start ingesting into it again

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

    ingest(
        &sandbox.indexer_rest_client,
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
        &sandbox.indexer_rest_client,
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
        &sandbox.indexer_rest_client,
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

    // Delete the index
    sandbox
        .indexer_rest_client
        .indexes()
        .delete(index_id, false)
        .await
        .unwrap();

    sandbox.shutdown().await.unwrap();
}

/// This tests checks what happens when we try to ingest into a non-existing index.
#[tokio::test]
async fn test_ingest_v2_index_not_found() {
    initialize_tests();
    let mut sandbox = ClusterSandboxBuilder::default()
        .add_node([QuickwitService::Indexer, QuickwitService::Janitor])
        .add_node([QuickwitService::Indexer, QuickwitService::Janitor])
        .add_node([
            QuickwitService::ControlPlane,
            QuickwitService::Metastore,
            QuickwitService::Searcher,
        ])
        .build_and_start()
        .await;
    sandbox.enable_ingest_v2();
    let missing_index_err: Error = sandbox
        .indexer_rest_client
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
    initialize_tests();
    let mut sandbox = ClusterSandboxBuilder::default()
        .add_node([QuickwitService::Indexer, QuickwitService::Janitor])
        .add_node([QuickwitService::Indexer, QuickwitService::Janitor])
        .add_node([
            QuickwitService::ControlPlane,
            QuickwitService::Metastore,
            QuickwitService::Searcher,
        ])
        .build_and_start()
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

    ingest(
        &sandbox.indexer_rest_client,
        index_id,
        ingest_json!({"body": "doc1"}),
        CommitType::Auto,
    )
    .await
    .unwrap();

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
async fn test_commit_force() {
    initialize_tests();
    let mut sandbox = ClusterSandboxBuilder::build_and_start_standalone().await;
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

    // Create index
    sandbox
        .indexer_rest_client
        .indexes()
        .create(index_config, ConfigFormat::Yaml, false)
        .await
        .unwrap();

    sandbox.enable_ingest_v2();

    // commit_timeout_secs is set to a large value, so this would timeout if
    // the commit isn't forced
    tokio::time::timeout(
        Duration::from_secs(20),
        ingest(
            &sandbox.indexer_rest_client,
            index_id,
            ingest_json!({"body": "force"}),
            CommitType::Force,
        ),
    )
    .await
    .unwrap()
    .unwrap();

    sandbox.assert_hit_count(index_id, "body:force", 1).await;

    sandbox.shutdown().await.unwrap();
}

#[tokio::test]
async fn test_commit_wait_for() {
    initialize_tests();
    let mut sandbox = ClusterSandboxBuilder::build_and_start_standalone().await;
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
        .indexer_rest_client
        .indexes()
        .create(index_config, ConfigFormat::Yaml, false)
        .await
        .unwrap();

    sandbox.enable_ingest_v2();

    // run 2 ingest requests at the same time on the same index
    // wait_for shouldn't force the commit so expect only 1 published split

    let ingest_1_fut = sandbox
        .indexer_rest_client
        .ingest(
            index_id,
            ingest_json!({"body": "wait for"}),
            None,
            None,
            CommitType::WaitFor,
        )
        .then(|res| async {
            res.unwrap();
            sandbox.assert_hit_count(index_id, "body:for", 1).await;
        });

    let ingest_2_fut = sandbox
        .indexer_rest_client
        .ingest(
            index_id,
            ingest_json!({"body": "wait again"}),
            None,
            None,
            CommitType::WaitFor,
        )
        .then(|res| async {
            res.unwrap();
            sandbox.assert_hit_count(index_id, "body:again", 1).await;
        });

    tokio::join!(ingest_1_fut, ingest_2_fut);

    sandbox.assert_hit_count(index_id, "body:wait", 2).await;

    let splits_query_params = ListSplitsQueryParams {
        split_states: Some(vec![SplitState::Published]),
        ..Default::default()
    };
    let published_splits = sandbox
        .indexer_rest_client
        .splits(index_id)
        .list(splits_query_params)
        .await
        .unwrap();
    assert_eq!(published_splits.len(), 1);

    sandbox.shutdown().await.unwrap();
}

#[tokio::test]
async fn test_commit_auto() {
    initialize_tests();
    let mut sandbox = ClusterSandboxBuilder::build_and_start_standalone().await;
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
        .indexer_rest_client
        .indexes()
        .create(index_config, ConfigFormat::Yaml, false)
        .await
        .unwrap();

    sandbox.enable_ingest_v2();

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
        .wait_for_splits(index_id, Some(vec![SplitState::Published]), 1)
        .await
        .unwrap();

    sandbox.assert_hit_count(index_id, "body:auto", 1).await;

    sandbox.shutdown().await.unwrap();
}

#[tokio::test]
async fn test_very_large_index_name() {
    initialize_tests();
    let mut sandbox = ClusterSandboxBuilder::default()
        .add_node([QuickwitService::Searcher])
        .add_node([QuickwitService::Metastore])
        .add_node([QuickwitService::Indexer])
        .add_node([QuickwitService::ControlPlane])
        .add_node([QuickwitService::Janitor])
        .build_and_start()
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
    ingest(
        &sandbox.indexer_rest_client,
        index_id,
        ingest_json!({"body": "not too long"}),
        CommitType::Auto,
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
    let mut sandbox = ClusterSandboxBuilder::build_and_start_standalone().await;
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
    ingest(
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

    tokio::time::timeout(Duration::from_secs(10), sandbox.shutdown())
        .await
        .unwrap()
        .unwrap();
}

#[tokio::test]
async fn test_shutdown_control_plane_first() {
    initialize_tests();
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

    ingest(
        &sandbox.indexer_rest_client,
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

    // The indexer should fail to shutdown because it cannot commit the
    // shard EOF
    if let Ok(Ok(_)) = tokio::time::timeout(Duration::from_secs(5), sandbox.shutdown()).await {
        panic!("Expected timeout or error on shutdown");
    }
}

#[tokio::test]
async fn test_shutdown_indexer_first() {
    initialize_tests();
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

    ingest(
        &sandbox.indexer_rest_client,
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
