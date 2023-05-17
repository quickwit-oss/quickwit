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

use std::time::Duration;

use bytes::Bytes;
use quickwit_metastore::SplitState;
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
    let index_config = Bytes::from(format!(
        r#"
            version: 0.5
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
    ));

    // Create the index.
    sandbox
        .indexer_rest_client
        .indexes()
        .create(
            index_config.clone(),
            quickwit_config::ConfigFormat::Yaml,
            false,
        )
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

    // Delete the indexq
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
        .create(index_config, quickwit_config::ConfigFormat::Yaml, false)
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

    sandbox.shutdown().await.unwrap();
}

#[tokio::test]
async fn test_commit_modes() {
    quickwit_common::setup_logging_for_tests();
    let sandbox = ClusterSandbox::start_standalone_node().await.unwrap();
    let index_id = "test_commit_modes_index";

    // Create index
    sandbox
        .indexer_rest_client
        .indexes()
        .create(
            r#"
            version: 0.5
            index_id: test_commit_modes_index
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

            "#
            .into(),
            quickwit_config::ConfigFormat::Yaml,
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

    // Test wait_for commit
    sandbox
        .indexer_rest_client
        .ingest(
            index_id,
            ingest_json!({"body": "wait"}),
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
            version: 0.5
            index_id: test_commit_modes_index
            doc_mapping:
              field_mappings:
              - name: body
                type: text
            indexing_settings:
              commit_timeout_secs: 1
            "#
            .into(),
            quickwit_config::ConfigFormat::Yaml,
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
