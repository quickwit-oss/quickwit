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

use quickwit_config::ConfigFormat;
use quickwit_config::service::QuickwitService;
use quickwit_metastore::SplitState;
use quickwit_rest_client::rest_client::CommitType;
use quickwit_serve::{ListSplitsQueryParams, SearchRequestQueryString};
use serde_json::json;

use crate::ingest_json;
use crate::test_utils::{ClusterSandboxBuilder, ingest};

#[tokio::test]
async fn test_secondary_timestamp_happy_path() {
    quickwit_common::setup_logging_for_tests();
    let sandbox = ClusterSandboxBuilder::build_and_start_standalone().await;
    let index_id = "test-secondary-timestamp";
    let index_config = format!(
        r#"
            version: 0.8
            index_id: {index_id}
            doc_mapping:
                field_mappings:
                - name: body
                  type: text
                - name: event_time
                  type: datetime
                  fast: true
                - name: ingestion_time
                  type: datetime
                  fast: true
                timestamp_field: event_time
                secondary_timestamp_field: ingestion_time
            indexing_settings:
                commit_timeout_secs: 1
                merge_policy:
                    type: stable_log
                    merge_factor: 2
                    max_merge_factor: 2
            retention:
                period: 20 years
                schedule: daily
                timestamp_type: secondary
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
        ingest_json!({"body": "first record", "event_time": 1735689600, "ingestion_time": 1735776000}),
        CommitType::Auto,
    )
    .await
    .unwrap();

    ingest(
        &sandbox.rest_client(QuickwitService::Indexer),
        index_id,
        ingest_json!({"body": "second record", "event_time": 1735689601, "ingestion_time": 1735776001}),
        CommitType::Auto,
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
        ingest_json!({"body": "third record", "event_time": 1735689602, "ingestion_time": 1735776002}),
        CommitType::Auto,
    )
    .await
    .unwrap();

    ingest(
        &sandbox.rest_client(QuickwitService::Indexer),
        index_id,
        ingest_json!({"body": "fourth record", "event_time": 1735689603, "ingestion_time": 1735776003}),
        CommitType::Auto,
    )
    .await
    .unwrap();

    // Wait for splits to merge, since we created 2 splits and merge factor is 2.
    sandbox
        .wait_for_splits(index_id, Some(vec![SplitState::MarkedForDeletion]), 2)
        .await
        .unwrap();
    sandbox.assert_hit_count(index_id, "body:record", 4).await;

    let merged_split = sandbox
        .rest_client(QuickwitService::Metastore)
        .splits(index_id)
        .list(ListSplitsQueryParams {
            split_states: Some(vec![SplitState::Published]),
            ..Default::default()
        })
        .await
        .unwrap()
        .into_iter()
        .next()
        .unwrap();

    assert_eq!(
        merged_split.split_metadata.time_range,
        Some(1735689600..=1735689603)
    ); // 2025-01-01 
    assert_eq!(
        merged_split.split_metadata.secondary_time_range,
        Some(1735776000..=1735776003)
    ); // 2025-01-02

    // Delete the index to avoid potential hanging on shutdown
    sandbox
        .rest_client(QuickwitService::Indexer)
        .indexes()
        .delete(index_id, false)
        .await
        .unwrap();

    sandbox.shutdown().await.unwrap();
}

#[tokio::test]
async fn test_secondary_timestamp_update() {
    quickwit_common::setup_logging_for_tests();
    let sandbox = ClusterSandboxBuilder::build_and_start_standalone().await;
    let index_id = "test-secondary-timestamp";
    let index_config = format!(
        r#"
            version: 0.8
            index_id: {index_id}
            doc_mapping:
                field_mappings:
                - name: body
                  type: text
                - name: event_time
                  type: datetime
                  fast: true
                - name: ingestion_time
                  type: datetime
                  fast: true
                timestamp_field: event_time
            indexing_settings:
                commit_timeout_secs: 1
                merge_policy:
                    type: stable_log
                    merge_factor: 2
                    max_merge_factor: 2
            retention:
                period: 20 years
                schedule: daily
                timestamp_type: secondary
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
        ingest_json!({"body": "first record", "event_time": 1735689600, "ingestion_time": 1735776000}),
        CommitType::Auto,
    )
    .await
    .unwrap();

    ingest(
        &sandbox.rest_client(QuickwitService::Indexer),
        index_id,
        ingest_json!({"body": "second record", "event_time": 1735689601, "ingestion_time": 1735776001}),
        CommitType::Auto,
    )
    .await
    .unwrap();

    sandbox
        .wait_for_splits(index_id, Some(vec![SplitState::Published]), 1)
        .await
        .unwrap();

    let updated_index_config = format!(
        r#"
            version: 0.8
            index_id: {index_id}
            doc_mapping:
                field_mappings:
                - name: body
                  type: text
                - name: event_time
                  type: datetime
                  fast: true
                - name: ingestion_time
                  type: datetime
                  fast: true
                timestamp_field: event_time
                secondary_timestamp_field: ingestion_time
            indexing_settings:
                commit_timeout_secs: 1
                merge_policy:
                    type: stable_log
                    merge_factor: 2
                    max_merge_factor: 2
            retention:
                period: 20 years
                schedule: daily
                timestamp_type: secondary
            "#
    );
    sandbox
        .rest_client(QuickwitService::Indexer)
        .indexes()
        .update(
            index_id,
            updated_index_config.clone(),
            ConfigFormat::Yaml,
            false,
        )
        .await
        .unwrap();

    ingest(
        &sandbox.rest_client(QuickwitService::Indexer),
        index_id,
        ingest_json!({"body": "third record", "event_time": 1735689602, "ingestion_time": 1735776002}),
        CommitType::Auto,
    )
    .await
    .unwrap();

    ingest(
        &sandbox.rest_client(QuickwitService::Indexer),
        index_id,
        ingest_json!({"body": "fourth record", "event_time": 1735689603, "ingestion_time": 1735776003}),
        CommitType::Auto,
    )
    .await
    .unwrap();

    // We don't expect splits to be merged since the doc mapping was updated
    sandbox
        .wait_for_splits(index_id, Some(vec![SplitState::Published]), 2)
        .await
        .unwrap();
    sandbox.assert_hit_count(index_id, "body:record", 4).await;
    tokio::time::sleep(std::time::Duration::from_secs(2)).await;

    let mut splits = sandbox
        .rest_client(QuickwitService::Metastore)
        .splits(index_id)
        .list(ListSplitsQueryParams {
            split_states: Some(vec![SplitState::Published]),
            ..Default::default()
        })
        .await
        .unwrap();

    splits.sort_by_key(|s| *s.split_metadata.time_range.as_ref().unwrap().start());

    assert_eq!(
        splits[0].split_metadata.time_range,
        Some(1735689600..=1735689601)
    ); // 2025-01-01
    assert_eq!(splits[0].split_metadata.secondary_time_range, None);

    assert_eq!(
        splits[1].split_metadata.time_range,
        Some(1735689602..=1735689603)
    ); // 2025-01-01
    assert_eq!(
        splits[1].split_metadata.secondary_time_range,
        Some(1735776002..=1735776003)
    ); // 2025-01-02

    let response = sandbox
        .rest_client(QuickwitService::Indexer)
        .search(
            index_id,
            SearchRequestQueryString {
                query: "*".to_string(),
                max_hits: 10,
                ..Default::default()
            },
        )
        .await
        .unwrap();
    assert_eq!(response.hits.len(), 4);

    let response = sandbox
        .rest_client(QuickwitService::Indexer)
        .search(
            index_id,
            SearchRequestQueryString {
                query: "ingestion_time:[2025-01-02T00:00:00 TO 2025-01-02T00:00:01]".to_string(),
                max_hits: 10,
                ..Default::default()
            },
        )
        .await
        .unwrap();
    assert_eq!(response.hits.len(), 2);

    let response = sandbox
        .rest_client(QuickwitService::Indexer)
        .search(
            index_id,
            SearchRequestQueryString {
                query: "ingestion_time:[2025-01-02T00:00:02 TO 2025-01-02T00:00:03]".to_string(),
                max_hits: 10,
                ..Default::default()
            },
        )
        .await
        .unwrap();
    assert_eq!(response.hits.len(), 2);

    // Delete the index to avoid potential hanging on shutdown
    sandbox
        .rest_client(QuickwitService::Indexer)
        .indexes()
        .delete(index_id, false)
        .await
        .unwrap();

    sandbox.shutdown().await.unwrap();
}
