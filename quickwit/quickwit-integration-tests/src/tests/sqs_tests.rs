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

use std::io::Write;
use std::iter;
use std::str::FromStr;
use std::time::Duration;

use aws_sdk_sqs::types::QueueAttributeName;
use quickwit_common::test_utils::wait_until_predicate;
use quickwit_common::uri::Uri;
use quickwit_config::service::QuickwitService;
use quickwit_config::ConfigFormat;
use quickwit_indexing::source::sqs_queue::test_helpers as sqs_test_helpers;
use quickwit_metastore::SplitState;
use tempfile::NamedTempFile;

use crate::test_utils::ClusterSandboxBuilder;

fn create_mock_data_file(num_lines: usize) -> (NamedTempFile, Uri) {
    let mut temp_file = tempfile::NamedTempFile::new().unwrap();
    for i in 0..num_lines {
        writeln!(temp_file, "{{\"body\": \"hello {}\"}}", i).unwrap()
    }
    temp_file.flush().unwrap();
    let path = temp_file.path().to_str().unwrap();
    let uri = Uri::from_str(path).unwrap();
    (temp_file, uri)
}

#[tokio::test]
async fn test_sqs_with_duplicates() {
    quickwit_common::setup_logging_for_tests();
    let sandbox = ClusterSandboxBuilder::build_and_start_standalone().await;
    let index_id = "test-sqs-source-duplicates";
    let index_config = format!(
        r#"
            version: 0.8
            index_id: {}
            doc_mapping:
                field_mappings:
                - name: body
                  type: text
            indexing_settings:
                commit_timeout_secs: 3
            "#,
        index_id
    );

    let sqs_client = sqs_test_helpers::get_localstack_sqs_client().await.unwrap();
    let queue_url = sqs_test_helpers::create_queue(&sqs_client, "test-single-node-cluster").await;

    sandbox
        .rest_client(QuickwitService::Indexer)
        .indexes()
        .create(index_config.clone(), ConfigFormat::Yaml, false)
        .await
        .unwrap();

    let source_id: &str = "test-sqs-single-node-cluster";
    let source_config_input = format!(
        r#"
            version: 0.7
            source_id: {}
            desired_num_pipelines: 1
            max_num_pipelines_per_indexer: 1
            source_type: file
            params:
                notifications:
                  - type: sqs
                    queue_url: {}
                    message_type: raw_uri
            input_format: plain_text
        "#,
        source_id, queue_url
    );

    sandbox
        .rest_client(QuickwitService::Indexer)
        .sources(index_id)
        .create(source_config_input, ConfigFormat::Yaml)
        .await
        .unwrap();

    // Send messages with duplicates
    let tmp_mock_data_files: Vec<_> = iter::repeat_with(|| create_mock_data_file(1000))
        .take(10)
        .collect();
    for (_, uri) in &tmp_mock_data_files {
        sqs_test_helpers::send_message(&sqs_client, &queue_url, uri.as_str()).await;
    }
    sqs_test_helpers::send_message(&sqs_client, &queue_url, tmp_mock_data_files[0].1.as_str())
        .await;
    sqs_test_helpers::send_message(&sqs_client, &queue_url, tmp_mock_data_files[5].1.as_str())
        .await;

    sandbox
        .wait_for_splits(index_id, Some(vec![SplitState::Published]), 1)
        .await
        .unwrap();

    sandbox.assert_hit_count(index_id, "", 10 * 1000).await;

    // The two duplicates could not be acknowledged when the were received
    // because at that point the relevant data was not yet committed. Now it is
    // committed, but their visibility timeout will still take a while to be
    // reached.
    wait_until_predicate(
        || async {
            let in_flight_count: usize = sqs_test_helpers::get_queue_attribute(
                &sqs_client,
                &queue_url,
                QueueAttributeName::ApproximateNumberOfMessagesNotVisible,
            )
            .await
            .parse()
            .unwrap();
            in_flight_count == 2
        },
        Duration::from_secs(5),
        Duration::from_millis(100),
    )
    .await
    .expect("number of in-flight messages didn't reach 2 within the timeout");

    sandbox
        .rest_client(QuickwitService::Indexer)
        .indexes()
        .delete(index_id, false)
        .await
        .unwrap();

    sandbox.shutdown().await.unwrap();
}

#[tokio::test]
async fn test_sqs_garbage_collect() {
    quickwit_common::setup_logging_for_tests();
    let sandbox = ClusterSandboxBuilder::build_and_start_standalone().await;
    let index_id = "test-sqs-source-garbage-collect";
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
            "#,
        index_id
    );

    let sqs_client = sqs_test_helpers::get_localstack_sqs_client().await.unwrap();
    let queue_url = sqs_test_helpers::create_queue(&sqs_client, "test-single-node-cluster").await;

    sandbox
        .rest_client(QuickwitService::Indexer)
        .indexes()
        .create(index_config.clone(), ConfigFormat::Yaml, false)
        .await
        .unwrap();

    let source_id: &str = "test-sqs-single-node-cluster";
    let source_config_input = format!(
        r#"
            version: 0.7
            source_id: {}
            desired_num_pipelines: 1
            max_num_pipelines_per_indexer: 1
            source_type: file
            params:
                notifications:
                  - type: sqs
                    queue_url: {}
                    message_type: raw_uri
                    deduplication_window_max_messages: 5
                    deduplication_cleanup_interval_secs: 3
            input_format: plain_text
        "#,
        source_id, queue_url
    );

    sandbox
        .rest_client(QuickwitService::Indexer)
        .sources(index_id)
        .create(source_config_input, ConfigFormat::Yaml)
        .await
        .unwrap();

    let tmp_mock_data_files: Vec<_> = iter::repeat_with(|| create_mock_data_file(1000))
        .take(10)
        .collect();
    for (_, uri) in &tmp_mock_data_files {
        sqs_test_helpers::send_message(&sqs_client, &queue_url, uri.as_str()).await;
    }

    sandbox
        .wait_for_splits(index_id, Some(vec![SplitState::Published]), 1)
        .await
        .unwrap();

    sandbox.assert_hit_count(index_id, "", 10 * 1000).await;

    wait_until_predicate(
        || async {
            let shard_count = sandbox
                .rest_client(QuickwitService::Indexer)
                .sources(index_id)
                .get_shards(source_id)
                .await
                .unwrap()
                .len();
            tracing::info!("shard_count: {}", shard_count);
            shard_count == 5
        },
        Duration::from_secs(6),
        Duration::from_millis(200),
    )
    .await
    .expect("shards where not pruned within the timeout");

    sandbox
        .rest_client(QuickwitService::Indexer)
        .indexes()
        .delete(index_id, false)
        .await
        .unwrap();

    sandbox.shutdown().await.unwrap();
}
