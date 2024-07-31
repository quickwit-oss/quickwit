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
use quickwit_config::ConfigFormat;
use quickwit_indexing::source::sqs_queue::test_helpers as sqs_test_helpers;
use quickwit_metastore::SplitState;
use quickwit_serve::SearchRequestQueryString;
use tempfile::NamedTempFile;
use tracing::info;

use crate::test_utils::ClusterSandbox;

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
async fn test_sqs_single_node_cluster() {
    tracing_subscriber::fmt::init();
    let sandbox = ClusterSandbox::start_standalone_node().await.unwrap();
    let index_id = "test-sqs-source-single-node-cluster";
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

    info!("create SQS queue");
    let sqs_client = sqs_test_helpers::get_localstack_sqs_client().await.unwrap();
    let queue_url = sqs_test_helpers::create_queue(&sqs_client, "test-single-node-cluster").await;

    sandbox.wait_for_cluster_num_ready_nodes(1).await.unwrap();

    info!("create index");
    sandbox
        .indexer_rest_client
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
                mode: sqs
                queue_url: {}
                message_type: raw_uri
            input_format: plain_text
        "#,
        source_id, queue_url
    );

    info!("create file source with SQS notification");
    sandbox
        .indexer_rest_client
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

    info!("wait for split to be published");
    sandbox
        .wait_for_splits(index_id, Some(vec![SplitState::Published]), 1)
        .await
        .unwrap();

    info!("count docs using search");
    let search_result = sandbox
        .indexer_rest_client
        .search(
            index_id,
            SearchRequestQueryString {
                query: "".to_string(),
                max_hits: 0,
                ..Default::default()
            },
        )
        .await
        .unwrap();
    assert_eq!(search_result.num_hits, 10 * 1000);

    wait_until_predicate(
        || async {
            let queue_attributes = sqs_client
                .get_queue_attributes()
                .queue_url(&queue_url)
                .attribute_names(QueueAttributeName::All)
                .send()
                .await
                .unwrap();
            let in_flight_count: usize = queue_attributes
                .attributes
                .unwrap()
                .get(&QueueAttributeName::ApproximateNumberOfMessagesNotVisible)
                .unwrap()
                .parse()
                .unwrap();
            in_flight_count == 2
        },
        Duration::from_secs(5),
        Duration::from_millis(100),
    )
    .await
    .expect("Number of in-flight messages didn't reach 2 within the timeout");

    info!("delete index");
    sandbox
        .indexer_rest_client
        .indexes()
        .delete(index_id, false)
        .await
        .unwrap();

    sandbox.shutdown().await.unwrap();
}
