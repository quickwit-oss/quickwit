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
use std::str::FromStr;

use quickwit_common::uri::Uri;
use quickwit_config::ConfigFormat;
use quickwit_indexing::source::sqs_queue::test_helpers as sqs_test_helpers;
use quickwit_metastore::SplitState;
use tempfile::NamedTempFile;

use crate::test_utils::ClusterSandbox;

fn write_to_tmp() -> NamedTempFile {
    let mut temp_file = tempfile::NamedTempFile::new().unwrap();
    for _ in 0..10 {
        temp_file.write_all(br#"{"body": "hello"}"#).unwrap()
    }
    temp_file.flush().unwrap();
    temp_file
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

    let sqs_client = sqs_test_helpers::get_localstack_sqs_client().await.unwrap();
    let queue_url = sqs_test_helpers::create_queue(&sqs_client, "test-single-node-cluster").await;

    sandbox.wait_for_cluster_num_ready_nodes(1).await.unwrap();

    // Create the index.
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

    // Create SQS source
    sandbox
        .indexer_rest_client
        .sources(index_id)
        .create(source_config_input, ConfigFormat::Yaml)
        .await
        .unwrap();

    // Send one message.
    let tmp_file = write_to_tmp();
    let path = tmp_file.path().to_str().unwrap();
    let uri = Uri::from_str(path).unwrap();
    sqs_test_helpers::send_message(&sqs_client, &queue_url, uri.as_str()).await;

    // Wait for the split to be published.
    // TODO check why this is so slow
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

    sandbox.shutdown().await.unwrap();
}
