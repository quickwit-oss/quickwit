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

use quickwit_common::rand::append_random_suffix;
use quickwit_common::test_utils::wait_until_predicate;
use quickwit_config::ConfigFormat;
use quickwit_config::service::QuickwitService;
use quickwit_metastore::SplitState;
use quickwit_serve::ListSplitsQueryParams;
use rdkafka::admin::{AdminClient, AdminOptions, NewTopic, TopicReplication};
use rdkafka::client::DefaultClientContext;
use rdkafka::config::ClientConfig;
use rdkafka::producer::{FutureProducer, FutureRecord};

use crate::test_utils::ClusterSandboxBuilder;

fn create_admin_client() -> AdminClient<DefaultClientContext> {
    ClientConfig::new()
        .set("bootstrap.servers", "localhost:9092")
        .set("broker.address.family", "v4")
        .create()
        .unwrap()
}

async fn create_topic(
    admin_client: &AdminClient<DefaultClientContext>,
    topic: &str,
    num_partitions: i32,
) -> anyhow::Result<()> {
    admin_client
        .create_topics(
            &[NewTopic::new(
                topic,
                num_partitions,
                TopicReplication::Fixed(1),
            )],
            &AdminOptions::new().operation_timeout(Some(Duration::from_secs(5))),
        )
        .await?
        .into_iter()
        .collect::<Result<Vec<_>, _>>()
        .map_err(|(topic, err_code)| {
            anyhow::anyhow!(
                "failed to create topic `{}`. error code: `{}`",
                topic,
                err_code
            )
        })?;
    Ok(())
}

async fn populate_topic(topic: &str) -> anyhow::Result<()> {
    let producer: &FutureProducer = &ClientConfig::new()
        .set("bootstrap.servers", "localhost:9092")
        .set("broker.address.family", "v4")
        .set("message.timeout.ms", "30000")
        .create()?;

    let message = r#"{"message":"test","id":1}"#;

    producer
        .send(
            FutureRecord {
                topic,
                partition: None,
                timestamp: None,
                key: None::<&[u8]>,
                payload: Some(message),
                headers: None,
            },
            Duration::from_secs(5),
        )
        .await
        .map_err(|(err, _)| err)?;
    Ok(())
}

#[tokio::test]
async fn test_kafka_source() {
    quickwit_common::setup_logging_for_tests();

    let sandbox = ClusterSandboxBuilder::build_and_start_standalone().await;
    let index_id = append_random_suffix("test-kafka-source");
    let topic = append_random_suffix("test-kafka-source-topic");

    let admin_client = create_admin_client();
    create_topic(&admin_client, &topic, 1).await.unwrap();

    let index_config = format!(
        r#"
            version: 0.8
            index_id: {index_id}
            doc_mapping:
                field_mappings:
                - name: message
                  type: text
                - name: id
                  type: i64
            indexing_settings:
                commit_timeout_secs: 3
            "#
    );

    sandbox
        .rest_client(QuickwitService::Indexer)
        .indexes()
        .create(index_config.clone(), ConfigFormat::Yaml, false)
        .await
        .unwrap();

    let source_id = "test-kafka-source-no-override";
    let source_config = format!(
        r#"
            version: 0.7
            source_id: {source_id}
            desired_num_pipelines: 1
            max_num_pipelines_per_indexer: 1
            source_type: kafka
            params:
                topic: {topic}
                client_params:
                    bootstrap.servers: localhost:9092
                    broker.address.family: v4
                    auto.offset.reset: earliest
                    enable.auto.commit: false
            input_format: json
        "#
    );

    sandbox
        .rest_client(QuickwitService::Indexer)
        .sources(&index_id)
        .create(source_config, ConfigFormat::Yaml)
        .await
        .unwrap();

    populate_topic(&topic).await.unwrap();

    let result = wait_until_predicate(
        || async {
            let splits_query_params = ListSplitsQueryParams {
                split_states: Some(vec![SplitState::Published]),
                ..Default::default()
            };
            sandbox
                .rest_client(QuickwitService::Indexer)
                .splits(&index_id)
                .list(splits_query_params)
                .await
                .map(|splits| !splits.is_empty())
                .unwrap_or(false)
        },
        Duration::from_secs(15),
        Duration::from_millis(500),
    )
    .await;

    assert!(
        result.is_ok(),
        "Splits should be published within 15 seconds using index config settings"
    );

    sandbox.assert_hit_count(&index_id, "", 1).await;

    sandbox
        .rest_client(QuickwitService::Indexer)
        .indexes()
        .delete(&index_id, false)
        .await
        .unwrap();

    sandbox.shutdown().await.unwrap();
}

#[tokio::test]
async fn test_kafka_source_with_indexing_settings_override() {
    quickwit_common::setup_logging_for_tests();

    let sandbox = ClusterSandboxBuilder::build_and_start_standalone().await;
    let index_id = append_random_suffix("test-kafka-indexing-settings-override");
    let topic = append_random_suffix("test-kafka-indexing-settings-override-topic");

    let admin_client = create_admin_client();
    create_topic(&admin_client, &topic, 1).await.unwrap();

    // Create index with high commit_timeout (300 seconds)
    // This would normally mean splits take 5 minutes to commit
    let index_config = format!(
        r#"
            version: 0.8
            index_id: {index_id}
            doc_mapping:
                field_mappings:
                - name: message
                  type: text
                - name: id
                  type: i64
            indexing_settings:
                commit_timeout_secs: 300
            "#
    );

    sandbox
        .rest_client(QuickwitService::Indexer)
        .indexes()
        .create(index_config.clone(), ConfigFormat::Yaml, false)
        .await
        .unwrap();

    // Create Kafka source with indexing_settings override to lower commit_timeout to 3 seconds
    // This tests that the source-level override works correctly
    let source_id = "test-kafka-source";
    let source_config = format!(
        r#"
            version: 0.7
            source_id: {source_id}
            desired_num_pipelines: 1
            max_num_pipelines_per_indexer: 1
            source_type: kafka
            params:
                topic: {topic}
                client_params:
                    bootstrap.servers: localhost:9092
                    broker.address.family: v4
                    auto.offset.reset: earliest
                    enable.auto.commit: false
                    indexing_settings:
                        commit_timeout_secs: 3
            input_format: json
        "#
    );

    sandbox
        .rest_client(QuickwitService::Indexer)
        .sources(&index_id)
        .create(source_config, ConfigFormat::Yaml)
        .await
        .unwrap();

    populate_topic(&topic).await.unwrap();

    let result = wait_until_predicate(
        || async {
            let splits_query_params = ListSplitsQueryParams {
                split_states: Some(vec![SplitState::Published]),
                ..Default::default()
            };
            sandbox
                .rest_client(QuickwitService::Indexer)
                .splits(&index_id)
                .list(splits_query_params)
                .await
                .map(|splits| !splits.is_empty())
                .unwrap_or(false)
        },
        Duration::from_secs(15),
        Duration::from_millis(500),
    )
    .await;

    assert!(
        result.is_ok(),
        "Splits should be published within 15 seconds when using indexing_settings override. If \
         this test fails, the override may not be working correctly."
    );

    sandbox.assert_hit_count(&index_id, "", 1).await;

    sandbox
        .rest_client(QuickwitService::Indexer)
        .indexes()
        .delete(&index_id, false)
        .await
        .unwrap();

    sandbox.shutdown().await.unwrap();
}
