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

use std::collections::BTreeMap;
use std::fmt;
use std::time::{Duration, Instant};

use anyhow::{anyhow, Context};
use async_trait::async_trait;
use bytes::Bytes;
use futures::StreamExt;
use pulsar::authentication::oauth2::{OAuth2Authentication, OAuth2Params};
use pulsar::consumer::Message;
use pulsar::message::proto::MessageIdData;
use pulsar::{
    Authentication, Consumer, DeserializeMessage, Payload, Pulsar, SubType, TokioExecutor,
};
use quickwit_actors::{ActorContext, ActorExitStatus, Mailbox};
use quickwit_config::{PulsarSourceAuth, PulsarSourceParams};
use quickwit_metastore::checkpoint::{PartitionId, SourceCheckpoint};
use quickwit_proto::metastore::SourceType;
use quickwit_proto::types::{IndexUid, Position};
use serde_json::{json, Value as JsonValue};
use tokio::time;
use tracing::{debug, info, warn};

use crate::actors::DocProcessor;
use crate::source::{
    BatchBuilder, Source, SourceActor, SourceContext, SourceRuntime, TypedSourceFactory,
    BATCH_NUM_BYTES_LIMIT, EMIT_BATCHES_TIMEOUT,
};

type PulsarConsumer = Consumer<PulsarMessage, TokioExecutor>;

pub struct PulsarSourceFactory;

#[async_trait]
impl TypedSourceFactory for PulsarSourceFactory {
    type Source = PulsarSource;
    type Params = PulsarSourceParams;

    async fn typed_create_source(
        source_runtime: SourceRuntime,
        source_params: PulsarSourceParams,
    ) -> anyhow::Result<Self::Source> {
        PulsarSource::try_new(source_runtime, source_params).await
    }
}

#[derive(Default, Debug)]
pub struct PulsarSourceState {
    /// Number of bytes processed by the source.
    pub num_bytes_processed: u64,
    /// Number of messages processed by the source (including invalid messages).
    pub num_messages_processed: u64,
    /// Number of invalid messages, i.e., that were empty or could not be parsed.
    pub num_invalid_messages: u64,
    /// The number of messages that were skipped due to the message being older
    /// than the current checkpoint position
    pub num_skipped_messages: u64,
}

pub struct PulsarSource {
    source_runtime: SourceRuntime,
    source_params: PulsarSourceParams,
    pulsar_consumer: PulsarConsumer,
    subscription_name: String,
    current_positions: BTreeMap<PartitionId, Position>,
    state: PulsarSourceState,
}

impl fmt::Debug for PulsarSource {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.debug_struct("PulsarSource")
            .field("index_uid", self.source_runtime.index_uid())
            .field("source_id", &self.source_runtime.source_id())
            .field("subscription_name", &self.subscription_name)
            .field("topics", &self.source_params.topics.join(", "))
            .finish()
    }
}

impl PulsarSource {
    pub async fn try_new(
        source_runtime: SourceRuntime,
        source_params: PulsarSourceParams,
    ) -> anyhow::Result<Self> {
        let subscription_name =
            subscription_name(source_runtime.index_uid(), source_runtime.source_id());
        info!(
            index_id=%source_runtime.index_id(),
            source_id=%source_runtime.source_id(),
            topics=?source_params.topics,
            subscription_name=%subscription_name,
            "Create Pulsar source."
        );
        let pulsar = connect_pulsar(&source_params).await?;
        let checkpoint = source_runtime.fetch_checkpoint().await?;

        // Current positions are built mapping the topic ID to the last-saved
        // message ID, pulsar ensures these topics (and topic partitions) are
        // unique so that we don't inadvertently clash.
        let mut current_positions = BTreeMap::new();
        for topic in source_params.topics.iter() {
            let partitions = pulsar.lookup_partitioned_topic(topic).await?;

            for (partition, _) in partitions {
                let partition_id = PartitionId::from(partition);
                let position_opt = checkpoint.position_for_partition(&partition_id).cloned();

                if let Some(position) = position_opt {
                    current_positions.insert(partition_id, position);
                }
            }
        }
        let pulsar_consumer = create_pulsar_consumer(
            subscription_name.clone(),
            source_params.clone(),
            pulsar,
            current_positions.clone(),
        )
        .await?;

        Ok(Self {
            source_runtime,
            source_params,
            pulsar_consumer,
            subscription_name,
            current_positions,
            state: PulsarSourceState::default(),
        })
    }

    fn process_message(
        &mut self,
        message: Message<PulsarMessage>,
        batch: &mut BatchBuilder,
    ) -> anyhow::Result<()> {
        let current_position = msg_id_to_position(message.message_id());
        let doc = message.deserialize();
        self.add_doc_to_batch(&message.topic, current_position, doc, batch)
    }

    fn add_doc_to_batch(
        &mut self,
        topic: &str,
        msg_position: Position,
        doc: Bytes,
        batch: &mut BatchBuilder,
    ) -> anyhow::Result<()> {
        if doc.is_empty() {
            warn!("message received from queue was empty");
            self.state.num_invalid_messages += 1;
            return Ok(());
        }

        let partition = PartitionId::from(topic);
        let num_bytes = doc.len() as u64;

        if let Some(current_position) = self.current_positions.get(&partition) {
            // We skip messages older or equal to the current recorded position.
            // This is because Pulsar may replay messages which have not yet been acknowledged but
            // are in the process of being published, this can occur in situations like pulsar
            // re-balancing topic partitions if a node leaves, node failure, etc...
            if &msg_position <= current_position {
                self.state.num_skipped_messages += 1;
                return Ok(());
            }
        }

        let current_position = self
            .current_positions
            .insert(partition.clone(), msg_position.clone())
            .unwrap_or(Position::Beginning);

        batch
            .checkpoint_delta
            .record_partition_delta(partition, current_position, msg_position)
            .context("failed to record partition delta")?;
        batch.add_doc(doc);

        self.state.num_bytes_processed += num_bytes;
        self.state.num_messages_processed += 1;

        Ok(())
    }

    async fn try_ack_messages(&mut self, checkpoint: SourceCheckpoint) -> anyhow::Result<()> {
        debug!(ckpt = ?checkpoint, "truncating message queue");
        for (partition, position) in checkpoint.iter() {
            if let Some(msg_id) = msg_id_from_position(&position) {
                self.pulsar_consumer
                    .cumulative_ack_with_id(partition.0.as_ref(), msg_id)
                    .await?;
            }
        }
        Ok(())
    }
}

#[async_trait]
impl Source for PulsarSource {
    async fn emit_batches(
        &mut self,
        doc_processor_mailbox: &Mailbox<DocProcessor>,
        ctx: &SourceContext,
    ) -> Result<Duration, ActorExitStatus> {
        let now = Instant::now();
        let mut batch_builder = BatchBuilder::new(SourceType::Pulsar);
        let deadline = time::sleep(*EMIT_BATCHES_TIMEOUT);
        tokio::pin!(deadline);

        loop {
            tokio::select! {
                // This does not actually acquire the lock of the mutex internally
                // we're using the mutex in order to convince the Rust compiler
                // that we can use the consumer within this Sync context.
                message = self.pulsar_consumer.next() => {
                    let message = message
                        .ok_or_else(|| ActorExitStatus::from(anyhow!("consumer was dropped")))?
                        .map_err(|e| ActorExitStatus::from(anyhow!("failed to get message from consumer: {:?}", e)))?;

                    self.process_message(message, &mut batch_builder).map_err(ActorExitStatus::from)?;

                    if batch_builder.num_bytes >= BATCH_NUM_BYTES_LIMIT {
                        break;
                    }
                }
                _ = &mut deadline => {
                    break;
                }
            }
            ctx.record_progress();
        }

        if !batch_builder.checkpoint_delta.is_empty() {
            debug!(
                num_docs=%batch_builder.docs.len(),
                num_bytes=%batch_builder.num_bytes,
                num_millis=%now.elapsed().as_millis(),
                "sending doc batch to indexer"
            );
            let message = batch_builder.build();
            ctx.send_message(doc_processor_mailbox, message).await?;
        }
        Ok(Duration::default())
    }

    async fn suggest_truncate(
        &mut self,
        checkpoint: SourceCheckpoint,
        _ctx: &ActorContext<SourceActor>,
    ) -> anyhow::Result<()> {
        self.try_ack_messages(checkpoint).await
    }

    fn name(&self) -> String {
        format!("{:?}", self)
    }

    async fn finalize(
        &mut self,
        _exit_status: &ActorExitStatus,
        _ctx: &SourceContext,
    ) -> anyhow::Result<()> {
        self.pulsar_consumer.close().await?;
        Ok(())
    }

    fn observable_state(&self) -> JsonValue {
        json!({
            "index_id": self.source_runtime.index_id(),
            "source_id": self.source_runtime.source_id(),
            "topics": self.source_params.topics,
            "subscription_name": self.subscription_name,
            "consumer_name": self.source_params.consumer_name,
            "num_bytes_processed": self.state.num_bytes_processed,
            "num_messages_processed": self.state.num_messages_processed,
            "num_invalid_messages": self.state.num_invalid_messages,
        })
    }
}

#[derive(Debug)]
struct PulsarMessage;

impl DeserializeMessage for PulsarMessage {
    type Output = Bytes;

    fn deserialize_message(payload: &Payload) -> Self::Output {
        Bytes::from(payload.data.clone())
    }
}

#[tracing::instrument(name = "pulsar-consumer", skip(pulsar))]
/// Creates a new pulsar consumer
async fn create_pulsar_consumer(
    subscription_name: String,
    params: PulsarSourceParams,
    pulsar: Pulsar<TokioExecutor>,
    current_positions: BTreeMap<PartitionId, Position>,
) -> anyhow::Result<PulsarConsumer> {
    let mut consumer: Consumer<PulsarMessage, _> = pulsar
        .consumer()
        .with_topics(&params.topics)
        .with_consumer_name(&params.consumer_name)
        .with_subscription(subscription_name)
        .with_subscription_type(SubType::Failover)
        .build()
        .await?;

    let consumer_ids = consumer
        .consumer_id()
        .into_iter()
        .map(|id| id.to_string())
        .collect::<Vec<_>>();
    info!(positions = ?current_positions, "seeking to last checkpoint positions");
    for (_, position) in current_positions {
        let seek_to = msg_id_from_position(&position);

        if seek_to.is_some() {
            consumer
                .seek(Some(consumer_ids.clone()), seek_to, None, pulsar.clone())
                .await?;
        }
    }
    Ok(consumer)
}

fn msg_id_to_position(msg: &MessageIdData) -> Position {
    // The order of these fields are important as they affect the sorting
    // of the checkpoint positions.
    //
    // The key parts of the ID used for ordering are:
    // - The ledger ID which is a sequentially increasing ID.
    // - The entry ID the unique ID of the message within the ledger.
    // - The batch position for the current chunk of messages.
    //
    // The remaining keys are not required for sorting but are required
    // in order to re-construct the message ID in order to send back to pulsar.
    // The ledger_id, entry_id and the batch_index form a unique composite key which will
    // prevent the remaining parts of the ID from interfering with the sorting.
    let position_str = format!(
        "{:0>20},{:0>20},{},{},{}",
        msg.ledger_id,
        msg.entry_id,
        msg.batch_index
            .map(|v| format!("{v:010}"))
            .unwrap_or_default(),
        msg.partition
            .and_then(|v| if v < 0 {
                None
            } else {
                Some(format!("{v:010}"))
            })
            .unwrap_or_default(),
        msg.batch_size
            .map(|v| format!("{v:010}"))
            .unwrap_or_default(),
    );

    Position::from(position_str)
}

fn msg_id_from_position(position: &Position) -> Option<MessageIdData> {
    let Position::Offset(offset) = position else {
        return None;
    };
    let mut parts = offset.as_str().split(',');

    let ledger_id = parts.next()?.parse::<u64>().ok()?;
    let entry_id = parts.next()?.parse::<u64>().ok()?;
    let batch_index = parts.next()?.parse::<i32>().ok();
    let partition = parts.next()?.parse::<i32>().unwrap_or(-1);
    let batch_size = parts.next()?.parse::<i32>().ok();

    Some(MessageIdData {
        ledger_id,
        entry_id,
        batch_index,
        batch_size,
        partition: Some(partition),
        ack_set: Vec::new(),
        first_chunk_message_id: None,
    })
}

async fn connect_pulsar(params: &PulsarSourceParams) -> anyhow::Result<Pulsar<TokioExecutor>> {
    let mut builder = Pulsar::builder(&params.address, TokioExecutor);

    match params.authentication.clone() {
        None => {}
        Some(PulsarSourceAuth::Token(token)) => {
            let auth = Authentication {
                name: "token".to_string(),
                data: token.as_bytes().to_vec(),
            };

            builder = builder.with_auth(auth);
        }
        Some(PulsarSourceAuth::Oauth2 {
            issuer_url,
            credentials_url,
            audience,
            scope,
        }) => {
            let auth = OAuth2Params {
                issuer_url,
                credentials_url,
                audience,
                scope,
            };
            builder = builder.with_auth_provider(OAuth2Authentication::client_credentials(auth));
        }
    }
    let pulsar: Pulsar<_> = builder.build().await?;
    Ok(pulsar)
}

/// Checks whether we can establish a connection to the pulsar broker.
pub(crate) async fn check_connectivity(params: &PulsarSourceParams) -> anyhow::Result<()> {
    connect_pulsar(params).await?;
    Ok(())
}

fn subscription_name(index_uid: &IndexUid, source_id: &str) -> String {
    format!("quickwit-{index_uid}-{source_id}")
}

#[cfg(all(test, feature = "pulsar-broker-tests"))]
mod pulsar_broker_tests {
    use std::collections::HashSet;
    use std::num::NonZeroUsize;
    use std::ops::Range;

    use futures::future::join_all;
    use quickwit_actors::{ActorHandle, Inbox, Universe, HEARTBEAT};
    use quickwit_common::rand::append_random_suffix;
    use quickwit_config::{SourceConfig, SourceInputFormat, SourceParams};
    use quickwit_metastore::checkpoint::{PartitionId, SourceCheckpointDelta};
    use quickwit_metastore::metastore_for_test;
    use quickwit_proto::metastore::MetastoreServiceClient;
    use reqwest::StatusCode;

    use super::*;
    use crate::source::pulsar_source::{msg_id_from_position, msg_id_to_position};
    use crate::source::test_setup_helper::setup_index;
    use crate::source::tests::SourceRuntimeBuilder;
    use crate::source::{quickwit_supported_sources, RawDocBatch, SuggestTruncate};

    static PULSAR_URI: &str = "pulsar://localhost:6650";
    static PULSAR_ADMIN_URI: &str = "http://localhost:8081";
    static CLIENT_NAME: &str = "quickwit-tester";

    macro_rules! positions {
        ($($partition:expr => $position:expr $(,)?)*) => {{
            let mut positions = BTreeMap::new();
            $(
                positions.insert(PartitionId::from($partition), Position::offset($position));
            )*
            positions
        }};
    }

    macro_rules! checkpoints {
        ($($partition:expr => $position:expr $(,)?)*) => {{
            let mut checkpoint = SourceCheckpointDelta::default();
            $(
                checkpoint.record_partition_delta(
                    PartitionId::from($partition),
                    Position::Beginning,
                    $position,
                ).unwrap();
            )*
            checkpoint
        }};
    }

    fn get_source_config<S: AsRef<str>>(
        topics: impl IntoIterator<Item = S>,
    ) -> (String, SourceConfig) {
        let source_id = append_random_suffix("test-pulsar-source--source");
        let source_config = SourceConfig {
            source_id: source_id.clone(),
            num_pipelines: NonZeroUsize::new(1).unwrap(),
            enabled: true,
            source_params: SourceParams::Pulsar(PulsarSourceParams {
                topics: topics.into_iter().map(|v| v.as_ref().to_string()).collect(),
                address: PULSAR_URI.to_string(),
                consumer_name: CLIENT_NAME.to_string(),
                authentication: None,
            }),
            transform_config: None,
            input_format: SourceInputFormat::Json,
        };
        (source_id, source_config)
    }

    fn merge_doc_batches(batches: Vec<RawDocBatch>) -> RawDocBatch {
        let mut merged_batch = RawDocBatch::default();
        for batch in batches {
            merged_batch.docs.extend(batch.docs);
            merged_batch
                .checkpoint_delta
                .extend(batch.checkpoint_delta)
                .unwrap();
        }
        merged_batch.docs.sort();
        merged_batch
    }

    struct TopicData {
        messages: Vec<String>,
        expected_position: Position,
    }

    impl TopicData {
        fn num_bytes(&self) -> usize {
            self.messages
                .iter()
                .map(|v| v.as_bytes().len())
                .sum::<usize>()
        }

        fn len(&self) -> usize {
            self.messages.len()
        }
    }

    /// Populates a given set of topics with messages produced by closure `M`
    ///
    /// A set of messages and it's expected last checkpoint position is returned
    /// for each topic provided.
    async fn populate_topic<'a, S: AsRef<str> + 'a, M>(
        topics: impl IntoIterator<Item = S>,
        range_message_ids: Range<usize>,
        message_fn: M,
    ) -> anyhow::Result<Vec<TopicData>>
    where
        M: Fn(&str, usize) -> JsonValue,
    {
        let client = Pulsar::builder(PULSAR_URI, TokioExecutor).build().await?;

        let mut pending_messages = Vec::new();
        for topic in topics {
            let mut topic_messages = Vec::with_capacity(range_message_ids.len());
            let mut producer = client
                .producer()
                .with_name(append_random_suffix(CLIENT_NAME))
                .with_topic(topic.as_ref())
                .build()
                .await?;

            for id in range_message_ids.clone() {
                let msg = (message_fn)(topic.as_ref(), id).to_string();
                topic_messages.push(msg);
            }

            let futures = producer.send_all(topic_messages.clone()).await?;
            let receipts = join_all(futures).await;

            let mut last_expected_position = Position::Beginning;
            for result in receipts {
                let msg_id = result?.message_id.unwrap();
                last_expected_position = msg_id_to_position(&msg_id);
            }

            topic_messages.sort();
            pending_messages.push(TopicData {
                messages: topic_messages,
                expected_position: last_expected_position,
            });
            producer.close().await.expect("Close connection.");
        }

        Ok(pending_messages)
    }

    async fn wait_for_completion(
        source_handle: ActorHandle<SourceActor>,
        num_expected: usize,
        partition: PartitionId,
        truncate_to: Position,
    ) -> JsonValue {
        loop {
            let observation = source_handle.observe().await;
            let value = observation.state;
            let num_messages_processed = value
                .get("num_messages_processed")
                .unwrap()
                .as_u64()
                .unwrap();
            if num_messages_processed >= num_expected as u64 {
                break;
            }
            tokio::time::sleep(Duration::from_secs(1)).await;
        }

        let mut checkpoint = SourceCheckpoint::default();
        checkpoint
            .try_apply_delta(checkpoints!(partition => truncate_to))
            .expect("Create checkpoint");
        let truncate = SuggestTruncate(checkpoint);
        source_handle
            .mailbox()
            .send_message(truncate)
            .await
            .expect("Truncate");

        let (_exit_status, exit_state) = source_handle.quit().await;
        exit_state
    }

    async fn create_partitioned_topic(topic: &str, num_partitions: usize) {
        let client = reqwest::Client::new();
        let res = client
            .put(format!(
                "{PULSAR_ADMIN_URI}/admin/v2/persistent/public/default/{topic}/partitions"
            ))
            .body(num_partitions.to_string())
            .header("content-type", b"application/json".as_ref())
            .send()
            .await
            .expect("Send admin request");

        assert_eq!(
            res.status(),
            StatusCode::NO_CONTENT,
            "Expect 204 status code."
        );
    }

    async fn create_source(
        universe: &Universe,
        _metastore: MetastoreServiceClient,
        index_uid: IndexUid,
        source_config: SourceConfig,
        _start_checkpoint: SourceCheckpoint,
    ) -> anyhow::Result<(ActorHandle<SourceActor>, Inbox<DocProcessor>)> {
        let source_loader = quickwit_supported_sources();
        let source_runtime = SourceRuntimeBuilder::new(index_uid, source_config).build();
        let source = source_loader.load_source(source_runtime).await?;
        let (doc_processor_mailbox, doc_processor_inbox) = universe.create_test_mailbox();
        let source_actor = SourceActor {
            source,
            doc_processor_mailbox,
        };
        let (_source_mailbox, source_handle) = universe.spawn_builder().spawn(source_actor);

        Ok((source_handle, doc_processor_inbox))
    }

    fn message_generator(topic: &str, id: usize) -> JsonValue {
        json!({
            "id": id.to_string(),
            "topic": topic,
            "timestamp": 1674515715,
            "body": "Hello, world! This is some test data.",
        })
    }

    fn count_unique_messages_in_batches(batches: &[RawDocBatch]) -> usize {
        let message_ids_topic: HashSet<String> = batches
            .iter()
            .flat_map(|batch| &batch.docs)
            .map(|doc| {
                let json_doc = serde_json::from_slice::<serde_json::Value>(doc).unwrap();
                let id: &str = json_doc.get("id").unwrap().as_str().unwrap();
                let topic: &str = json_doc.get("topic").unwrap().as_str().unwrap();
                format!("{id}-{topic}")
            })
            .collect();
        message_ids_topic.len()
    }

    #[test]
    fn test_position_serialization() {
        let populated_id = MessageIdData {
            ledger_id: 1,
            entry_id: 134,
            batch_index: Some(3),
            partition: Some(-1),
            batch_size: Some(6),

            // We never serialize these fields.
            ack_set: Vec::new(),
            first_chunk_message_id: None,
        };

        let position = msg_id_to_position(&populated_id);
        assert_eq!(
            position.to_string(),
            format!("{:0>20},{:0>20},{:010},,{:010}", 1, 134, 3, 6)
        );
        let retrieved_id = msg_id_from_position(&position)
            .expect("Successfully deserialize message ID from position.");
        assert_eq!(retrieved_id, populated_id);

        let partitioned_id = MessageIdData {
            ledger_id: 1,
            entry_id: 134,
            batch_index: Some(3),
            partition: Some(5),
            batch_size: Some(6),

            // We never serialize these fields.
            ack_set: Vec::new(),
            first_chunk_message_id: None,
        };

        let position = msg_id_to_position(&partitioned_id);
        assert_eq!(
            position.to_string(),
            format!("{:0>20},{:0>20},{:010},{:010},{:010}", 1, 134, 3, 5, 6)
        );
        let retrieved_id = msg_id_from_position(&position)
            .expect("Successfully deserialize message ID from position.");
        assert_eq!(retrieved_id, partitioned_id);

        let sparse_id = MessageIdData {
            ledger_id: 1,
            entry_id: 4,
            batch_index: None,
            partition: Some(-1),
            batch_size: Some(0),

            // We never serialize these fields.
            ack_set: Vec::new(),
            first_chunk_message_id: None,
        };

        let position = msg_id_to_position(&sparse_id);
        assert_eq!(
            position.to_string(),
            format!("{:0>20},{:0>20},,,{:010}", 1, 4, 0)
        );
        let retrieved_id = msg_id_from_position(&position)
            .expect("Successfully deserialize message ID from position.");
        assert_eq!(retrieved_id, sparse_id);
    }

    #[tokio::test]
    async fn test_doc_batching_logic() {
        let topic = append_random_suffix("test-pulsar-source-topic");

        let index_id = append_random_suffix("test-pulsar-source-index");
        let index_uid = IndexUid::new_with_random_ulid(&index_id);
        let (_source_id, source_config) = get_source_config([&topic]);
        let params = if let SourceParams::Pulsar(params) = source_config.clone().source_params {
            params
        } else {
            unreachable!()
        };

        let source_runtime = SourceRuntimeBuilder::new(index_uid, source_config).build();
        let mut pulsar_source = PulsarSource::try_new(source_runtime, params)
            .await
            .expect("Setup pulsar source");

        let position = Position::Beginning;
        let mut batch = BatchBuilder::new(SourceType::Pulsar);
        pulsar_source
            .add_doc_to_batch(&topic, position, Bytes::from_static(b""), &mut batch)
            .expect("Add batch should not error on empty doc.");
        assert_eq!(pulsar_source.state.num_invalid_messages, 1);
        assert_eq!(pulsar_source.state.num_messages_processed, 0);
        assert_eq!(pulsar_source.state.num_bytes_processed, 0);
        assert!(pulsar_source.current_positions.is_empty());
        assert_eq!(batch.num_bytes, 0);
        assert!(batch.docs.is_empty());

        let position = Position::offset(1u64); // Used for testing simplicity.
        let mut batch = BatchBuilder::new(SourceType::Pulsar);
        let doc = Bytes::from_static(b"some-demo-data");
        pulsar_source
            .add_doc_to_batch(&topic, position, doc, &mut batch)
            .expect("Add batch should not error on empty doc.");

        assert_eq!(pulsar_source.state.num_invalid_messages, 1);
        assert_eq!(pulsar_source.state.num_messages_processed, 1);
        assert_eq!(pulsar_source.state.num_bytes_processed, 14);
        assert_eq!(
            pulsar_source.current_positions,
            positions!(topic.as_str() => 1u64)
        );
        assert_eq!(batch.num_bytes, 14);
        assert_eq!(batch.docs.len(), 1);

        let position = Position::offset(4u64); // Used for testing simplicity.
        let mut batch = BatchBuilder::new(SourceType::Pulsar);
        let doc = Bytes::from_static(b"some-demo-data-2");
        pulsar_source
            .add_doc_to_batch(&topic, position, doc, &mut batch)
            .expect("Add batch should not error on empty doc.");
        assert_eq!(pulsar_source.state.num_invalid_messages, 1);
        assert_eq!(pulsar_source.state.num_messages_processed, 2);
        assert_eq!(pulsar_source.state.num_bytes_processed, 30);
        assert_eq!(
            pulsar_source.current_positions,
            positions!(topic.as_str() => 4u64)
        );
        assert_eq!(batch.num_bytes, 16);
        assert_eq!(batch.docs.len(), 1);

        let mut expected_checkpoint_delta = SourceCheckpointDelta::default();
        expected_checkpoint_delta
            .record_partition_delta(
                PartitionId::from(topic.as_str()),
                Position::offset(1u64),
                Position::offset(4u64),
            )
            .unwrap();
        assert_eq!(batch.checkpoint_delta, expected_checkpoint_delta);
    }

    #[tokio::test]
    async fn test_topic_ingestion() {
        let universe = Universe::with_accelerated_time();
        let metastore = metastore_for_test();
        let topic = append_random_suffix("test-pulsar-source--topic-ingestion--topic");

        let index_id = append_random_suffix("test-pulsar-source--topic-ingestion--index");
        let (source_id, source_config) = get_source_config([&topic]);

        let index_uid = setup_index(metastore.clone(), &index_id, &source_config, &[]).await;

        let (source_handle, doc_processor_inbox) = create_source(
            &universe,
            metastore,
            index_uid.clone(),
            source_config,
            SourceCheckpoint::default(),
        )
        .await
        .expect("Create source");

        let expected_docs = populate_topic([&topic], 0..10, message_generator)
            .await
            .unwrap();

        let exit_state = wait_for_completion(
            source_handle,
            expected_docs[0].len(),
            PartitionId::from(topic.clone()),
            expected_docs[0].expected_position.clone(),
        )
        .await;
        let messages: Vec<RawDocBatch> = doc_processor_inbox.drain_for_test_typed();
        assert!(!messages.is_empty());

        let batch = merge_doc_batches(messages);
        assert_eq!(batch.docs, expected_docs[0].messages);
        assert_eq!(
            batch.checkpoint_delta,
            checkpoints!(topic.as_str() => expected_docs[0].expected_position.clone())
        );

        let num_bytes = expected_docs[0].num_bytes();
        let expected_state = json!({
            "index_id": index_id,
            "source_id": source_id,
            "topics": vec![topic],
            "subscription_name": subscription_name(&index_uid, &source_id),
            "consumer_name": CLIENT_NAME,
            "num_bytes_processed": num_bytes,
            "num_messages_processed": 10,
            "num_invalid_messages": 0,
        });
        assert_eq!(exit_state, expected_state);
    }

    #[tokio::test]
    async fn test_multi_topic_ingestion() {
        let universe = Universe::with_accelerated_time();
        let metastore = metastore_for_test();
        let topic1 = append_random_suffix("test-pulsar-source--topic-ingestion--topic");
        let topic2 = append_random_suffix("test-pulsar-source--topic-ingestion--topic");

        let index_id = append_random_suffix("test-pulsar-source--topic-ingestion--index");
        let (source_id, source_config) = get_source_config([&topic1, &topic2]);

        let index_uid = setup_index(metastore.clone(), &index_id, &source_config, &[]).await;

        let (source_handle, doc_processor_inbox) = create_source(
            &universe,
            metastore,
            index_uid.clone(),
            source_config,
            SourceCheckpoint::default(),
        )
        .await
        .expect("Create source");

        let expected_docs = populate_topic([&topic1, &topic2], 0..10, message_generator)
            .await
            .unwrap();

        let mut combined_messages = expected_docs
            .iter()
            .flat_map(|v| &v.messages)
            .cloned()
            .collect::<Vec<_>>();
        combined_messages.sort();

        let exit_state = wait_for_completion(
            source_handle,
            combined_messages.len(),
            PartitionId::from(topic1.clone()),
            expected_docs[0].expected_position.clone(),
        )
        .await;
        let messages: Vec<RawDocBatch> = doc_processor_inbox.drain_for_test_typed();
        assert!(!messages.is_empty());

        let batch = merge_doc_batches(messages);
        assert_eq!(batch.docs, combined_messages);
        assert_eq!(
            batch.checkpoint_delta,
            checkpoints! {
                topic1.as_str() => expected_docs[0].expected_position.clone(),
                topic2.as_str() => expected_docs[1].expected_position.clone(),
            }
        );

        let num_bytes = expected_docs[0].num_bytes() + expected_docs[1].num_bytes();
        let expected_state = json!({
            "index_id": index_id,
            "source_id": source_id,
            "topics": vec![topic1, topic2],
            "subscription_name": subscription_name(&index_uid, &source_id),
            "consumer_name": CLIENT_NAME,
            "num_bytes_processed": num_bytes,
            "num_messages_processed": 20,
            "num_invalid_messages": 0,
        });
        assert_eq!(exit_state, expected_state);
    }

    #[tokio::test]
    async fn test_partitioned_topic_single_consumer_ingestion() {
        let universe = Universe::with_accelerated_time();
        let metastore = metastore_for_test();
        let topic = append_random_suffix("test-pulsar-source--partitioned-single-consumer--topic");

        let index_id =
            append_random_suffix("test-pulsar-source--partitioned-single-consumer--index");
        let (source_id, source_config) = get_source_config([&topic]);

        create_partitioned_topic(&topic, 2).await;
        let index_uid = setup_index(metastore.clone(), &index_id, &source_config, &[]).await;

        let (source_handle, doc_processor_inbox) = create_source(
            &universe,
            metastore,
            index_uid.clone(),
            source_config,
            SourceCheckpoint::default(),
        )
        .await
        .expect("Create source");

        let expected_docs = populate_topic([&topic], 0..10, message_generator)
            .await
            .unwrap();

        let exit_state = wait_for_completion(
            source_handle,
            expected_docs.len(),
            PartitionId::from(topic.clone()),
            expected_docs[0].expected_position.clone(),
        )
        .await;
        let messages: Vec<RawDocBatch> = doc_processor_inbox.drain_for_test_typed();
        assert!(!messages.is_empty());

        let batch = merge_doc_batches(messages);
        assert_eq!(batch.docs, expected_docs[0].messages);

        let num_bytes = expected_docs[0].num_bytes();
        let expected_state = json!({
            "index_id": index_id,
            "source_id": source_id,
            "topics": vec![topic],
            "subscription_name": subscription_name(&index_uid, &source_id),
            "consumer_name": CLIENT_NAME,
            "num_bytes_processed": num_bytes,
            "num_messages_processed": 10,
            "num_invalid_messages": 0,
        });
        assert_eq!(exit_state, expected_state);
    }

    #[tokio::test]
    async fn test_partitioned_topic_multi_consumer_ingestion() {
        let universe = Universe::with_accelerated_time();
        let metastore = metastore_for_test();
        let topic = append_random_suffix("test-pulsar-source--partitioned-multi-consumer--topic");

        let index_id =
            append_random_suffix("test-pulsar-source--partitioned-multi-consumer--index");
        let (source_id, source_config) = get_source_config([&topic]);

        create_partitioned_topic(&topic, 2).await;
        let index_uid = setup_index(metastore.clone(), &index_id, &source_config, &[]).await;

        let topic_partition_1 = format!("{topic}-partition-0");
        let topic_partition_2 = format!("{topic}-partition-1");

        let (source_handle1, doc_processor_inbox1) = create_source(
            &universe,
            metastore.clone(),
            index_uid.clone(),
            source_config.clone(),
            SourceCheckpoint::default(),
        )
        .await
        .expect("Create source");

        let (source_handle2, doc_processor_inbox2) = create_source(
            &universe,
            metastore,
            index_uid.clone(),
            source_config,
            SourceCheckpoint::default(),
        )
        .await
        .expect("Create source");

        let expected_docs = populate_topic(
            [&topic_partition_1, &topic_partition_2],
            0..10,
            message_generator,
        )
        .await
        .unwrap();

        let exit_state1 = wait_for_completion(
            source_handle1,
            10,
            PartitionId::from(topic_partition_1.clone()),
            expected_docs[0].expected_position.clone(),
        )
        .await;
        let exit_state2 = wait_for_completion(
            source_handle2,
            10,
            PartitionId::from(topic_partition_2.clone()),
            expected_docs[1].expected_position.clone(),
        )
        .await;
        let messages1: Vec<RawDocBatch> = doc_processor_inbox1.drain_for_test_typed();
        assert!(!messages1.is_empty());
        let messages2: Vec<RawDocBatch> = doc_processor_inbox2.drain_for_test_typed();
        assert!(!messages2.is_empty());

        let batch1 = merge_doc_batches(messages1);
        assert_eq!(batch1.docs, expected_docs[0].messages);

        let batch2 = merge_doc_batches(messages2);
        assert_eq!(batch2.docs, expected_docs[1].messages);

        let num_bytes = expected_docs[1].num_bytes();
        let expected_state = json!({
            "index_id": index_id,
            "source_id": source_id,
            "topics": vec![topic],
            "subscription_name": subscription_name(&index_uid, &source_id),
            "consumer_name": CLIENT_NAME,
            "num_bytes_processed": num_bytes,
            "num_messages_processed": 10,
            "num_invalid_messages": 0,
        });
        assert_eq!(exit_state1, expected_state);
        assert_eq!(exit_state2, expected_state);
    }

    #[tokio::test]
    async fn test_partitioned_topic_multi_consumer_ingestion_with_failover() {
        // We test successive failures of one source and observe pulsar failover mechanism.
        quickwit_common::setup_logging_for_tests();
        let universe = Universe::new();
        let metastore = metastore_for_test();
        let topic =
            append_random_suffix("test-pulsar-source--partitioned-multi-consumer-failure--topic");

        let index_id =
            append_random_suffix("test-pulsar-source--partitioned-multi-consumer-failure--index");
        let (_, source_config) = get_source_config([&topic]);

        create_partitioned_topic(&topic, 2).await;
        let index_uid = setup_index(metastore.clone(), &index_id, &source_config, &[]).await;

        let topic_partition_1 = format!("{topic}-partition-0");
        let topic_partition_2 = format!("{topic}-partition-1");

        let (_source_handle1, doc_processor_inbox1) = create_source(
            &universe,
            metastore.clone(),
            index_uid.clone(),
            source_config.clone(),
            SourceCheckpoint::default(),
        )
        .await
        .expect("Create source");

        // Send 10 messages on each topic and kill the source 5 times.
        for idx in 0..5 {
            let (source_handle2, _) = create_source(
                &universe,
                metastore.clone(),
                index_uid.clone(),
                source_config.clone(),
                SourceCheckpoint::default(),
            )
            .await
            .expect("Create source");
            populate_topic(
                [&topic_partition_1, &topic_partition_2],
                idx * 10..(idx + 1) * 10,
                message_generator,
            )
            .await
            .unwrap();
            tokio::time::sleep(*HEARTBEAT * 5).await;
            source_handle2.kill().await;
        }

        let messages1: Vec<RawDocBatch> = doc_processor_inbox1.drain_for_test_typed();
        assert!(!messages1.is_empty());
        let num_docs_sent_to_doc_processor: usize =
            messages1.iter().map(|batch| batch.docs.len()).sum();
        assert_eq!(100, num_docs_sent_to_doc_processor);
        // Check that we have received all the messages without duplicates.
        assert_eq!(100, count_unique_messages_in_batches(&messages1));
        universe.assert_quit().await;
    }
}
