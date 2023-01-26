// Copyright (C) 2022 Quickwit, Inc.
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

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::{anyhow, Context};
use async_trait::async_trait;
use futures::StreamExt;
use pulsar::authentication::oauth2::{OAuth2Authentication, OAuth2Params};
use pulsar::consumer::Message;
use pulsar::message::proto::MessageIdData;
use pulsar::{
    Authentication, Consumer, DeserializeMessage, Payload, Pulsar, SubType, TokioExecutor,
};
use quickwit_actors::{ActorContext, ActorExitStatus, Mailbox};
use quickwit_config::{PulsarSourceAuth, PulsarSourceParams};
use quickwit_metastore::checkpoint::{
    PartitionId, Position, SourceCheckpoint, SourceCheckpointDelta,
};
use serde_json::{json, Value as JsonValue};
use tokio::sync::{mpsc, watch};
use tokio::task::LocalSet;
use tokio::time;
use tokio::time::timeout;
use tracing::{debug, info, warn};

use crate::actors::DocProcessor;
use crate::models::RawDocBatch;
use crate::source::{
    Source, SourceActor, SourceContext, SourceExecutionContext, TypedSourceFactory,
};

/// Number of bytes after which we cut a new batch.
///
/// We try to emit chewable batches for the indexer.
/// One batch = one message to the indexer actor.
///
/// If batches are too large:
/// - we might not be able to observe the state of the indexer for 5 seconds.
/// - we will be needlessly occupying resident memory in the mailbox.
/// - we will not have a precise control of the timeout before commit.
///
/// 5MB seems like a good one size fits all value.
const BATCH_NUM_BYTES_LIMIT: u64 = 5_000_000;

/// The duration that the pulsar consumer waits for a new message
/// to be dispatched.
const MESSAGE_WAIT_TIMEOUT: Duration = Duration::from_secs(1);

type ConsumerMessage = Result<Message<PulsarMessage>, pulsar::Error>;

pub struct PulsarSourceFactory;

#[async_trait]
impl TypedSourceFactory for PulsarSourceFactory {
    type Source = PulsarSource;
    type Params = PulsarSourceParams;

    async fn typed_create_source(
        ctx: Arc<SourceExecutionContext>,
        params: PulsarSourceParams,
        checkpoint: SourceCheckpoint,
    ) -> anyhow::Result<Self::Source> {
        PulsarSource::try_new(ctx, params, checkpoint).await
    }
}

#[derive(Default)]
pub struct PulsarSourceState {
    /// Number of bytes processed by the source.
    pub num_bytes_processed: u64,
    /// Number of messages processed by the source (including invalid messages).
    pub num_messages_processed: u64,
    /// Number of invalid messages, i.e., that were empty or could not be parsed.
    pub num_invalid_messages: u64,
}

pub struct PulsarSource {
    ctx: Arc<SourceExecutionContext>,
    pulsar: PulsarConsumer,
    params: PulsarSourceParams,
    previous_positions: BTreeMap<PartitionId, Position>,
    state: PulsarSourceState,
}

impl PulsarSource {
    pub async fn try_new(
        ctx: Arc<SourceExecutionContext>,
        params: PulsarSourceParams,
        checkpoint: SourceCheckpoint,
    ) -> anyhow::Result<Self> {
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
                builder =
                    builder.with_auth_provider(OAuth2Authentication::client_credentials(auth));
            }
        }

        info!(
            index_id=%ctx.index_id,
            source_id=%ctx.source_config.source_id,
            topics=?params.topics,
            "Starting Pulsar source."
        );

        let pulsar: Pulsar<_> = builder.build().await?;

        let mut previous_positions = BTreeMap::new();
        for topic in params.topics.iter() {
            let partition_id = PartitionId::from(topic.as_str());
            let position_opt = checkpoint.position_for_partition(&partition_id).cloned();

            if let Some(position) = position_opt {
                previous_positions.insert(partition_id, position);
            }
        }
        let pulsar =
            spawn_message_listener(params.clone(), pulsar, previous_positions.clone()).await?;

        Ok(Self {
            ctx,
            pulsar,
            params,
            previous_positions,
            state: PulsarSourceState::default(),
        })
    }

    fn process_message(
        &mut self,
        message: Message<PulsarMessage>,
        batch: &mut BatchBuilder,
    ) -> anyhow::Result<()> {
        let current_position = msg_id_to_position(message.message_id());

        let doc = match message.deserialize() {
            Err(e) => {
                warn!(error = ?e, "Failed to parse message from queue.");
                self.state.num_invalid_messages += 1;
                return Ok(());
            }
            Ok(doc) => doc,
        };

        self.add_doc_to_batch(&message.topic, current_position, doc, batch)
    }

    fn add_doc_to_batch(
        &mut self,
        topic: &str,
        position: Position,
        doc: String,
        batch: &mut BatchBuilder,
    ) -> anyhow::Result<()> {
        if doc.is_empty() {
            warn!("Message received from queue was empty.");
            self.state.num_invalid_messages += 1;
            return Ok(());
        }

        let partition = PartitionId::from(topic);
        let num_bytes = doc.as_bytes().len();

        let previous_position = self
            .previous_positions
            .insert(partition.clone(), position.clone())
            .unwrap_or(Position::Beginning);

        batch
            .checkpoint_delta
            .record_partition_delta(partition, previous_position, position)
            .context("Failed to record partition delta.")?;
        batch.push(doc, num_bytes as u64);

        self.state.num_bytes_processed += num_bytes as u64;
        self.state.num_messages_processed += 1;

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
        let mut batch = BatchBuilder::default();
        let deadline = time::sleep(quickwit_actors::HEARTBEAT / 2);
        tokio::pin!(deadline);

        loop {
            tokio::select! {
                message = self.pulsar.messages.recv() => {
                    let message = message
                        .ok_or_else(|| ActorExitStatus::from(anyhow!("Consumer was dropped.")))?
                        .map_err(|e| ActorExitStatus::from(anyhow!("Failed to get message from consumer: {:?}", e)))?;
                    self.process_message(message, &mut batch).map_err(ActorExitStatus::from)?;

                    if batch.num_bytes >= BATCH_NUM_BYTES_LIMIT {
                        break;
                    }
                }
                _ = &mut deadline => {
                    break;
                }
            }
            ctx.record_progress();
        }

        if !batch.checkpoint_delta.is_empty() {
            debug!(
                num_docs=%batch.docs.len(),
                num_bytes=%batch.num_bytes,
                num_millis=%now.elapsed().as_millis(),
                "Sending doc batch to indexer.");
            let message = batch.build();
            ctx.send_message(doc_processor_mailbox, message).await?;
        }

        Ok(Duration::default())
    }

    async fn suggest_truncate(
        &self,
        checkpoint: SourceCheckpoint,
        _ctx: &ActorContext<SourceActor>,
    ) -> anyhow::Result<()> {
        debug!(ckpt = ?checkpoint, "Truncating message queue.");
        let _ = self.pulsar.last_ack.send(checkpoint);
        Ok(())
    }

    fn name(&self) -> String {
        format!(
            "PulsarSource{{source_id={}}}",
            self.ctx.source_config.source_id
        )
    }

    fn observable_state(&self) -> JsonValue {
        json!({
            "index_id": self.ctx.index_id,
            "source_id": self.ctx.source_config.source_id,
            "topics": self.params.topics,
            "subscription": self.params.subscription,
            "consumer_name": self.params.consumer_name,
            "num_bytes_processed": self.state.num_bytes_processed,
            "num_messages_processed": self.state.num_messages_processed,
            "num_invalid_messages": self.state.num_invalid_messages,
        })
    }
}

#[derive(Debug)]
struct PulsarMessage;

impl DeserializeMessage for PulsarMessage {
    type Output = anyhow::Result<String>;

    fn deserialize_message(payload: &Payload) -> Self::Output {
        String::from_utf8(payload.data.clone()).map_err(anyhow::Error::from)
    }
}

#[derive(Debug, Default)]
struct BatchBuilder {
    docs: Vec<String>,
    num_bytes: u64,
    checkpoint_delta: SourceCheckpointDelta,
}

impl BatchBuilder {
    fn build(self) -> RawDocBatch {
        RawDocBatch {
            docs: self.docs,
            checkpoint_delta: self.checkpoint_delta,
        }
    }

    fn push(&mut self, doc: String, num_bytes: u64) {
        self.docs.push(doc);
        self.num_bytes += num_bytes;
    }
}

struct PulsarConsumer {
    messages: mpsc::Receiver<ConsumerMessage>,
    last_ack: watch::Sender<SourceCheckpoint>,
}

#[tracing::instrument(name = "pulsar-consumer", skip(pulsar))]
/// Spawns a background task listening for incoming messages to
/// process.
///
/// The exists because the `Consumer` is not `Sync` or `Send` in places which means we cannot
/// have it as part of the `Source` impl itself or as a background tokio task.
///
/// Instead we spawn a separate tokio runtime and use a local set to ensure the bounds.
async fn spawn_message_listener(
    params: PulsarSourceParams,
    pulsar: Pulsar<TokioExecutor>,
    previous_positions: BTreeMap<PartitionId, Position>,
) -> anyhow::Result<PulsarConsumer> {
    let (messages_tx, messages_rx) = mpsc::channel(100);
    let (last_ack_tx, last_ack_rx) = watch::channel(SourceCheckpoint::default());
    let (ready, waiter) = oneshot::channel();

    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;

    let handle = std::thread::spawn(move || {
        let set = LocalSet::new();
        let fut = set.run_until(async move {
            let mut consumer: Consumer<PulsarMessage, _> = pulsar
                .consumer()
                .with_topics(&params.topics)
                .with_consumer_name(&params.consumer_name)
                .with_subscription(&params.subscription)
                .with_subscription_type(SubType::Shared)
                .build()
                .await?;

            debug!("Seeking to last checkpoint position.");
            for topic in params.topics.iter() {
                let partition = PartitionId::from(topic.as_str());
                let seek_to = previous_positions
                    .get(&partition)
                    .and_then(msg_id_from_position);

                if seek_to.is_some() {
                    consumer.seek(None, seek_to, None, pulsar.clone()).await?;
                }
            }

            let _ = ready.send(());

            'outer: loop {
                while let Ok(msg_opt) = timeout(MESSAGE_WAIT_TIMEOUT, consumer.next()).await {
                    match msg_opt {
                        None => break 'outer,
                        Some(msg) => {
                            if messages_tx.send(msg).await.is_err() {
                                break 'outer;
                            }
                        }
                    };
                }

                if last_ack_rx.has_changed().unwrap_or_default() {
                    let checkpoint = last_ack_rx.borrow();

                    for (_, position) in checkpoint.iter() {
                        if let Some(_msg_id) = msg_id_from_position(&position) {
                            // TODO: Implement missing behaviour in fork: consumer.cumulative_ack()
                        }
                    }
                }
            }

            Ok::<_, anyhow::Error>(())
        });

        rt.block_on(fut)
    });

    if waiter.await.is_err() {
        handle.join().expect("Join background thread task.")?;
    }

    Ok(PulsarConsumer {
        messages: messages_rx,
        last_ack: last_ack_tx,
    })
}

fn msg_id_to_position(msg: &MessageIdData) -> Position {
    // The order of these fields are important as they affect the sorting
    // of the checkpoint positions.
    // TODO: Confirm this layout is correct?
    let id_str = format!(
        "{:0>20},{:0>20},{},{},{}",
        msg.ledger_id,
        msg.entry_id,
        msg.batch_index
            .map(|v| format!("{:010}", v))
            .unwrap_or_default(),
        msg.partition
            .map(|v| format!("{:010}", v))
            .unwrap_or_default(),
        msg.batch_size
            .map(|v| format!("{:010}", v))
            .unwrap_or_default(),
    );

    Position::from(id_str)
}

fn msg_id_from_position(pos: &Position) -> Option<MessageIdData> {
    let id_str = pos.as_str();
    let mut parts = id_str.split(',');

    let ledger_id = parts.next()?.parse::<u64>().ok()?;
    let entry_id = parts.next()?.parse::<u64>().ok()?;
    let batch_index = parts.next()?.parse::<i32>().ok();
    let partition = parts.next()?.parse::<i32>().ok();
    let batch_size = parts.next()?.parse::<i32>().ok();

    Some(MessageIdData {
        ledger_id,
        entry_id,
        batch_index,
        batch_size,
        partition,
        ack_set: Vec::new(),
        first_chunk_message_id: None,
    })
}

#[cfg(all(test, feature = "pulsar-broker-tests"))]
mod pulsar_broker_tests {
    use std::path::PathBuf;
    use std::sync::Arc;

    use futures::future::join_all;
    use quickwit_actors::{ActorHandle, Universe};
    use quickwit_common::rand::append_random_suffix;
    use quickwit_config::{IndexConfig, SourceConfig, SourceParams};
    use quickwit_metastore::checkpoint::{
        IndexCheckpointDelta, PartitionId, Position, SourceCheckpointDelta,
    };
    use quickwit_metastore::{metastore_for_test, Metastore, SplitMetadata};

    use super::*;
    use crate::new_split_id;
    use crate::source::pulsar_source::{msg_id_from_position, msg_id_to_position};
    use crate::source::quickwit_supported_sources;

    static PULSAR_URI: &str = "pulsar://localhost:6650";
    static SUBSCRIPTION: &str = "quickwit-test";
    static CONSUMER_NAME: &str = "quickwit-tester";

    macro_rules! positions {
        ($($partition:expr => $position:expr $(,)?)*) => {{
            let mut positions = BTreeMap::new();
            $(
                positions.insert(PartitionId::from($partition), Position::from($position));
            )*
            positions
        }};
    }

    async fn setup_index(
        metastore: Arc<dyn Metastore>,
        index_id: &str,
        source_id: &str,
        partition_deltas: &[(&str, Position, Position)],
    ) {
        let index_uri = format!("ram:///indexes/{index_id}");
        let index_config = IndexConfig::for_test(index_id, &index_uri);
        metastore.create_index(index_config).await.unwrap();

        if partition_deltas.is_empty() {
            return;
        }
        let split_id = new_split_id();
        let split_metadata = SplitMetadata::for_test(split_id.clone());
        metastore
            .stage_splits(index_id, vec![split_metadata])
            .await
            .unwrap();

        let mut source_delta = SourceCheckpointDelta::default();
        for (partition_id, from_position, to_position) in partition_deltas {
            source_delta
                .record_partition_delta(
                    PartitionId::from(&**partition_id),
                    from_position.clone(),
                    to_position.clone(),
                )
                .unwrap();
        }
        let index_delta = IndexCheckpointDelta {
            source_id: source_id.to_string(),
            source_delta,
        };
        metastore
            .publish_splits(index_id, &[&split_id], &[], Some(index_delta))
            .await
            .unwrap();
    }

    fn get_source_config<S: AsRef<str>>(
        topics: impl IntoIterator<Item = S>,
    ) -> (String, SourceConfig) {
        let source_id = append_random_suffix("test-pulsar-source--source");
        let source_config = SourceConfig {
            source_id: source_id.clone(),
            max_num_pipelines_per_indexer: 1,
            desired_num_pipelines: 1,
            enabled: true,
            source_params: SourceParams::Pulsar(PulsarSourceParams {
                topics: topics.into_iter().map(|v| v.as_ref().to_string()).collect(),
                subscription: SUBSCRIPTION.to_string(),
                address: PULSAR_URI.to_string(),
                consumer_name: CONSUMER_NAME.to_string(),
                authentication: None,
            }),
            transform_config: None,
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
                .expect("Merge batches.");
        }
        merged_batch.docs.sort();
        merged_batch
    }

    async fn populate_topic<M>(
        topic: &str,
        num_messages: usize,
        message_fn: M,
    ) -> anyhow::Result<Vec<String>>
    where
        M: Fn(usize) -> JsonValue,
    {
        let client = Pulsar::builder(PULSAR_URI, TokioExecutor).build().await?;
        let mut producer = client.producer().with_topic(topic).build().await?;

        let mut pending_messages = Vec::with_capacity(num_messages);
        for id in 0..num_messages {
            let msg = (message_fn)(id).to_string();
            pending_messages.push(msg);
        }

        let futures = producer.send_all(pending_messages.clone()).await?;
        let receipts = join_all(futures).await;

        for receipt in receipts {
            receipt?;
        }

        pending_messages.sort();
        Ok(pending_messages)
    }

    async fn wait_for_completion(
        source_handle: ActorHandle<SourceActor>,
        num_expected: usize,
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
        let (_exit_status, exit_state) = source_handle.quit().await;
        exit_state
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
            position.as_str(),
            format!("{:0>20},{:0>20},{:010},{:010},{:010}", 1, 134, 3, -1, 6)
        );
        let retrieved_id = msg_id_from_position(&position)
            .expect("Successfully deserialize message ID from position.");
        assert_eq!(retrieved_id, populated_id);

        let sparse_id = MessageIdData {
            ledger_id: 1,
            entry_id: 4,
            batch_index: None,
            partition: None,
            batch_size: Some(0),

            // We never serialize these fields.
            ack_set: Vec::new(),
            first_chunk_message_id: None,
        };

        let position = msg_id_to_position(&sparse_id);
        assert_eq!(
            position.as_str(),
            format!("{:0>20},{:0>20},,,{:010}", 1, 4, 0)
        );
        let retrieved_id = msg_id_from_position(&position)
            .expect("Successfully deserialize message ID from position.");
        assert_eq!(retrieved_id, sparse_id);
    }

    #[tokio::test]
    async fn test_doc_batching_logic() {
        let metastore = metastore_for_test();
        let topic = append_random_suffix("test-pulsar-source-topic");

        let index_id = append_random_suffix("test-pulsar-source-index");
        let (_source_id, source_config) = get_source_config([&topic]);
        let params = if let SourceParams::Pulsar(params) = source_config.clone().source_params {
            params
        } else {
            unreachable!()
        };

        let ctx = SourceExecutionContext::for_test(
            metastore,
            &index_id,
            PathBuf::from("./queues"),
            source_config,
        );
        let start_checkpoint = SourceCheckpoint::default();

        let mut pulsar_source = PulsarSource::try_new(ctx, params, start_checkpoint)
            .await
            .expect("Setup pulsar source");

        let position = Position::Beginning;
        let mut batch = BatchBuilder::default();
        pulsar_source
            .add_doc_to_batch(&topic, position, "".to_string(), &mut batch)
            .expect("Add batch should not error on empty doc.");
        assert_eq!(pulsar_source.state.num_invalid_messages, 1);
        assert_eq!(pulsar_source.state.num_messages_processed, 0);
        assert_eq!(pulsar_source.state.num_bytes_processed, 0);
        assert!(pulsar_source.previous_positions.is_empty());
        assert_eq!(batch.num_bytes, 0);
        assert!(batch.docs.is_empty());

        let position = Position::from(1u64); // Used for testing simplicity.
        let mut batch = BatchBuilder::default();
        let doc = "some-demo-data".to_string();
        pulsar_source
            .add_doc_to_batch(&topic, position, doc, &mut batch)
            .expect("Add batch should not error on empty doc.");

        assert_eq!(pulsar_source.state.num_invalid_messages, 1);
        assert_eq!(pulsar_source.state.num_messages_processed, 1);
        assert_eq!(pulsar_source.state.num_bytes_processed, 14);
        assert_eq!(
            pulsar_source.previous_positions,
            positions!(topic.as_str() => 1u64)
        );
        assert_eq!(batch.num_bytes, 14);
        assert_eq!(batch.docs.len(), 1);

        let position = Position::from(4u64); // Used for testing simplicity.
        let mut batch = BatchBuilder::default();
        let doc = "some-demo-data-2".to_string();
        pulsar_source
            .add_doc_to_batch(&topic, position, doc, &mut batch)
            .expect("Add batch should not error on empty doc.");
        assert_eq!(pulsar_source.state.num_invalid_messages, 1);
        assert_eq!(pulsar_source.state.num_messages_processed, 2);
        assert_eq!(pulsar_source.state.num_bytes_processed, 30);
        assert_eq!(
            pulsar_source.previous_positions,
            positions!(topic.as_str() => 4u64)
        );
        assert_eq!(batch.num_bytes, 16);
        assert_eq!(batch.docs.len(), 1);

        let mut expected_checkpoint_delta = SourceCheckpointDelta::default();
        expected_checkpoint_delta
            .record_partition_delta(
                PartitionId::from(topic.as_str()),
                Position::from(1u64),
                Position::from(4u64),
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

        setup_index(metastore.clone(), &index_id, &source_id, &[]).await;

        let ctx = SourceExecutionContext::for_test(
            metastore,
            &index_id,
            PathBuf::from("./queues"),
            source_config,
        );
        let start_checkpoint = SourceCheckpoint::default();

        let source_loader = quickwit_supported_sources();
        let source = source_loader
            .load_source(ctx, start_checkpoint)
            .await
            .expect("Load source.");

        let (doc_processor_mailbox, doc_processor_inbox) = universe.create_test_mailbox();
        let source_actor = SourceActor {
            source,
            doc_processor_mailbox: doc_processor_mailbox.clone(),
        };
        let (_source_mailbox, source_handle) = universe.spawn_builder().spawn(source_actor);

        let expected_docs = populate_topic(&topic, 10, move |id| {
            json!({
                "id": id,
                "timestamp": 1674515715,
                "body": "Hello, world! This is some test data.",
            })
        })
        .await
        .unwrap();

        let exit_state = wait_for_completion(source_handle, expected_docs.len()).await;
        let messages: Vec<RawDocBatch> = doc_processor_inbox.drain_for_test_typed();
        assert!(!messages.is_empty());

        let batch = merge_doc_batches(messages);
        assert_eq!(batch.docs, expected_docs);

        let num_bytes = expected_docs
            .iter()
            .map(|v| v.as_bytes().len())
            .sum::<usize>();
        let expected_state = json!({
            "index_id": index_id,
            "source_id": source_id,
            "topics": vec![topic],
            "subscription": SUBSCRIPTION,
            "consumer_name": CONSUMER_NAME,
            "num_bytes_processed": num_bytes,
            "num_messages_processed": 10,
            "num_invalid_messages": 0,
        });
        assert_eq!(exit_state, expected_state);
    }

    #[tokio::test]
    async fn test_multi_topic_ingestion() {
        tracing_subscriber::fmt::init();

        let universe = Universe::with_accelerated_time();
        let metastore = metastore_for_test();
        let topic1 = append_random_suffix("test-pulsar-source--topic-ingestion--topic");
        let topic2 = append_random_suffix("test-pulsar-source--topic-ingestion--topic");

        let index_id = append_random_suffix("test-pulsar-source--topic-ingestion--index");
        let (source_id, source_config) = get_source_config([&topic1, &topic2]);

        setup_index(metastore.clone(), &index_id, &source_id, &[]).await;

        let ctx = SourceExecutionContext::for_test(
            metastore,
            &index_id,
            PathBuf::from("./queues"),
            source_config,
        );
        let start_checkpoint = SourceCheckpoint::default();

        let source_loader = quickwit_supported_sources();
        let source = source_loader
            .load_source(ctx, start_checkpoint)
            .await
            .expect("Load source.");

        let (doc_processor_mailbox, doc_processor_inbox) = universe.create_test_mailbox();
        let source_actor = SourceActor {
            source,
            doc_processor_mailbox: doc_processor_mailbox.clone(),
        };
        let (_source_mailbox, source_handle) = universe.spawn_builder().spawn(source_actor);

        let mut expected_docs = Vec::new();
        let expected_docs_topic1 = populate_topic(&topic1, 10, move |id| {
            json!({
                "id": id,
                "timestamp": 1674515715,
                "body": "Hello, world! This is some test data. From topic 1",
            })
        })
        .await
        .unwrap();
        expected_docs.extend(expected_docs_topic1);

        let expected_docs_topic2 = populate_topic(&topic2, 10, move |id| {
            json!({
                "id": id,
                "timestamp": 1674515715,
                "body": "Hello, world! This is some test data. From topic 2.",
            })
        })
        .await
        .unwrap();
        expected_docs.extend(expected_docs_topic2);
        expected_docs.sort();

        let exit_state = wait_for_completion(source_handle, expected_docs.len()).await;
        let messages: Vec<RawDocBatch> = doc_processor_inbox.drain_for_test_typed();
        assert!(!messages.is_empty());

        let batch = merge_doc_batches(messages);
        assert_eq!(batch.docs, expected_docs);

        let num_bytes = expected_docs
            .iter()
            .map(|v| v.as_bytes().len())
            .sum::<usize>();
        let expected_state = json!({
            "index_id": index_id,
            "source_id": source_id,
            "topics": vec![topic1, topic2],
            "subscription": SUBSCRIPTION,
            "consumer_name": CONSUMER_NAME,
            "num_bytes_processed": num_bytes,
            "num_messages_processed": 20,
            "num_invalid_messages": 0,
        });
        assert_eq!(exit_state, expected_state);
    }
}
