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

use std::collections::HashMap;
use std::fmt;
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::{anyhow, bail, Context};
use async_trait::async_trait;
use itertools::Itertools;
use oneshot;
use quickwit_actors::{ActorExitStatus, Mailbox};
use quickwit_config::KafkaSourceParams;
use quickwit_metastore::checkpoint::{
    PartitionId, Position, SourceCheckpoint, SourceCheckpointDelta,
};
use rdkafka::config::{ClientConfig, RDKafkaLogLevel};
use rdkafka::consumer::{
    BaseConsumer, Consumer, ConsumerContext, DefaultConsumerContext, Rebalance, RebalanceProtocol,
};
use rdkafka::error::KafkaError;
use rdkafka::message::BorrowedMessage;
use rdkafka::util::Timeout;
use rdkafka::{ClientContext, Message, Offset, TopicPartitionList};
use serde_json::json;
use tokio::sync::mpsc;
use tokio::task::{spawn_blocking, JoinHandle};
use tokio::time;
use tracing::{debug, info, warn};

use crate::actors::DocProcessor;
use crate::models::{NewPublishLock, PublishLock, RawDocBatch};
use crate::source::{Source, SourceContext, SourceExecutionContext, TypedSourceFactory};

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

/// Factory for instantiating a `KafkaSource`.
pub struct KafkaSourceFactory;

#[async_trait]
impl TypedSourceFactory for KafkaSourceFactory {
    type Source = KafkaSource;
    type Params = KafkaSourceParams;

    async fn typed_create_source(
        ctx: Arc<SourceExecutionContext>,
        params: KafkaSourceParams,
        checkpoint: SourceCheckpoint,
    ) -> anyhow::Result<Self::Source> {
        KafkaSource::try_new(ctx, params, checkpoint).await
    }
}

#[derive(Debug)]
enum KafkaEvent {
    Message(KafkaMessage),
    AssignPartitions {
        partitions: Vec<i32>,
        assignment_tx: oneshot::Sender<Vec<(i32, Offset)>>,
    },
    RevokePartitions {
        ack_tx: oneshot::Sender<()>,
    },
    PartitionEOF(i32),
    Error(anyhow::Error),
}

#[derive(Debug)]
struct KafkaMessage {
    doc_opt: Option<String>,
    payload_len: u64,
    partition: i32,
    offset: i64,
}

impl From<BorrowedMessage<'_>> for KafkaMessage {
    fn from(message: BorrowedMessage<'_>) -> Self {
        Self {
            doc_opt: parse_message_payload(&message),
            payload_len: message.payload_len() as u64,
            partition: message.partition(),
            offset: message.offset() as i64,
        }
    }
}

struct RdKafkaContext {
    group_id: String,
    topic: String,
    events_tx: mpsc::Sender<KafkaEvent>,
}

impl ClientContext for RdKafkaContext {}

macro_rules! return_if_err {
    ($expression:expr, $lit: literal) => {
        match $expression {
            Ok(v) => v,
            Err(_) => {
                debug!(concat!($lit, "The source was dropped."));
                return;
            }
        }
    };
}

/// The rebalance protocol at a very high level:
/// - A consumer joins or leaves a consumer group.
/// - Consumers receive a revoke partitions notification, which gives them the opportunity to commit
/// the work in progress.
/// - Broker waits for ALL the consumers to ack the revoke notification (synchronization barrier).
/// - Consumers receive new partition assignmennts.
///
/// The API of the rebalance callback is better explained in the docs of `librdkafka`:
/// <https://docs.confluent.io/2.0.0/clients/librdkafka/classRdKafka_1_1RebalanceCb.html>
impl ConsumerContext for RdKafkaContext {
    fn pre_rebalance(&self, rebalance: &Rebalance) {
        if let Rebalance::Revoke(tpl) = rebalance {
            let partitions = collect_partitions(tpl, &self.topic);
            debug!(partitions=?partitions, "Revoke partitions.");

            let (ack_tx, ack_rx) = oneshot::channel();
            return_if_err!(
                self.events_tx
                    .blocking_send(KafkaEvent::RevokePartitions { ack_tx }),
                "Failed to send revoke message to source."
            );
            return_if_err!(ack_rx.recv(), "Failed to receive revoke ack from source");
        }
        if let Rebalance::Assign(tpl) = rebalance {
            let partitions = collect_partitions(tpl, &self.topic);
            debug!(partitions=?partitions, "Assign partitions.");

            let (assignment_tx, assignment_rx) = oneshot::channel();
            return_if_err!(
                self.events_tx.blocking_send(KafkaEvent::AssignPartitions {
                    partitions,
                    assignment_tx,
                }),
                "Failed to send assign message to source."
            );
            let assignment = return_if_err!(
                assignment_rx.recv(),
                "Failed to receive assignment from source."
            );
            info!(
                topic=%self.topic,
                partitions=%assignment.iter().map(|(partition, _)| partition).join(","),
                "New partition assignment"
            );
            for (partition, offset) in assignment {
                let mut partition = tpl
                    .find_partition(&self.topic, partition)
                    .expect("Failed to find partition in assignment. This should never happen! Please, report on https://github.com/quickwit-oss/quickwit/issues.");
                partition
                    .set_offset(offset)
                    .expect("Failed to convert `Offset` to `i64`. This should never happen! Please, report on https://github.com/quickwit-oss/quickwit/issues.");
            }
        }
    }
}

fn collect_partitions(tpl: &TopicPartitionList, topic: &str) -> Vec<i32> {
    tpl.elements()
        .iter()
        .map(|tple| {
            assert_eq!(tple.topic(), topic);
            tple.partition()
        })
        .collect()
}

type RdKafkaConsumer = BaseConsumer<RdKafkaContext>;

#[derive(Default)]
pub struct KafkaSourceState {
    /// Partitions IDs assigned to the source.
    pub assigned_partitions: HashMap<i32, PartitionId>,
    /// Offset for each partition of the last message received.
    pub current_positions: HashMap<i32, Position>,
    /// Number of inactive partitions, i.e., that have reached EOF.
    pub num_inactive_partitions: usize,
    /// Number of bytes processed by the source.
    pub num_bytes_processed: u64,
    /// Number of messages processed by the source (including invalid messages).
    pub num_messages_processed: u64,
    // Number of invalid messages, i.e., that were empty or could not be parsed.
    pub num_invalid_messages: u64,
    /// Number of rebalances the consumer went through.
    pub num_rebalances: usize,
}

/// A `KafkaSource` consumes a topic and forwards its messages to an `Indexer`.
pub struct KafkaSource {
    ctx: Arc<SourceExecutionContext>,
    topic: String,
    state: KafkaSourceState,
    backfill_mode_enabled: bool,
    events_rx: mpsc::Receiver<KafkaEvent>,
    consumer: Arc<RdKafkaConsumer>,
    poll_loop_jh: JoinHandle<()>,
    publish_lock: PublishLock,
}

impl fmt::Debug for KafkaSource {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "KafkaSource {{ source_id: {}, topic: {} }}",
            self.ctx.source_config.source_id, self.topic
        )
    }
}

impl KafkaSource {
    /// Instantiates a new `KafkaSource`.
    pub async fn try_new(
        ctx: Arc<SourceExecutionContext>,
        params: KafkaSourceParams,
        _ignored_checkpoint: SourceCheckpoint,
    ) -> anyhow::Result<Self> {
        let topic = params.topic.clone();
        let backfill_mode_enabled = params.enable_backfill_mode;

        let (events_tx, events_rx) = mpsc::channel(100);
        let consumer = create_consumer(
            &ctx.index_id,
            &ctx.source_config.source_id,
            params,
            events_tx.clone(),
        )?;
        consumer
            .subscribe(&[&topic])
            .with_context(|| format!("Failed to subscribe to topic `{topic}`."))?;
        let poll_loop_jh = spawn_consumer_poll_loop(consumer.clone(), events_tx);
        let publish_lock = PublishLock::default();

        let rebalance_protocol_str = match consumer.rebalance_protocol() {
            RebalanceProtocol::None => "off group", // The consumer has not joined the group yet.
            RebalanceProtocol::Eager => "eager",
            RebalanceProtocol::Cooperative => "cooperative",
        };
        info!(
            index_id=%ctx.index_id,
            source_id=%ctx.source_config.source_id,
            group_id=%consumer.client().context().group_id,
            topic=%topic,
            rebalance_protocol=%rebalance_protocol_str,
            "Starting Kafka source."
        );
        let state = KafkaSourceState {
            ..Default::default()
        };
        Ok(KafkaSource {
            ctx,
            topic,
            state,
            backfill_mode_enabled,
            events_rx,
            consumer,
            poll_loop_jh,
            publish_lock,
        })
    }

    async fn process_message(
        &mut self,
        message: KafkaMessage,
        batch: &mut BatchBuilder,
    ) -> anyhow::Result<()> {
        let KafkaMessage {
            doc_opt,
            payload_len,
            partition,
            offset,
            ..
        } = message;

        if let Some(doc) = doc_opt {
            batch.push(doc, payload_len);
        } else {
            self.state.num_invalid_messages += 1;
        }
        self.state.num_bytes_processed += payload_len;
        self.state.num_messages_processed += 1;

        let partition_id = self
            .state
            .assigned_partitions
            .get(&partition)
            .ok_or_else(|| {
                anyhow::anyhow!(
                    "Received message from unassigned partition `{}`. Assigned partitions: \
                     `{{{}}}`.",
                    partition,
                    self.state.assigned_partitions.keys().join(", "),
                )
            })?
            .clone();
        let current_position = Position::from(offset);
        let previous_position = self
            .state
            .current_positions
            .insert(partition, current_position.clone())
            .unwrap_or_else(|| previous_position_for_offset(offset));
        batch
            .checkpoint_delta
            .record_partition_delta(partition_id, previous_position, current_position)
            .context("Failed to record partition delta.")?;
        Ok(())
    }

    async fn process_assign_partitions(
        &mut self,
        ctx: &SourceContext,
        partitions: &[i32],
        assignment_tx: oneshot::Sender<Vec<(i32, Offset)>>,
    ) -> anyhow::Result<()> {
        let index_metadata = ctx
            .protect_future(self.ctx.metastore.index_metadata(&self.ctx.index_id))
            .await
            .with_context(|| {
                format!(
                    "Failed to fetch index metadata for index `{}`.",
                    self.ctx.index_id
                )
            })?;
        let checkpoint = index_metadata
            .checkpoint
            .source_checkpoint(&self.ctx.source_config.source_id)
            .cloned()
            .unwrap_or_default();

        self.state.assigned_partitions.clear();
        self.state.current_positions.clear();
        self.state.num_inactive_partitions = 0;

        let mut next_offsets: Vec<(i32, Offset)> = Vec::with_capacity(partitions.len());

        for &partition in partitions {
            let partition_id = PartitionId::from(partition as i64);
            let current_position = checkpoint
                .position_for_partition(&partition_id)
                .cloned()
                .unwrap_or(Position::Beginning);
            let next_offset = match &current_position {
                Position::Beginning => Offset::Beginning,
                Position::Offset(offset_str) => {
                    let offset: i64 = offset_str.parse().expect("Failed to parse checkpoint position to i64. This should never happen! Please, report on https://github.com/quickwit-oss/quickwit/issues.");
                    Offset::Offset(offset + 1)
                }
            };
            self.state
                .assigned_partitions
                .insert(partition, partition_id);
            self.state
                .current_positions
                .insert(partition, current_position);
            next_offsets.push((partition, next_offset));
        }
        assignment_tx
            .send(next_offsets)
            .map_err(|_| anyhow!("Consumer context was dropped."))?;
        Ok(())
    }

    async fn process_revoke_partitions(
        &mut self,
        ctx: &SourceContext,
        doc_processor_mailbox: &Mailbox<DocProcessor>,
        batch: &mut BatchBuilder,
        ack_tx: oneshot::Sender<()>,
    ) -> anyhow::Result<()> {
        ctx.protect_future(self.publish_lock.kill()).await;
        ack_tx
            .send(())
            .map_err(|_| anyhow!("Consumer context was dropped."))?;

        batch.clear();
        self.publish_lock = PublishLock::default();
        self.state.num_rebalances += 1;
        ctx.send_message(
            doc_processor_mailbox,
            NewPublishLock(self.publish_lock.clone()),
        )
        .await?;
        Ok(())
    }

    fn process_partition_eof(&mut self, partition: i32) {
        self.state.num_inactive_partitions += 1;

        info!(
            topic=%self.topic,
            partition=%partition,
            num_inactive_partitions=?self.state.num_inactive_partitions,
            "Reached end of partition."
        );
    }

    fn should_exit(&self) -> bool {
        self.backfill_mode_enabled
            // This check ensures that we don't shutdown the source before the first partition assignment.
            && self.state.num_inactive_partitions > 0
            && self.state.num_inactive_partitions == self.state.assigned_partitions.len()
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

    fn clear(&mut self) {
        self.docs.clear();
        self.num_bytes = 0;
        self.checkpoint_delta = SourceCheckpointDelta::default();
    }

    fn push(&mut self, doc: String, num_bytes: u64) {
        self.docs.push(doc);
        self.num_bytes += num_bytes;
    }
}

#[async_trait]
impl Source for KafkaSource {
    async fn initialize(
        &mut self,
        doc_processor_mailbox: &Mailbox<DocProcessor>,
        ctx: &SourceContext,
    ) -> Result<(), ActorExitStatus> {
        let publish_lock = self.publish_lock.clone();
        ctx.send_message(doc_processor_mailbox, NewPublishLock(publish_lock))
            .await?;
        Ok(())
    }

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
                event_opt = self.events_rx.recv() => {
                    let event = event_opt.ok_or_else(|| ActorExitStatus::from(anyhow!("Consumer was dropped.")))?;
                    match event {
                        KafkaEvent::Message(message) => self.process_message(message, &mut batch).await?,
                        KafkaEvent::AssignPartitions { partitions, assignment_tx} => self.process_assign_partitions(ctx, &partitions, assignment_tx).await?,
                        KafkaEvent::RevokePartitions { ack_tx } => self.process_revoke_partitions(ctx, doc_processor_mailbox, &mut batch, ack_tx).await?,
                        KafkaEvent::PartitionEOF(partition) => self.process_partition_eof(partition),
                        KafkaEvent::Error(error) => Err(ActorExitStatus::from(error))?,
                    }
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
        if self.should_exit() {
            info!(topic = %self.topic, "Reached end of topic.");
            ctx.send_exit_with_success(doc_processor_mailbox).await?;
            return Err(ActorExitStatus::Success);
        }
        Ok(Duration::default())
    }

    async fn finalize(
        &mut self,
        _exit_status: &ActorExitStatus,
        _ctx: &SourceContext,
    ) -> anyhow::Result<()> {
        self.poll_loop_jh.abort();

        let consumer = self.consumer.clone();
        spawn_blocking(move || {
            consumer.unsubscribe();
        });
        Ok(())
    }

    fn name(&self) -> String {
        format!(
            "KafkaSource{{source_id={}}}",
            self.ctx.source_config.source_id
        )
    }

    fn observable_state(&self) -> serde_json::Value {
        let assigned_partitions: Vec<&i32> =
            self.state.assigned_partitions.keys().sorted().collect();
        let current_positions: Vec<(&i32, &str)> = self
            .state
            .current_positions
            .iter()
            .map(|(partition, position)| (partition, position.as_str()))
            .sorted()
            .collect();
        json!({
            "index_id": self.ctx.index_id,
            "source_id": self.ctx.source_config.source_id,
            "topic": self.topic,
            "assigned_partitions": assigned_partitions,
            "current_positions": current_positions,
            "num_inactive_partitions": self.state.num_inactive_partitions,
            "num_bytes_processed": self.state.num_bytes_processed,
            "num_messages_processed": self.state.num_messages_processed,
            "num_invalid_messages": self.state.num_invalid_messages,
            "num_rebalances": self.state.num_rebalances,
        })
    }
}

// `rust-rdkafka` provides an async API via `StreamConsumer` for consuming topics asynchronously,
// BUT the async calls to `recev()` end up being sync when a rebalance occurs because the rebalance
// callback is sync. Until `rust-rdkafka` offers a fully asynchronous API, we poll the consumer in a
// blocking tokio task and handle the rebalance events via message passing between the rebalance
// callback and the source.
fn spawn_consumer_poll_loop(
    consumer: Arc<RdKafkaConsumer>,
    events_tx: mpsc::Sender<KafkaEvent>,
) -> JoinHandle<()> {
    spawn_blocking(move || {
        while !events_tx.is_closed() {
            if let Some(message_res) = consumer.poll(Some(Duration::from_secs(1))) {
                let event = match message_res {
                    Ok(message) => KafkaEvent::Message(message.into()),
                    Err(KafkaError::PartitionEOF(partition)) => KafkaEvent::PartitionEOF(partition),
                    Err(error) => KafkaEvent::Error(anyhow!(error)),
                };
                if events_tx.blocking_send(event).is_err() {
                    break;
                }
            }
        }
        debug!("Exiting consumer poll loop.");
        consumer.unsubscribe();
    })
}

/// Returns the preceding `Position` for the offset.
fn previous_position_for_offset(offset: i64) -> Position {
    if offset == 0 {
        Position::Beginning
    } else {
        Position::from(offset - 1)
    }
}

/// Checks whether we can establish a connection to the Kafka broker.
pub(super) async fn check_connectivity(params: KafkaSourceParams) -> anyhow::Result<()> {
    let mut client_config = parse_client_params(params.client_params)?;

    let consumer: BaseConsumer<DefaultConsumerContext> = client_config
        .set("group.id", "quickwit-connectivity-check".to_string())
        .set_log_level(RDKafkaLogLevel::Error)
        .create()?;

    let topic = params.topic.clone();
    let timeout = Timeout::After(Duration::from_secs(5));
    let cluster_metadata = spawn_blocking(move || {
        consumer
            .fetch_metadata(Some(&topic), timeout)
            .with_context(|| format!("Failed to fetch metadata for topic `{}`.", topic))
    })
    .await??;

    if cluster_metadata.topics().is_empty() {
        bail!("Topic `{}` does not exist.", params.topic);
    }
    let topic_metadata = &cluster_metadata.topics()[0];
    assert_eq!(topic_metadata.name(), params.topic); // Belt and suspenders.

    if topic_metadata.partitions().is_empty() {
        bail!("Topic `{}` has no partitions.", params.topic);
    }
    Ok(())
}

/// Creates a new `KafkaSourceConsumer`.
fn create_consumer(
    index_id: &str,
    source_id: &str,
    params: KafkaSourceParams,
    events_tx: mpsc::Sender<KafkaEvent>,
) -> anyhow::Result<Arc<RdKafkaConsumer>> {
    let mut client_config = parse_client_params(params.client_params)?;

    // Group ID is limited to 255 characters.
    let mut group_id = format!("quickwit-{index_id}-{source_id}");
    group_id.truncate(255);

    let log_level = parse_client_log_level(params.client_log_level)?;
    let consumer: RdKafkaConsumer = client_config
        .set("enable.auto.commit", "false") // We manage offsets ourselves: we always want to set this value to `false`.
        .set(
            "enable.partition.eof",
            params.enable_backfill_mode.to_string(),
        )
        .set("group.id", &group_id)
        .set_log_level(log_level)
        .create_with_context(RdKafkaContext {
            group_id,
            topic: params.topic,
            events_tx,
        })
        .context("Failed to create Kafka consumer.")?;

    Ok(Arc::new(consumer))
}

fn parse_client_log_level(client_log_level: Option<String>) -> anyhow::Result<RDKafkaLogLevel> {
    let log_level = match client_log_level
        .map(|log_level| log_level.to_lowercase())
        .as_deref()
    {
        Some("debug") => RDKafkaLogLevel::Debug,
        Some("info") | None => RDKafkaLogLevel::Info,
        Some("warn") | Some("warning") => RDKafkaLogLevel::Warning,
        Some("error") => RDKafkaLogLevel::Error,
        Some(level) => bail!(
            "Failed to parse Kafka client log level. Value `{}` is not supported.",
            level
        ),
    };
    Ok(log_level)
}

fn parse_client_params(client_params: serde_json::Value) -> anyhow::Result<ClientConfig> {
    let params = if let serde_json::Value::Object(params) = client_params {
        params
    } else {
        bail!("Failed to parse Kafka client parameters. `client_params` must be a JSON object.");
    };
    let mut client_config = ClientConfig::new();
    for (key, value_json) in params {
        let value = match value_json {
            serde_json::Value::Bool(value_bool) => value_bool.to_string(),
            serde_json::Value::Number(value_number) => value_number.to_string(),
            serde_json::Value::String(value_string) => value_string,
            serde_json::Value::Null => continue,
            serde_json::Value::Array(_) | serde_json::Value::Object(_) => bail!(
                "Failed to parse Kafka client parameters. `client_params.{}` must be a boolean, \
                 number, or string.",
                key
            ),
        };
        client_config.set(key, value);
    }
    Ok(client_config)
}

/// Converts the raw bytes of the message payload to a `String` skipping corrupted or empty
/// messages.
fn parse_message_payload(message: &BorrowedMessage) -> Option<String> {
    match message.payload_view::<str>() {
        Some(Ok(payload)) if !payload.is_empty() => {
            let doc = payload.to_string();
            return Some(doc);
        }
        Some(Ok(_)) => debug!(
            topic = ?message.topic(),
            partition = ?message.partition(),
            offset = ?message.offset(),
            timestamp = ?message.timestamp(),
            "Document is empty."
        ),
        Some(Err(error)) => warn!(
            topic = ?message.topic(),
            partition = ?message.partition(),
            offset = ?message.offset(),
            timestamp = ?message.timestamp(),
            error = ?error,
            "Failed to deserialize message payload."
        ),
        None => debug!(
            topic = ?message.topic(),
            partition = ?message.partition(),
            offset = ?message.offset(),
            timestamp = ?message.timestamp(),
            "Message payload is empty."
        ),
    }
    None
}

#[cfg(all(test, feature = "kafka-broker-tests"))]
mod kafka_broker_tests {
    use std::path::PathBuf;

    use quickwit_actors::{create_test_mailbox, ActorContext, Universe};
    use quickwit_common::rand::append_random_suffix;
    use quickwit_config::{SourceConfig, SourceParams};
    use quickwit_metastore::checkpoint::{IndexCheckpointDelta, SourceCheckpointDelta};
    use quickwit_metastore::{metastore_for_test, IndexMetadata, Metastore, SplitMetadata};
    use rdkafka::admin::{AdminClient, AdminOptions, NewTopic, TopicReplication};
    use rdkafka::client::DefaultClientContext;
    use rdkafka::message::ToBytes;
    use rdkafka::producer::{FutureProducer, FutureRecord};
    use tokio::sync::watch;

    use super::*;
    use crate::new_split_id;
    use crate::source::{quickwit_supported_sources, SourceActor};

    fn create_admin_client() -> anyhow::Result<AdminClient<DefaultClientContext>> {
        let admin_client = ClientConfig::new()
            .set("bootstrap.servers", "localhost:9092")
            .create()?;
        Ok(admin_client)
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
                    "Failed to create topic `{}`. Error code: `{}`",
                    topic,
                    err_code
                )
            })?;
        Ok(())
    }

    async fn populate_topic<K, M, J, Q>(
        topic: &str,
        num_messages: i32,
        key_fn: &K,
        message_fn: &M,
        partition: Option<i32>,
        timestamp: Option<i64>,
    ) -> anyhow::Result<HashMap<(i32, i64), i32>>
    where
        K: Fn(i32) -> J,
        M: Fn(i32) -> Q,
        J: ToBytes,
        Q: ToBytes,
    {
        let producer: &FutureProducer = &ClientConfig::new()
            .set("bootstrap.servers", "localhost:9092")
            .set("statistics.interval.ms", "500")
            .set("api.version.request", "true")
            .set("debug", "all")
            .set("message.timeout.ms", "30000")
            .create()?;
        let tasks = (0..num_messages).map(|id| async move {
            producer
                .send(
                    FutureRecord {
                        topic,
                        partition,
                        timestamp,
                        key: Some(&key_fn(id)),
                        payload: Some(&message_fn(id)),
                        headers: None,
                    },
                    Duration::from_secs(1),
                )
                .await
                .map(|(partition, offset)| (id, partition, offset))
                .map_err(|(err, _)| err)
        });
        let message_map = futures::future::try_join_all(tasks)
            .await?
            .into_iter()
            .fold(HashMap::new(), |mut acc, (id, partition, offset)| {
                acc.insert((partition, offset), id);
                acc
            });
        Ok(message_map)
    }

    fn key_fn(id: i32) -> String {
        format!("Key {}", id)
    }

    fn get_source_config(topic: &str) -> (String, SourceConfig) {
        let source_id = append_random_suffix("test-kafka-source--source");
        let source_config = SourceConfig {
            source_id: source_id.clone(),
            num_pipelines: 1,
            enabled: true,
            source_params: SourceParams::Kafka(KafkaSourceParams {
                topic: topic.to_string(),
                client_log_level: None,
                client_params: json!({
                    "bootstrap.servers": "localhost:9092",
                }),
                enable_backfill_mode: true,
            }),
        };
        (source_id, source_config)
    }

    fn merge_doc_batches(batches: Vec<RawDocBatch>) -> anyhow::Result<RawDocBatch> {
        let mut merged_batch = RawDocBatch::default();
        for batch in batches {
            merged_batch.docs.extend(batch.docs);
            merged_batch
                .checkpoint_delta
                .extend(batch.checkpoint_delta)?;
        }
        merged_batch.docs.sort();
        Ok(merged_batch)
    }

    async fn setup_index(
        metastore: Arc<dyn Metastore>,
        index_id: &str,
        source_id: &str,
        partition_deltas: &[(u64, i64, i64)],
    ) {
        let index_uri = format!("ram:///indexes/{index_id}");
        let index_metadata = IndexMetadata::for_test(index_id, &index_uri);
        metastore.create_index(index_metadata).await.unwrap();

        if partition_deltas.is_empty() {
            return;
        }
        let split_id = new_split_id();
        let split_metadata = SplitMetadata::for_test(split_id.clone());
        metastore
            .stage_split(index_id, split_metadata)
            .await
            .unwrap();

        let mut source_delta = SourceCheckpointDelta::default();
        for (partition_id, from_position, to_position) in partition_deltas {
            source_delta
                .record_partition_delta(
                    (*partition_id).into(),
                    {
                        if *from_position < 0 {
                            Position::Beginning
                        } else {
                            (*from_position).into()
                        }
                    },
                    (*to_position).into(),
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

    #[tokio::test]
    async fn test_kafka_source_process_message() {
        let admin_client = create_admin_client().unwrap();
        let topic = append_random_suffix("test-kafka-source--process-message--topic");
        create_topic(&admin_client, &topic, 2).await.unwrap();

        let metastore = metastore_for_test();
        let index_id = append_random_suffix("test-kafka-source--process-message--index");
        let (_source_id, source_config) = get_source_config(&topic);
        let params = if let SourceParams::Kafka(params) = source_config.clone().source_params {
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
        let ignored_checkpoint = SourceCheckpoint::default();
        let mut kafka_source = KafkaSource::try_new(ctx, params, ignored_checkpoint)
            .await
            .unwrap();

        let partition_id_1 = PartitionId::from(1u64);
        let partition_id_2 = PartitionId::from(2u64);

        kafka_source.state.assigned_partitions =
            HashMap::from_iter([(1, partition_id_1.clone()), (2, partition_id_2.clone())]);

        assert_eq!(kafka_source.state.num_messages_processed, 0);
        assert_eq!(kafka_source.state.num_invalid_messages, 0);

        let mut batch = BatchBuilder::default();

        let message = KafkaMessage {
            doc_opt: None,
            payload_len: 7,
            partition: 1,
            offset: 0,
        };
        kafka_source
            .process_message(message, &mut batch)
            .await
            .unwrap();

        assert_eq!(batch.docs.len(), 0);
        assert_eq!(batch.num_bytes, 0);
        assert_eq!(
            kafka_source.state.current_positions.get(&1).unwrap(),
            &Position::from(0u64)
        );
        assert_eq!(kafka_source.state.num_bytes_processed, 7);
        assert_eq!(kafka_source.state.num_messages_processed, 1);
        assert_eq!(kafka_source.state.num_invalid_messages, 1);

        let message = KafkaMessage {
            doc_opt: Some("test-doc".to_string()),
            payload_len: 8,
            partition: 1,
            offset: 1,
        };
        kafka_source
            .process_message(message, &mut batch)
            .await
            .unwrap();

        assert_eq!(batch.docs.len(), 1);
        assert_eq!(batch.docs[0], "test-doc");
        assert_eq!(batch.num_bytes, 8);
        assert_eq!(
            kafka_source.state.current_positions.get(&1).unwrap(),
            &Position::from(1u64)
        );
        assert_eq!(kafka_source.state.num_bytes_processed, 15);
        assert_eq!(kafka_source.state.num_messages_processed, 2);
        assert_eq!(kafka_source.state.num_invalid_messages, 1);

        let message = KafkaMessage {
            doc_opt: Some("test-doc".to_string()),
            payload_len: 8,
            partition: 2,
            offset: 42,
        };
        kafka_source
            .process_message(message, &mut batch)
            .await
            .unwrap();

        assert_eq!(batch.docs.len(), 2);
        assert_eq!(batch.docs[1], "test-doc");
        assert_eq!(batch.num_bytes, 16);
        assert_eq!(
            kafka_source.state.current_positions.get(&2).unwrap(),
            &Position::from(42u64)
        );
        assert_eq!(kafka_source.state.num_bytes_processed, 23);
        assert_eq!(kafka_source.state.num_messages_processed, 3);
        assert_eq!(kafka_source.state.num_invalid_messages, 1);

        let mut expected_checkpoint_delta = SourceCheckpointDelta::default();
        expected_checkpoint_delta
            .record_partition_delta(partition_id_1, Position::Beginning, Position::from(1u64))
            .unwrap();
        expected_checkpoint_delta
            .record_partition_delta(partition_id_2, Position::from(41u64), Position::from(42u64))
            .unwrap();
        assert_eq!(batch.checkpoint_delta, expected_checkpoint_delta);

        // Message from unassigned partition
        let message = KafkaMessage {
            doc_opt: Some("test-doc".to_string()),
            payload_len: 8,
            partition: 3,
            offset: 42,
        };
        kafka_source
            .process_message(message, &mut batch)
            .await
            .unwrap_err();
    }

    #[tokio::test]
    async fn test_kafka_source_process_assign_partitions() {
        let admin_client = create_admin_client().unwrap();
        let topic = append_random_suffix("test-kafka-source--process-assign-partitions--topic");
        create_topic(&admin_client, &topic, 2).await.unwrap();

        let metastore = metastore_for_test();
        let index_id = append_random_suffix("test-kafka-source--process-assign-partitions--index");
        let (source_id, source_config) = get_source_config(&topic);

        setup_index(metastore.clone(), &index_id, &source_id, &[(2, -1, 42)]).await;

        let params = if let SourceParams::Kafka(params) = source_config.clone().source_params {
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
        let ignored_checkpoint = SourceCheckpoint::default();
        let mut kafka_source = KafkaSource::try_new(ctx, params, ignored_checkpoint)
            .await
            .unwrap();
        kafka_source.state.num_inactive_partitions = 1;

        let universe = Universe::new();
        let (source_mailbox, _source_inbox) = create_test_mailbox();
        let (observable_state_tx, _observable_state_rx) = watch::channel(json!({}));
        let ctx: ActorContext<SourceActor> =
            ActorContext::for_test(&universe, source_mailbox, observable_state_tx);
        let (assignment_tx, assignment_rx) = oneshot::channel();

        kafka_source
            .process_assign_partitions(&ctx, &[1, 2], assignment_tx)
            .await
            .unwrap();

        assert_eq!(kafka_source.state.num_inactive_partitions, 0);

        let expected_assigned_partitions =
            HashMap::from_iter([(1, PartitionId::from(1u64)), (2, PartitionId::from(2u64))]);
        assert_eq!(
            kafka_source.state.assigned_partitions,
            expected_assigned_partitions
        );
        let expected_current_positions =
            HashMap::from_iter([(1, Position::Beginning), (2, Position::from(42u64))]);
        assert_eq!(
            kafka_source.state.current_positions,
            expected_current_positions
        );

        let assignment = assignment_rx.await.unwrap();
        assert_eq!(
            assignment,
            &[(1, Offset::Beginning), (2, Offset::Offset(43))]
        )
    }

    #[tokio::test]
    async fn test_kafka_source_process_revoke_partitions() {
        let admin_client = create_admin_client().unwrap();
        let topic = append_random_suffix("test-kafka-source--process-revoke-partitions--topic");
        create_topic(&admin_client, &topic, 1).await.unwrap();

        let metastore = metastore_for_test();
        let index_id = append_random_suffix("test-kafka-source--process-revoke--partitions--index");
        let (_source_id, source_config) = get_source_config(&topic);
        let params = if let SourceParams::Kafka(params) = source_config.clone().source_params {
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
        let ignored_checkpoint = SourceCheckpoint::default();
        let mut kafka_source = KafkaSource::try_new(ctx, params, ignored_checkpoint)
            .await
            .unwrap();

        let universe = Universe::new();
        let (source_mailbox, _source_inbox) = create_test_mailbox();
        let (indexer_mailbox, indexer_inbox) = create_test_mailbox();
        let (observable_state_tx, _observable_state_rx) = watch::channel(json!({}));
        let ctx: ActorContext<SourceActor> =
            ActorContext::for_test(&universe, source_mailbox, observable_state_tx);
        let (ack_tx, ack_rx) = oneshot::channel();

        let mut batch = BatchBuilder::default();
        batch.push("test-doc".to_string(), 8);

        let publish_lock = kafka_source.publish_lock.clone();
        assert!(publish_lock.is_alive());
        assert_eq!(kafka_source.state.num_rebalances, 0);

        kafka_source
            .process_revoke_partitions(&ctx, &indexer_mailbox, &mut batch, ack_tx)
            .await
            .unwrap();

        ack_rx.await.unwrap();
        assert!(batch.docs.is_empty());
        assert!(publish_lock.is_dead());

        assert_eq!(kafka_source.state.num_rebalances, 1);

        let indexer_messages: Vec<NewPublishLock> = indexer_inbox.drain_for_test_typed();
        assert_eq!(indexer_messages.len(), 1);
        assert!(indexer_messages[0].0.is_alive());
    }

    #[tokio::test]
    async fn test_kafka_source_process_partition_eof() {
        let admin_client = create_admin_client().unwrap();
        let topic = append_random_suffix("test-kafka-source--process-partition-eof--topic");
        create_topic(&admin_client, &topic, 1).await.unwrap();

        let metastore = metastore_for_test();
        let index_id = append_random_suffix("test-kafka-source--process-partiton-eof--index");
        let (_source_id, source_config) = get_source_config(&topic);
        let params = if let SourceParams::Kafka(params) = source_config.clone().source_params {
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
        let ignored_checkpoint = SourceCheckpoint::default();
        let mut kafka_source = KafkaSource::try_new(ctx, params, ignored_checkpoint)
            .await
            .unwrap();
        let partition_id_1 = PartitionId::from(1u64);
        kafka_source.state.assigned_partitions = HashMap::from_iter([(1, partition_id_1)]);

        assert!(!kafka_source.should_exit());

        kafka_source.process_partition_eof(1);
        assert_eq!(kafka_source.state.num_inactive_partitions, 1);
        assert!(kafka_source.should_exit());

        kafka_source.backfill_mode_enabled = false;
        assert!(!kafka_source.should_exit());
    }

    #[tokio::test]
    async fn test_kafka_source() -> anyhow::Result<()> {
        let universe = Universe::new();
        let admin_client = create_admin_client()?;
        let topic = append_random_suffix("test-kafka-source--topic");
        create_topic(&admin_client, &topic, 3).await?;

        let source_loader = quickwit_supported_sources();
        {
            let metastore = metastore_for_test();
            let index_id = append_random_suffix("test-kafka-source--index");

            let (source_id, source_config) = get_source_config(&topic);
            let source = source_loader
                .load_source(
                    SourceExecutionContext::for_test(
                        metastore.clone(),
                        &index_id,
                        PathBuf::from("./queues"),
                        source_config.clone(),
                    ),
                    SourceCheckpoint::default(),
                )
                .await?;

            setup_index(metastore.clone(), &index_id, &source_id, &[]).await;

            let (doc_processor_mailbox, doc_processor_inbox) = create_test_mailbox();
            let source_actor = SourceActor {
                source,
                doc_processor_mailbox: doc_processor_mailbox.clone(),
            };
            let (_source_mailbox, source_handle) = universe.spawn_builder().spawn(source_actor);
            let (exit_status, exit_state) = source_handle.join().await;
            assert!(exit_status.is_success());

            let messages: Vec<RawDocBatch> = doc_processor_inbox.drain_for_test_typed();
            assert!(messages.is_empty());

            let expected_state = json!({
                "index_id": index_id,
                "source_id": source_id,
                "topic":  topic,
                "assigned_partitions": vec![0, 1, 2],
                "current_positions": vec![(0, ""), (1, ""), (2, "")],
                "num_inactive_partitions": 3,
                "num_bytes_processed": 0,
                "num_messages_processed": 0,
                "num_invalid_messages": 0,
                "num_rebalances": 0,
            });
            assert_eq!(exit_state, expected_state);
        }
        for partition_id in 0..3 {
            populate_topic(
                &topic,
                3,
                &key_fn,
                &|message_id| {
                    if message_id == 1 {
                        "".to_string()
                    } else {
                        format!("Message #{:0>3}", partition_id * 100 + message_id)
                    }
                },
                Some(partition_id),
                None,
            )
            .await?;
        }
        {
            let metastore = metastore_for_test();
            let index_id = append_random_suffix("test-kafka-source--index");

            let (source_id, source_config) = get_source_config(&topic);
            let source = source_loader
                .load_source(
                    SourceExecutionContext::for_test(
                        metastore.clone(),
                        &index_id,
                        PathBuf::from("./queues"),
                        source_config.clone(),
                    ),
                    SourceCheckpoint::default(),
                )
                .await?;

            setup_index(metastore.clone(), &index_id, &source_id, &[]).await;

            let (doc_processor_mailbox, doc_processor_inbox) = create_test_mailbox();
            let source_actor = SourceActor {
                source,
                doc_processor_mailbox: doc_processor_mailbox.clone(),
            };
            let (_source_mailbox, source_handle) = universe.spawn_builder().spawn(source_actor);
            let (exit_status, exit_state) = source_handle.join().await;
            assert!(exit_status.is_success());

            let messages: Vec<RawDocBatch> = doc_processor_inbox.drain_for_test_typed();
            assert!(!messages.is_empty());

            let batch = merge_doc_batches(messages)?;
            let expected_docs = vec![
                "Message #000",
                "Message #002",
                "Message #100",
                "Message #102",
                "Message #200",
                "Message #202",
            ];
            assert_eq!(batch.docs, expected_docs);

            let mut expected_checkpoint_delta = SourceCheckpointDelta::default();
            for partition in 0u64..3u64 {
                expected_checkpoint_delta.record_partition_delta(
                    PartitionId::from(partition),
                    Position::Beginning,
                    Position::from(2u64),
                )?;
            }
            assert_eq!(batch.checkpoint_delta, expected_checkpoint_delta);

            let expected_state = json!({
                "index_id": index_id,
                "source_id": source_id,
                "topic":  topic,
                "assigned_partitions": vec![0, 1, 2],
                "current_positions":  vec![(0, "00000000000000000002"), (1, "00000000000000000002"), (2, "00000000000000000002")],
                "num_inactive_partitions": 3,
                "num_bytes_processed": 72,
                "num_messages_processed": 9,
                "num_invalid_messages": 3,
                "num_rebalances": 0,
            });
            assert_eq!(exit_state, expected_state);
        }
        {
            let metastore = metastore_for_test();
            let index_id = append_random_suffix("test-kafka-source--index");

            let (source_id, source_config) = get_source_config(&topic);
            let source = source_loader
                .load_source(
                    SourceExecutionContext::for_test(
                        metastore.clone(),
                        &index_id,
                        PathBuf::from("./queues"),
                        source_config.clone(),
                    ),
                    SourceCheckpoint::default(),
                )
                .await?;

            setup_index(
                metastore.clone(),
                &index_id,
                &source_id,
                &[(0, -1, 0), (1, -1, 2)],
            )
            .await;

            let (doc_processor_mailbox, doc_processor_inbox) = create_test_mailbox();
            let source_actor = SourceActor {
                source,
                doc_processor_mailbox: doc_processor_mailbox.clone(),
            };
            let (_source_mailbox, source_handle) = universe.spawn_builder().spawn(source_actor);
            let (exit_status, exit_state) = source_handle.join().await;
            assert!(exit_status.is_success());

            let messages: Vec<RawDocBatch> = doc_processor_inbox.drain_for_test_typed();
            assert!(!messages.is_empty());

            let batch = merge_doc_batches(messages)?;
            let expected_docs = vec!["Message #002", "Message #200", "Message #202"];
            assert_eq!(batch.docs, expected_docs);

            let mut expected_checkpoint_delta = SourceCheckpointDelta::default();
            expected_checkpoint_delta.record_partition_delta(
                PartitionId::from(0u64),
                Position::from(0u64),
                Position::from(2u64),
            )?;
            expected_checkpoint_delta.record_partition_delta(
                PartitionId::from(2u64),
                Position::Beginning,
                Position::from(2u64),
            )?;
            assert_eq!(batch.checkpoint_delta, expected_checkpoint_delta,);

            let expected_exit_state = json!({
                "index_id": index_id,
                "source_id": source_id,
                "topic":  topic,
                "assigned_partitions": vec![0, 1, 2],
                "current_positions":  vec![(0, "00000000000000000002"), (1, "00000000000000000002"), (2, "00000000000000000002")],
                "num_inactive_partitions": 3,
                "num_bytes_processed": 36,
                "num_messages_processed": 5,
                "num_invalid_messages": 2,
                "num_rebalances": 0,
            });
            assert_eq!(exit_state, expected_exit_state);
        }
        Ok(())
    }

    #[tokio::test]
    async fn test_kafka_connectivity() {
        let bootstrap_servers = "localhost:9092".to_string();
        let topic = append_random_suffix("test-kafka-connectivity-topic");

        let admin_client = create_admin_client().unwrap();
        create_topic(&admin_client, &topic, 1).await.unwrap();

        // Check valid connectivity
        check_connectivity(KafkaSourceParams {
            topic: topic.clone(),
            client_log_level: None,
            client_params: json!({ "bootstrap.servers": bootstrap_servers }),
            enable_backfill_mode: true,
        })
        .await
        .unwrap();

        // TODO: these tests should be checking the specific errors.
        // Non existent topic should throw an error.
        check_connectivity(KafkaSourceParams {
            topic: "non-existent-topic".to_string(),
            client_log_level: None,
            client_params: json!({ "bootstrap.servers": bootstrap_servers }),
            enable_backfill_mode: true,
        })
        .await
        .unwrap_err();

        // Invalid brokers should throw an error
        let _result = check_connectivity(KafkaSourceParams {
            topic: topic.clone(),
            client_log_level: None,
            client_params: json!({
                "bootstrap.servers": "192.0.2.10:9092"
            }),
            enable_backfill_mode: true,
        })
        .await
        .unwrap_err();
    }
}
