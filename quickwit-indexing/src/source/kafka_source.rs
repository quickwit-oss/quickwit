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
    BaseConsumer, Consumer, ConsumerContext, DefaultConsumerContext, Rebalance,
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

use crate::actors::Indexer;
use crate::models::RawDocBatch;
use crate::source::{Source, SourceContext, SourceExecutionContext, TypedSourceFactory};

/// We try to emit chewable batches for the indexer.
/// One batch = one message to the indexer actor.
///
/// If batches are too large:
/// - we might not be able to observe the state of the indexer for 5 seconds.
/// - we will be needlessly occupying resident memory in the mailbox.
/// - we will not have a precise control of the timeout before commit.
///
/// 5MB seems like a good one size fits all value.
const TARGET_BATCH_NUM_BYTES: u64 = 5_000_000;

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
    RevokePartitions,
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
    topic: String,
    events_tx: mpsc::Sender<KafkaEvent>,
}

impl ClientContext for RdKafkaContext {}

impl ConsumerContext for RdKafkaContext {
    fn pre_rebalance(&self, rebalance: &Rebalance) {
        if let Rebalance::Assign(tpl) = rebalance {
            let partitions = collect_partitions(tpl, &self.topic);
            debug!(partitions=?partitions, "Assign partitions.");
            let (assignment_tx, assignment_rx) = oneshot::channel();
            self.events_tx
                .blocking_send(KafkaEvent::AssignPartitions {
                    partitions,
                    assignment_tx,
                })
                .expect("Failed to send assign message to consumer.");
            let assignment = assignment_rx
                .recv()
                .expect("Failed to receive assignment from consumer.");

            info!(
                topic=%self.topic,
                partitions=%assignment.iter().map(|(partition, _)| partition).join(","),
                "New partition assignment"
            );

            for (partition, offset) in assignment {
                let mut partition = tpl.find_partition(&self.topic, partition).expect("");
                partition.set_offset(offset).expect("");
            }
        }
        if let Rebalance::Revoke(tpl) = rebalance {
            let partitions = collect_partitions(tpl, &self.topic);
            debug!(partitions=?partitions, "Revoke partitions.");
            self.events_tx
                .blocking_send(KafkaEvent::RevokePartitions)
                .expect("Failed to send revoke message to consumer.");
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
        _checkpoint: SourceCheckpoint,
    ) -> anyhow::Result<Self> {
        let topic = params.topic.clone();
        let backfill_mode_enabled = params.enable_backfill_mode;

        info!(
            index_id=%ctx.index_id,
            source_id=%ctx.source_config.source_id,
            topic=%topic,
            "Starting Kafka source."
        );

        let (events_tx, events_rx) = mpsc::channel(100);
        let consumer = create_consumer(&ctx.source_config.source_id, params, events_tx.clone())?;
        consumer
            .subscribe(&[&topic])
            .with_context(|| format!("Failed to subscribe to topic `{topic}`."))?;
        let poll_loop_jh = spawn_consumer_poll_loop(consumer.clone(), events_tx);

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

        let next_offsets: Vec<(i32, Offset)> = partitions
            .iter()
            .map(|partition| (*partition, compute_next_offset(*partition, &checkpoint)))
            .collect();

        assignment_tx
            .send(next_offsets)
            .map_err(|_| anyhow!("Consumer context was dropped."))?;

        self.state.num_inactive_partitions = 0;
        self.state.assigned_partitions = partitions
            .iter()
            .map(|partition| (*partition, PartitionId::from(*partition as i64)))
            .collect();
        Ok(())
    }

    async fn process_revoke_partitions(&mut self) -> anyhow::Result<()> {
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
    fn push(&mut self, doc: String, num_bytes: u64) {
        self.docs.push(doc);
        self.num_bytes += num_bytes;
    }

    fn build(self) -> RawDocBatch {
        RawDocBatch {
            docs: self.docs,
            checkpoint_delta: self.checkpoint_delta,
        }
    }
}

#[async_trait]
impl Source for KafkaSource {
    async fn emit_batches(
        &mut self,
        indexer_mailbox: &Mailbox<Indexer>,
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
                        KafkaEvent::RevokePartitions => self.process_revoke_partitions().await?,
                        KafkaEvent::PartitionEOF(partition) => self.process_partition_eof(partition),
                        KafkaEvent::Error(error) => Err(ActorExitStatus::from(error))?,
                    }
                    if batch.num_bytes >= TARGET_BATCH_NUM_BYTES {
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
            ctx.send_message(indexer_mailbox, message).await?;
        }
        if self.should_exit() {
            info!(topic = %self.topic, "Reached end of topic.");
            ctx.send_exit_with_success(indexer_mailbox).await?;
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
        let current_positions: Vec<(&i32, i64)> = self
            .state
            .current_positions
            .iter()
            .map(|(partition_id, position)| {
                let offset = match position {
                    Position::Beginning => -1,
                    Position::Offset(offset_str) => offset_str
                        .parse::<i64>()
                        .expect("Failed to parse offset to i64. This should never happen! Please, report on https://github.com/quickwit-oss/quickwit/issues."),
                    };
                (partition_id, offset)
            })
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
        })
    }
}

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
                if let Err(_) = events_tx.blocking_send(event) {
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

fn compute_next_offset(partition: i32, checkpoint: &SourceCheckpoint) -> Offset {
    let partition_id = PartitionId::from(partition as i64);
    match checkpoint.position_for_partition(&partition_id) {
        Some(Position::Offset(offset_str)) => {
            let offset_i64 = offset_str.parse::<i64>().expect("Failed to parse offset to i64. This should never happen! Please, report on https://github.com/quickwit-oss/quickwit/issues.");
            if offset_i64 < 0 {
                Offset::Beginning
            } else {
                Offset::Offset(offset_i64 + 1)
            }
        }
        Some(Position::Beginning) | None => Offset::Beginning,
    }
}

/// Creates a new `KafkaSourceConsumer`.
fn create_consumer(
    source_id: &str,
    params: KafkaSourceParams,
    events_tx: mpsc::Sender<KafkaEvent>,
) -> anyhow::Result<Arc<RdKafkaConsumer>> {
    let mut client_config = parse_client_params(params.client_params)?;

    // Group ID is limited to 255 characters.
    let mut group_id = format!("quickwit-{}", source_id);
    group_id.truncate(255);
    debug!("Initializing consumer for group_id {}", group_id);

    let log_level = parse_client_log_level(params.client_log_level)?;
    let consumer: RdKafkaConsumer = client_config
        .set("enable.auto.commit", "false") // We manage offsets ourselves: we always want to set this value to `false`.
        .set(
            "enable.partition.eof",
            params.enable_backfill_mode.to_string(),
        )
        .set("group.id", group_id)
        .set_log_level(log_level)
        .create_with_context(RdKafkaContext {
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
            debug!(
                topic = ?message.topic(),
                partition_id = ?message.partition(),
                offset = ?message.offset(),
                timestamp = ?message.timestamp(),
                num_bytes = ?message.payload_len(),
                "Message received.",
            );
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
    use quickwit_actors::{create_test_mailbox, Universe};
    use quickwit_common::rand::append_random_suffix;
    use quickwit_config::{SourceConfig, SourceParams};
    use quickwit_metastore::checkpoint::{IndexCheckpointDelta, SourceCheckpointDelta};
    use quickwit_metastore::{metastore_for_test, IndexMetadata, SplitMetadata};
    use rdkafka::admin::{AdminClient, AdminOptions, NewTopic, TopicReplication};
    use rdkafka::client::DefaultClientContext;
    use rdkafka::message::ToBytes;
    use rdkafka::producer::{FutureProducer, FutureRecord};

    use super::*;
    use crate::new_split_id;
    use crate::source::{quickwit_supported_sources, SourceActor};

    fn create_admin_client(
        bootstrap_servers: &str,
    ) -> anyhow::Result<AdminClient<DefaultClientContext>> {
        let admin_client = ClientConfig::new()
            .set("bootstrap.servers", bootstrap_servers)
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
        bootstrap_servers: &str,
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
            .set("bootstrap.servers", bootstrap_servers)
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

    fn get_source_config(topic: &str, bootstrap_servers: &str) -> (String, SourceConfig) {
        let source_id = append_random_suffix("test-kafka-source--source");
        let source_config = SourceConfig {
            source_id: source_id.clone(),
            source_params: SourceParams::Kafka(KafkaSourceParams {
                topic: topic.to_string(),
                client_log_level: None,
                client_params: json!({
                    "bootstrap.servers": bootstrap_servers,
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

    #[tokio::test]
    async fn test_kafka_source() -> anyhow::Result<()> {
        let universe = Universe::new();
        let topic = append_random_suffix("test-kafka-source--topic");

        let bootstrap_servers = "localhost:9092".to_string();
        let admin_client = create_admin_client(&bootstrap_servers)?;
        create_topic(&admin_client, &topic, 3).await?;

        let source_loader = quickwit_supported_sources();
        {
            let index_id = append_random_suffix("test-kafka-source--index");
            let index_uri = format!("ram:///indexes/{index_id}");

            let index_metadata = IndexMetadata::for_test(&index_id, &index_uri);
            let metastore = metastore_for_test();
            metastore.create_index(index_metadata).await?;

            let (source_id, source_config) = get_source_config(&topic, &bootstrap_servers);
            let source = source_loader
                .load_source(
                    Arc::new(SourceExecutionContext {
                        metastore,
                        source_config: source_config.clone(),
                        index_id: index_id.clone(),
                    }),
                    SourceCheckpoint::default(),
                )
                .await?;

            let (indexer_mailbox, indexer_inbox) = create_test_mailbox();
            let source_actor = SourceActor {
                source,
                indexer_mailbox: indexer_mailbox.clone(),
            };
            let (_source_mailbox, source_handle) = universe.spawn_actor(source_actor).spawn();
            let (exit_status, exit_state) = source_handle.join().await;
            assert!(exit_status.is_success());

            let messages: Vec<RawDocBatch> = indexer_inbox.drain_for_test_typed();
            assert!(messages.is_empty());

            let expected_current_positions: Vec<(i32, i64)> = vec![];
            let expected_state = json!({
                "index_id": index_id,
                "source_id": source_id,
                "topic":  topic,
                "assigned_partitions": vec![0u64, 1u64, 2u64],
                "current_positions":  expected_current_positions,
                "num_inactive_partitions": 3u64,
                "num_bytes_processed": 0u64,
                "num_messages_processed": 0u64,
                "num_invalid_messages": 0u64,
            });
            assert_eq!(exit_state, expected_state);
        }
        for partition_id in 0..3 {
            populate_topic(
                &bootstrap_servers,
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
            let index_id = append_random_suffix("test-kafka-source--index");
            let index_uri = format!("ram:///indexes/{index_id}");

            let index_metadata = IndexMetadata::for_test(&index_id, &index_uri);
            let metastore = metastore_for_test();
            metastore.create_index(index_metadata).await?;

            let (source_id, source_config) = get_source_config(&topic, &bootstrap_servers);
            let source = source_loader
                .load_source(
                    Arc::new(SourceExecutionContext {
                        metastore,
                        source_config: source_config.clone(),
                        index_id: index_id.clone(),
                    }),
                    SourceCheckpoint::default(),
                )
                .await?;

            let (indexer_mailbox, indexer_inbox) = create_test_mailbox();
            let source_actor = SourceActor {
                source,
                indexer_mailbox: indexer_mailbox.clone(),
            };
            let (_source_mailbox, source_handle) = universe.spawn_actor(source_actor).spawn();
            let (exit_status, exit_state) = source_handle.join().await;
            assert!(exit_status.is_success());

            let messages: Vec<RawDocBatch> = indexer_inbox.drain_for_test_typed();
            assert!(messages.len() >= 1);

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
                "assigned_partitions": vec![0u64, 1u64, 2u64],
                "current_positions":  vec![(0u32, 2u64), (1u32, 2u64), (2u32, 2u64)],
                "num_inactive_partitions": 3usize,
                "num_bytes_processed": 72u64,
                "num_messages_processed": 9u64,
                "num_invalid_messages": 3u64,
            });
            assert_eq!(exit_state, expected_state);
        }
        {
            let index_id = append_random_suffix("test-kafka-source--index");
            let index_uri = format!("ram:///indexes/{index_id}");

            let index_metadata = IndexMetadata::for_test(&index_id, &index_uri);
            let metastore = metastore_for_test();
            metastore.create_index(index_metadata).await?;

            let split_id = new_split_id();
            let split_metadata = SplitMetadata::for_test(split_id.clone());
            metastore.stage_split(&index_id, split_metadata).await?;

            let mut source_delta = SourceCheckpointDelta::default();
            source_delta
                .record_partition_delta(0u64.into(), Position::Beginning, 0u64.into())
                .unwrap();
            source_delta
                .record_partition_delta(1u64.into(), Position::Beginning, 2u64.into())
                .unwrap();

            let (source_id, source_config) = get_source_config(&topic, &bootstrap_servers);
            let index_delta = IndexCheckpointDelta {
                source_id: source_id.clone(),
                source_delta,
            };
            metastore
                .publish_splits(&index_id, &[&split_id], &[], Some(index_delta))
                .await
                .unwrap();

            let source = source_loader
                .load_source(
                    Arc::new(SourceExecutionContext {
                        metastore,
                        source_config: source_config.clone(),
                        index_id: index_id.clone(),
                    }),
                    SourceCheckpoint::default(),
                )
                .await?;

            let (indexer_mailbox, indexer_inbox) = create_test_mailbox();
            let source_actor = SourceActor {
                source,
                indexer_mailbox: indexer_mailbox.clone(),
            };
            let (_source_mailbox, source_handle) = universe.spawn_actor(source_actor).spawn();
            let (exit_status, exit_state) = source_handle.join().await;
            assert!(exit_status.is_success());

            let messages: Vec<RawDocBatch> = indexer_inbox.drain_for_test_typed();
            assert!(messages.len() >= 1);

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
                "assigned_partitions": vec![0u64, 1u64, 2u64],
                "current_positions":  vec![(0u64, 2u64), (2u64, 2u64)],
                "num_inactive_partitions": 3usize,
                "num_bytes_processed": 36u64,
                "num_messages_processed": 5u64,
                "num_invalid_messages": 2u64,
            });
            assert_eq!(exit_state, expected_exit_state);
        }
        Ok(())
    }

    #[tokio::test]
    async fn test_kafka_connectivity() -> anyhow::Result<()> {
        let bootstrap_servers = "localhost:9092".to_string();
        let topic = append_random_suffix("test-kafka-connectivity-topic");

        let admin_client = create_admin_client(&bootstrap_servers)?;
        create_topic(&admin_client, &topic, 1).await?;

        // Check valid connectivity
        let result = check_connectivity(KafkaSourceParams {
            topic: topic.clone(),
            client_log_level: None,
            client_params: json!({ "bootstrap.servers": bootstrap_servers }),
            enable_backfill_mode: true,
        })
        .await?;

        assert_eq!(result, ());

        // TODO: these tests should be checking the specific errors.
        // Non existent topic should throw an error.
        let result = check_connectivity(KafkaSourceParams {
            topic: "non-existent-topic".to_string(),
            client_log_level: None,
            client_params: json!({ "bootstrap.servers": bootstrap_servers }),
            enable_backfill_mode: true,
        })
        .await;

        assert!(result.is_err());

        // Invalid brokers should throw an error
        let result = check_connectivity(KafkaSourceParams {
            topic: topic.clone(),
            client_log_level: None,
            client_params: json!({
                "bootstrap.servers": "192.0.2.10:9092"
            }),
            enable_backfill_mode: true,
        })
        .await;

        assert!(result.is_err());
        Ok(())
    }
}
