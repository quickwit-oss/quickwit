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

use std::collections::HashMap;
use std::fmt;
use std::time::{Duration, Instant};

use anyhow::{anyhow, bail, Context};
use async_trait::async_trait;
use bytes::Bytes;
use itertools::Itertools;
use oneshot;
use quickwit_actors::{ActorExitStatus, Mailbox};
use quickwit_config::KafkaSourceParams;
use quickwit_metastore::checkpoint::{PartitionId, SourceCheckpoint};
use quickwit_proto::metastore::SourceType;
use quickwit_proto::types::{IndexUid, Position};
use rdkafka::config::{ClientConfig, RDKafkaLogLevel};
use rdkafka::consumer::{
    BaseConsumer, CommitMode, Consumer, ConsumerContext, DefaultConsumerContext, Rebalance,
};
use rdkafka::error::KafkaError;
use rdkafka::message::BorrowedMessage;
use rdkafka::util::Timeout;
use rdkafka::{ClientContext, Message, Offset, TopicPartitionList};
use serde_json::{json, Value as JsonValue};
use tokio::sync::{mpsc, watch};
use tokio::task::{spawn_blocking, JoinHandle};
use tokio::time;
use tracing::{debug, error, info, warn};

use crate::actors::DocProcessor;
use crate::models::{NewPublishLock, PublishLock};
use crate::source::{
    BatchBuilder, Source, SourceContext, SourceRuntime, TypedSourceFactory, BATCH_NUM_BYTES_LIMIT,
    EMIT_BATCHES_TIMEOUT,
};

type GroupId = String;

/// Factory for instantiating a `KafkaSource`.
pub struct KafkaSourceFactory;

#[async_trait]
impl TypedSourceFactory for KafkaSourceFactory {
    type Source = KafkaSource;
    type Params = KafkaSourceParams;

    async fn typed_create_source(
        source_runtime: SourceRuntime,
        params: KafkaSourceParams,
    ) -> anyhow::Result<Self::Source> {
        KafkaSource::try_new(source_runtime, params).await
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
    doc_opt: Option<Bytes>,
    payload_len: u64,
    partition: i32,
    offset: i64,
}

impl From<BorrowedMessage<'_>> for KafkaMessage {
    fn from(message: BorrowedMessage<'_>) -> Self {
        Self {
            doc_opt: message_payload_to_doc(&message),
            payload_len: message.payload_len() as u64,
            partition: message.partition(),
            offset: message.offset(),
        }
    }
}

struct RdKafkaContext {
    topic: String,
    events_tx: mpsc::Sender<KafkaEvent>,
}

impl ClientContext for RdKafkaContext {}

macro_rules! return_if_err {
    ($expression:expr, $lit: literal) => {
        match $expression {
            Ok(v) => v,
            Err(_) => {
                debug!(concat!($lit, "the source was dropped"));
                return;
            }
        }
    };
}

/// The rebalance protocol at a very high level:
/// - A consumer joins or leaves a consumer group.
/// - Consumers receive a revoke partitions notification, which gives them the opportunity to commit
///   the work in progress.
/// - Broker waits for ALL the consumers to ack the revoke notification (synchronization barrier).
/// - Consumers receive new partition assignmennts.
///
/// The API of the rebalance callback is better explained in the docs of `librdkafka`:
/// <https://docs.confluent.io/2.0.0/clients/librdkafka/classRdKafka_1_1RebalanceCb.html>
impl ConsumerContext for RdKafkaContext {
    fn pre_rebalance(&self, rebalance: &Rebalance) {
        crate::metrics::INDEXER_METRICS.kafka_rebalance_total.inc();
        quickwit_common::rate_limited_info!(limit_per_min = 3, topic = self.topic, "rebalance");
        if let Rebalance::Revoke(tpl) = rebalance {
            let partitions = collect_partitions(tpl, &self.topic);
            debug!(partitions=?partitions, "revoke partitions");

            let (ack_tx, ack_rx) = oneshot::channel();
            return_if_err!(
                self.events_tx
                    .blocking_send(KafkaEvent::RevokePartitions { ack_tx }),
                "failed to send revoke message to source"
            );
            return_if_err!(ack_rx.recv(), "failed to receive revoke ack from source");
        }
        if let Rebalance::Assign(tpl) = rebalance {
            let partitions = collect_partitions(tpl, &self.topic);
            debug!(partitions=?partitions, "assign partitions");

            let (assignment_tx, assignment_rx) = oneshot::channel();
            return_if_err!(
                self.events_tx.blocking_send(KafkaEvent::AssignPartitions {
                    partitions,
                    assignment_tx,
                }),
                "failed to send assign message to source"
            );
            let assignment = return_if_err!(
                assignment_rx.recv(),
                "failed to receive assignment from source"
            );
            for (partition_id, offset) in assignment {
                let Some(mut partition) = tpl.find_partition(&self.topic, partition_id) else {
                    warn!("partition `{partition_id}` not found in assignment");
                    continue;
                };
                if let Err(error) = partition.set_offset(offset) {
                    warn!(
                        "failed to set offset to `{offset:?}` for partition `{partition_id}`: \
                         {error}"
                    );
                }
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
    source_runtime: SourceRuntime,
    topic: String,
    group_id: GroupId,
    state: KafkaSourceState,
    backfill_mode_enabled: bool,
    events_rx: mpsc::Receiver<KafkaEvent>,
    truncate_tx: watch::Sender<SourceCheckpoint>,
    poll_loop_jh: JoinHandle<()>,
    publish_lock: PublishLock,
}

impl fmt::Debug for KafkaSource {
    fn fmt(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
        formatter
            .debug_struct("KafkaSource")
            .field("index_uid", self.source_runtime.index_uid())
            .field("source_id", &self.source_runtime.source_id())
            .field("topic", &self.topic)
            .finish()
    }
}

impl KafkaSource {
    /// Instantiates a new `KafkaSource`.
    pub async fn try_new(
        source_runtime: SourceRuntime,
        source_params: KafkaSourceParams,
    ) -> anyhow::Result<Self> {
        let topic = source_params.topic.clone();
        let backfill_mode_enabled = source_params.enable_backfill_mode;

        let (events_tx, events_rx) = mpsc::channel(100);
        let (truncate_tx, truncate_rx) = watch::channel(SourceCheckpoint::default());
        let (client_config, consumer, group_id) = create_consumer(
            source_runtime.index_uid(),
            source_runtime.source_id(),
            source_params,
            events_tx.clone(),
        )?;
        let native_client_config = client_config.create_native_config()?;
        let session_timeout_ms = native_client_config
            .get("session.timeout.ms")?
            .parse::<u64>()?;
        let max_poll_interval_ms = native_client_config
            .get("max.poll.interval.ms")?
            .parse::<u64>()?;

        let poll_loop_jh =
            spawn_consumer_poll_loop(consumer, topic.clone(), events_tx, truncate_rx);
        let publish_lock = PublishLock::default();

        info!(
            index_uid=%source_runtime.index_uid(),
            source_id=%source_runtime.source_id(),
            topic,
            group_id,
            max_poll_interval_ms,
            session_timeout_ms,
            "starting Kafka source"
        );
        if max_poll_interval_ms <= 60_000 {
            warn!(
                "`max.poll.interval.ms` is set to a short duration that may cause the source to \
                 crash when back pressure from the indexer occurs. The recommended value is \
                 `300000` (5 minutes)."
            );
        }
        Ok(KafkaSource {
            source_runtime,
            topic,
            group_id,
            state: KafkaSourceState::default(),
            backfill_mode_enabled,
            events_rx,
            truncate_tx,
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
            batch.add_doc(doc);
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
                    "received message from unassigned partition `{}`. Assigned partitions: \
                     `{{{}}}`",
                    partition,
                    self.state.assigned_partitions.keys().join(", "),
                )
            })?
            .clone();
        let current_position = Position::offset(offset);
        let previous_position = self
            .state
            .current_positions
            .insert(partition, current_position.clone())
            .unwrap_or_else(|| previous_position_for_offset(offset));
        batch
            .checkpoint_delta
            .record_partition_delta(partition_id, previous_position, current_position)
            .context("failed to record partition delta")?;
        Ok(())
    }

    async fn process_assign_partitions(
        &mut self,
        ctx: &SourceContext,
        partitions: &[i32],
        assignment_tx: oneshot::Sender<Vec<(i32, Offset)>>,
    ) -> anyhow::Result<()> {
        let checkpoint = ctx
            .protect_future(self.source_runtime.fetch_checkpoint())
            .await?;

        self.state.assigned_partitions.clear();
        self.state.current_positions.clear();
        self.state.num_inactive_partitions = 0;

        let mut next_offsets: Vec<(i32, Offset)> = Vec::with_capacity(partitions.len());

        for &partition in partitions {
            let partition_id = PartitionId::from(partition as i64);

            self.state
                .assigned_partitions
                .insert(partition, partition_id.clone());

            let Some(current_position) = checkpoint.position_for_partition(&partition_id).cloned()
            else {
                continue;
            };
            let next_offset = match &current_position {
                Position::Beginning => Offset::Beginning,
                Position::Offset(offset) => {
                    let Some(offset) = offset.as_i64() else {
                        error!("Kafka offset should be stored as i64, skipping partition");
                        continue;
                    };

                    Offset::Offset(offset + 1)
                }
                Position::Eof(_) => {
                    panic!("position of a Kafka partition should never be EOF")
                }
            };
            self.state
                .current_positions
                .insert(partition, current_position);
            next_offsets.push((partition, next_offset));
        }
        info!(
            index_id=%self.source_runtime.index_id(),
            source_id=%self.source_runtime.source_id(),
            topic=%self.topic,
            group_id=%self.group_id,
            partitions=?partitions,
            "new partition assignment after rebalance",
        );
        assignment_tx
            .send(next_offsets)
            .context("Kafka consumer context was dropped")?;
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
            .context("Kafka consumer context was dropped")?;

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
            "reached end of partition"
        );
    }

    fn should_exit(&self) -> bool {
        self.backfill_mode_enabled
            // This check ensures that we don't shutdown the source before the first partition assignment.
            && self.state.num_inactive_partitions > 0
            && self.state.num_inactive_partitions == self.state.assigned_partitions.len()
    }

    fn truncate(&self, checkpoint: SourceCheckpoint) -> anyhow::Result<()> {
        self.truncate_tx
            .send(checkpoint)
            .context("Kafka consumer was dropped")?;
        Ok(())
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
        let mut batch_builder = BatchBuilder::new(SourceType::Kafka);
        let deadline = time::sleep(*EMIT_BATCHES_TIMEOUT);
        tokio::pin!(deadline);

        loop {
            tokio::select! {
                event_opt = self.events_rx.recv() => {
                    let event = event_opt.ok_or_else(|| ActorExitStatus::from(anyhow!("consumer was dropped")))?;
                    match event {
                        KafkaEvent::Message(message) => self.process_message(message, &mut batch_builder).await?,
                        KafkaEvent::AssignPartitions { partitions, assignment_tx} => self.process_assign_partitions(ctx, &partitions, assignment_tx).await?,
                        KafkaEvent::RevokePartitions { ack_tx } => self.process_revoke_partitions(ctx, doc_processor_mailbox, &mut batch_builder, ack_tx).await?,
                        KafkaEvent::PartitionEOF(partition) => self.process_partition_eof(partition),
                        KafkaEvent::Error(error) => Err(ActorExitStatus::from(error))?,
                    }
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
        if self.should_exit() {
            info!(topic = %self.topic, "reached end of topic");
            ctx.send_exit_with_success(doc_processor_mailbox).await?;
            return Err(ActorExitStatus::Success);
        }
        Ok(Duration::default())
    }

    async fn suggest_truncate(
        &mut self,
        checkpoint: SourceCheckpoint,
        _ctx: &SourceContext,
    ) -> anyhow::Result<()> {
        self.truncate(checkpoint)?;
        Ok(())
    }

    async fn finalize(
        &mut self,
        _exit_status: &ActorExitStatus,
        _ctx: &SourceContext,
    ) -> anyhow::Result<()> {
        self.poll_loop_jh.abort();
        Ok(())
    }

    fn name(&self) -> String {
        format!("{:?}", self)
    }

    fn observable_state(&self) -> JsonValue {
        let assigned_partitions: Vec<&i32> =
            self.state.assigned_partitions.keys().sorted().collect();
        let current_positions: Vec<(&i32, &Position)> =
            self.state.current_positions.iter().sorted().collect();
        json!({
            "index_id": self.source_runtime.index_id(),
            "source_id": self.source_runtime.source_id(),
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
    consumer: RdKafkaConsumer,
    topic: String,
    events_tx: mpsc::Sender<KafkaEvent>,
    mut truncate_rx: watch::Receiver<SourceCheckpoint>,
) -> JoinHandle<()> {
    spawn_blocking(move || {
        // `subscribe()` returns immediately but triggers the execution of synchronous code (e.g.
        // rebalance callback) so it must be called in a blocking task.
        //
        // From the librdkafka docs:
        // `subscribe()` is an asynchronous method which returns immediately: background threads
        // will (re)join the group, wait for group rebalance, issue any registered rebalance_cb,
        // assign() the assigned partitions, and then start fetching messages.
        if let Err(error) = consumer.subscribe(&[&topic]) {
            let _ = events_tx.blocking_send(KafkaEvent::Error(anyhow!(error)));
            return;
        }
        while !events_tx.is_closed() {
            if let Some(message_res) = consumer.poll(Some(Duration::from_secs(1))) {
                let event = match message_res {
                    Ok(message) => KafkaEvent::Message(message.into()),
                    Err(KafkaError::PartitionEOF(partition)) => KafkaEvent::PartitionEOF(partition),
                    Err(error) => KafkaEvent::Error(anyhow!(error)),
                };
                // When the source experiences backpressure, this channel becomes full and the
                // consumer might not call `poll()` for a duration that exceeds
                // `max.poll.interval.ms`. When that happens the consumer is kicked out of the group
                // and the source fails. This should not happen in practice with a
                // sufficiently large value for `max.poll.interval.ms`. The default value is 5
                // minutes.
                if events_tx.blocking_send(event).is_err() {
                    break;
                }
            }
            if let Ok(true) = truncate_rx.has_changed() {
                let checkpoint = truncate_rx.borrow_and_update();

                let mut tpl = TopicPartitionList::new();
                for (partition_id, position) in checkpoint.iter() {
                    let partition = partition_id
                        .as_i64()
                        .expect("Kafka partition should be stored as i64.")
                        as i32;
                    // Quickwit positions are inclusive whereas Kafka offsets are exclusive, hence
                    // the increment by 1.

                    let Some(next_position) = position.as_i64() else {
                        error!("Kafka offset should be stored as i64, skipping partition");
                        continue;
                    };

                    let offset = Offset::Offset(next_position + 1);
                    tpl.add_partition_offset(&topic, partition, offset)
                        .expect("The offset should be valid.");
                }
                if let Err(error) = consumer.commit(&tpl, CommitMode::Async) {
                    warn!(error=?error, "failed to commit offsets");
                }
            }
        }
        debug!("exiting consumer poll loop");
        consumer.unsubscribe();
    })
}

/// Returns the preceding `Position` for the offset.
fn previous_position_for_offset(offset: i64) -> Position {
    if offset == 0 {
        Position::Beginning
    } else {
        Position::offset(offset - 1)
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
            .with_context(|| format!("failed to fetch metadata for topic `{topic}`"))
    })
    .await??;

    if cluster_metadata.topics().is_empty() {
        bail!("topic `{}` does not exist", params.topic);
    }
    let topic_metadata = &cluster_metadata.topics()[0];
    assert_eq!(topic_metadata.name(), params.topic); // Belt and suspenders.

    if topic_metadata.partitions().is_empty() {
        bail!("topic `{}` has no partitions", params.topic);
    }
    Ok(())
}

/// Creates a new `KafkaSourceConsumer`.
fn create_consumer(
    index_uid: &IndexUid,
    source_id: &str,
    params: KafkaSourceParams,
    events_tx: mpsc::Sender<KafkaEvent>,
) -> anyhow::Result<(ClientConfig, RdKafkaConsumer, GroupId)> {
    // Group ID is limited to 255 characters.
    let mut group_id = match &params.client_params["group.id"] {
        JsonValue::String(group_id) => group_id.clone(),
        _ => format!("quickwit-{index_uid}-{source_id}"),
    };
    group_id.truncate(255);

    let mut client_config = parse_client_params(params.client_params)?;

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
            topic: params.topic,
            events_tx,
        })
        .context("failed to create Kafka consumer")?;

    Ok((client_config, consumer, group_id))
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
        Some("critical") => RDKafkaLogLevel::Critical,
        Some("alert") => RDKafkaLogLevel::Alert,
        Some("emerg") => RDKafkaLogLevel::Emerg,
        Some(level) => bail!(
            "failed to parse Kafka client log level. value `{}` is not supported",
            level
        ),
    };
    Ok(log_level)
}

fn parse_client_params(client_params: JsonValue) -> anyhow::Result<ClientConfig> {
    let params = if let JsonValue::Object(params) = client_params {
        params
    } else {
        bail!("failed to parse Kafka client parameters. `client_params` must be a JSON object");
    };
    let mut client_config = ClientConfig::new();
    for (key, value_json) in params {
        let value = match value_json {
            JsonValue::Bool(value_bool) => value_bool.to_string(),
            JsonValue::Number(value_number) => value_number.to_string(),
            JsonValue::String(value_string) => value_string,
            JsonValue::Null => continue,
            JsonValue::Array(_) | JsonValue::Object(_) => bail!(
                "failed to parse Kafka client parameters. `client_params.{}` must be a boolean, \
                 number, or string",
                key
            ),
        };
        client_config.set(key, value);
    }
    Ok(client_config)
}

/// Returns the message payload as a `Bytes` object if it exists and is not empty.
fn message_payload_to_doc(message: &BorrowedMessage) -> Option<Bytes> {
    match message.payload() {
        Some(payload) if !payload.is_empty() => {
            let doc = Bytes::from(payload.to_vec());
            return Some(doc);
        }
        Some(_) => debug!(
            topic=%message.topic(),
            partition=%message.partition(),
            offset=%message.offset(),
            timestamp=?message.timestamp(),
            "Document is empty."
        ),
        None => debug!(
            topic=%message.topic(),
            partition=%message.partition(),
            offset=%message.offset(),
            timestamp=?message.timestamp(),
            "Message payload is empty."
        ),
    }
    None
}

#[cfg(all(test, feature = "kafka-broker-tests"))]
mod kafka_broker_tests {
    use std::num::NonZeroUsize;

    use quickwit_actors::{ActorContext, Universe};
    use quickwit_common::rand::append_random_suffix;
    use quickwit_config::{SourceConfig, SourceInputFormat, SourceParams};
    use quickwit_metastore::checkpoint::SourceCheckpointDelta;
    use quickwit_metastore::metastore_for_test;
    use quickwit_proto::types::IndexUid;
    use rdkafka::admin::{AdminClient, AdminOptions, NewTopic, TopicReplication};
    use rdkafka::client::DefaultClientContext;
    use rdkafka::message::ToBytes;
    use rdkafka::producer::{FutureProducer, FutureRecord};
    use tokio::sync::watch;

    use super::*;
    use crate::source::test_setup_helper::setup_index;
    use crate::source::tests::SourceRuntimeBuilder;
    use crate::source::{quickwit_supported_sources, RawDocBatch, SourceActor};

    fn create_base_consumer(group_id: &str) -> BaseConsumer {
        ClientConfig::new()
            .set("bootstrap.servers", "localhost:9092")
            .set("group.id", group_id)
            .create()
            .unwrap()
    }

    fn create_admin_client() -> AdminClient<DefaultClientContext> {
        let admin_client = ClientConfig::new()
            .set("bootstrap.servers", "localhost:9092")
            .create()
            .unwrap();
        admin_client
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
        format!("Key {id}")
    }

    fn get_source_config(topic: &str, auto_offset_reset: &str) -> (String, SourceConfig) {
        let source_id = append_random_suffix("test-kafka-source--source");
        let source_config = SourceConfig {
            source_id: source_id.clone(),
            num_pipelines: NonZeroUsize::new(1).unwrap(),
            enabled: true,
            source_params: SourceParams::Kafka(KafkaSourceParams {
                topic: topic.to_string(),
                client_log_level: None,
                client_params: json!({
                    "auto.offset.reset": auto_offset_reset,
                    "bootstrap.servers": "localhost:9092",
                }),
                enable_backfill_mode: true,
            }),
            transform_config: None,
            input_format: SourceInputFormat::Json,
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
    async fn test_kafka_source_process_message() {
        let admin_client = create_admin_client();
        let topic = append_random_suffix("test-kafka-source--process-message--topic");
        create_topic(&admin_client, &topic, 2).await.unwrap();

        let index_id = append_random_suffix("test-kafka-source--process-message--index");
        let index_uid = IndexUid::new_with_random_ulid(&index_id);
        let (_source_id, source_config) = get_source_config(&topic, "earliest");
        let SourceParams::Kafka(params) = source_config.clone().source_params else {
            panic!(
                "Expected Kafka source params, got {:?}.",
                source_config.source_params
            );
        };
        let source_runtime = SourceRuntimeBuilder::new(index_uid, source_config).build();
        let mut kafka_source = KafkaSource::try_new(source_runtime, params).await.unwrap();

        let partition_id_1 = PartitionId::from(1u64);
        let partition_id_2 = PartitionId::from(2u64);

        kafka_source.state.assigned_partitions =
            HashMap::from_iter([(1, partition_id_1.clone()), (2, partition_id_2.clone())]);

        assert_eq!(kafka_source.state.num_messages_processed, 0);
        assert_eq!(kafka_source.state.num_invalid_messages, 0);

        let mut batch_builder = BatchBuilder::new(SourceType::Kafka);

        let message = KafkaMessage {
            doc_opt: None,
            payload_len: 7,
            partition: 1,
            offset: 0,
        };
        kafka_source
            .process_message(message, &mut batch_builder)
            .await
            .unwrap();

        assert_eq!(batch_builder.docs.len(), 0);
        assert_eq!(batch_builder.num_bytes, 0);
        assert_eq!(
            kafka_source.state.current_positions.get(&1).unwrap(),
            &Position::offset(0u64)
        );
        assert_eq!(kafka_source.state.num_bytes_processed, 7);
        assert_eq!(kafka_source.state.num_messages_processed, 1);
        assert_eq!(kafka_source.state.num_invalid_messages, 1);

        let message = KafkaMessage {
            doc_opt: Some(Bytes::from_static(b"test-doc")),
            payload_len: 8,
            partition: 1,
            offset: 1,
        };
        kafka_source
            .process_message(message, &mut batch_builder)
            .await
            .unwrap();

        assert_eq!(batch_builder.docs.len(), 1);
        assert_eq!(batch_builder.docs[0], "test-doc");
        assert_eq!(batch_builder.num_bytes, 8);
        assert_eq!(
            kafka_source.state.current_positions.get(&1).unwrap(),
            &Position::offset(1u64)
        );
        assert_eq!(kafka_source.state.num_bytes_processed, 15);
        assert_eq!(kafka_source.state.num_messages_processed, 2);
        assert_eq!(kafka_source.state.num_invalid_messages, 1);

        let message = KafkaMessage {
            doc_opt: Some(Bytes::from_static(b"test-doc")),
            payload_len: 8,
            partition: 2,
            offset: 42,
        };
        kafka_source
            .process_message(message, &mut batch_builder)
            .await
            .unwrap();

        assert_eq!(batch_builder.docs.len(), 2);
        assert_eq!(batch_builder.docs[1], "test-doc");
        assert_eq!(batch_builder.num_bytes, 16);
        assert_eq!(
            kafka_source.state.current_positions.get(&2).unwrap(),
            &Position::offset(42u64)
        );
        assert_eq!(kafka_source.state.num_bytes_processed, 23);
        assert_eq!(kafka_source.state.num_messages_processed, 3);
        assert_eq!(kafka_source.state.num_invalid_messages, 1);

        let mut expected_checkpoint_delta = SourceCheckpointDelta::default();
        expected_checkpoint_delta
            .record_partition_delta(partition_id_1, Position::Beginning, Position::offset(1u64))
            .unwrap();
        expected_checkpoint_delta
            .record_partition_delta(
                partition_id_2,
                Position::offset(41u64),
                Position::offset(42u64),
            )
            .unwrap();
        assert_eq!(batch_builder.checkpoint_delta, expected_checkpoint_delta);

        // Message from unassigned partition
        let message = KafkaMessage {
            doc_opt: Some(Bytes::from_static(b"test-doc")),
            payload_len: 8,
            partition: 3,
            offset: 42,
        };
        kafka_source
            .process_message(message, &mut batch_builder)
            .await
            .unwrap_err();
    }

    #[tokio::test]
    async fn test_kafka_source_process_assign_partitions() {
        let admin_client = create_admin_client();
        let topic = append_random_suffix("test-kafka-source--process-assign-partitions--topic");
        create_topic(&admin_client, &topic, 2).await.unwrap();

        let metastore = metastore_for_test();
        let index_id = append_random_suffix("test-kafka-source--process-assign-partitions--index");
        let (_source_id, source_config) = get_source_config(&topic, "earliest");

        let index_uid = setup_index(
            metastore.clone(),
            &index_id,
            &source_config,
            &[(
                PartitionId::from(2u64),
                Position::Beginning,
                Position::offset(42u64),
            )],
        )
        .await;

        let SourceParams::Kafka(params) = source_config.clone().source_params else {
            panic!(
                "Expected Kafka source params, got {:?}.",
                source_config.source_params
            );
        };
        let source_runtime = SourceRuntimeBuilder::new(index_uid, source_config)
            .with_metastore(metastore)
            .build();
        let mut kafka_source = KafkaSource::try_new(source_runtime, params).await.unwrap();
        kafka_source.state.num_inactive_partitions = 1;

        let universe = Universe::with_accelerated_time();
        let (source_mailbox, _source_inbox) = universe.create_test_mailbox();
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
        let expected_current_positions = HashMap::from_iter([(2, Position::offset(42u64))]);
        assert_eq!(
            kafka_source.state.current_positions,
            expected_current_positions
        );

        let assignment = assignment_rx.await.unwrap();
        assert_eq!(assignment, &[(2, Offset::Offset(43))])
    }

    #[tokio::test]
    async fn test_kafka_source_process_revoke_partitions() {
        let admin_client = create_admin_client();
        let topic = append_random_suffix("test-kafka-source--process-revoke-partitions--topic");
        create_topic(&admin_client, &topic, 1).await.unwrap();

        let index_id = append_random_suffix("test-kafka-source--process-revoke--partitions--index");
        let index_uid = IndexUid::new_with_random_ulid(&index_id);
        let (_source_id, source_config) = get_source_config(&topic, "earliest");
        let SourceParams::Kafka(params) = source_config.clone().source_params else {
            panic!(
                "Expected Kafka source params, got {:?}.",
                source_config.source_params
            );
        };
        let source_runtime = SourceRuntimeBuilder::new(index_uid, source_config).build();
        let mut kafka_source = KafkaSource::try_new(source_runtime, params).await.unwrap();

        let universe = Universe::with_accelerated_time();
        let (source_mailbox, _source_inbox) = universe.create_test_mailbox();
        let (indexer_mailbox, indexer_inbox) = universe.create_test_mailbox();
        let (observable_state_tx, _observable_state_rx) = watch::channel(json!({}));
        let ctx: ActorContext<SourceActor> =
            ActorContext::for_test(&universe, source_mailbox, observable_state_tx);
        let (ack_tx, ack_rx) = oneshot::channel();

        let mut batch_builder = BatchBuilder::new(SourceType::Kafka);
        batch_builder.add_doc(Bytes::from_static(b"test-doc"));

        let publish_lock = kafka_source.publish_lock.clone();
        assert!(publish_lock.is_alive());
        assert_eq!(kafka_source.state.num_rebalances, 0);

        kafka_source
            .process_revoke_partitions(&ctx, &indexer_mailbox, &mut batch_builder, ack_tx)
            .await
            .unwrap();

        ack_rx.await.unwrap();
        assert!(batch_builder.docs.is_empty());
        assert!(publish_lock.is_dead());

        assert_eq!(kafka_source.state.num_rebalances, 1);

        let indexer_messages: Vec<NewPublishLock> = indexer_inbox.drain_for_test_typed();
        assert_eq!(indexer_messages.len(), 1);
        assert!(indexer_messages[0].0.is_alive());
    }

    #[tokio::test]
    async fn test_kafka_source_process_partition_eof() {
        let admin_client = create_admin_client();
        let topic = append_random_suffix("test-kafka-source--process-partition-eof--topic");
        create_topic(&admin_client, &topic, 1).await.unwrap();

        let index_id = append_random_suffix("test-kafka-source--process-partition-eof--index");
        let index_uid = IndexUid::new_with_random_ulid(&index_id);
        let (_source_id, source_config) = get_source_config(&topic, "earliest");
        let SourceParams::Kafka(params) = source_config.clone().source_params else {
            panic!(
                "Expected Kafka source params, got {:?}.",
                source_config.source_params
            );
        };
        let source_runtime = SourceRuntimeBuilder::new(index_uid, source_config).build();
        let mut kafka_source = KafkaSource::try_new(source_runtime, params).await.unwrap();
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
    async fn test_kafka_source_suggest_truncate() {
        let admin_client = create_admin_client();
        let topic = append_random_suffix("test-kafka-source--suggest-truncate--topic");
        create_topic(&admin_client, &topic, 2).await.unwrap();

        let metastore = metastore_for_test();
        let index_id = append_random_suffix("test-kafka-source--suggest-truncate--index");
        let (_source_id, source_config) = get_source_config(&topic, "earliest");
        let index_uid = setup_index(
            metastore.clone(),
            &index_id,
            &source_config,
            &[(
                PartitionId::from(2u64),
                Position::Beginning,
                Position::offset(42u64),
            )],
        )
        .await;

        let SourceParams::Kafka(params) = source_config.clone().source_params else {
            panic!(
                "Expected Kafka source params, got {:?}.",
                source_config.source_params
            );
        };
        let source_runtime = SourceRuntimeBuilder::new(index_uid, source_config)
            .with_metastore(metastore)
            .build();
        let mut kafka_source = KafkaSource::try_new(source_runtime, params).await.unwrap();

        let universe = Universe::with_accelerated_time();
        let (source_mailbox, _source_inbox) = universe.create_test_mailbox();
        let (observable_state_tx, _observable_state_rx) = watch::channel(json!({}));
        let ctx: ActorContext<SourceActor> =
            ActorContext::for_test(&universe, source_mailbox, observable_state_tx);

        let KafkaEvent::AssignPartitions {
            partitions,
            assignment_tx,
        } = kafka_source.events_rx.recv().await.unwrap()
        else {
            panic!("Expected `AssignPartitions` event.");
        };
        kafka_source
            .process_assign_partitions(&ctx, &partitions, assignment_tx)
            .await
            .unwrap();

        let checkpoint: SourceCheckpoint = [(0u64, 1u64), (1u64, 2u64)]
            .into_iter()
            .map(|(partition_id, offset)| {
                (PartitionId::from(partition_id), Position::offset(offset))
            })
            .collect();
        kafka_source.truncate(checkpoint).unwrap();

        tokio::time::sleep(Duration::from_secs(1)).await;

        let mut tpl = TopicPartitionList::new();
        tpl.add_partition(&topic, 0);
        tpl.add_partition(&topic, 1);

        let consumer = create_base_consumer(&kafka_source.group_id);
        let committed_offsets = consumer
            .committed_offsets(tpl.clone(), Duration::from_secs(10))
            .unwrap();

        assert_eq!(
            committed_offsets
                .find_partition(&topic, 0)
                .unwrap()
                .offset(),
            Offset::Offset(2)
        );
        assert_eq!(
            committed_offsets
                .find_partition(&topic, 1)
                .unwrap()
                .offset(),
            Offset::Offset(3)
        );
    }

    #[tokio::test]
    async fn test_kafka_source() -> anyhow::Result<()> {
        let universe = Universe::with_accelerated_time();
        let admin_client = create_admin_client();
        let topic = append_random_suffix("test-kafka-source--topic");
        create_topic(&admin_client, &topic, 3).await?;

        let source_loader = quickwit_supported_sources();
        {
            // Test Kafka source with empty topic.
            let metastore = metastore_for_test();
            let index_id = append_random_suffix("test-kafka-source--index");
            let (source_id, source_config) = get_source_config(&topic, "earliest");
            let index_uid = setup_index(metastore.clone(), &index_id, &source_config, &[]).await;
            let source_runtime = SourceRuntimeBuilder::new(index_uid, source_config)
                .with_metastore(metastore)
                .build();
            let source = source_loader.load_source(source_runtime).await?;
            let (doc_processor_mailbox, doc_processor_inbox) = universe.create_test_mailbox();
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
                "current_positions": json!([]),
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
            let (source_id, source_config) = get_source_config(&topic, "earliest");
            let index_uid = setup_index(metastore.clone(), &index_id, &source_config, &[]).await;
            let source_runtime = SourceRuntimeBuilder::new(index_uid, source_config)
                .with_metastore(metastore)
                .build();
            let (doc_processor_mailbox, doc_processor_inbox) = universe.create_test_mailbox();
            let source = source_loader.load_source(source_runtime).await?;
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
                    Position::offset(2u64),
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
            // Test Kafka source with `earliest` offset reset.
            let metastore = metastore_for_test();
            let index_id = append_random_suffix("test-kafka-source--index");
            let (source_id, source_config) = get_source_config(&topic, "earliest");
            let index_uid = setup_index(
                metastore.clone(),
                &index_id,
                &source_config,
                &[
                    (
                        PartitionId::from(0u64),
                        Position::Beginning,
                        Position::offset(0u64),
                    ),
                    (
                        PartitionId::from(1u64),
                        Position::Beginning,
                        Position::offset(2u64),
                    ),
                ],
            )
            .await;
            let source_runtime = SourceRuntimeBuilder::new(index_uid, source_config)
                .with_metastore(metastore)
                .build();
            let source = source_loader.load_source(source_runtime).await?;
            let (doc_processor_mailbox, doc_processor_inbox) = universe.create_test_mailbox();
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
                Position::offset(0u64),
                Position::offset(2u64),
            )?;
            expected_checkpoint_delta.record_partition_delta(
                PartitionId::from(2u64),
                Position::Beginning,
                Position::offset(2u64),
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
        {
            // Test Kafka source with `latest` offset reset.
            let metastore = metastore_for_test();
            let index_id = append_random_suffix("test-kafka-source--index");
            let (source_id, source_config) = get_source_config(&topic, "latest");
            let index_uid = setup_index(metastore.clone(), &index_id, &source_config, &[]).await;
            let source_runtime = SourceRuntimeBuilder::new(index_uid, source_config)
                .with_metastore(metastore)
                .build();
            let source = source_loader.load_source(source_runtime).await?;
            let (doc_processor_mailbox, doc_processor_inbox) = universe.create_test_mailbox();
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
                "current_positions": json!([]),
                "num_inactive_partitions": 3,
                "num_bytes_processed": 0,
                "num_messages_processed": 0,
                "num_invalid_messages": 0,
                "num_rebalances": 0,
            });
            assert_eq!(exit_state, expected_state);
        }
        Ok(())
    }

    #[tokio::test]
    async fn test_kafka_connectivity() {
        let bootstrap_servers = "localhost:9092".to_string();
        let topic = append_random_suffix("test-kafka-connectivity-topic");

        let admin_client = create_admin_client();
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

    #[test]
    fn test_client_config_default_max_poll_interval() {
        // If the client config does not specify `max.poll.interval.ms`, then the default value
        // provided by the native config will be used.
        //
        // This unit test will warn us if the current default value of 5 minutes changes.
        let config = ClientConfig::new();
        let native_config = config.create_native_config().unwrap();
        let default_max_poll_interval_ms = native_config.get("max.poll.interval.ms").unwrap();
        assert_eq!(default_max_poll_interval_ms, "300000");
    }
}
