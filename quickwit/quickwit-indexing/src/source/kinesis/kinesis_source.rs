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
use std::time::Duration;

use anyhow::{Context, bail};
use async_trait::async_trait;
use aws_sdk_kinesis::Client as KinesisClient;
use bytes::Bytes;
use itertools::Itertools;
use quickwit_actors::{ActorExitStatus, Mailbox};
use quickwit_aws::get_aws_config;
use quickwit_common::retry::RetryParams;
use quickwit_config::{KinesisSourceParams, RegionOrEndpoint};
use quickwit_metastore::checkpoint::{PartitionId, SourceCheckpoint};
use quickwit_proto::metastore::SourceType;
use quickwit_proto::types::Position;
use serde_json::{Value as JsonValue, json};
use tokio::sync::mpsc;
use tokio::time;
use tracing::{info, warn};

use super::api::list_shards;
use super::shard_consumer::{ShardConsumer, ShardConsumerHandle, ShardConsumerMessage};
use crate::actors::DocProcessor;
use crate::source::kinesis::helpers::get_kinesis_client;
use crate::source::{
    BATCH_NUM_BYTES_LIMIT, BatchBuilder, EMIT_BATCHES_TIMEOUT, Source, SourceContext,
    SourceRuntime, TypedSourceFactory,
};

type ShardId = String;

/// Factory for instantiating a `KafkaSource`.
pub struct KinesisSourceFactory;

#[async_trait]
impl TypedSourceFactory for KinesisSourceFactory {
    type Source = KinesisSource;
    type Params = KinesisSourceParams;

    async fn typed_create_source(
        source_runtime: SourceRuntime,
        source_params: KinesisSourceParams,
    ) -> anyhow::Result<Self::Source> {
        KinesisSource::try_new(source_runtime, source_params).await
    }
}

struct ShardConsumerState {
    partition_id: PartitionId,
    current_position: Position,
    lag_millis: Option<i64>,
    _shard_consumer_handle: ShardConsumerHandle,
}

#[derive(Default)]
pub struct KinesisSourceState {
    /// Pool of [`ShardConsumer`] managed by the source.
    shard_consumers: HashMap<ShardId, ShardConsumerState>,
    /// Number of bytes processed by the source.
    pub num_bytes_processed: u64,
    /// Number of records processed by the source (including invalid messages).
    pub num_records_processed: u64,
    // Number of invalid records, i.e., that were empty or could not be parsed.
    pub num_invalid_records: u64,
}

pub struct KinesisSource {
    // Runtime arguments.
    source_runtime: SourceRuntime,
    // Target stream to consume.
    stream_name: String,
    kinesis_client: KinesisClient,
    // Retry parameters (max attempts, max delay, ...).
    retry_params: RetryParams,
    // Sender for the communication channel between the source and the shard consumers.
    shard_consumers_tx: mpsc::Sender<ShardConsumerMessage>,
    // Receiver for the communication channel between the source and the shard consumers.
    shard_consumers_rx: mpsc::Receiver<ShardConsumerMessage>,
    state: KinesisSourceState,
    backfill_mode_enabled: bool,
}

impl fmt::Debug for KinesisSource {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.debug_struct("KinesisSource")
            .field("index_uid", self.source_runtime.index_uid())
            .field("source_id", &self.source_runtime.source_id())
            .field("stream_Name", &self.stream_name)
            .finish()
    }
}

impl KinesisSource {
    /// Instantiates a new `KinesisSource`.
    pub async fn try_new(
        source_runtime: SourceRuntime,
        source_params: KinesisSourceParams,
    ) -> anyhow::Result<Self> {
        let stream_name = source_params.stream_name;
        let backfill_mode_enabled = source_params.enable_backfill_mode;
        let region = get_region(source_params.region_or_endpoint).await?;
        let kinesis_client = get_kinesis_client(region).await?;
        let (shard_consumers_tx, shard_consumers_rx) = mpsc::channel(1_000);
        let state = KinesisSourceState::default();
        let retry_params = RetryParams::aggressive();
        let kinesis_source = KinesisSource {
            source_runtime,
            stream_name,
            kinesis_client,
            shard_consumers_tx,
            shard_consumers_rx,
            state,
            backfill_mode_enabled,
            retry_params,
        };
        Ok(kinesis_source)
    }

    fn spawn_shard_consumer(
        &mut self,
        ctx: &SourceContext,
        shard_id: ShardId,
        checkpoint: &SourceCheckpoint,
    ) {
        if self.state.shard_consumers.contains_key(&shard_id) {
            info!(
                stream_name = %self.stream_name,
                shard_id = %shard_id,
                "Shard consumer already exists, skipping creation."
            );
            return;
        }

        let partition_id = PartitionId::from(shard_id.as_str());
        let from_position = checkpoint
            .position_for_partition(&partition_id)
            .cloned()
            .unwrap_or(Position::Beginning);
        let from_sequence_number_exclusive = match &from_position {
            Position::Beginning => None,
            Position::Offset(offset) => Some(offset.to_string()),
            Position::Eof(_) => panic!("position of a Kinesis shard should never be EOF"),
        };
        info!(
            stream_name = %self.stream_name,
            shard_id = %shard_id,
            start_position = ?from_position,
            "Spawning new shard consumer"
        );
        let shard_consumer = ShardConsumer::new(
            self.stream_name.clone(),
            shard_id.clone(),
            from_sequence_number_exclusive,
            self.backfill_mode_enabled,
            self.kinesis_client.clone(),
            self.shard_consumers_tx.clone(),
            self.retry_params,
        );
        let _shard_consumer_handle = shard_consumer.spawn(ctx);
        let shard_consumer_state = ShardConsumerState {
            partition_id,
            current_position: from_position,
            lag_millis: None,
            _shard_consumer_handle,
        };
        self.state
            .shard_consumers
            .insert(shard_id, shard_consumer_state);
    }
}

#[async_trait]
impl Source for KinesisSource {
    async fn initialize(
        &mut self,
        _doc_processor_mailbox: &Mailbox<DocProcessor>,
        ctx: &SourceContext,
    ) -> Result<(), ActorExitStatus> {
        let shards = ctx
            .protect_future(list_shards(
                &self.kinesis_client,
                &self.retry_params,
                &self.stream_name,
                None,
            ))
            .await?;
        let checkpoint = self
            .source_runtime
            .fetch_checkpoint()
            .await
            .context("failed to fetch checkpoint")?;

        for shard in shards {
            self.spawn_shard_consumer(ctx, shard.shard_id, &checkpoint);
        }
        info!(
            stream_name = %self.stream_name,
            assigned_shards = %self.state.shard_consumers.keys().sorted().join(", "),
            "Starting Kinesis source."
        );
        Ok(())
    }

    async fn emit_batches(
        &mut self,
        indexer_mailbox: &Mailbox<DocProcessor>,
        ctx: &SourceContext,
    ) -> Result<Duration, ActorExitStatus> {
        let mut batch_builder = BatchBuilder::new(SourceType::Kinesis);
        let deadline = time::sleep(*EMIT_BATCHES_TIMEOUT);
        tokio::pin!(deadline);

        loop {
            tokio::select! {
                message_opt = self.shard_consumers_rx.recv() => {
                    // The source always carries a sender for this channel.
                    match message_opt.expect("Channel unexpectedly closed.") {
                        ShardConsumerMessage::ChildShards(shard_ids) => {
                            let checkpoint = self.source_runtime.fetch_checkpoint().await.context("failed to fetch checkpoint")?;

                            for shard_id in shard_ids {
                                self.spawn_shard_consumer(ctx, shard_id, &checkpoint);
                            }
                        }
                        ShardConsumerMessage::Records { shard_id, records, lag_millis } => {
                            let num_records = records.len();

                            for (i, record) in records.into_iter().enumerate() {
                                let record_data = record.data.into_inner();

                                if record_data.is_empty() {
                                    warn!(
                                        stream_name=%self.stream_name,
                                        shard_id=%shard_id,
                                        sequence_number=%record.sequence_number,
                                        "record is empty"
                                    );
                                    self.state.num_invalid_records += 1;
                                    continue;
                                }
                                batch_builder.add_doc(Bytes::from(record_data));

                                if i == num_records - 1 {
                                    let shard_consumer_state = self
                                        .state
                                        .shard_consumers
                                        .get_mut(&shard_id)
                                        .ok_or_else(|| {
                                            anyhow::anyhow!(
                                                "received record from unassigned shard `{}`", shard_id,
                                            )
                                        })?;
                                    shard_consumer_state.lag_millis = lag_millis;

                                    let partition_id = shard_consumer_state.partition_id.clone();
                                    let current_position = Position::from(record.sequence_number);
                                    let previous_position = std::mem::replace(&mut shard_consumer_state.current_position, current_position.clone());

                                    batch_builder.checkpoint_delta.record_partition_delta(
                                        partition_id,
                                        previous_position,
                                        current_position,
                                    ).context("failed to record partition delta")?;
                                }
                            }
                            if batch_builder.num_bytes >= BATCH_NUM_BYTES_LIMIT {
                                break;
                            }
                        }
                        ShardConsumerMessage::ShardClosed(shard_id) => {
                            info!(
                                stream_name = %self.stream_name,
                                shard_id = %shard_id,
                                num_active_shards = %self.state.shard_consumers.len(),
                                "Shard is closed."
                            );
                            self.state.shard_consumers.remove(&shard_id);

                        }
                        ShardConsumerMessage::ShardEOF(shard_id) => {
                            info!(
                                stream_name = %self.stream_name,
                                shard_id = %shard_id,
                                num_active_shards = %self.state.shard_consumers.len(),
                                "Reached end of shard."
                            );
                            self.state.shard_consumers.remove(&shard_id);
                        }
                    }
                    ctx.record_progress();
                }
                _ = &mut deadline => {
                    break;
                }
            }
        }
        self.state.num_bytes_processed += batch_builder.num_bytes;
        self.state.num_records_processed += batch_builder.docs.len() as u64;

        if !batch_builder.checkpoint_delta.is_empty() {
            ctx.send_message(indexer_mailbox, batch_builder.build())
                .await?;
        }
        if self.state.shard_consumers.is_empty() {
            info!(stream_name = %self.stream_name, "reached end of stream");
            ctx.send_exit_with_success(indexer_mailbox).await?;
            return Err(ActorExitStatus::Success);
        }
        Ok(Duration::default())
    }

    fn name(&self) -> String {
        format!("{self:?}")
    }

    fn observable_state(&self) -> JsonValue {
        let shard_consumer_positions: Vec<(&ShardId, &Position)> = self
            .state
            .shard_consumers
            .iter()
            .map(|(shard_id, shard_consumer_state)| {
                (shard_id, &shard_consumer_state.current_position)
            })
            .sorted()
            .collect();
        json!({
            "stream_name": self.stream_name,
            "shard_consumer_positions": shard_consumer_positions,
            "num_bytes_processed": self.state.num_bytes_processed,
            "num_records_processed": self.state.num_records_processed,
            "num_invalid_records": self.state.num_invalid_records,
        })
    }
}

pub(super) async fn get_region(
    region_or_endpoint_opt: Option<RegionOrEndpoint>,
) -> anyhow::Result<RegionOrEndpoint> {
    if let Some(region_or_endpoint) = region_or_endpoint_opt {
        return Ok(region_or_endpoint);
    }
    //< We fallback to AWS region if `region_or_endpoint` is `None`
    let sdk_config = get_aws_config().await;

    if let Some(region) = sdk_config.region() {
        return Ok(RegionOrEndpoint::Region(region.to_string()));
    }
    if let Some(endpoint) = sdk_config.endpoint_url() {
        return Ok(RegionOrEndpoint::Endpoint(endpoint.to_string()));
    }
    bail!("unable to sniff region from environment")
}

#[cfg(all(test, feature = "kinesis-localstack-tests"))]
mod tests {

    use quickwit_actors::Universe;
    use quickwit_config::{SourceConfig, SourceParams};
    use quickwit_metastore::checkpoint::SourceCheckpointDelta;
    use quickwit_proto::types::IndexUid;

    use super::*;
    use crate::models::RawDocBatch;
    use crate::source::SourceActor;
    use crate::source::kinesis::helpers::tests::{
        make_shard_id, put_records_into_shards, setup, teardown,
    };
    use crate::source::tests::SourceRuntimeBuilder;

    // Sequence number
    type SeqNo = String;

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

    #[ignore]
    #[tokio::test]
    async fn test_kinesis_source_handles_resharding_with_split() {
        use crate::source::kinesis::api::tests::split_shard;
        use crate::source::kinesis::helpers::tests::wait_for_active_stream;

        let universe = Universe::with_accelerated_time();
        let (doc_processor_mailbox, _doc_processor_inbox) = universe.create_test_mailbox();
        let (kinesis_client, stream_name) = setup("test-resharding-split", 1).await.unwrap();
        let index_id = "test-kinesis-resharding-index";
        let index_uid = IndexUid::new_with_random_ulid(index_id);

        // Split the shard (1 -> 2 shards)
        let shard_id_0 = make_shard_id(0);
        split_shard(
            &kinesis_client,
            &stream_name,
            &shard_id_0,
            "85070591730234615865843651857942052864",
        )
        .await
        .unwrap();

        // Wait for stream to be active after split
        let _ = wait_for_active_stream(&kinesis_client, &stream_name)
            .await
            .unwrap();

        // Initialize source after split
        let kinesis_params = KinesisSourceParams {
            stream_name: stream_name.clone(),
            region_or_endpoint: Some(RegionOrEndpoint::Endpoint(
                "http://localhost:4566".to_string(),
            )),
            enable_backfill_mode: true,
        };
        let source_params = SourceParams::Kinesis(kinesis_params.clone());
        let source_config = SourceConfig::for_test("test-kinesis-resharding", source_params);
        let source_runtime = SourceRuntimeBuilder::new(index_uid, source_config).build();

        let kinesis_source = KinesisSource::try_new(source_runtime, kinesis_params)
            .await
            .unwrap();

        let actor = SourceActor {
            source: Box::new(kinesis_source),
            doc_processor_mailbox: doc_processor_mailbox.clone(),
        };
        let (_mailbox, handle) = universe.spawn_builder().spawn(actor);
        let (exit_status, _exit_state) = handle.join().await;
        assert!(exit_status.is_success());

        teardown(&kinesis_client, &stream_name).await;
    }

    #[ignore]
    #[tokio::test]
    async fn test_kinesis_source() {
        let universe = Universe::with_accelerated_time();
        let (doc_processor_mailbox, doc_processor_inbox) = universe.create_test_mailbox();
        let (kinesis_client, stream_name) = setup("test-kinesis-source", 3).await.unwrap();
        let index_id = "test-kinesis-index";
        let index_uid = IndexUid::new_with_random_ulid(index_id);
        let kinesis_params = KinesisSourceParams {
            stream_name: stream_name.clone(),
            region_or_endpoint: Some(RegionOrEndpoint::Endpoint(
                "http://localhost:4566".to_string(),
            )),
            enable_backfill_mode: true,
        };
        let source_params = SourceParams::Kinesis(kinesis_params.clone());
        let source_config = SourceConfig::for_test("test-kinesis-source", source_params);
        let source_runtime = SourceRuntimeBuilder::new(index_uid, source_config).build();
        {
            let kinesis_source =
                KinesisSource::try_new(source_runtime.clone(), kinesis_params.clone())
                    .await
                    .unwrap();
            let actor = SourceActor {
                source: Box::new(kinesis_source),
                doc_processor_mailbox: doc_processor_mailbox.clone(),
            };
            let (_mailbox, handle) = universe.spawn_builder().spawn(actor);
            let (exit_status, exit_state) = handle.join().await;
            assert!(exit_status.is_success());

            let next_message = doc_processor_inbox
                .drain_for_test()
                .into_iter()
                .flat_map(|box_any| box_any.downcast::<RawDocBatch>().ok())
                .map(|box_raw_doc_batch| *box_raw_doc_batch)
                .next();
            assert!(next_message.is_none());

            let expected_shard_consumer_positions: Vec<(ShardId, SeqNo)> = Vec::new();
            let expected_state = json!({
                "stream_name":  stream_name,
                "shard_consumer_positions": expected_shard_consumer_positions,
                "num_bytes_processed": 0,
                "num_records_processed": 0,
                "num_invalid_records": 0,
            });
            assert_eq!(exit_state, expected_state);
        }
        let sequence_numbers = put_records_into_shards(
            &kinesis_client,
            &stream_name,
            [
                (0, "Record #00"),
                (0, "Record #01"),
                (1, "Record #10"),
                (1, "Record #11"),
                (2, "Record #20"),
                (2, "Record #21"),
            ],
        )
        .await
        .unwrap();
        let shard_sequence_numbers: HashMap<usize, SeqNo> = sequence_numbers
            .iter()
            .map(|(shard_id, records)| (*shard_id, records.last().unwrap().clone()))
            .collect();
        let shard_positions: HashMap<usize, Position> = shard_sequence_numbers
            .iter()
            .map(|(shard_id, seqno)| (*shard_id, Position::from(seqno.clone())))
            .collect();
        {
            let kinesis_source =
                KinesisSource::try_new(source_runtime.clone(), kinesis_params.clone())
                    .await
                    .unwrap();
            let actor = SourceActor {
                source: Box::new(kinesis_source),
                doc_processor_mailbox: doc_processor_mailbox.clone(),
            };
            let (_mailbox, handle) = universe.spawn_builder().spawn(actor);
            let (exit_status, exit_state) = handle.join().await;
            assert!(exit_status.is_success());

            let messages: Vec<RawDocBatch> = doc_processor_inbox
                .drain_for_test()
                .into_iter()
                .flat_map(|box_any| box_any.downcast::<RawDocBatch>().ok())
                .map(|box_raw_doc_batch| *box_raw_doc_batch)
                .collect();
            assert!(!messages.is_empty());

            let batch = merge_doc_batches(messages).unwrap();
            let expected_docs = vec![
                "Record #00",
                "Record #01",
                "Record #10",
                "Record #11",
                "Record #20",
                "Record #21",
            ];
            assert_eq!(batch.docs, expected_docs);

            let mut expected_checkpoint_delta = SourceCheckpointDelta::default();
            for shard_id in 0..3 {
                expected_checkpoint_delta
                    .record_partition_delta(
                        PartitionId::from(make_shard_id(shard_id)),
                        Position::Beginning,
                        shard_positions.get(&shard_id).unwrap().clone(),
                    )
                    .unwrap();
            }
            assert_eq!(batch.checkpoint_delta, expected_checkpoint_delta);

            let expected_shard_consumer_positions: Vec<(ShardId, SeqNo)> = Vec::new();
            let expected_state = json!({
                "stream_name":  stream_name,
                "shard_consumer_positions": expected_shard_consumer_positions,
                "num_bytes_processed": 60,
                "num_records_processed": 6,
                "num_invalid_records": 0,
            });
            assert_eq!(exit_state, expected_state);
        }
        {
            let from_sequence_number_exclusive_shard_1 =
                sequence_numbers.get(&1).unwrap().first().unwrap().clone();
            let from_sequence_number_exclusive_shard_2 =
                sequence_numbers.get(&2).unwrap().last().unwrap().clone();
            let _checkpoint: SourceCheckpoint = vec![
                (
                    make_shard_id(1),
                    from_sequence_number_exclusive_shard_1.clone(),
                ),
                (
                    make_shard_id(2),
                    from_sequence_number_exclusive_shard_2.clone(),
                ),
            ]
            .into_iter()
            .map(|(partition_id, offset)| (PartitionId::from(partition_id), Position::from(offset)))
            .collect();
            let kinesis_source = KinesisSource::try_new(source_runtime, kinesis_params)
                .await
                .unwrap();
            let actor = SourceActor {
                source: Box::new(kinesis_source),
                doc_processor_mailbox: doc_processor_mailbox.clone(),
            };
            let (_mailbox, handle) = universe.spawn_builder().spawn(actor);
            let (exit_status, exit_state) = handle.join().await;
            assert!(exit_status.is_success());

            let messages: Vec<RawDocBatch> = doc_processor_inbox
                .drain_for_test()
                .into_iter()
                .flat_map(|box_any| box_any.downcast::<RawDocBatch>().ok())
                .map(|box_raw_doc_batch| *box_raw_doc_batch)
                .collect();
            assert!(!messages.is_empty());

            let batch = merge_doc_batches(messages).unwrap();
            let expected_docs = vec!["Record #00", "Record #01", "Record #11"];
            assert_eq!(batch.docs, expected_docs);

            let mut expected_checkpoint_delta = SourceCheckpointDelta::default();
            for (shard_id, from_position) in [
                Position::Beginning,
                Position::from(from_sequence_number_exclusive_shard_1),
            ]
            .into_iter()
            .enumerate()
            {
                expected_checkpoint_delta
                    .record_partition_delta(
                        PartitionId::from(make_shard_id(shard_id)),
                        from_position,
                        shard_positions.get(&shard_id).unwrap().clone(),
                    )
                    .unwrap();
            }
            assert_eq!(batch.checkpoint_delta, expected_checkpoint_delta);

            let expected_shard_consumer_positions: Vec<(ShardId, SeqNo)> = Vec::new();
            let expected_state = json!({
                "stream_name":  stream_name,
                "shard_consumer_positions": expected_shard_consumer_positions,
                "num_bytes_processed": 30,
                "num_records_processed": 3,
                "num_invalid_records": 0,
            });
            assert_eq!(exit_state, expected_state);
        }
        teardown(&kinesis_client, &stream_name).await;
    }
}
