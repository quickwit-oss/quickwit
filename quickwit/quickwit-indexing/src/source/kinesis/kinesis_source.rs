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
use std::time::Duration;

use anyhow::Context;
use async_trait::async_trait;
use itertools::Itertools;
use quickwit_actors::{ActorExitStatus, Mailbox};
use quickwit_aws::region::sniff_aws_region_and_cache;
use quickwit_aws::retry::RetryParams;
use quickwit_config::{KinesisSourceParams, RegionOrEndpoint};
use quickwit_metastore::checkpoint::{
    PartitionId, Position, SourceCheckpoint, SourceCheckpointDelta,
};
use rusoto_core::Region;
use rusoto_kinesis::KinesisClient;
use serde_json::json;
use tokio::sync::mpsc;
use tokio::time;
use tracing::{info, warn};

use super::api::list_shards;
use super::shard_consumer::{ShardConsumer, ShardConsumerHandle, ShardConsumerMessage};
use crate::actors::DocProcessor;
use crate::models::RawDocBatch;
use crate::source::kinesis::helpers::get_kinesis_client;
use crate::source::{Source, SourceContext, SourceExecutionContext, TypedSourceFactory};

const TARGET_BATCH_NUM_BYTES: u64 = 5_000_000;

type ShardId = String;

/// Factory for instantiating a `KafkaSource`.
pub struct KinesisSourceFactory;

#[async_trait]
impl TypedSourceFactory for KinesisSourceFactory {
    type Source = KinesisSource;
    type Params = KinesisSourceParams;

    async fn typed_create_source(
        ctx: Arc<SourceExecutionContext>,
        params: KinesisSourceParams,
        checkpoint: SourceCheckpoint,
    ) -> anyhow::Result<Self::Source> {
        KinesisSource::try_new(ctx.source_config.source_id.clone(), params, checkpoint).await
    }
}

struct ShardConsumerState {
    partition_id: PartitionId,
    position: Position,
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
    // Source ID
    source_id: String,
    // Target stream to consume.
    stream_name: String,
    // Initialization checkpoint.
    checkpoint: SourceCheckpoint,
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
        write!(
            f,
            "KinesisSource {{ source_id: {}, stream_name: {} }}",
            self.source_id, self.stream_name
        )
    }
}

impl KinesisSource {
    /// Instantiates a new `KinesisSource`.
    pub async fn try_new(
        source_id: String,
        params: KinesisSourceParams,
        checkpoint: SourceCheckpoint,
    ) -> anyhow::Result<Self> {
        let stream_name = params.stream_name;
        let backfill_mode_enabled = params.enable_backfill_mode;
        let region = get_region(params.region_or_endpoint)?;
        let kinesis_client = get_kinesis_client(region)?;
        let (shard_consumers_tx, shard_consumers_rx) = mpsc::channel(1_000);
        let state = KinesisSourceState::default();
        let retry_params = RetryParams::default();
        Ok(KinesisSource {
            source_id,
            stream_name,
            checkpoint,
            kinesis_client,
            shard_consumers_tx,
            shard_consumers_rx,
            state,
            backfill_mode_enabled,
            retry_params,
        })
    }

    fn spawn_shard_consumer(&mut self, ctx: &SourceContext, shard_id: ShardId) {
        assert!(!self.state.shard_consumers.contains_key(&shard_id));

        let partition_id = PartitionId::from(shard_id.as_ref());
        let position = self
            .checkpoint
            .position_for_partition(&partition_id)
            .cloned()
            .unwrap_or(Position::Beginning);
        let from_sequence_number_exclusive = match &position {
            Position::Offset(offset) => Some(offset.to_string()),
            Position::Beginning => None,
        };
        let shard_consumer = ShardConsumer::new(
            self.stream_name.clone(),
            shard_id.clone(),
            from_sequence_number_exclusive,
            self.backfill_mode_enabled,
            self.kinesis_client.clone(),
            self.shard_consumers_tx.clone(),
            self.retry_params.clone(),
        );
        let _shard_consumer_handle = shard_consumer.spawn(ctx);
        let shard_consumer_state = ShardConsumerState {
            partition_id,
            position,
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
        for shard in shards {
            self.spawn_shard_consumer(ctx, shard.shard_id);
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
        let mut batch_num_bytes = 0;
        let mut docs = Vec::new();
        let mut checkpoint_delta = SourceCheckpointDelta::default();

        let deadline = time::sleep(quickwit_actors::HEARTBEAT / 2);
        tokio::pin!(deadline);

        loop {
            tokio::select! {
                message_opt = self.shard_consumers_rx.recv() => {
                    // The source always carries a sender for this channel.
                    match message_opt.expect("Channel unexpectedly closed.") {
                        ShardConsumerMessage::ChildShards(shard_ids) => {
                            for shard_id in shard_ids {
                                self.spawn_shard_consumer(ctx, shard_id);
                            }
                        }
                        ShardConsumerMessage::Records { shard_id, records, lag_millis } => {
                            let num_records = records.len();

                            for (i, record) in records.into_iter().enumerate() {
                                match String::from_utf8(record.data.to_vec()) {
                                    Ok(doc) if !doc.is_empty() => docs.push(doc),
                                    Ok(_) => {
                                        warn!(
                                            stream_name = %self.stream_name,
                                            shard_id = %shard_id,
                                            sequence_number = %record.sequence_number,
                                            "Record is empty."
                                        );
                                        self.state.num_invalid_records += 1;
                                    }
                                    Err(error) => {
                                        warn!(
                                            stream_name = %self.stream_name,
                                            shard_id = %shard_id,
                                            sequence_number = %record.sequence_number,
                                            error = ?error, "Record contains invalid UTF-8 characters."
                                        );
                                        self.state.num_invalid_records += 1;
                                    }
                                };
                                batch_num_bytes += record.data.len() as u64;
                                self.state.num_bytes_processed += record.data.len() as u64;
                                self.state.num_records_processed += 1;

                                if i == num_records - 1 {
                                    let shard_consumer_state = self
                                        .state
                                        .shard_consumers
                                        .get_mut(&shard_id)
                                        .ok_or_else(|| {
                                            anyhow::anyhow!(
                                                "Received record from unassigned shard `{}`.", shard_id,
                                            )
                                        })?;
                                    shard_consumer_state.lag_millis = lag_millis;

                                    let partition_id = shard_consumer_state.partition_id.clone();
                                    let current_position = Position::from(record.sequence_number);
                                    let previous_position = std::mem::replace(&mut shard_consumer_state.position, current_position.clone());

                                    checkpoint_delta.record_partition_delta(
                                        partition_id,
                                        previous_position,
                                        current_position,
                                    ).context("Failed to record partition delta.")?;
                                }
                            }
                            if batch_num_bytes >= TARGET_BATCH_NUM_BYTES {
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
        if !checkpoint_delta.is_empty() {
            let batch = RawDocBatch {
                docs,
                checkpoint_delta,
            };
            ctx.send_message(indexer_mailbox, batch).await?;
        }
        if self.state.shard_consumers.is_empty() {
            info!(stream_name = %self.stream_name, "Reached end of stream.");
            ctx.send_exit_with_success(indexer_mailbox).await?;
            return Err(ActorExitStatus::Success);
        }
        Ok(Duration::default())
    }

    fn name(&self) -> String {
        format!("KinesisSource{{source_id={}}}", self.source_id)
    }

    fn observable_state(&self) -> serde_json::Value {
        let shard_consumer_positions: Vec<(&ShardId, &str)> = self
            .state
            .shard_consumers
            .iter()
            .map(|(shard_id, shard_consumer_state)| {
                (shard_id, shard_consumer_state.position.as_str())
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

pub(super) fn get_region(region_or_endpoint: Option<RegionOrEndpoint>) -> anyhow::Result<Region> {
    if let Some(RegionOrEndpoint::Endpoint(endpoint)) = region_or_endpoint {
        return Ok(Region::Custom {
            name: "Custom".to_string(),
            endpoint,
        });
    }

    if let Some(RegionOrEndpoint::Region(region)) = region_or_endpoint {
        return region
            .parse()
            .with_context(|| format!("Failed to parse region: `{}`", region));
    }

    sniff_aws_region_and_cache() //< We fallback to AWS region if `region_or_endpoint` is `None`
}

#[cfg(all(test, feature = "kinesis-localstack-tests"))]
mod tests {
    use quickwit_actors::{create_test_mailbox, Universe};

    use super::*;
    use crate::source::kinesis::helpers::tests::{
        make_shard_id, put_records_into_shards, setup, teardown,
    };
    use crate::source::SourceActor;

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

    #[test]
    fn test_kinesis_region_resolution() {
        {
            let region_or_endpoint = Some(RegionOrEndpoint::Endpoint(
                "mycustomendpoint.quickwit".to_string(),
            ));
            let region = get_region(region_or_endpoint).unwrap();
            assert_eq!(
                Region::Custom {
                    name: "Custom".to_string(),
                    endpoint: "mycustomendpoint.quickwit".to_string()
                },
                region
            );
        }

        {
            let region_or_endpoint = Some(RegionOrEndpoint::Region("us-east-1".to_string()));
            let region = get_region(region_or_endpoint).unwrap();
            assert_eq!(Region::UsEast1, region);
        }

        {
            let region_or_endpoint = Some(RegionOrEndpoint::Region("quickwit-hq-1".to_string()));
            get_region(region_or_endpoint).unwrap_err();
        }
    }

    #[tokio::test]
    async fn test_kinesis_source() {
        let universe = Universe::new();
        let (doc_processor_mailbox, doc_processor_inbox) = create_test_mailbox();
        let (kinesis_client, stream_name) = setup("test-kinesis-source", 3).await.unwrap();
        let params = KinesisSourceParams {
            stream_name: stream_name.clone(),
            region_or_endpoint: Some(RegionOrEndpoint::Endpoint(
                "http://localhost:4566".to_string(),
            )),
            enable_backfill_mode: true,
        };
        {
            let checkpoint = SourceCheckpoint::default();
            let kinesis_source =
                KinesisSource::try_new("my-kinesis-source".to_string(), params.clone(), checkpoint)
                    .await
                    .unwrap();
            let actor = SourceActor {
                source: Box::new(kinesis_source),
                doc_processor_mailbox: doc_processor_mailbox.clone(),
            };
            let (_mailbox, handle) = universe.spawn_actor(actor).spawn();
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
            let checkpoint = SourceCheckpoint::default();
            let kinesis_source =
                KinesisSource::try_new("my-kinesis-source".to_string(), params.clone(), checkpoint)
                    .await
                    .unwrap();
            let actor = SourceActor {
                source: Box::new(kinesis_source),
                doc_processor_mailbox: doc_processor_mailbox.clone(),
            };
            let (_mailbox, handle) = universe.spawn_actor(actor).spawn();
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
            let checkpoint: SourceCheckpoint = vec![
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
            let kinesis_source =
                KinesisSource::try_new("my-kinesis-source".to_string(), params.clone(), checkpoint)
                    .await
                    .unwrap();
            let actor = SourceActor {
                source: Box::new(kinesis_source),
                doc_processor_mailbox: doc_processor_mailbox.clone(),
            };
            let (_mailbox, handle) = universe.spawn_actor(actor).spawn();
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
