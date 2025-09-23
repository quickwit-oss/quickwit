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

use std::collections::BTreeSet;
use std::fmt;
use std::time::Duration;

use anyhow::Context;
use async_trait::async_trait;
use fnv::FnvHashMap;
use itertools::Itertools;
use quickwit_actors::{ActorExitStatus, Mailbox};
use quickwit_common::pubsub::EventBroker;
use quickwit_common::retry::RetryParams;
use quickwit_ingest::{
    FetchStreamError, IngesterPool, MRecord, MultiFetchStream, decoded_mrecords,
};
use quickwit_metastore::checkpoint::{PartitionId, SourceCheckpoint};
use quickwit_proto::ingest::IngestV2Error;
use quickwit_proto::ingest::ingester::{
    FetchEof, FetchPayload, IngesterService, TruncateShardsRequest, TruncateShardsSubrequest,
    fetch_message,
};
use quickwit_proto::metastore::{
    AcquireShardsRequest, AcquireShardsResponse, MetastoreService, MetastoreServiceClient,
    SourceType,
};
use quickwit_proto::types::{
    NodeId, PipelineUid, Position, PublishToken, ShardId, SourceId, SourceUid,
};
use serde::Serialize;
use serde_json::json;
use tokio::time;
use tracing::{debug, error, info, warn};
use ulid::Ulid;

use super::{
    BATCH_NUM_BYTES_LIMIT, BatchBuilder, EMIT_BATCHES_TIMEOUT, Source, SourceContext,
    SourceRuntime, TypedSourceFactory,
};
use crate::actors::DocProcessor;
use crate::models::{LocalShardPositionsUpdate, NewPublishLock, NewPublishToken, PublishLock};

pub struct IngestSourceFactory;

#[async_trait]
impl TypedSourceFactory for IngestSourceFactory {
    type Source = IngestSource;
    type Params = ();

    async fn typed_create_source(
        source_runtime: SourceRuntime,
        _params: Self::Params,
    ) -> anyhow::Result<Self::Source> {
        // Retry parameters for the fetch stream: retry indefinitely until the shard is complete or
        // unassigned.
        let retry_params = RetryParams {
            max_attempts: usize::MAX,
            base_delay: Duration::from_secs(5),
            max_delay: Duration::from_secs(10 * 60), // 10 minutes
        };
        IngestSource::try_new(source_runtime, retry_params).await
    }
}

/// The [`ClientId`] is a unique identifier for a client of the ingest service and allows to
/// distinguish which indexers are streaming documents from a shard. It is also used to form a
/// publish token.
#[derive(Debug, Clone)]
struct ClientId {
    node_id: NodeId,
    source_uid: SourceUid,
    pipeline_uid: PipelineUid,
}

impl fmt::Display for ClientId {
    fn fmt(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
        write!(
            formatter,
            "indexer/{}/{}/{}/{}",
            self.node_id, self.source_uid.index_uid, self.source_uid.source_id, self.pipeline_uid
        )
    }
}

impl ClientId {
    fn new(node_id: NodeId, source_uid: SourceUid, pipeline_uid: PipelineUid) -> Self {
        ClientId {
            node_id,
            source_uid,
            pipeline_uid,
        }
    }

    fn new_publish_token(&self) -> String {
        let ulid = if cfg!(test) { Ulid::nil() } else { Ulid::new() };
        format!("{self}/{ulid}")
    }
}

#[derive(Debug, Clone, Copy, Default, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
enum IndexingStatus {
    #[default]
    // Indexing is in progress.
    Active,
    // All documents have been indexed AND published.
    Complete,
    Error,
    // The shard no longer exists.
    NotFound,
    // We have received all documents from the stream. Note that they
    // are not necessarily published yet.
    ReachedEof,
}

#[derive(Debug, Eq, PartialEq)]
struct AssignedShard {
    leader_id: NodeId,
    follower_id_opt: Option<NodeId>,
    // This is just the shard id converted to a partition id object.
    partition_id: PartitionId,
    current_position_inclusive: Position,
    status: IndexingStatus,
}

/// Streams documents from a set of shards.
pub struct IngestSource {
    client_id: ClientId,
    metastore: MetastoreServiceClient,
    ingester_pool: IngesterPool,
    assigned_shards: FnvHashMap<ShardId, AssignedShard>,
    fetch_stream: MultiFetchStream,
    publish_lock: PublishLock,
    publish_token: PublishToken,
    event_broker: EventBroker,
}

impl fmt::Debug for IngestSource {
    fn fmt(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
        formatter.debug_struct("IngestSource").finish()
    }
}

impl IngestSource {
    pub async fn try_new(
        source_runtime: SourceRuntime,
        retry_params: RetryParams,
    ) -> anyhow::Result<IngestSource> {
        let self_node_id: NodeId = source_runtime.node_id().into();
        let client_id = ClientId::new(
            self_node_id.clone(),
            SourceUid {
                index_uid: source_runtime.index_uid().clone(),
                source_id: source_runtime.source_id().to_string(),
            },
            source_runtime.pipeline_uid(),
        );
        let metastore = source_runtime.metastore.clone();
        let ingester_pool = source_runtime.ingester_pool.clone();
        let assigned_shards = FnvHashMap::default();
        let fetch_stream = MultiFetchStream::new(
            self_node_id,
            client_id.to_string(),
            ingester_pool.clone(),
            retry_params,
        );
        // We start as dead. The first reset with a non-empty list of shards will create an alive
        // publish lock.
        let publish_lock = PublishLock::dead();
        let publish_token = client_id.new_publish_token();

        Ok(IngestSource {
            client_id,
            metastore,
            ingester_pool,
            assigned_shards,
            fetch_stream,
            publish_lock,
            publish_token,
            event_broker: source_runtime.event_broker.clone(),
        })
    }

    fn process_fetch_payload(
        &mut self,
        batch_builder: &mut BatchBuilder,
        fetch_payload: FetchPayload,
    ) -> anyhow::Result<()> {
        let mrecord_batch = match &fetch_payload.mrecord_batch {
            Some(mrecord_batch) if !mrecord_batch.is_empty() => mrecord_batch,
            _ => {
                warn!("received empty mrecord batch");
                return Ok(());
            }
        };
        let assigned_shard = self
            .assigned_shards
            .get_mut(fetch_payload.shard_id())
            .expect("shard should be assigned");

        assigned_shard.status = IndexingStatus::Active;

        let partition_id = assigned_shard.partition_id.clone();
        let from_position_exclusive = fetch_payload.from_position_exclusive();
        let to_position_inclusive = fetch_payload.to_position_inclusive();

        for mrecord in decoded_mrecords(mrecord_batch) {
            match mrecord {
                MRecord::Doc(doc) => {
                    batch_builder.add_doc(doc);
                }
                MRecord::Commit => {
                    batch_builder.force_commit();
                }
            }
        }
        batch_builder
            .checkpoint_delta
            .record_partition_delta(
                partition_id,
                from_position_exclusive,
                to_position_inclusive.clone(),
            )
            .context("failed to record partition delta")?;
        assigned_shard.current_position_inclusive = to_position_inclusive;
        Ok(())
    }

    fn process_fetch_eof(
        &mut self,
        batch_builder: &mut BatchBuilder,
        fetch_eof: FetchEof,
    ) -> anyhow::Result<()> {
        let assigned_shard = self
            .assigned_shards
            .get_mut(fetch_eof.shard_id())
            .expect("shard should be assigned");

        assigned_shard.status = IndexingStatus::ReachedEof;

        let partition_id = assigned_shard.partition_id.clone();
        let from_position_exclusive = assigned_shard.current_position_inclusive.clone();
        let to_position_inclusive = fetch_eof.eof_position();

        batch_builder
            .checkpoint_delta
            .record_partition_delta(
                partition_id,
                from_position_exclusive,
                to_position_inclusive.clone(),
            )
            .context("failed to record partition delta")?;
        assigned_shard.current_position_inclusive = to_position_inclusive;
        Ok(())
    }

    fn process_fetch_stream_error(
        &mut self,
        batch_builder: &mut BatchBuilder,
        fetch_stream_error: FetchStreamError,
    ) -> anyhow::Result<()> {
        let Some(assigned_shard) = self.assigned_shards.get_mut(&fetch_stream_error.shard_id)
        else {
            return Ok(());
        };
        if assigned_shard.status == IndexingStatus::Complete {
            return Ok(());
        }
        if let IngestV2Error::ShardNotFound { .. } = fetch_stream_error.ingest_error {
            batch_builder.checkpoint_delta.record_partition_delta(
                assigned_shard.partition_id.clone(),
                assigned_shard.current_position_inclusive.clone(),
                assigned_shard.current_position_inclusive.as_eof(),
            )?;
            assigned_shard.current_position_inclusive.to_eof();
            assigned_shard.status = IndexingStatus::NotFound;
        } else if assigned_shard.status != IndexingStatus::ReachedEof {
            assigned_shard.status = IndexingStatus::Error;
        }
        Ok(())
    }

    async fn truncate(&mut self, truncate_up_to_positions: Vec<(ShardId, Position)>) {
        if truncate_up_to_positions.is_empty() {
            return;
        }
        let shard_positions_update = LocalShardPositionsUpdate::new(
            self.client_id.source_uid.clone(),
            truncate_up_to_positions.clone(),
        );
        // Let's record all shards that have reached Eof as complete.
        for (shard, truncate_up_to_position_inclusive) in &truncate_up_to_positions {
            if truncate_up_to_position_inclusive.is_eof()
                && let Some(assigned_shard) = self.assigned_shards.get_mut(shard)
            {
                assigned_shard.status = IndexingStatus::Complete;
            }
        }

        // We publish the event to the event broker.
        self.event_broker.publish(shard_positions_update);

        // Finally, we push the information to ingesters in a best effort manner.
        // If the request fails, we just log an error.
        let mut per_ingester_truncate_subrequests: FnvHashMap<
            &NodeId,
            Vec<TruncateShardsSubrequest>,
        > = FnvHashMap::default();

        for (shard_id, truncate_up_to_position_inclusive) in truncate_up_to_positions {
            if truncate_up_to_position_inclusive.is_beginning() {
                continue;
            }
            let Some(shard) = self.assigned_shards.get(&shard_id) else {
                warn!("failed to truncate shard `{shard_id}`: shard is no longer assigned");
                continue;
            };
            let truncate_shards_subrequest = TruncateShardsSubrequest {
                index_uid: self.client_id.source_uid.index_uid.clone().into(),
                source_id: self.client_id.source_uid.source_id.clone(),
                shard_id: Some(shard_id),
                truncate_up_to_position_inclusive: Some(truncate_up_to_position_inclusive),
            };
            if let Some(follower_id) = &shard.follower_id_opt {
                per_ingester_truncate_subrequests
                    .entry(follower_id)
                    .or_default()
                    .push(truncate_shards_subrequest.clone());
            }
            per_ingester_truncate_subrequests
                .entry(&shard.leader_id)
                .or_default()
                .push(truncate_shards_subrequest);
        }
        for (ingester_id, truncate_subrequests) in per_ingester_truncate_subrequests {
            let Some(ingester) = self.ingester_pool.get(ingester_id) else {
                warn!("failed to truncate shard(s): ingester `{ingester_id}` is unavailable");
                continue;
            };
            let truncate_shards_request = TruncateShardsRequest {
                ingester_id: ingester_id.clone().into(),
                subrequests: truncate_subrequests,
            };
            let truncate_future = async move {
                let retry_params = RetryParams {
                    base_delay: Duration::from_secs(1),
                    max_delay: Duration::from_secs(10),
                    max_attempts: 5,
                };
                for num_attempts in 1..=retry_params.max_attempts {
                    let Err(error) = ingester
                        .truncate_shards(truncate_shards_request.clone())
                        .await
                    else {
                        return;
                    };
                    let delay = retry_params.compute_delay(num_attempts);
                    time::sleep(delay).await;

                    if num_attempts == retry_params.max_attempts {
                        warn!(
                            ingester_id=%truncate_shards_request.ingester_id,
                            "failed to truncate shard(s): {error}"
                        );
                    }
                }
            };
            // Truncation is best-effort, so fire and forget.
            tokio::spawn(truncate_future);
        }
    }

    /// If the new assignment removes a shard that we were in the middle of indexing (ie they have
    /// not reached `IndexingStatus::Complete` status yet), we need to reset the pipeline:
    ///
    /// Ongoing work and splits traveling through the pipeline will be dropped.
    ///
    /// After this method has returned we are guaranteed to have the following post condition:
    /// - a alive publish lock / non-empty publish token
    /// - all currently assigned shards included in the `new_assigned_shard_ids` set.
    async fn reset_if_needed(
        &mut self,
        new_assigned_shard_ids: &BTreeSet<ShardId>,
        doc_processor_mailbox: &Mailbox<DocProcessor>,
        ctx: &SourceContext,
    ) -> anyhow::Result<()> {
        // No need to do anything if the list of shards before and after are empty.
        if new_assigned_shard_ids.is_empty() && self.assigned_shards.is_empty() {
            return Ok(());
        }
        // There are two reasons why we might want to reset the pipeline.
        // 1) it has never been initialized in the first place. This happens typically on the first
        // call to `assign_shards` with a non-empty list of shards. We check that by looking at
        // whether the publish lock is dead or not.
        // 2) we are removing a shard that has not reached the complete status yet.
        let reset_needed: bool = self.publish_lock.is_dead()
            || self
                .assigned_shards
                .keys()
                .filter(|&shard_id| !new_assigned_shard_ids.contains(shard_id))
                .cloned()
                .any(|removed_shard_id| {
                    let Some(assigned_shard) = self.assigned_shards.get(&removed_shard_id) else {
                        return false;
                    };
                    assigned_shard.status != IndexingStatus::Complete
                });

        if !reset_needed {
            // Not need to reset the fetch streams, we can just remove the shard that have been
            // completely indexed.
            self.assigned_shards.retain(|shard_id, assignment| {
                if new_assigned_shard_ids.contains(shard_id) {
                    true
                } else {
                    assert_eq!(assignment.status, IndexingStatus::Complete);
                    false
                }
            });
            return Ok(());
        }
        info!(
            index_uid=%self.client_id.source_uid.index_uid,
            pipeline_uid=%self.client_id.pipeline_uid,
            "resetting indexing pipeline"
        );
        self.assigned_shards.clear();
        self.fetch_stream.reset();
        self.publish_lock.kill().await;
        self.publish_lock = PublishLock::default();
        self.publish_token = self.client_id.new_publish_token();
        ctx.send_message(
            doc_processor_mailbox,
            NewPublishLock(self.publish_lock.clone()),
        )
        .await?;
        ctx.send_message(
            doc_processor_mailbox,
            NewPublishToken(self.publish_token.clone()),
        )
        .await?;
        Ok(())
    }
}

#[async_trait]
impl Source for IngestSource {
    async fn emit_batches(
        &mut self,
        doc_processor_mailbox: &Mailbox<DocProcessor>,
        ctx: &SourceContext,
    ) -> Result<Duration, ActorExitStatus> {
        let mut batch_builder = BatchBuilder::new(SourceType::IngestV2);

        let now = time::Instant::now();
        let deadline = now + *EMIT_BATCHES_TIMEOUT;
        loop {
            match time::timeout_at(deadline, self.fetch_stream.next()).await {
                Ok(Ok(fetch_message)) => match fetch_message.message {
                    Some(fetch_message::Message::Payload(fetch_payload)) => {
                        self.process_fetch_payload(&mut batch_builder, fetch_payload)?;

                        if batch_builder.num_bytes >= BATCH_NUM_BYTES_LIMIT {
                            break;
                        }
                    }
                    Some(fetch_message::Message::Eof(fetch_eof)) => {
                        self.process_fetch_eof(&mut batch_builder, fetch_eof)?;
                    }
                    None => {
                        warn!("received empty fetch message");
                        continue;
                    }
                },
                Ok(Err(fetch_stream_error)) => {
                    self.process_fetch_stream_error(&mut batch_builder, fetch_stream_error)?;
                }
                Err(_) => {
                    // The deadline has elapsed.
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
                "Sending doc batch to indexer."
            );
            let message = batch_builder.build();
            ctx.send_message(doc_processor_mailbox, message).await?;
        }
        Ok(Duration::default())
    }

    async fn assign_shards(
        &mut self,
        new_assigned_shard_ids: BTreeSet<ShardId>,
        doc_processor_mailbox: &Mailbox<DocProcessor>,
        ctx: &SourceContext,
    ) -> anyhow::Result<()> {
        self.reset_if_needed(&new_assigned_shard_ids, doc_processor_mailbox, ctx)
            .await?;

        // As enforced by `reset_if_needed`, at this point, all currently assigned shards should be
        // in the new_assigned_shards.
        debug_assert!(
            self.assigned_shards
                .keys()
                .all(|shard_id| new_assigned_shard_ids.contains(shard_id))
        );

        if self.assigned_shards.len() == new_assigned_shard_ids.len() {
            // Nothing to do.
            // The set shards is unchanged.
            return Ok(());
        }

        let added_shard_ids: Vec<ShardId> = new_assigned_shard_ids
            .into_iter()
            .filter(|shard_id| !self.assigned_shards.contains_key(shard_id))
            .collect();

        assert!(!added_shard_ids.is_empty());
        info!(added_shards=?added_shard_ids, "adding shards assignment");

        let acquire_shards_request = AcquireShardsRequest {
            index_uid: Some(self.client_id.source_uid.index_uid.clone()),
            source_id: self.client_id.source_uid.source_id.clone(),
            shard_ids: added_shard_ids.clone(),
            publish_token: self.publish_token.clone(),
        };
        let acquire_shards_response: AcquireShardsResponse = ctx
            .protect_future(self.metastore.acquire_shards(acquire_shards_request))
            .await
            .context("failed to acquire shards")?;

        if acquire_shards_response.acquired_shards.len() != added_shard_ids.len() {
            let missing_shards = added_shard_ids
                .iter()
                .filter(|shard_id| {
                    !acquire_shards_response
                        .acquired_shards
                        .iter()
                        .any(|acquired_shard| acquired_shard.shard_id() == *shard_id)
                })
                .collect::<Vec<_>>();
            // This can happen if the shards have been deleted by the control plane, after building
            // the plan and before the apply terminated. See #4888.
            info!(missing_shards=?missing_shards, "failed to acquire all assigned shards");
        }

        let mut truncate_up_to_positions =
            Vec::with_capacity(acquire_shards_response.acquired_shards.len());

        for acquired_shard in acquire_shards_response.acquired_shards {
            let index_uid = acquired_shard.index_uid().clone();
            let shard_id = acquired_shard.shard_id().clone();
            let mut current_position_inclusive = acquired_shard.publish_position_inclusive();
            let leader_id: NodeId = acquired_shard.leader_id.into();
            let follower_id_opt: Option<NodeId> = acquired_shard.follower_id.map(Into::into);
            let source_id: SourceId = acquired_shard.source_id;
            let partition_id = PartitionId::from(shard_id.as_str());
            let from_position_exclusive = current_position_inclusive.clone();

            let status = if from_position_exclusive.is_eof() {
                IndexingStatus::Complete
            } else if let Err(error) = ctx
                .protect_future(self.fetch_stream.subscribe(
                    leader_id.clone(),
                    follower_id_opt.clone(),
                    index_uid,
                    source_id,
                    shard_id.clone(),
                    from_position_exclusive,
                ))
                .await
            {
                if let IngestV2Error::ShardNotFound { .. } = error {
                    error!("failed to subscribe to shard `{shard_id}`: shard not found");
                    current_position_inclusive.to_eof();
                    IndexingStatus::NotFound
                } else {
                    error!(%error, "failed to subscribe to shard `{shard_id}`");
                    IndexingStatus::Error
                }
            } else {
                IndexingStatus::Active
            };
            truncate_up_to_positions.push((shard_id.clone(), current_position_inclusive.clone()));

            let assigned_shard = AssignedShard {
                leader_id,
                follower_id_opt,
                partition_id,
                current_position_inclusive,
                status,
            };
            self.assigned_shards.insert(shard_id, assigned_shard);
        }

        self.truncate(truncate_up_to_positions).await;

        Ok(())
    }

    async fn suggest_truncate(
        &mut self,
        checkpoint: SourceCheckpoint,
        _ctx: &SourceContext,
    ) -> anyhow::Result<()> {
        let truncate_up_to_positions: Vec<(ShardId, Position)> = checkpoint
            .iter()
            .map(|(partition_id, position)| {
                let shard_id = ShardId::from(partition_id.as_str());
                (shard_id, position)
            })
            .collect();
        self.truncate(truncate_up_to_positions).await;
        Ok(())
    }

    fn name(&self) -> String {
        "IngestSource".to_string()
    }

    fn observable_state(&self) -> serde_json::Value {
        let assigned_shards: Vec<serde_json::Value> = self
            .assigned_shards
            .iter()
            .sorted_by(|(left_shard_id, _), (right_shard_id, _)| left_shard_id.cmp(right_shard_id))
            .map(|(shard_id, assigned_shard)| {
                json!({
                    "shard_id": *shard_id,
                    "current_position": assigned_shard.current_position_inclusive,
                    "status": assigned_shard.status,
                })
            })
            .collect();
        json!({
            "client_id": self.client_id.to_string(),
            "assigned_shards": assigned_shards,
            "publish_token": self.publish_token,
        })
    }
}

#[cfg(test)]
mod tests {
    use std::iter::once;
    use std::path::PathBuf;
    use std::sync::Arc;
    use std::sync::atomic::AtomicBool;

    use bytesize::ByteSize;
    use itertools::Itertools;
    use quickwit_actors::{ActorContext, Universe};
    use quickwit_common::ServiceStream;
    use quickwit_common::metrics::MEMORY_METRICS;
    use quickwit_common::stream_utils::InFlightValue;
    use quickwit_config::{IndexingSettings, SourceConfig, SourceParams};
    use quickwit_proto::indexing::IndexingPipelineId;
    use quickwit_proto::ingest::ingester::{
        FetchMessage, IngesterServiceClient, MockIngesterService, TruncateShardsResponse,
    };
    use quickwit_proto::ingest::{IngestV2Error, MRecordBatch, Shard, ShardState};
    use quickwit_proto::metastore::{AcquireShardsResponse, MockMetastoreService};
    use quickwit_proto::types::{DocMappingUid, IndexUid, PipelineUid};
    use quickwit_storage::StorageResolver;
    use tokio::sync::mpsc::error::TryRecvError;
    use tokio::sync::watch;

    use super::*;
    use crate::models::RawDocBatch;
    use crate::source::SourceActor;

    // In this test, we simulate a source to which we sequentially assign the following set of
    // shards []
    // [1] (triggers a reset, and the creation of a publish lock)
    // [1,2]
    // [2,3] (which triggers a reset)
    #[tokio::test]
    async fn test_ingest_source_assign_shards() {
        let pipeline_id = IndexingPipelineId {
            node_id: NodeId::from("test-node"),
            index_uid: IndexUid::for_test("test-index", 0),
            source_id: "test-source".to_string(),
            pipeline_uid: PipelineUid::default(),
        };
        let source_config = SourceConfig::for_test("test-source", SourceParams::Ingest);
        let publish_token = "indexer/test-node/test-index:0/test-source/\
                             00000000000000000000000000/00000000000000000000000000";

        let mut mock_metastore = MockMetastoreService::new();
        mock_metastore
            .expect_acquire_shards()
            .withf(|request| request.shard_ids == [ShardId::from(0)])
            .once()
            .returning(|request| {
                assert_eq!(request.index_uid(), &("test-index", 0));
                assert_eq!(request.source_id, "test-source");
                let response = AcquireShardsResponse {
                    acquired_shards: vec![Shard {
                        index_uid: Some(IndexUid::for_test("test-index", 0)),
                        source_id: "test-source".to_string(),
                        leader_id: "test-ingester-0".to_string(),
                        follower_id: None,
                        shard_id: Some(ShardId::from(0)),
                        shard_state: ShardState::Open as i32,
                        doc_mapping_uid: Some(DocMappingUid::default()),
                        publish_position_inclusive: Some(Position::offset(10u64)),
                        publish_token: Some(publish_token.to_string()),
                        update_timestamp: 1724158996,
                    }],
                };
                Ok(response)
            });
        mock_metastore
            .expect_acquire_shards()
            .once()
            .withf(|request| request.shard_ids == [ShardId::from(1)])
            .returning(|request| {
                assert_eq!(request.index_uid(), &("test-index", 0));
                assert_eq!(request.source_id, "test-source");

                let response = AcquireShardsResponse {
                    acquired_shards: vec![Shard {
                        leader_id: "test-ingester-0".to_string(),
                        follower_id: None,
                        index_uid: Some(IndexUid::for_test("test-index", 0)),
                        source_id: "test-source".to_string(),
                        shard_id: Some(ShardId::from(1)),
                        shard_state: ShardState::Open as i32,
                        doc_mapping_uid: Some(DocMappingUid::default()),
                        publish_position_inclusive: Some(Position::offset(11u64)),
                        publish_token: Some(publish_token.to_string()),
                        update_timestamp: 1724158996,
                    }],
                };
                Ok(response)
            });
        mock_metastore
            .expect_acquire_shards()
            .withf(|request| request.shard_ids == [ShardId::from(1), ShardId::from(2)])
            .once()
            .returning(|request| {
                assert_eq!(request.index_uid(), &("test-index", 0));
                assert_eq!(request.source_id, "test-source");

                let response = AcquireShardsResponse {
                    acquired_shards: vec![
                        Shard {
                            leader_id: "test-ingester-0".to_string(),
                            follower_id: None,
                            index_uid: Some(IndexUid::for_test("test-index", 0)),
                            source_id: "test-source".to_string(),
                            shard_id: Some(ShardId::from(1)),
                            shard_state: ShardState::Open as i32,
                            doc_mapping_uid: Some(DocMappingUid::default()),
                            publish_position_inclusive: Some(Position::offset(11u64)),
                            publish_token: Some(publish_token.to_string()),
                            update_timestamp: 1724158996,
                        },
                        Shard {
                            leader_id: "test-ingester-0".to_string(),
                            follower_id: None,
                            index_uid: Some(IndexUid::for_test("test-index", 0)),
                            source_id: "test-source".to_string(),
                            shard_id: Some(ShardId::from(2)),
                            shard_state: ShardState::Open as i32,
                            doc_mapping_uid: Some(DocMappingUid::default()),
                            publish_position_inclusive: Some(Position::offset(12u64)),
                            publish_token: Some(publish_token.to_string()),
                            update_timestamp: 1724158996,
                        },
                    ],
                };
                Ok(response)
            });
        let ingester_pool = IngesterPool::default();

        // This sequence is used to remove the race condition by waiting for the fetch stream
        // request.
        let (sequence_tx, mut sequence_rx) = tokio::sync::mpsc::unbounded_channel::<usize>();

        let mut mock_ingester_0 = MockIngesterService::new();
        let sequence_tx_clone1 = sequence_tx.clone();
        mock_ingester_0
            .expect_open_fetch_stream()
            .withf(|request| {
                request.from_position_exclusive() == Position::offset(10u64)
                    && request.shard_id() == ShardId::from(0)
            })
            .once()
            .returning(move |request| {
                sequence_tx_clone1.send(1).unwrap();
                assert_eq!(
                    request.client_id,
                    "indexer/test-node/test-index:00000000000000000000000000/test-source/\
                     00000000000000000000000000"
                );
                assert_eq!(request.index_uid(), &("test-index", 0));
                assert_eq!(request.source_id, "test-source");

                let (_service_stream_tx, service_stream) = ServiceStream::new_bounded(1);
                Ok(service_stream)
            });
        let sequence_tx_clone2 = sequence_tx.clone();
        mock_ingester_0
            .expect_open_fetch_stream()
            .withf(|request| {
                request.from_position_exclusive() == Position::offset(11u64)
                    && request.shard_id() == ShardId::from(1)
            })
            .times(2)
            .returning(move |request| {
                sequence_tx_clone2.send(2).unwrap();
                assert_eq!(
                    request.client_id,
                    "indexer/test-node/test-index:00000000000000000000000000/test-source/\
                     00000000000000000000000000"
                );
                assert_eq!(request.index_uid(), &("test-index", 0));
                assert_eq!(request.source_id, "test-source");

                let (_service_stream_tx, service_stream) = ServiceStream::new_bounded(1);
                Ok(service_stream)
            });
        let sequence_tx_clone3 = sequence_tx.clone();
        mock_ingester_0
            .expect_open_fetch_stream()
            .withf(|request| {
                request.from_position_exclusive() == Position::offset(12u64)
                    && request.shard_id() == ShardId::from(2)
            })
            .once()
            .returning(move |request| {
                sequence_tx_clone3.send(3).unwrap();
                assert_eq!(
                    request.client_id,
                    "indexer/test-node/test-index:00000000000000000000000000/test-source/\
                     00000000000000000000000000"
                );
                assert_eq!(request.index_uid(), &("test-index", 0));
                assert_eq!(request.source_id, "test-source");

                let (_service_stream_tx, service_stream) = ServiceStream::new_bounded(1);
                Ok(service_stream)
            });
        mock_ingester_0
            .expect_truncate_shards()
            .withf(|truncate_req| truncate_req.subrequests[0].shard_id() == ShardId::from(0))
            .once()
            .returning(|request| {
                assert_eq!(request.ingester_id, "test-ingester-0");
                assert_eq!(request.subrequests.len(), 1);

                let subrequest = &request.subrequests[0];
                assert_eq!(subrequest.index_uid(), &("test-index", 0));
                assert_eq!(subrequest.source_id, "test-source");
                assert_eq!(
                    subrequest.truncate_up_to_position_inclusive(),
                    Position::offset(10u64)
                );

                let response = TruncateShardsResponse {};
                Ok(response)
            });

        mock_ingester_0
            .expect_truncate_shards()
            .withf(|truncate_req| truncate_req.subrequests[0].shard_id() == ShardId::from(1))
            .once()
            .returning(|request| {
                assert_eq!(request.ingester_id, "test-ingester-0");
                assert_eq!(request.subrequests.len(), 1);

                let subrequest = &request.subrequests[0];
                assert_eq!(subrequest.index_uid(), &("test-index", 0));
                assert_eq!(subrequest.source_id, "test-source");
                assert_eq!(
                    subrequest.truncate_up_to_position_inclusive(),
                    Position::offset(11u64)
                );

                Ok(TruncateShardsResponse {})
            });
        mock_ingester_0
            .expect_truncate_shards()
            .withf(|truncate_req| {
                truncate_req.subrequests.len() == 2
                    && truncate_req.subrequests[0].shard_id() == ShardId::from(1)
                    && truncate_req.subrequests[1].shard_id() == ShardId::from(2)
            })
            .once()
            .returning(|request| {
                assert_eq!(request.ingester_id, "test-ingester-0");

                let subrequest = &request.subrequests[0];
                assert_eq!(subrequest.index_uid(), &("test-index", 0));
                assert_eq!(subrequest.source_id, "test-source");
                assert_eq!(
                    subrequest.truncate_up_to_position_inclusive(),
                    Position::offset(11u64)
                );

                let subrequest = &request.subrequests[1];
                assert_eq!(subrequest.index_uid(), &("test-index", 0));
                assert_eq!(subrequest.source_id, "test-source");
                assert_eq!(
                    subrequest.truncate_up_to_position_inclusive(),
                    Position::offset(12u64)
                );

                let response = TruncateShardsResponse {};
                Ok(response)
            });

        let ingester_0 = IngesterServiceClient::from_mock(mock_ingester_0);
        ingester_pool.insert("test-ingester-0".into(), ingester_0.clone());

        let event_broker = EventBroker::default();

        let source_runtime = SourceRuntime {
            pipeline_id,
            source_config,
            metastore: MetastoreServiceClient::from_mock(mock_metastore),
            ingester_pool: ingester_pool.clone(),
            queues_dir_path: PathBuf::from("./queues"),
            storage_resolver: StorageResolver::for_test(),
            event_broker,
            indexing_setting: IndexingSettings::default(),
        };
        let retry_params = RetryParams::no_retries();
        let mut source = IngestSource::try_new(source_runtime, retry_params)
            .await
            .unwrap();

        let universe = Universe::with_accelerated_time();
        let (source_mailbox, _source_inbox) = universe.create_test_mailbox::<SourceActor>();
        let (doc_processor_mailbox, doc_processor_inbox) =
            universe.create_test_mailbox::<DocProcessor>();
        let (observable_state_tx, _observable_state_rx) = watch::channel(serde_json::Value::Null);
        let ctx: SourceContext =
            ActorContext::for_test(&universe, source_mailbox, observable_state_tx);

        // We assign [0] (previously []).
        // The stream does not need to be reset.
        let shard_ids: BTreeSet<ShardId> = once(0).map(ShardId::from).collect();
        let publish_lock = source.publish_lock.clone();
        source
            .assign_shards(shard_ids, &doc_processor_mailbox, &ctx)
            .await
            .unwrap();
        assert_eq!(sequence_rx.recv().await.unwrap(), 1);
        assert!(!publish_lock.is_alive());

        assert!(source.publish_lock.is_alive());
        assert!(!source.publish_token.is_empty());

        // We assign [0,1] (previously [0]). This should just add the shard 1.
        // The stream does not need to be reset.
        let shard_ids: BTreeSet<ShardId> = (0..2).map(ShardId::from).collect();
        let publish_lock = source.publish_lock.clone();
        source
            .assign_shards(shard_ids, &doc_processor_mailbox, &ctx)
            .await
            .unwrap();
        assert_eq!(sequence_rx.recv().await.unwrap(), 2);
        assert!(publish_lock.is_alive());
        assert_eq!(publish_lock, source.publish_lock);

        // We assign [1,2]. (previously [0,1]) This should reset the stream
        // because the shard 0 has to be removed.
        // The publish lock should be killed and a new one should be created.
        let shard_ids: BTreeSet<ShardId> = (1..3).map(ShardId::from).collect();
        let publish_lock = source.publish_lock.clone();
        source
            .assign_shards(shard_ids, &doc_processor_mailbox, &ctx)
            .await
            .unwrap();

        assert_eq!(sequence_rx.recv().await.unwrap(), 2);
        assert_eq!(sequence_rx.recv().await.unwrap(), 3);
        assert!(!publish_lock.is_alive());
        assert!(source.publish_lock.is_alive());
        assert_ne!(publish_lock, source.publish_lock);

        let NewPublishLock(publish_lock) = doc_processor_inbox
            .recv_typed_message::<NewPublishLock>()
            .await
            .unwrap();
        assert_ne!(&source.publish_lock, &publish_lock);

        // assert!(publish_token != source.publish_token);

        let NewPublishToken(publish_token) = doc_processor_inbox
            .recv_typed_message::<NewPublishToken>()
            .await
            .unwrap();
        assert_eq!(source.publish_token, publish_token);

        assert_eq!(source.assigned_shards.len(), 2);

        let assigned_shard = source.assigned_shards.get(&ShardId::from(1)).unwrap();
        let expected_assigned_shard = AssignedShard {
            leader_id: "test-ingester-0".into(),
            follower_id_opt: None,
            partition_id: 1u64.into(),
            current_position_inclusive: Position::offset(11u64),
            status: IndexingStatus::Active,
        };
        assert_eq!(assigned_shard, &expected_assigned_shard);

        let assigned_shard = source.assigned_shards.get(&ShardId::from(2)).unwrap();
        let expected_assigned_shard = AssignedShard {
            leader_id: "test-ingester-0".into(),
            follower_id_opt: None,
            partition_id: 2u64.into(),
            current_position_inclusive: Position::offset(12u64),
            status: IndexingStatus::Active,
        };
        assert_eq!(assigned_shard, &expected_assigned_shard);

        // Wait for the truncate future to complete.
        time::sleep(Duration::from_millis(1)).await;
    }

    #[tokio::test]
    async fn test_ingest_source_assign_shards_all_eof() {
        // In this test, we check that if all assigned shards are originally marked as EOF in the
        // metastore, we observe the following:
        // - emission of a suggest truncate
        // - no stream request is emitted
        let pipeline_id = IndexingPipelineId {
            node_id: NodeId::from("test-node"),
            index_uid: IndexUid::for_test("test-index", 0),
            source_id: "test-source".to_string(),
            pipeline_uid: PipelineUid::default(),
        };
        let source_config = SourceConfig::for_test("test-source", SourceParams::Ingest);
        let publish_token = "indexer/test-node/test-index:0/test-source/\
                             00000000000000000000000000/00000000000000000000000000";

        let mut mock_metastore = MockMetastoreService::new();
        mock_metastore
            .expect_acquire_shards()
            .once()
            .returning(|request| {
                assert_eq!(request.index_uid(), &("test-index", 0));
                assert_eq!(request.source_id, "test-source");
                assert_eq!(request.shard_ids, [ShardId::from(1), ShardId::from(2)]);

                let response = AcquireShardsResponse {
                    acquired_shards: vec![
                        Shard {
                            leader_id: "test-ingester-0".to_string(),
                            follower_id: None,
                            index_uid: Some(IndexUid::for_test("test-index", 0)),
                            source_id: "test-source".to_string(),
                            shard_id: Some(ShardId::from(1)),
                            shard_state: ShardState::Open as i32,
                            doc_mapping_uid: Some(DocMappingUid::default()),
                            publish_position_inclusive: Some(Position::eof(11u64)),
                            publish_token: Some(publish_token.to_string()),
                            update_timestamp: 1724158996,
                        },
                        Shard {
                            leader_id: "test-ingester-0".to_string(),
                            follower_id: None,
                            index_uid: Some(IndexUid::for_test("test-index", 0)),
                            source_id: "test-source".to_string(),
                            shard_id: Some(ShardId::from(2)),
                            shard_state: ShardState::Open as i32,
                            doc_mapping_uid: Some(DocMappingUid::default()),
                            publish_position_inclusive: Some(Position::Beginning.as_eof()),
                            publish_token: Some(publish_token.to_string()),
                            update_timestamp: 1724158996,
                        },
                    ],
                };
                Ok(response)
            });
        let ingester_pool = IngesterPool::default();

        let mut mock_ingester_0 = MockIngesterService::new();
        mock_ingester_0
            .expect_truncate_shards()
            .once()
            .returning(|request| {
                assert_eq!(request.ingester_id, "test-ingester-0");
                assert_eq!(request.subrequests.len(), 2);

                let subrequest_0 = &request.subrequests[0];
                assert_eq!(subrequest_0.index_uid(), &("test-index", 0));
                assert_eq!(subrequest_0.source_id, "test-source");
                assert_eq!(subrequest_0.shard_id(), ShardId::from(1));
                assert_eq!(
                    subrequest_0.truncate_up_to_position_inclusive(),
                    Position::eof(11u64)
                );

                let subrequest_1 = &request.subrequests[1];
                assert_eq!(subrequest_1.index_uid(), &("test-index", 0));
                assert_eq!(subrequest_1.source_id, "test-source");
                assert_eq!(subrequest_1.shard_id(), ShardId::from(2));
                assert_eq!(
                    subrequest_1.truncate_up_to_position_inclusive(),
                    Position::Beginning.as_eof()
                );

                let response = TruncateShardsResponse {};
                Ok(response)
            });

        let ingester_0 = IngesterServiceClient::from_mock(mock_ingester_0);
        ingester_pool.insert("test-ingester-0".into(), ingester_0.clone());

        let event_broker = EventBroker::default();
        let (shard_positions_update_tx, mut shard_positions_update_rx) =
            tokio::sync::mpsc::unbounded_channel::<LocalShardPositionsUpdate>();
        event_broker
            .subscribe::<LocalShardPositionsUpdate>(move |update| {
                shard_positions_update_tx.send(update).unwrap();
            })
            .forever();

        let source_runtime = SourceRuntime {
            pipeline_id,
            source_config,
            metastore: MetastoreServiceClient::from_mock(mock_metastore),
            ingester_pool: ingester_pool.clone(),
            queues_dir_path: PathBuf::from("./queues"),
            storage_resolver: StorageResolver::for_test(),
            event_broker,
            indexing_setting: IndexingSettings::default(),
        };
        let retry_params = RetryParams::for_test();
        let mut source = IngestSource::try_new(source_runtime, retry_params)
            .await
            .unwrap();

        let universe = Universe::with_accelerated_time();
        let (source_mailbox, _source_inbox) = universe.create_test_mailbox::<SourceActor>();
        let (doc_processor_mailbox, _doc_processor_inbox) =
            universe.create_test_mailbox::<DocProcessor>();
        let (observable_state_tx, _observable_state_rx) = watch::channel(serde_json::Value::Null);
        let ctx: SourceContext =
            ActorContext::for_test(&universe, source_mailbox, observable_state_tx);

        // In this scenario, the indexer will be able to acquire shard 1 and 2.
        let shard_ids: BTreeSet<ShardId> =
            BTreeSet::from_iter([ShardId::from(1), ShardId::from(2)]);

        source
            .assign_shards(shard_ids, &doc_processor_mailbox, &ctx)
            .await
            .unwrap();

        let expected_local_update = LocalShardPositionsUpdate::new(
            SourceUid {
                index_uid: IndexUid::for_test("test-index", 0),
                source_id: "test-source".to_string(),
            },
            vec![
                (ShardId::from(1), Position::eof(11u64)),
                (ShardId::from(2), Position::Beginning.as_eof()),
            ],
        );
        let local_update = shard_positions_update_rx.recv().await.unwrap();
        assert_eq!(local_update, expected_local_update);
    }

    #[tokio::test]
    async fn test_ingest_source_assign_shards_some_eof() {
        // In this test, we check that if some shards that are originally marked as EOF in the
        // metastore, we observe the following:
        // - emission of a suggest truncate
        // - the stream request emitted does not include the EOF shards
        let pipeline_id = IndexingPipelineId {
            node_id: NodeId::from("test-node"),
            index_uid: IndexUid::for_test("test-index", 0),
            source_id: "test-source".to_string(),
            pipeline_uid: PipelineUid::default(),
        };
        let source_config = SourceConfig::for_test("test-source", SourceParams::Ingest);
        let publish_token = "indexer/test-node/test-index:0/test-source/\
                             00000000000000000000000000/00000000000000000000000000";

        let mut mock_metastore = MockMetastoreService::new();
        mock_metastore
            .expect_acquire_shards()
            .once()
            .returning(|request| {
                assert_eq!(request.index_uid(), &("test-index", 0));
                assert_eq!(request.source_id, "test-source");
                assert_eq!(request.shard_ids, [ShardId::from(1), ShardId::from(2)]);

                let response = AcquireShardsResponse {
                    acquired_shards: vec![
                        Shard {
                            leader_id: "test-ingester-0".to_string(),
                            follower_id: None,
                            index_uid: Some(IndexUid::for_test("test-index", 0)),
                            source_id: "test-source".to_string(),
                            shard_id: Some(ShardId::from(1)),
                            shard_state: ShardState::Open as i32,
                            doc_mapping_uid: Some(DocMappingUid::default()),
                            publish_position_inclusive: Some(Position::offset(11u64)),
                            publish_token: Some(publish_token.to_string()),
                            update_timestamp: 1724158996,
                        },
                        Shard {
                            leader_id: "test-ingester-0".to_string(),
                            follower_id: None,
                            index_uid: Some(IndexUid::for_test("test-index", 0)),
                            source_id: "test-source".to_string(),
                            shard_id: Some(ShardId::from(2)),
                            shard_state: ShardState::Closed as i32,
                            doc_mapping_uid: Some(DocMappingUid::default()),
                            publish_position_inclusive: Some(Position::eof(22u64)),
                            publish_token: Some(publish_token.to_string()),
                            update_timestamp: 1724158996,
                        },
                    ],
                };
                Ok(response)
            });
        let ingester_pool = IngesterPool::default();

        let mut mock_ingester_0 = MockIngesterService::new();
        mock_ingester_0
            .expect_open_fetch_stream()
            .once()
            .returning(|request| {
                assert_eq!(
                    request.client_id,
                    "indexer/test-node/test-index:00000000000000000000000000/test-source/\
                     00000000000000000000000000"
                );
                assert_eq!(request.index_uid(), &("test-index", 0));
                assert_eq!(request.source_id, "test-source");
                assert_eq!(request.shard_id(), ShardId::from(1));
                assert_eq!(request.from_position_exclusive(), Position::offset(11u64));

                let (_service_stream_tx, service_stream) = ServiceStream::new_bounded(1);
                Ok(service_stream)
            });
        mock_ingester_0
            .expect_truncate_shards()
            .once()
            .returning(|mut request| {
                assert_eq!(request.ingester_id, "test-ingester-0");
                assert_eq!(request.subrequests.len(), 2);
                request
                    .subrequests
                    .sort_unstable_by(|left, right| left.shard_id.cmp(&right.shard_id));

                let subrequest = &request.subrequests[0];
                assert_eq!(subrequest.index_uid(), &("test-index", 0));
                assert_eq!(subrequest.source_id, "test-source");
                assert_eq!(subrequest.shard_id(), ShardId::from(1));
                assert_eq!(
                    subrequest.truncate_up_to_position_inclusive(),
                    Position::offset(11u64)
                );

                let subrequest = &request.subrequests[1];
                assert_eq!(subrequest.index_uid(), &("test-index", 0));
                assert_eq!(subrequest.source_id, "test-source");
                assert_eq!(subrequest.shard_id(), ShardId::from(2));
                assert_eq!(
                    subrequest.truncate_up_to_position_inclusive(),
                    Position::eof(22u64)
                );

                let response = TruncateShardsResponse {};
                Ok(response)
            });

        let ingester_0 = IngesterServiceClient::from_mock(mock_ingester_0);
        ingester_pool.insert("test-ingester-0".into(), ingester_0.clone());

        let event_broker = EventBroker::default();
        let (shard_positions_update_tx, mut shard_positions_update_rx) =
            tokio::sync::mpsc::unbounded_channel::<LocalShardPositionsUpdate>();
        event_broker
            .subscribe::<LocalShardPositionsUpdate>(move |update| {
                shard_positions_update_tx.send(update).unwrap();
            })
            .forever();

        let source_runtime = SourceRuntime {
            pipeline_id,
            source_config,
            metastore: MetastoreServiceClient::from_mock(mock_metastore),
            ingester_pool: ingester_pool.clone(),
            queues_dir_path: PathBuf::from("./queues"),
            storage_resolver: StorageResolver::for_test(),
            event_broker,
            indexing_setting: IndexingSettings::default(),
        };
        let retry_params = RetryParams::for_test();
        let mut source = IngestSource::try_new(source_runtime, retry_params)
            .await
            .unwrap();

        let universe = Universe::with_accelerated_time();
        let (source_mailbox, _source_inbox) = universe.create_test_mailbox::<SourceActor>();
        let (doc_processor_mailbox, _doc_processor_inbox) =
            universe.create_test_mailbox::<DocProcessor>();
        let (observable_state_tx, _observable_state_rx) = watch::channel(serde_json::Value::Null);
        let ctx: SourceContext =
            ActorContext::for_test(&universe, source_mailbox, observable_state_tx);

        // In this scenario, the indexer will only be able to acquire shard 1.
        let shard_ids: BTreeSet<ShardId> = (1..3).map(ShardId::from).collect();
        assert_eq!(
            shard_positions_update_rx.try_recv().unwrap_err(),
            TryRecvError::Empty
        );

        // In this scenario, the indexer will only be able to acquire shard 1.
        source
            .assign_shards(shard_ids, &doc_processor_mailbox, &ctx)
            .await
            .unwrap();

        let local_shard_positions_update = shard_positions_update_rx.recv().await.unwrap();
        let expected_local_shard_positions_update = LocalShardPositionsUpdate::new(
            SourceUid {
                index_uid: IndexUid::for_test("test-index", 0),
                source_id: "test-source".to_string(),
            },
            vec![
                (ShardId::from(1), Position::offset(11u64)),
                (ShardId::from(2), Position::eof(22u64)),
            ],
        );
        assert_eq!(
            local_shard_positions_update,
            expected_local_shard_positions_update,
        );
    }

    #[tokio::test]
    async fn test_ingest_source_emit_batches() {
        let pipeline_id = IndexingPipelineId {
            node_id: NodeId::from("test-node"),
            index_uid: IndexUid::for_test("test-index", 0),
            source_id: "test-source".to_string(),
            pipeline_uid: PipelineUid::default(),
        };
        let source_config = SourceConfig::for_test("test-source", SourceParams::Ingest);
        let mock_metastore = MockMetastoreService::new();
        let ingester_pool = IngesterPool::default();
        let event_broker = EventBroker::default();

        let source_runtime = SourceRuntime {
            pipeline_id,
            source_config,
            metastore: MetastoreServiceClient::from_mock(mock_metastore),
            ingester_pool: ingester_pool.clone(),
            queues_dir_path: PathBuf::from("./queues"),
            storage_resolver: StorageResolver::for_test(),
            event_broker,
            indexing_setting: IndexingSettings::default(),
        };
        let retry_params = RetryParams::for_test();
        let mut source = IngestSource::try_new(source_runtime, retry_params)
            .await
            .unwrap();

        let universe = Universe::with_accelerated_time();
        let (source_mailbox, _source_inbox) = universe.create_test_mailbox::<SourceActor>();
        let (doc_processor_mailbox, doc_processor_inbox) =
            universe.create_test_mailbox::<DocProcessor>();
        let (observable_state_tx, _observable_state_rx) = watch::channel(serde_json::Value::Null);
        let ctx: SourceContext =
            ActorContext::for_test(&universe, source_mailbox, observable_state_tx);

        // In this scenario, the ingester receives fetch responses from shard 1 and 2.
        source.assigned_shards.insert(
            ShardId::from(1),
            AssignedShard {
                leader_id: "test-ingester-0".into(),
                follower_id_opt: None,
                partition_id: 1u64.into(),
                current_position_inclusive: Position::offset(11u64),
                status: IndexingStatus::Active,
            },
        );
        source.assigned_shards.insert(
            ShardId::from(2),
            AssignedShard {
                leader_id: "test-ingester-1".into(),
                follower_id_opt: None,
                partition_id: 2u64.into(),
                current_position_inclusive: Position::offset(22u64),
                status: IndexingStatus::Active,
            },
        );
        let fetch_message_tx = source.fetch_stream.fetch_message_tx();

        let fetch_payload = FetchPayload {
            index_uid: Some(IndexUid::for_test("test-index", 0)),
            source_id: "test-source".into(),
            shard_id: Some(ShardId::from(1)),
            mrecord_batch: MRecordBatch::for_test([
                "\0\0test-doc-foo",
                "\0\0test-doc-bar",
                "\0\x01",
            ]),
            from_position_exclusive: Some(Position::offset(11u64)),
            to_position_inclusive: Some(Position::offset(14u64)),
        };
        let batch_size = fetch_payload.estimate_size();
        let fetch_message = FetchMessage::new_payload(fetch_payload);
        let in_flight_value = InFlightValue::new(
            fetch_message,
            batch_size,
            &MEMORY_METRICS.in_flight.fetch_stream,
        );
        fetch_message_tx.send(Ok(in_flight_value)).await.unwrap();

        let fetch_payload = FetchPayload {
            index_uid: Some(IndexUid::for_test("test-index", 0)),
            source_id: "test-source".into(),
            shard_id: Some(ShardId::from(2)),
            mrecord_batch: MRecordBatch::for_test(["\0\0test-doc-qux"]),
            from_position_exclusive: Some(Position::offset(22u64)),
            to_position_inclusive: Some(Position::offset(23u64)),
        };
        let batch_size = fetch_payload.estimate_size();
        let fetch_message = FetchMessage::new_payload(fetch_payload);
        let in_flight_value = InFlightValue::new(
            fetch_message,
            batch_size,
            &MEMORY_METRICS.in_flight.fetch_stream,
        );
        fetch_message_tx.send(Ok(in_flight_value)).await.unwrap();

        let fetch_eof = FetchEof {
            index_uid: Some(IndexUid::for_test("test-index", 0)),
            source_id: "test-source".into(),
            shard_id: Some(ShardId::from(2)),
            eof_position: Some(Position::eof(23u64)),
        };
        let fetch_message = FetchMessage::new_eof(fetch_eof);
        let in_flight_value = InFlightValue::new(
            fetch_message,
            ByteSize(0),
            &MEMORY_METRICS.in_flight.fetch_stream,
        );
        fetch_message_tx.send(Ok(in_flight_value)).await.unwrap();

        source
            .emit_batches(&doc_processor_mailbox, &ctx)
            .await
            .unwrap();
        let doc_batch = doc_processor_inbox
            .recv_typed_message::<RawDocBatch>()
            .await
            .unwrap();
        assert_eq!(doc_batch.docs.len(), 3);
        assert_eq!(doc_batch.docs[0], "test-doc-foo");
        assert_eq!(doc_batch.docs[1], "test-doc-bar");
        assert_eq!(doc_batch.docs[2], "test-doc-qux");
        assert!(doc_batch.force_commit);

        let partition_deltas = doc_batch
            .checkpoint_delta
            .iter()
            .sorted_by(|left, right| left.0.cmp(&right.0))
            .collect::<Vec<_>>();

        assert_eq!(partition_deltas.len(), 2);
        assert_eq!(partition_deltas[0].0, 1u64.into());
        assert_eq!(partition_deltas[0].1.from, Position::offset(11u64));
        assert_eq!(partition_deltas[0].1.to, Position::offset(14u64));

        assert_eq!(partition_deltas[1].0, 2u64.into());
        assert_eq!(partition_deltas[1].1.from, Position::offset(22u64));
        assert_eq!(partition_deltas[1].1.to, Position::eof(23u64));

        source
            .emit_batches(&doc_processor_mailbox, &ctx)
            .await
            .unwrap();
        let shard = source.assigned_shards.get(&ShardId::from(2)).unwrap();
        assert_eq!(shard.status, IndexingStatus::ReachedEof);

        fetch_message_tx
            .send(Err(FetchStreamError {
                index_uid: IndexUid::for_test("test-index", 0),
                source_id: "test-source".into(),
                shard_id: ShardId::from(1),
                ingest_error: IngestV2Error::Internal("test-error".to_string()),
            }))
            .await
            .unwrap();

        source
            .emit_batches(&doc_processor_mailbox, &ctx)
            .await
            .unwrap();
        let shard = source.assigned_shards.get(&ShardId::from(1)).unwrap();
        assert_eq!(shard.status, IndexingStatus::Error);

        let fetch_payload = FetchPayload {
            index_uid: Some(IndexUid::for_test("test-index", 0)),
            source_id: "test-source".into(),
            shard_id: Some(ShardId::from(1)),
            mrecord_batch: MRecordBatch::for_test(["\0\0test-doc-baz"]),
            from_position_exclusive: Some(Position::offset(14u64)),
            to_position_inclusive: Some(Position::offset(15u64)),
        };
        let batch_size = fetch_payload.estimate_size();
        let fetch_message = FetchMessage::new_payload(fetch_payload);
        let in_flight_value = InFlightValue::new(
            fetch_message,
            batch_size,
            &MEMORY_METRICS.in_flight.fetch_stream,
        );
        fetch_message_tx.send(Ok(in_flight_value)).await.unwrap();

        source
            .emit_batches(&doc_processor_mailbox, &ctx)
            .await
            .unwrap();
        let shard = source.assigned_shards.get(&ShardId::from(1)).unwrap();
        assert_eq!(shard.status, IndexingStatus::Active);
    }

    #[tokio::test]
    async fn test_ingest_source_emit_batches_shard_not_found() {
        let pipeline_id = IndexingPipelineId {
            node_id: NodeId::from("test-node"),
            index_uid: IndexUid::for_test("test-index", 0),
            source_id: "test-source".to_string(),
            pipeline_uid: PipelineUid::default(),
        };
        let source_config = SourceConfig::for_test("test-source", SourceParams::Ingest);
        let publish_token = "indexer/test-node/test-index:0/test-source/\
                             00000000000000000000000000/00000000000000000000000000";

        let mut mock_metastore = MockMetastoreService::new();
        mock_metastore
            .expect_acquire_shards()
            .once()
            .returning(|request: AcquireShardsRequest| {
                assert_eq!(request.index_uid(), &("test-index", 0));
                assert_eq!(request.source_id, "test-source");
                assert_eq!(request.shard_ids, [ShardId::from(1)]);

                let response = AcquireShardsResponse {
                    acquired_shards: vec![Shard {
                        leader_id: "test-ingester-0".to_string(),
                        follower_id: None,
                        index_uid: Some(IndexUid::for_test("test-index", 0)),
                        source_id: "test-source".to_string(),
                        shard_id: Some(ShardId::from(1)),
                        shard_state: ShardState::Open as i32,
                        doc_mapping_uid: Some(DocMappingUid::default()),
                        publish_position_inclusive: Some(Position::Beginning),
                        publish_token: Some(publish_token.to_string()),
                        update_timestamp: 1724158996,
                    }],
                };
                Ok(response)
            });
        let ingester_pool = IngesterPool::default();

        let mut mock_ingester_0 = MockIngesterService::new();
        mock_ingester_0
            .expect_open_fetch_stream()
            .once()
            .returning(|request| {
                assert_eq!(request.index_uid(), &("test-index", 0));
                assert_eq!(request.source_id, "test-source");
                assert_eq!(request.shard_id(), ShardId::from(1));
                assert_eq!(request.from_position_exclusive(), Position::Beginning);

                Err(IngestV2Error::ShardNotFound {
                    shard_id: ShardId::from(1),
                })
            });

        let ingester_0 = IngesterServiceClient::from_mock(mock_ingester_0);
        ingester_pool.insert("test-ingester-0".into(), ingester_0.clone());

        let event_broker = EventBroker::default();
        let source_runtime = SourceRuntime {
            pipeline_id,
            source_config,
            metastore: MetastoreServiceClient::from_mock(mock_metastore),
            ingester_pool,
            queues_dir_path: PathBuf::from("./queues"),
            storage_resolver: StorageResolver::for_test(),
            event_broker,
            indexing_setting: IndexingSettings::default(),
        };
        let retry_params = RetryParams::for_test();
        let mut source = IngestSource::try_new(source_runtime, retry_params)
            .await
            .unwrap();

        let universe = Universe::with_accelerated_time();
        let (source_mailbox, _source_inbox) = universe.create_test_mailbox::<SourceActor>();
        let (doc_processor_mailbox, doc_processor_inbox) =
            universe.create_test_mailbox::<DocProcessor>();
        let (observable_state_tx, _observable_state_rx) = watch::channel(serde_json::Value::Null);
        let ctx: SourceContext =
            ActorContext::for_test(&universe, source_mailbox, observable_state_tx);

        let shard_ids: BTreeSet<ShardId> = BTreeSet::from_iter([ShardId::from(1)]);

        source
            .assign_shards(shard_ids, &doc_processor_mailbox, &ctx)
            .await
            .unwrap();

        source
            .emit_batches(&doc_processor_mailbox, &ctx)
            .await
            .unwrap();

        let shard = source.assigned_shards.get(&ShardId::from(1)).unwrap();
        assert_eq!(shard.status, IndexingStatus::NotFound);
        assert_eq!(
            shard.current_position_inclusive,
            Position::Beginning.as_eof()
        );
        let raw_doc_batch = doc_processor_inbox
            .recv_typed_message::<RawDocBatch>()
            .await
            .unwrap();

        let (partition_id, position) = raw_doc_batch.checkpoint_delta.iter().next().unwrap();
        assert_eq!(partition_id, PartitionId::from(1u64));
        assert_eq!(position.from, Position::Beginning);
        assert_eq!(position.to, Position::Beginning.as_eof());
    }

    #[tokio::test]
    async fn test_ingest_source_suggest_truncate() {
        let pipeline_id = IndexingPipelineId {
            node_id: NodeId::from("test-node"),
            index_uid: IndexUid::for_test("test-index", 0),
            source_id: "test-source".to_string(),
            pipeline_uid: PipelineUid::default(),
        };
        let source_config = SourceConfig::for_test("test-source", SourceParams::Ingest);
        let mock_metastore = MockMetastoreService::new();

        let ingester_pool = IngesterPool::default();

        let mut mock_ingester_0 = MockIngesterService::new();
        mock_ingester_0
            .expect_truncate_shards()
            .once()
            .returning(|request| {
                assert_eq!(request.ingester_id, "test-ingester-0");
                assert_eq!(request.subrequests.len(), 3);

                let subrequest_0 = &request.subrequests[0];
                assert_eq!(subrequest_0.shard_id(), ShardId::from(1));
                assert_eq!(
                    subrequest_0.truncate_up_to_position_inclusive(),
                    Position::offset(11u64)
                );

                let subrequest_1 = &request.subrequests[1];
                assert_eq!(subrequest_1.shard_id(), ShardId::from(2));
                assert_eq!(
                    subrequest_1.truncate_up_to_position_inclusive(),
                    Position::offset(22u64)
                );

                let subrequest_2 = &request.subrequests[2];
                assert_eq!(subrequest_2.shard_id(), ShardId::from(3));
                assert_eq!(
                    subrequest_2.truncate_up_to_position_inclusive(),
                    Position::eof(33u64)
                );

                Ok(TruncateShardsResponse {})
            });
        let ingester_0 = IngesterServiceClient::from_mock(mock_ingester_0);
        ingester_pool.insert("test-ingester-0".into(), ingester_0.clone());

        let mut mock_ingester_1 = MockIngesterService::new();
        mock_ingester_1
            .expect_truncate_shards()
            .once()
            .returning(|request| {
                assert_eq!(request.ingester_id, "test-ingester-1");
                assert_eq!(request.subrequests.len(), 2);

                let subrequest_0 = &request.subrequests[0];
                assert_eq!(subrequest_0.shard_id(), ShardId::from(2));
                assert_eq!(
                    subrequest_0.truncate_up_to_position_inclusive(),
                    Position::offset(22u64)
                );

                let subrequest_1 = &request.subrequests[1];
                assert_eq!(subrequest_1.shard_id(), ShardId::from(3));
                assert_eq!(
                    subrequest_1.truncate_up_to_position_inclusive(),
                    Position::eof(33u64)
                );

                Ok(TruncateShardsResponse {})
            });
        let ingester_1 = IngesterServiceClient::from_mock(mock_ingester_1);
        ingester_pool.insert("test-ingester-1".into(), ingester_1.clone());

        let mut mock_ingester_3 = MockIngesterService::new();
        mock_ingester_3
            .expect_truncate_shards()
            .once()
            .returning(|request| {
                assert_eq!(request.ingester_id, "test-ingester-3");
                assert_eq!(request.subrequests.len(), 1);

                let subrequest_0 = &request.subrequests[0];
                assert_eq!(subrequest_0.shard_id(), ShardId::from(4));
                assert_eq!(
                    subrequest_0.truncate_up_to_position_inclusive(),
                    Position::offset(44u64)
                );

                Ok(TruncateShardsResponse {})
            });
        let ingester_3 = IngesterServiceClient::from_mock(mock_ingester_3);
        ingester_pool.insert("test-ingester-3".into(), ingester_3.clone());

        let event_broker = EventBroker::default();
        let (shard_positions_update_tx, mut shard_positions_update_rx) =
            tokio::sync::mpsc::unbounded_channel::<LocalShardPositionsUpdate>();
        event_broker
            .subscribe::<LocalShardPositionsUpdate>(move |update| {
                shard_positions_update_tx.send(update).unwrap();
            })
            .forever();

        let source_runtime = SourceRuntime {
            pipeline_id,
            source_config,
            metastore: MetastoreServiceClient::from_mock(mock_metastore),
            ingester_pool: ingester_pool.clone(),
            queues_dir_path: PathBuf::from("./queues"),
            storage_resolver: StorageResolver::for_test(),
            event_broker,
            indexing_setting: IndexingSettings::default(),
        };
        let retry_params = RetryParams::for_test();
        let mut source = IngestSource::try_new(source_runtime, retry_params)
            .await
            .unwrap();

        let universe = Universe::with_accelerated_time();
        let (source_mailbox, _source_inbox) = universe.create_test_mailbox::<SourceActor>();
        let (observable_state_tx, _observable_state_rx) = watch::channel(serde_json::Value::Null);
        let ctx: SourceContext =
            ActorContext::for_test(&universe, source_mailbox, observable_state_tx);

        // In this scenario, the ingester 2 is not available and the shard 6 is no longer assigned.
        source.assigned_shards.insert(
            ShardId::from(1),
            AssignedShard {
                leader_id: "test-ingester-0".into(),
                follower_id_opt: None,
                partition_id: 1u64.into(),
                current_position_inclusive: Position::offset(11u64),
                status: IndexingStatus::Active,
            },
        );
        source.assigned_shards.insert(
            ShardId::from(2),
            AssignedShard {
                leader_id: "test-ingester-0".into(),
                follower_id_opt: Some("test-ingester-1".into()),
                partition_id: 2u64.into(),
                current_position_inclusive: Position::offset(22u64),
                status: IndexingStatus::Active,
            },
        );
        source.assigned_shards.insert(
            ShardId::from(3),
            AssignedShard {
                leader_id: "test-ingester-1".into(),
                follower_id_opt: Some("test-ingester-0".into()),
                partition_id: 3u64.into(),
                current_position_inclusive: Position::offset(33u64),
                status: IndexingStatus::Active,
            },
        );
        source.assigned_shards.insert(
            ShardId::from(4),
            AssignedShard {
                leader_id: "test-ingester-2".into(),
                follower_id_opt: Some("test-ingester-3".into()),
                partition_id: 4u64.into(),
                current_position_inclusive: Position::offset(44u64),
                status: IndexingStatus::Active,
            },
        );
        source.assigned_shards.insert(
            ShardId::from(5),
            AssignedShard {
                leader_id: "test-ingester-2".into(),
                follower_id_opt: Some("test-ingester-3".into()),
                partition_id: 5u64.into(),
                current_position_inclusive: Position::Beginning,
                status: IndexingStatus::Active,
            },
        );

        let checkpoint = SourceCheckpoint::from_iter(vec![
            (1u64.into(), Position::offset(11u64)),
            (2u64.into(), Position::offset(22u64)),
            (3u64.into(), Position::eof(33u64)),
            (4u64.into(), Position::offset(44u64)),
            (5u64.into(), Position::Beginning),
            (6u64.into(), Position::offset(66u64)),
        ]);
        source.suggest_truncate(checkpoint, &ctx).await.unwrap();

        let local_shards_update = shard_positions_update_rx.recv().await.unwrap();
        let expected_local_shards_update = LocalShardPositionsUpdate::new(
            SourceUid {
                index_uid: IndexUid::for_test("test-index", 0),
                source_id: "test-source".to_string(),
            },
            vec![
                (ShardId::from(1u64), Position::offset(11u64)),
                (ShardId::from(2u64), Position::offset(22u64)),
                (ShardId::from(3u64), Position::eof(33u64)),
                (ShardId::from(4u64), Position::offset(44u64)),
                (ShardId::from(5u64), Position::Beginning),
                (ShardId::from(6u64), Position::offset(66u64)),
            ],
        );
        assert_eq!(local_shards_update, expected_local_shards_update);
    }

    // Motivated by #4888
    #[tokio::test]
    async fn test_assigned_deleted_shards() {
        // It is possible for the control plan to assign a shard to an indexer and delete it right
        // away. In that case, the ingester should just ignore the assigned shard, as
        // opposed to fail as the metastore does not let it `acquire` the shard.
        let pipeline_id = IndexingPipelineId {
            node_id: NodeId::from("test-node"),
            index_uid: IndexUid::for_test("test-index", 0),
            source_id: "test-source".to_string(),
            pipeline_uid: PipelineUid::default(),
        };
        let source_config = SourceConfig::for_test("test-source", SourceParams::Ingest);

        let mut mock_metastore = MockMetastoreService::new();
        mock_metastore
            .expect_acquire_shards()
            .once()
            .returning(|request: AcquireShardsRequest| {
                assert_eq!(request.index_uid(), &("test-index", 0));
                assert_eq!(request.source_id, "test-source");
                assert_eq!(request.shard_ids, [ShardId::from(1)]);

                let response = AcquireShardsResponse {
                    acquired_shards: Vec::new(),
                };
                Ok(response)
            });
        let ingester_pool = IngesterPool::default();

        let event_broker = EventBroker::default();
        let source_runtime = SourceRuntime {
            pipeline_id,
            source_config,
            metastore: MetastoreServiceClient::from_mock(mock_metastore),
            ingester_pool,
            queues_dir_path: PathBuf::from("./queues"),
            storage_resolver: StorageResolver::for_test(),
            event_broker: event_broker.clone(),
            indexing_setting: IndexingSettings::default(),
        };
        let retry_params = RetryParams::for_test();
        let mut source = IngestSource::try_new(source_runtime, retry_params)
            .await
            .unwrap();

        let universe = Universe::with_accelerated_time();
        let (source_mailbox, _source_inbox) = universe.create_test_mailbox::<SourceActor>();
        let (doc_processor_mailbox, _doc_processor_inbox) =
            universe.create_test_mailbox::<DocProcessor>();
        let (observable_state_tx, _observable_state_rx) = watch::channel(serde_json::Value::Null);
        let ctx: SourceContext =
            ActorContext::for_test(&universe, source_mailbox, observable_state_tx);

        let shard_ids: BTreeSet<ShardId> = BTreeSet::from_iter([ShardId::from(1)]);

        let truncation_happened = Arc::new(AtomicBool::new(false));
        let truncation_happened_clone = truncation_happened.clone();

        let _subscription_guard = event_broker.subscribe(move |_: LocalShardPositionsUpdate| {
            truncation_happened_clone.store(true, std::sync::atomic::Ordering::Relaxed);
        });

        source
            .assign_shards(shard_ids, &doc_processor_mailbox, &ctx)
            .await
            .unwrap();

        tokio::time::sleep(Duration::from_millis(100)).await;
        assert!(!truncation_happened.load(std::sync::atomic::Ordering::Relaxed));
    }
}
