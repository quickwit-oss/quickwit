// Copyright (C) 2023 Quickwit, Inc.
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
use quickwit_ingest::{IngesterPool, MultiFetchStream};
use quickwit_metastore::checkpoint::{PartitionId, Position, SourceCheckpoint};
use quickwit_metastore::Metastore;
use quickwit_proto::ingest::ingester::{
    FetchResponseV2, IngesterService, TruncateRequest, TruncateSubrequest,
};
use quickwit_proto::metastore::{AcquireShardsRequest, AcquireShardsSubrequest};
use quickwit_proto::types::NodeId;
use quickwit_proto::{IndexUid, PublishToken, ShardId, SourceId};
use serde_json::json;
use tokio::time;
use tracing::{debug, error, info, warn};
use ulid::Ulid;

use super::{
    Assignment, BatchBuilder, Source, SourceContext, SourceRuntimeArgs, TypedSourceFactory,
};
use crate::actors::DocProcessor;
use crate::models::{NewPublishLock, NewPublishToken, PublishLock};

const EMIT_BATCHES_TIMEOUT: Duration = Duration::from_millis(if cfg!(test) { 100 } else { 1_000 });

pub struct IngestSourceFactory;

#[async_trait]
impl TypedSourceFactory for IngestSourceFactory {
    type Source = IngestSource;
    type Params = ();

    async fn typed_create_source(
        runtime_args: Arc<SourceRuntimeArgs>,
        _: Self::Params,
        checkpoint: SourceCheckpoint,
    ) -> anyhow::Result<Self::Source> {
        IngestSource::try_new(runtime_args, checkpoint).await
    }
}

/// The [`ClientId`] is a unique identifier for a client of the ingest service and allows to
/// distinguish which indexers are streaming documents from a shard. It is also used to form a
/// publish token.
#[derive(Debug, Clone)]
struct ClientId {
    node_id: NodeId,
    index_uid: IndexUid,
    source_id: SourceId,
    pipeline_ord: usize,
}

impl ClientId {
    fn new(node_id: NodeId, index_uid: IndexUid, source_id: SourceId, pipeline_ord: usize) -> Self {
        Self {
            node_id,
            index_uid,
            source_id,
            pipeline_ord,
        }
    }

    fn client_id(&self) -> String {
        format!(
            "indexer/{}/{}/{}/{}",
            self.node_id, self.index_uid, self.source_id, self.pipeline_ord
        )
    }
}

#[derive(Debug, Eq, PartialEq)]
struct AssignedShard {
    leader_id: NodeId,
    follower_id_opt: Option<NodeId>,
    partition_id: PartitionId,
    current_position_inclusive: Position,
}

/// Streams documents from a set of shards.
pub struct IngestSource {
    client_id: ClientId,
    metastore: Arc<dyn Metastore>,
    ingester_pool: IngesterPool,
    assigned_shards: HashMap<ShardId, AssignedShard>,
    fetch_stream: MultiFetchStream,
    publish_lock: PublishLock,
    publish_token: PublishToken,
}

impl fmt::Debug for IngestSource {
    fn fmt(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
        formatter.debug_struct("IngestSource").finish()
    }
}

impl IngestSource {
    pub async fn try_new(
        runtime_args: Arc<SourceRuntimeArgs>,
        _checkpoint: SourceCheckpoint,
    ) -> anyhow::Result<Self> {
        let node_id: NodeId = runtime_args.node_id().into();
        let client_id = ClientId::new(
            node_id.clone(),
            runtime_args.index_uid().clone(),
            runtime_args.source_id().to_string(),
            runtime_args.pipeline_ord(),
        );
        let metastore = runtime_args.metastore.clone();
        let ingester_pool = runtime_args.ingester_pool.clone();
        let assigned_shards = HashMap::new();
        let fetch_stream =
            MultiFetchStream::new(node_id, client_id.client_id(), ingester_pool.clone());
        let publish_lock = PublishLock::default();
        let publish_token = format!("{}:{}", client_id.client_id(), Ulid::new());

        Ok(Self {
            client_id,
            metastore,
            ingester_pool,
            assigned_shards,
            fetch_stream,
            publish_lock,
            publish_token,
        })
    }

    fn process_fetch_response(
        &mut self,
        batch_builder: &mut BatchBuilder,
        fetch_response: FetchResponseV2,
    ) -> anyhow::Result<()> {
        let assigned_shard = self
            .assigned_shards
            .get_mut(&fetch_response.shard_id)
            .expect("shard should be assigned");
        let partition_id = assigned_shard.partition_id.clone();

        for doc in fetch_response.docs() {
            batch_builder.add_doc(doc);
        }
        let from_position_exclusive = assigned_shard.current_position_inclusive.clone();
        let to_position_inclusive = fetch_response
            .to_position_inclusive()
            .map(Position::from)
            .unwrap_or(Position::Beginning);
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

    async fn truncate(&self, truncation_point: &[(ShardId, Position)]) {
        let mut per_ingester_truncate_subrequests: HashMap<&NodeId, Vec<TruncateSubrequest>> =
            HashMap::new();

        for (shard_id, truncate_position) in truncation_point {
            if matches!(truncate_position, Position::Beginning) {
                continue;
            }
            let Some(shard) = self.assigned_shards.get(shard_id) else {
                warn!(
                    "failed to truncate shard: shard `{}` is no longer assigned",
                    shard_id
                );
                continue;
            };
            let to_position_inclusive = truncate_position
                .as_u64()
                .expect("position should be a u64");

            let truncate_subrequest = TruncateSubrequest {
                index_uid: self.client_id.index_uid.clone().into(),
                source_id: self.client_id.source_id.clone(),
                shard_id: *shard_id,
                to_position_inclusive,
            };
            if let Some(follower_id) = &shard.follower_id_opt {
                per_ingester_truncate_subrequests
                    .entry(follower_id)
                    .or_default()
                    .push(truncate_subrequest.clone());
            }
            per_ingester_truncate_subrequests
                .entry(&shard.leader_id)
                .or_default()
                .push(truncate_subrequest);
        }
        for (ingester_id, truncate_subrequests) in per_ingester_truncate_subrequests {
            let Some(mut ingester) = self.ingester_pool.get(ingester_id) else {
                warn!(
                    "failed to truncate shard: ingester `{}` is unavailable",
                    ingester_id
                );
                continue;
            };
            let truncate_request = TruncateRequest {
                ingester_id: ingester_id.clone().into(),
                subrequests: truncate_subrequests,
            };
            let truncate_future = async move {
                if let Err(error) = ingester.truncate(truncate_request).await {
                    warn!("failed to truncate shard(s): {error}");
                }
            };
            // Truncation is best-effort, so fire and forget.
            tokio::spawn(truncate_future);
        }
    }
}

#[async_trait]
impl Source for IngestSource {
    async fn emit_batches(
        &mut self,
        doc_processor_mailbox: &Mailbox<DocProcessor>,
        ctx: &SourceContext,
    ) -> Result<Duration, ActorExitStatus> {
        let mut batch_builder = BatchBuilder::default();

        let now = time::Instant::now();
        let deadline = now + EMIT_BATCHES_TIMEOUT;

        loop {
            match time::timeout_at(deadline, self.fetch_stream.next()).await {
                Ok(Ok(fetch_payload)) => {
                    self.process_fetch_response(&mut batch_builder, fetch_payload)?;

                    if batch_builder.num_bytes >= 5 * 1024 * 1024 {
                        break;
                    }
                }
                Ok(Err(error)) => {
                    error!(error=?error, "failed to fetch payload");
                }
                Err(_) => {
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
        assignment: Assignment,
        doc_processor_mailbox: &Mailbox<DocProcessor>,
        ctx: &SourceContext,
    ) -> anyhow::Result<()> {
        // TODO: Remove this check once the control plane stops sending identical assignments.
        let current_assigned_shard_ids = self
            .assigned_shards
            .keys()
            .copied()
            .sorted()
            .collect::<Vec<ShardId>>();

        let mut new_assigned_shard_ids: Vec<ShardId> = assignment.shard_ids;
        new_assigned_shard_ids.sort();

        if current_assigned_shard_ids == new_assigned_shard_ids {
            return Ok(());
        }
        info!("new shard assignment: `{:?}`", new_assigned_shard_ids);

        self.publish_lock.kill().await;

        self.publish_lock = PublishLock::default();
        ctx.send_message(
            doc_processor_mailbox,
            NewPublishLock(self.publish_lock.clone()),
        )
        .await?;

        self.publish_token = format!("{}:{}", self.client_id.client_id(), Ulid::new());
        ctx.send_message(
            doc_processor_mailbox,
            NewPublishToken(self.publish_token.clone()),
        )
        .await?;

        let acquire_shards_subrequest = AcquireShardsSubrequest {
            index_uid: self.client_id.index_uid.to_string(),
            source_id: self.client_id.source_id.clone(),
            shard_ids: new_assigned_shard_ids,
            publish_token: self.publish_token.clone(),
        };
        let acquire_shards_request = AcquireShardsRequest {
            subrequests: vec![acquire_shards_subrequest],
        };
        let acquire_shards_response = ctx
            .protect_future(self.metastore.acquire_shards(acquire_shards_request))
            .await
            .context("failed to acquire shards")?;

        let Some(acquire_shards_subresponse) = acquire_shards_response
            .subresponses
            .into_iter()
            .find(|subresponse| {
                self.client_id.index_uid.as_str() == subresponse.index_uid
                    && subresponse.source_id == self.client_id.source_id
            })
        else {
            return Ok(());
        };
        self.assigned_shards.clear();
        self.fetch_stream.reset();

        let mut truncation_point =
            Vec::with_capacity(acquire_shards_subresponse.acquired_shards.len());

        for acquired_shard in acquire_shards_subresponse.acquired_shards {
            let leader_id: NodeId = acquired_shard.leader_id.into();
            let follower_id_opt: Option<NodeId> = acquired_shard.follower_id.map(Into::into);
            let index_uid: IndexUid = acquired_shard.index_uid.into();
            let source_id: SourceId = acquired_shard.source_id;
            let shard_id = acquired_shard.shard_id;
            let partition_id = PartitionId::from(shard_id);
            let current_position_inclusive =
                Position::from(acquired_shard.publish_position_inclusive);
            let from_position_exclusive = current_position_inclusive.as_u64();
            let to_position_inclusive = None;

            if let Err(error) = ctx
                .protect_future(self.fetch_stream.subscribe(
                    leader_id.clone(),
                    follower_id_opt.clone(),
                    index_uid,
                    source_id,
                    shard_id,
                    from_position_exclusive,
                    to_position_inclusive,
                ))
                .await
            {
                error!(error=%error, "failed to subscribe to shard");
                continue;
            }
            truncation_point.push((shard_id, current_position_inclusive.clone()));

            let assigned_shard = AssignedShard {
                leader_id,
                follower_id_opt,
                partition_id,
                current_position_inclusive,
            };
            self.assigned_shards.insert(shard_id, assigned_shard);
        }
        self.truncate(&truncation_point).await;
        Ok(())
    }

    async fn suggest_truncate(
        &mut self,
        checkpoint: SourceCheckpoint,
        _ctx: &SourceContext,
    ) -> anyhow::Result<()> {
        let mut truncation_point = Vec::with_capacity(checkpoint.num_partitions());

        for (partition_id, position) in checkpoint.iter() {
            let shard_id = partition_id.as_u64().expect("shard ID should be a u64");
            truncation_point.push((shard_id, position));
        }
        self.truncate(&truncation_point).await;
        Ok(())
    }

    fn name(&self) -> String {
        "IngestSource".to_string()
    }

    fn observable_state(&self) -> serde_json::Value {
        json!({
            "client_id": self.client_id.client_id(),
            "assigned_shards": self.assigned_shards.keys().copied().collect::<Vec<ShardId>>(),
            "publish_token": self.publish_token,
        })
    }
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use bytes::Bytes;
    use quickwit_actors::{ActorContext, Universe};
    use quickwit_common::ServiceStream;
    use quickwit_config::{SourceConfig, SourceParams};
    use quickwit_metastore::MockMetastore;
    use quickwit_proto::indexing::IndexingPipelineId;
    use quickwit_proto::ingest::ingester::{IngesterServiceClient, TruncateResponse};
    use quickwit_proto::ingest::{DocBatchV2, Shard};
    use quickwit_proto::metastore::{AcquireShardsResponse, AcquireShardsSubresponse};
    use quickwit_storage::StorageResolver;
    use tokio::sync::watch;

    use super::*;
    use crate::models::RawDocBatch;
    use crate::source::SourceActor;

    #[tokio::test]
    async fn test_ingest_source_assign_shards() {
        let pipeline_id = IndexingPipelineId {
            node_id: "test-node".to_string(),
            index_uid: "test-index:0".into(),
            source_id: "test-source".to_string(),
            pipeline_ord: 0,
        };
        let source_config = SourceConfig::for_test("test-source", SourceParams::Ingest);
        let mut mock_metastore = MockMetastore::default();
        mock_metastore
            .expect_acquire_shards()
            .once()
            .returning(|request| {
                assert_eq!(request.subrequests.len(), 1);

                let subrequest = &request.subrequests[0];
                assert_eq!(subrequest.index_uid, "test-index:0");
                assert_eq!(subrequest.source_id, "test-source");
                assert_eq!(subrequest.shard_ids, vec![1, 2, 3]);

                let response = AcquireShardsResponse {
                    subresponses: vec![AcquireShardsSubresponse {
                        index_uid: "test-index:0".to_string(),
                        source_id: "test-source".to_string(),
                        acquired_shards: vec![
                            Shard {
                                leader_id: "test-ingester-0".to_string(),
                                follower_id: None,
                                index_uid: "test-index:0".to_string(),
                                source_id: "test-source".to_string(),
                                shard_id: 1,
                                publish_position_inclusive: "00000000000000000011".to_string(),
                                ..Default::default()
                            },
                            Shard {
                                leader_id: "test-ingester-1".to_string(),
                                follower_id: None,
                                index_uid: "test-index:0".to_string(),
                                source_id: "test-source".to_string(),
                                shard_id: 2,
                                publish_position_inclusive: "00000000000000000022".to_string(),
                                ..Default::default()
                            },
                        ],
                    }],
                };
                Ok(response)
            });
        let metastore = Arc::new(mock_metastore);

        let ingester_pool = IngesterPool::default();

        let mut ingester_mock_0 = IngesterServiceClient::mock();
        ingester_mock_0
            .expect_open_fetch_stream()
            .once()
            .returning(|request| {
                assert_eq!(
                    request.client_id,
                    "indexer/test-node/test-index:0/test-source/0"
                );
                assert_eq!(request.index_uid, "test-index:0");
                assert_eq!(request.source_id, "test-source");
                assert_eq!(request.shard_id, 1);
                assert_eq!(request.from_position_exclusive, Some(11));

                let (_service_stream_tx, service_stream) = ServiceStream::new_bounded(1);
                Ok(service_stream)
            });
        ingester_mock_0
            .expect_truncate()
            .once()
            .returning(|request| {
                assert_eq!(request.ingester_id, "test-ingester-0");
                assert_eq!(request.subrequests.len(), 1);

                let subrequest = &request.subrequests[0];
                assert_eq!(subrequest.index_uid, "test-index:0");
                assert_eq!(subrequest.source_id, "test-source");
                assert_eq!(subrequest.shard_id, 1);
                assert_eq!(subrequest.to_position_inclusive, 11);

                let response = TruncateResponse {};
                Ok(response)
            });

        let ingester_0: IngesterServiceClient = ingester_mock_0.into();
        ingester_pool.insert("test-ingester-0".into(), ingester_0.clone());

        let runtime_args = Arc::new(SourceRuntimeArgs {
            pipeline_id,
            source_config,
            metastore,
            ingester_pool: ingester_pool.clone(),
            queues_dir_path: PathBuf::from("./queues"),
            storage_resolver: StorageResolver::ram_and_file_for_test(),
        });
        let checkpoint = SourceCheckpoint::default();
        let mut source = IngestSource::try_new(runtime_args, checkpoint)
            .await
            .unwrap();

        let universe = Universe::with_accelerated_time();
        let (source_mailbox, _source_inbox) = universe.create_test_mailbox::<SourceActor>();
        let (doc_processor_mailbox, doc_processor_inbox) =
            universe.create_test_mailbox::<DocProcessor>();
        let (observable_state_tx, _observable_state_rx) = watch::channel(serde_json::Value::Null);
        let ctx: SourceContext =
            ActorContext::for_test(&universe, source_mailbox, observable_state_tx);

        // In this scenario, the indexer will only be able to acquire shard 1 and 2 and will fail to
        // subscribe to shard 2.
        let assignment = Assignment {
            shard_ids: vec![1, 2, 3],
        };
        let publish_lock = source.publish_lock.clone();
        let publish_token = source.publish_token.clone();

        source
            .assign_shards(assignment, &doc_processor_mailbox, &ctx)
            .await
            .unwrap();

        assert!(publish_lock.is_dead());
        assert!(source.publish_lock.is_alive());

        let NewPublishLock(publish_lock) = doc_processor_inbox
            .recv_typed_message::<NewPublishLock>()
            .await
            .unwrap();
        assert_eq!(&source.publish_lock, &publish_lock);

        assert!(publish_token != source.publish_token);

        let NewPublishToken(publish_token) = doc_processor_inbox
            .recv_typed_message::<NewPublishToken>()
            .await
            .unwrap();
        assert_eq!(source.publish_token, publish_token);

        assert_eq!(source.assigned_shards.len(), 1);

        let assigned_shard = source.assigned_shards.get(&1).unwrap();
        let expected_assigned_shard = AssignedShard {
            leader_id: "test-ingester-0".into(),
            follower_id_opt: None,
            partition_id: 1u64.into(),
            current_position_inclusive: Position::from(11u64),
        };
        assert_eq!(assigned_shard, &expected_assigned_shard);

        // Wait for the truncate future to complete.
        time::sleep(Duration::from_millis(1)).await;
    }

    #[tokio::test]
    async fn test_ingest_source_emit_batches() {
        let pipeline_id = IndexingPipelineId {
            node_id: "test-node".to_string(),
            index_uid: "test-index:0".into(),
            source_id: "test-source".to_string(),
            pipeline_ord: 0,
        };
        let source_config = SourceConfig::for_test("test-source", SourceParams::Ingest);
        let mock_metastore = MockMetastore::default();
        let metastore = Arc::new(mock_metastore);
        let ingester_pool = IngesterPool::default();

        let runtime_args = Arc::new(SourceRuntimeArgs {
            pipeline_id,
            source_config,
            metastore,
            ingester_pool: ingester_pool.clone(),
            queues_dir_path: PathBuf::from("./queues"),
            storage_resolver: StorageResolver::ram_and_file_for_test(),
        });
        let checkpoint = SourceCheckpoint::default();
        let mut source = IngestSource::try_new(runtime_args, checkpoint)
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
            1,
            AssignedShard {
                leader_id: "test-ingester-0".into(),
                follower_id_opt: None,
                partition_id: 1u64.into(),
                current_position_inclusive: Position::from(11u64),
            },
        );
        source.assigned_shards.insert(
            2,
            AssignedShard {
                leader_id: "test-ingester-1".into(),
                follower_id_opt: None,
                partition_id: 2u64.into(),
                current_position_inclusive: Position::from(22u64),
            },
        );
        let fetch_response_tx = source.fetch_stream.fetch_response_tx();

        fetch_response_tx
            .send(Ok(FetchResponseV2 {
                index_uid: "test-index:0".into(),
                source_id: "test-source".into(),
                shard_id: 1,
                from_position_inclusive: 12,
                doc_batch: Some(DocBatchV2 {
                    doc_buffer: Bytes::from_static(b"test-doc-112test-doc-113"),
                    doc_lengths: vec![12, 12],
                }),
            }))
            .await
            .unwrap();

        fetch_response_tx
            .send(Ok(FetchResponseV2 {
                index_uid: "test-index:0".into(),
                source_id: "test-source".into(),
                shard_id: 2,
                from_position_inclusive: 23,
                doc_batch: Some(DocBatchV2 {
                    doc_buffer: Bytes::from_static(b"test-doc-223"),
                    doc_lengths: vec![12],
                }),
            }))
            .await
            .unwrap();

        source
            .emit_batches(&doc_processor_mailbox, &ctx)
            .await
            .unwrap();
        let doc_batch = doc_processor_inbox
            .recv_typed_message::<RawDocBatch>()
            .await
            .unwrap();
        assert_eq!(doc_batch.docs.len(), 3);
        assert_eq!(doc_batch.docs[0], "test-doc-112");
        assert_eq!(doc_batch.docs[1], "test-doc-113");
        assert_eq!(doc_batch.docs[2], "test-doc-223");

        let partition_deltas = doc_batch
            .checkpoint_delta
            .iter()
            .sorted_by(|left, right| left.0.cmp(&right.0))
            .collect::<Vec<_>>();

        assert_eq!(partition_deltas.len(), 2);
        assert_eq!(partition_deltas[0].0, 1u64.into());
        assert_eq!(partition_deltas[0].1.from, 11u64.into());
        assert_eq!(partition_deltas[0].1.to, 13u64.into());

        assert_eq!(partition_deltas[1].0, 2u64.into());
        assert_eq!(partition_deltas[1].1.from, 22u64.into());
        assert_eq!(partition_deltas[1].1.to, 23u64.into());
    }

    #[tokio::test]
    async fn test_ingest_source_suggest_truncate() {
        let pipeline_id = IndexingPipelineId {
            node_id: "test-node".to_string(),
            index_uid: "test-index:0".into(),
            source_id: "test-source".to_string(),
            pipeline_ord: 0,
        };
        let source_config = SourceConfig::for_test("test-source", SourceParams::Ingest);
        let mock_metastore = MockMetastore::default();
        let metastore = Arc::new(mock_metastore);

        let ingester_pool = IngesterPool::default();

        let mut ingester_mock_0 = IngesterServiceClient::mock();
        ingester_mock_0
            .expect_truncate()
            .once()
            .returning(|request| {
                assert_eq!(request.ingester_id, "test-ingester-0");
                assert_eq!(request.subrequests.len(), 3);

                let subrequest_0 = &request.subrequests[0];
                assert_eq!(subrequest_0.shard_id, 1);
                assert_eq!(subrequest_0.to_position_inclusive, 11);

                let subrequest_1 = &request.subrequests[1];
                assert_eq!(subrequest_1.shard_id, 2);
                assert_eq!(subrequest_1.to_position_inclusive, 22);

                let subrequest_2 = &request.subrequests[2];
                assert_eq!(subrequest_2.shard_id, 3);
                assert_eq!(subrequest_2.to_position_inclusive, 33);

                Ok(TruncateResponse {})
            });
        let ingester_0: IngesterServiceClient = ingester_mock_0.into();
        ingester_pool.insert("test-ingester-0".into(), ingester_0.clone());

        let mut ingester_mock_1 = IngesterServiceClient::mock();
        ingester_mock_1
            .expect_truncate()
            .once()
            .returning(|request| {
                assert_eq!(request.ingester_id, "test-ingester-1");
                assert_eq!(request.subrequests.len(), 2);

                let subrequest_0 = &request.subrequests[0];
                assert_eq!(subrequest_0.shard_id, 1);
                assert_eq!(subrequest_0.to_position_inclusive, 11);

                let subrequest_1 = &request.subrequests[1];
                assert_eq!(subrequest_1.shard_id, 2);
                assert_eq!(subrequest_1.to_position_inclusive, 22);

                Ok(TruncateResponse {})
            });
        let ingester_1: IngesterServiceClient = ingester_mock_1.into();
        ingester_pool.insert("test-ingester-1".into(), ingester_1.clone());

        let mut ingester_mock_3 = IngesterServiceClient::mock();
        ingester_mock_3
            .expect_truncate()
            .once()
            .returning(|request| {
                assert_eq!(request.ingester_id, "test-ingester-3");
                assert_eq!(request.subrequests.len(), 1);

                let subrequest_0 = &request.subrequests[0];
                assert_eq!(subrequest_0.shard_id, 4);
                assert_eq!(subrequest_0.to_position_inclusive, 44);

                Ok(TruncateResponse {})
            });
        let ingester_3: IngesterServiceClient = ingester_mock_3.into();
        ingester_pool.insert("test-ingester-3".into(), ingester_3.clone());

        let runtime_args = Arc::new(SourceRuntimeArgs {
            pipeline_id,
            source_config,
            metastore,
            ingester_pool: ingester_pool.clone(),
            queues_dir_path: PathBuf::from("./queues"),
            storage_resolver: StorageResolver::ram_and_file_for_test(),
        });
        let checkpoint = SourceCheckpoint::default();
        let mut source = IngestSource::try_new(runtime_args, checkpoint)
            .await
            .unwrap();

        let universe = Universe::with_accelerated_time();
        let (source_mailbox, _source_inbox) = universe.create_test_mailbox::<SourceActor>();
        let (observable_state_tx, _observable_state_rx) = watch::channel(serde_json::Value::Null);
        let ctx: SourceContext =
            ActorContext::for_test(&universe, source_mailbox, observable_state_tx);

        // In this scenario, the ingester 2 is not available and the shard 5 is no longer assigned.
        source.assigned_shards.insert(
            1,
            AssignedShard {
                leader_id: "test-ingester-0".into(),
                follower_id_opt: None,
                partition_id: 1u64.into(),
                current_position_inclusive: Position::from(11u64),
            },
        );
        source.assigned_shards.insert(
            2,
            AssignedShard {
                leader_id: "test-ingester-0".into(),
                follower_id_opt: Some("test-ingester-1".into()),
                partition_id: 2u64.into(),
                current_position_inclusive: Position::from(22u64),
            },
        );
        source.assigned_shards.insert(
            3,
            AssignedShard {
                leader_id: "test-ingester-1".into(),
                follower_id_opt: Some("test-ingester-0".into()),
                partition_id: 3u64.into(),
                current_position_inclusive: Position::from(33u64),
            },
        );
        source.assigned_shards.insert(
            4,
            AssignedShard {
                leader_id: "test-ingester-2".into(),
                follower_id_opt: Some("test-ingester-3".into()),
                partition_id: 4u64.into(),
                current_position_inclusive: Position::from(44u64),
            },
        );
        let checkpoint = SourceCheckpoint::from_iter(vec![
            (1u64.into(), 11u64.into()),
            (2u64.into(), 22u64.into()),
            (3u64.into(), 33u64.into()),
            (4u64.into(), 44u64.into()),
            (5u64.into(), 55u64.into()),
        ]);
        source.suggest_truncate(checkpoint, &ctx).await.unwrap();

        // Wait for the truncate future to complete.
        time::sleep(Duration::from_millis(1)).await;
    }
}
