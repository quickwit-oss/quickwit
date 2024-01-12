// Copyright (C) 2024 Quickwit, Inc.
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

use std::collections::BTreeSet;
use std::time::Duration;

use anyhow::Context;
use async_trait::async_trait;
use fnv::FnvHashSet;
use quickwit_actors::{
    Actor, ActorContext, ActorExitStatus, ActorHandle, Handler, Mailbox, Supervisor, Universe,
    WeakMailbox,
};
use quickwit_common::pubsub::EventSubscriber;
use quickwit_config::SourceConfig;
use quickwit_ingest::{IngesterPool, LocalShardsUpdate};
use quickwit_metastore::IndexMetadata;
use quickwit_proto::control_plane::{
    ControlPlaneError, ControlPlaneResult, GetDebugStateRequest, GetDebugStateResponse,
    GetOrCreateOpenShardsRequest, GetOrCreateOpenShardsResponse, PhysicalIndexingPlanEntry,
    ShardTableEntry,
};
use quickwit_proto::indexing::ShardPositionsUpdate;
use quickwit_proto::metastore::{
    serde_utils as metastore_serde_utils, AddSourceRequest, CreateIndexRequest,
    CreateIndexResponse, DeleteIndexRequest, DeleteShardsRequest, DeleteShardsSubrequest,
    DeleteSourceRequest, EmptyResponse, MetastoreError, MetastoreService, MetastoreServiceClient,
    ToggleSourceRequest,
};
use quickwit_proto::types::{IndexUid, NodeId, ShardId, SourceUid};
use serde::Serialize;
use tracing::error;

use crate::indexing_scheduler::{IndexingScheduler, IndexingSchedulerState};
use crate::ingest::IngestController;
use crate::model::{ControlPlaneModel, ControlPlaneModelMetrics};
use crate::IndexerPool;

/// Interval between two controls (or checks) of the desired plan VS running plan.
pub(crate) const CONTROL_PLAN_LOOP_INTERVAL: Duration = if cfg!(any(test, feature = "testsuite")) {
    Duration::from_millis(500)
} else {
    Duration::from_secs(3)
};

#[derive(Debug)]
struct ControlPlanLoop;

#[derive(Debug)]
pub struct ControlPlane {
    metastore: MetastoreServiceClient,
    model: ControlPlaneModel,
    // The control plane state is split into to independent functions, that we naturally isolated
    // code wise and state wise.
    //
    // - The indexing scheduler is in charge of managing indexers: it decides which indexer should
    // index which source/shards.
    // - the ingest controller is in charge of managing ingesters: it opens and closes shards on
    // the different ingesters.
    indexing_scheduler: IndexingScheduler,
    ingest_controller: IngestController,
}

impl ControlPlane {
    pub fn spawn(
        universe: &Universe,
        cluster_id: String,
        self_node_id: NodeId,
        indexer_pool: IndexerPool,
        ingester_pool: IngesterPool,
        metastore: MetastoreServiceClient,
        replication_factor: usize,
    ) -> (Mailbox<Self>, ActorHandle<Supervisor<Self>>) {
        universe.spawn_builder().supervise_fn(move || {
            let indexing_scheduler = IndexingScheduler::new(
                cluster_id.clone(),
                self_node_id.clone(),
                indexer_pool.clone(),
            );
            let ingest_controller =
                IngestController::new(metastore.clone(), ingester_pool.clone(), replication_factor);
            ControlPlane {
                model: Default::default(),
                metastore: metastore.clone(),
                indexing_scheduler,
                ingest_controller,
            }
        })
    }
}

#[derive(Debug, Clone, Serialize, Default)]
pub struct ControlPlaneObservableState {
    pub indexing_scheduler: IndexingSchedulerState,
    pub model_metrics: ControlPlaneModelMetrics,
}

#[async_trait]
impl Actor for ControlPlane {
    type ObservableState = ControlPlaneObservableState;

    fn name(&self) -> String {
        "ControlPlane".to_string()
    }

    fn observable_state(&self) -> Self::ObservableState {
        ControlPlaneObservableState {
            indexing_scheduler: self.indexing_scheduler.observable_state(),
            model_metrics: self.model.observable_state(),
        }
    }

    async fn initialize(&mut self, ctx: &ActorContext<Self>) -> Result<(), ActorExitStatus> {
        crate::metrics::CONTROL_PLANE_METRICS.restart_total.inc();
        self.model
            .load_from_metastore(&mut self.metastore, ctx.progress())
            .await
            .context("failed to initialize the model")?;

        self.indexing_scheduler
            .schedule_indexing_plan_if_needed(&self.model);

        self.ingest_controller.sync_with_all_ingesters(&self.model);

        ctx.schedule_self_msg(CONTROL_PLAN_LOOP_INTERVAL, ControlPlanLoop)
            .await;

        Ok(())
    }
}

impl ControlPlane {
    /// Deletes a set of shards from the metastore and the control plane model.
    ///
    /// If the shards were already absent this operation is considered successful.
    async fn delete_shards(
        &mut self,
        source_uid: &SourceUid,
        shards: &[ShardId],
    ) -> anyhow::Result<()> {
        let delete_shards_subrequest = DeleteShardsSubrequest {
            index_uid: source_uid.index_uid.to_string(),
            source_id: source_uid.source_id.to_string(),
            shard_ids: shards.to_vec(),
        };
        let delete_shards_request = DeleteShardsRequest {
            subrequests: vec![delete_shards_subrequest],
            force: false,
        };
        // We use a tiny bit different strategy here than for other handlers
        // All metastore errors end up fail/respawn the control plane.
        //
        // This is because deleting shards is done in reaction to an event
        // and we do not really have the freedom to return an error to a caller like for other
        // calls: there is no caller.
        self.metastore
            .delete_shards(delete_shards_request)
            .await
            .context("failed to delete shards in metastore")?;
        self.model.delete_shards(source_uid, shards);
        self.indexing_scheduler
            .schedule_indexing_plan_if_needed(&self.model);
        Ok(())
    }

    fn debug_state(&self) -> GetDebugStateResponse {
        let shard_table = self
            .model
            .all_shards_with_source()
            .map(|(source, shards)| ShardTableEntry {
                source_id: source.to_string(),
                shards: shards
                    .map(|shard_entry| shard_entry.shard.clone())
                    .collect(),
            })
            .collect();
        let physical_index_plan = self
            .indexing_scheduler
            .observable_state()
            .last_applied_physical_plan
            .map(|plan| {
                plan.indexing_tasks_per_indexer()
                    .iter()
                    .map(|(node_id, tasks)| PhysicalIndexingPlanEntry {
                        node_id: node_id.clone(),
                        tasks: tasks.clone(),
                    })
                    .collect()
            })
            .unwrap_or_default();
        GetDebugStateResponse {
            shard_table,
            physical_index_plan,
        }
    }
}

#[async_trait]
impl Handler<ShardPositionsUpdate> for ControlPlane {
    type Reply = ();

    async fn handle(
        &mut self,
        shard_positions_update: ShardPositionsUpdate,
        _ctx: &ActorContext<Self>,
    ) -> Result<(), ActorExitStatus> {
        let Some(shard_entries) = self
            .model
            .list_shards_for_source(&shard_positions_update.source_uid)
        else {
            // The source no longer exists.
            return Ok(());
        };
        let known_shard_ids: FnvHashSet<ShardId> = shard_entries
            .map(|shard_entry| shard_entry.shard_id())
            .cloned()
            .collect();
        // let's identify the shard that have reached EOF but have not yet been removed.
        let shard_ids_to_close: Vec<ShardId> = shard_positions_update
            .shard_positions
            .into_iter()
            .filter(|(shard_id, position)| position.is_eof() && known_shard_ids.contains(shard_id))
            .map(|(shard_id, _position)| shard_id)
            .collect();
        if shard_ids_to_close.is_empty() {
            return Ok(());
        }
        self.delete_shards(&shard_positions_update.source_uid, &shard_ids_to_close)
            .await?;
        Ok(())
    }
}

#[async_trait]
impl Handler<ControlPlanLoop> for ControlPlane {
    type Reply = ();

    async fn handle(
        &mut self,
        _message: ControlPlanLoop,
        ctx: &ActorContext<Self>,
    ) -> Result<(), ActorExitStatus> {
        self.indexing_scheduler.control_running_plan(&self.model);
        ctx.schedule_self_msg(CONTROL_PLAN_LOOP_INTERVAL, ControlPlanLoop)
            .await;
        Ok(())
    }
}

/// This function converts a metastore error into an actor error.
///
/// If the metastore error is implying the transaction has not been
/// successful, then we do not need to restart the metastore.
/// If the metastore error does not let us know whether the transaction was
/// successful or not, we need to restart the actor and have it load its state from
/// the metastore.
///
/// This function also logs errors.
fn convert_metastore_error<T>(
    metastore_error: MetastoreError,
) -> Result<ControlPlaneResult<T>, ActorExitStatus> {
    // If true, we know that the transactions has not been recorded in the Metastore.
    // If false, we simply are not sure whether the transaction has been recorded or not.
    let is_transaction_certainly_aborted = match &metastore_error {
        MetastoreError::AlreadyExists(_)
        | MetastoreError::FailedPrecondition { .. }
        | MetastoreError::Forbidden { .. }
        | MetastoreError::InvalidArgument { .. }
        | MetastoreError::JsonDeserializeError { .. }
        | MetastoreError::JsonSerializeError { .. }
        | MetastoreError::NotFound(_) => true,
        MetastoreError::Connection { .. }
        | MetastoreError::Db { .. }
        | MetastoreError::Internal { .. }
        | MetastoreError::Io { .. }
        | MetastoreError::Unavailable(_) => false,
    };
    if is_transaction_certainly_aborted {
        // If the metastore transaction is certain to have been aborted,
        // this is actually a good thing.
        // We do not need to restart the control plane.
        if !matches!(metastore_error, MetastoreError::AlreadyExists(_)) {
            // This is not always an error to attempt to create an object that already exists.
            // In particular, we create two otel indexes on startup.
            // It will be up to the client to decide what to do there.
            error!(err=?metastore_error, transaction_outcome="aborted", "metastore error");
        }
        crate::metrics::CONTROL_PLANE_METRICS
            .metastore_error_aborted
            .inc();
        Ok(Err(ControlPlaneError::Metastore(metastore_error)))
    } else {
        // If the metastore transaction may have been executed, we need to restart the control plane
        // so that it gets resynced with the metastore state.
        error!(err=?metastore_error, transaction_outcome="maybe-executed", "metastore error");
        crate::metrics::CONTROL_PLANE_METRICS
            .metastore_error_maybe_executed
            .inc();
        Err(ActorExitStatus::from(anyhow::anyhow!(metastore_error)))
    }
}

// This handler is a metastore call proxied through the control plane: we must first forward the
// request to the metastore, and then act on the event.
#[async_trait]
impl Handler<CreateIndexRequest> for ControlPlane {
    type Reply = ControlPlaneResult<CreateIndexResponse>;

    async fn handle(
        &mut self,
        request: CreateIndexRequest,
        _ctx: &ActorContext<Self>,
    ) -> Result<Self::Reply, ActorExitStatus> {
        let index_config = match metastore_serde_utils::from_json_str(&request.index_config_json) {
            Ok(index_config) => index_config,
            Err(error) => {
                return Ok(Err(ControlPlaneError::from(error)));
            }
        };
        let index_uid: IndexUid = match self.metastore.create_index(request).await {
            Ok(response) => response.index_uid.into(),
            Err(metastore_error) => return convert_metastore_error(metastore_error),
        };

        let index_metadata: IndexMetadata =
            IndexMetadata::new_with_index_uid(index_uid.clone(), index_config);

        self.model.add_index(index_metadata);

        self.indexing_scheduler
            .schedule_indexing_plan_if_needed(&self.model);

        let response = CreateIndexResponse {
            index_uid: index_uid.into(),
        };
        // We do not need to inform the indexing scheduler as there are no shards at this point.
        Ok(Ok(response))
    }
}

// This handler is a metastore call proxied through the control plane: we must first forward the
// request to the metastore, and then act on the event.
#[async_trait]
impl Handler<DeleteIndexRequest> for ControlPlane {
    type Reply = ControlPlaneResult<EmptyResponse>;

    async fn handle(
        &mut self,
        request: DeleteIndexRequest,
        _ctx: &ActorContext<Self>,
    ) -> Result<Self::Reply, ActorExitStatus> {
        let index_uid: IndexUid = request.index_uid.clone().into();

        if let Err(metastore_error) = self.metastore.delete_index(request).await {
            return convert_metastore_error(metastore_error);
        };

        let ingester_needing_resync: BTreeSet<NodeId> = self
            .model
            .list_shards_for_index(&index_uid)
            .flat_map(|shard_entry| shard_entry.ingester_nodes())
            .collect();

        self.model.delete_index(&index_uid);

        self.ingest_controller
            .sync_with_ingesters(&ingester_needing_resync, &self.model);

        // TODO: Refine the event. Notify index will have the effect to reload the entire state from
        // the metastore. We should update the state of the control plane.
        self.indexing_scheduler
            .schedule_indexing_plan_if_needed(&self.model);

        let response = EmptyResponse {};
        Ok(Ok(response))
    }
}

// This handler is a metastore call proxied through the control plane: we must first forward the
// request to the metastore, and then act on the event.
#[async_trait]
impl Handler<AddSourceRequest> for ControlPlane {
    type Reply = ControlPlaneResult<EmptyResponse>;

    async fn handle(
        &mut self,
        request: AddSourceRequest,
        _ctx: &ActorContext<Self>,
    ) -> Result<Self::Reply, ActorExitStatus> {
        let index_uid: IndexUid = request.index_uid.clone().into();
        let source_config: SourceConfig =
            match metastore_serde_utils::from_json_str(&request.source_config_json) {
                Ok(source_config) => source_config,
                Err(error) => {
                    return Ok(Err(ControlPlaneError::from(error)));
                }
            };
        if let Err(error) = self.metastore.add_source(request).await {
            return Ok(Err(ControlPlaneError::from(error)));
        };

        self.model
            .add_source(&index_uid, source_config)
            .context("failed to add source")?;

        // TODO: Refine the event. Notify index will have the effect to reload the entire state from
        // the metastore. We should update the state of the control plane.
        self.indexing_scheduler
            .schedule_indexing_plan_if_needed(&self.model);

        let response = EmptyResponse {};
        Ok(Ok(response))
    }
}

// This handler is a metastore call proxied through the control plane: we must first forward the
// request to the metastore, and then act on the event.
#[async_trait]
impl Handler<ToggleSourceRequest> for ControlPlane {
    type Reply = ControlPlaneResult<EmptyResponse>;

    async fn handle(
        &mut self,
        request: ToggleSourceRequest,
        _ctx: &ActorContext<Self>,
    ) -> Result<Self::Reply, ActorExitStatus> {
        let index_uid: IndexUid = request.index_uid.clone().into();
        let source_id = request.source_id.clone();
        let enable = request.enable;

        if let Err(error) = self.metastore.toggle_source(request).await {
            return Ok(Err(ControlPlaneError::from(error)));
        };

        let has_changed = self.model.toggle_source(&index_uid, &source_id, enable)?;

        if has_changed {
            self.indexing_scheduler
                .schedule_indexing_plan_if_needed(&self.model);
        }

        Ok(Ok(EmptyResponse {}))
    }
}

// This handler is a metastore call proxied through the control plane: we must first forward the
// request to the metastore, and then act on the event.
#[async_trait]
impl Handler<DeleteSourceRequest> for ControlPlane {
    type Reply = ControlPlaneResult<EmptyResponse>;

    async fn handle(
        &mut self,
        request: DeleteSourceRequest,
        _ctx: &ActorContext<Self>,
    ) -> Result<ControlPlaneResult<EmptyResponse>, ActorExitStatus> {
        let index_uid: IndexUid = request.index_uid.clone().into();
        let source_id = request.source_id.clone();

        let source_uid = SourceUid {
            index_uid: index_uid.clone(),
            source_id: source_id.clone(),
        };

        if let Err(metastore_error) = self.metastore.delete_source(request).await {
            // TODO If the metastore fails returns an error but somehow succeed deleting the source,
            // the control plane will restart and the shards will be remaining on the ingesters.
            //
            // This is tracked in #4274
            return convert_metastore_error(metastore_error);
        };

        let ingester_needing_resync: BTreeSet<NodeId> =
            if let Some(shards) = self.model.list_shards_for_source(&source_uid) {
                shards
                    .flat_map(|shard_entry| shard_entry.ingester_nodes())
                    .collect()
            } else {
                BTreeSet::new()
            };

        self.ingest_controller
            .sync_with_ingesters(&ingester_needing_resync, &self.model);

        self.model.delete_source(&source_uid);

        self.indexing_scheduler
            .schedule_indexing_plan_if_needed(&self.model);
        let response = EmptyResponse {};

        Ok(Ok(response))
    }
}

// This is neither a proxied call nor a metastore callback.
#[async_trait]
impl Handler<GetOrCreateOpenShardsRequest> for ControlPlane {
    type Reply = ControlPlaneResult<GetOrCreateOpenShardsResponse>;

    async fn handle(
        &mut self,
        request: GetOrCreateOpenShardsRequest,
        ctx: &ActorContext<Self>,
    ) -> Result<Self::Reply, ActorExitStatus> {
        let response = match self
            .ingest_controller
            .get_or_create_open_shards(request, &mut self.model, ctx.progress())
            .await
        {
            Ok(response) => response,
            Err(ControlPlaneError::Metastore(metastore_error)) => {
                return convert_metastore_error(metastore_error);
            }
            Err(control_plane_error) => {
                return Ok(Err(control_plane_error));
            }
        };
        // TODO: Why do we return an error if the indexing scheduler fails?
        self.indexing_scheduler
            .schedule_indexing_plan_if_needed(&self.model);
        Ok(Ok(response))
    }
}

#[async_trait]
impl Handler<LocalShardsUpdate> for ControlPlane {
    type Reply = ControlPlaneResult<()>;

    async fn handle(
        &mut self,
        local_shards_update: LocalShardsUpdate,
        ctx: &ActorContext<Self>,
    ) -> Result<Self::Reply, ActorExitStatus> {
        self.ingest_controller
            .handle_local_shards_update(local_shards_update, &mut self.model, ctx.progress())
            .await;
        self.indexing_scheduler
            .schedule_indexing_plan_if_needed(&self.model);
        Ok(Ok(()))
    }
}

#[async_trait]
impl Handler<GetDebugStateRequest> for ControlPlane {
    type Reply = ControlPlaneResult<GetDebugStateResponse>;

    async fn handle(
        &mut self,
        _: GetDebugStateRequest,
        _ctx: &ActorContext<Self>,
    ) -> Result<Self::Reply, ActorExitStatus> {
        Ok(Ok(self.debug_state()))
    }
}

#[derive(Clone)]
pub struct ControlPlaneEventSubscriber(WeakMailbox<ControlPlane>);

impl ControlPlaneEventSubscriber {
    pub fn new(weak_control_plane_mailbox: WeakMailbox<ControlPlane>) -> Self {
        Self(weak_control_plane_mailbox)
    }
}

#[async_trait]
impl EventSubscriber<LocalShardsUpdate> for ControlPlaneEventSubscriber {
    async fn handle_event(&mut self, local_shards_update: LocalShardsUpdate) {
        if let Some(control_plane_mailbox) = self.0.upgrade() {
            if let Err(error) = control_plane_mailbox
                .send_message(local_shards_update)
                .await
            {
                error!(error=%error, "failed to forward local shards update to control plane");
            }
        }
    }
}

#[async_trait]
impl EventSubscriber<ShardPositionsUpdate> for ControlPlaneEventSubscriber {
    async fn handle_event(&mut self, shard_positions_update: ShardPositionsUpdate) {
        if let Some(control_plane_mailbox) = self.0.upgrade() {
            if let Err(error) = control_plane_mailbox
                .send_message(shard_positions_update)
                .await
            {
                error!(error=%error, "failed to forward shard positions update to control plane");
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use mockall::Sequence;
    use quickwit_actors::{AskError, Observe, SupervisorMetrics};
    use quickwit_config::{IndexConfig, SourceParams, INGEST_V2_SOURCE_ID};
    use quickwit_indexing::IndexingService;
    use quickwit_metastore::{
        CreateIndexRequestExt, IndexMetadata, ListIndexesMetadataResponseExt,
    };
    use quickwit_proto::control_plane::GetOrCreateOpenShardsSubrequest;
    use quickwit_proto::indexing::{ApplyIndexingPlanRequest, CpuCapacity, IndexingServiceClient};
    use quickwit_proto::ingest::ingester::{IngesterServiceClient, RetainShardsResponse};
    use quickwit_proto::ingest::{Shard, ShardState};
    use quickwit_proto::metastore::{
        DeleteShardsResponse, EntityKind, ListIndexesMetadataRequest, ListIndexesMetadataResponse,
        ListShardsRequest, ListShardsResponse, ListShardsSubresponse, MetastoreError, SourceType,
    };
    use quickwit_proto::types::Position;

    use super::*;
    use crate::IndexerNodeInfo;

    #[tokio::test]
    async fn test_control_plane_create_index() {
        let universe = Universe::with_accelerated_time();

        let cluster_id = "test-cluster".to_string();
        let self_node_id: NodeId = "test-node".into();
        let indexer_pool = IndexerPool::default();
        let ingester_pool = IngesterPool::default();

        let mut mock_metastore = MetastoreServiceClient::mock();
        mock_metastore
            .expect_create_index()
            .withf(|create_index_request| {
                let index_config: IndexConfig =
                    serde_json::from_str(&create_index_request.index_config_json).unwrap();
                assert_eq!(index_config.index_id, "test-index");
                assert_eq!(index_config.index_uri, "ram:///test-index");
                true
            })
            .returning(|_| {
                Ok(CreateIndexResponse {
                    index_uid: "test-index:0".to_string(),
                })
            });
        mock_metastore
            .expect_list_indexes_metadata()
            .returning(|_| {
                Ok(ListIndexesMetadataResponse::try_from_indexes_metadata(Vec::new()).unwrap())
            });
        let replication_factor = 1;

        let (control_plane_mailbox, _control_plane_handle) = ControlPlane::spawn(
            &universe,
            cluster_id,
            self_node_id,
            indexer_pool,
            ingester_pool,
            MetastoreServiceClient::from(mock_metastore),
            replication_factor,
        );
        let index_config = IndexConfig::for_test("test-index", "ram:///test-index");
        let create_index_request = CreateIndexRequest {
            index_config_json: serde_json::to_string(&index_config).unwrap(),
        };
        let create_index_response = control_plane_mailbox
            .ask_for_res(create_index_request)
            .await
            .unwrap();
        assert_eq!(create_index_response.index_uid, "test-index:0");

        // TODO: Test that create index event is properly sent to ingest controller.

        universe.assert_quit().await;
    }

    #[tokio::test]
    async fn test_control_plane_delete_index() {
        let universe = Universe::with_accelerated_time();

        let cluster_id = "test-cluster".to_string();
        let self_node_id: NodeId = "test-node".into();
        let indexer_pool = IndexerPool::default();
        let ingester_pool = IngesterPool::default();

        let mut mock_metastore = MetastoreServiceClient::mock();
        mock_metastore
            .expect_delete_index()
            .withf(|delete_index_request| delete_index_request.index_uid == "test-index:0")
            .returning(|_| Ok(EmptyResponse {}));
        mock_metastore
            .expect_list_indexes_metadata()
            .returning(|_| {
                Ok(ListIndexesMetadataResponse::try_from_indexes_metadata(Vec::new()).unwrap())
            });
        let replication_factor = 1;

        let (control_plane_mailbox, _control_plane_handle) = ControlPlane::spawn(
            &universe,
            cluster_id,
            self_node_id,
            indexer_pool,
            ingester_pool,
            MetastoreServiceClient::from(mock_metastore),
            replication_factor,
        );
        let delete_index_request = DeleteIndexRequest {
            index_uid: "test-index:0".to_string(),
        };
        control_plane_mailbox
            .ask_for_res(delete_index_request)
            .await
            .unwrap();

        // TODO: Test that delete index event is properly sent to ingest controller.

        universe.assert_quit().await;
    }

    #[tokio::test]
    async fn test_control_plane_add_source() {
        let universe = Universe::with_accelerated_time();

        let cluster_id = "test-cluster".to_string();
        let self_node_id: NodeId = "test-node".into();
        let indexer_pool = IndexerPool::default();
        let ingester_pool = IngesterPool::default();

        let index_metadata = IndexMetadata::for_test("test-index", "ram://test");
        let index_uid = index_metadata.index_uid.clone();
        let mut mock_metastore = MetastoreServiceClient::mock();
        mock_metastore
            .expect_add_source()
            .withf(|add_source_request| {
                let source_config: SourceConfig =
                    serde_json::from_str(&add_source_request.source_config_json).unwrap();
                assert_eq!(source_config.source_id, "test-source");
                assert_eq!(source_config.source_type(), SourceType::Void);
                true
            })
            .returning(|_| Ok(EmptyResponse {}));
        let index_metadata = IndexMetadata::for_test("test-index", "ram://test");
        mock_metastore
            .expect_list_indexes_metadata()
            .returning(move |_| {
                Ok(ListIndexesMetadataResponse::try_from_indexes_metadata(vec![
                    index_metadata.clone()
                ])
                .unwrap())
            });
        let replication_factor = 1;
        let (control_plane_mailbox, _control_plane_handle) = ControlPlane::spawn(
            &universe,
            cluster_id,
            self_node_id,
            indexer_pool,
            ingester_pool,
            MetastoreServiceClient::from(mock_metastore),
            replication_factor,
        );

        let source_config = SourceConfig::for_test("test-source", SourceParams::void());
        let add_source_request = AddSourceRequest {
            index_uid: index_uid.to_string(),
            source_config_json: serde_json::to_string(&source_config).unwrap(),
        };
        control_plane_mailbox
            .ask_for_res(add_source_request)
            .await
            .unwrap();

        // TODO: Test that delete index event is properly sent to ingest controller.

        universe.assert_quit().await;
    }

    #[tokio::test]
    async fn test_control_plane_toggle_source() {
        let universe = Universe::with_accelerated_time();

        let cluster_id = "test-cluster".to_string();
        let self_node_id: NodeId = "test-node".into();
        let indexer_pool = IndexerPool::default();
        let ingester_pool = IngesterPool::default();

        let mut mock_metastore = MetastoreServiceClient::mock();
        let mut index_metadata = IndexMetadata::for_test("test-index", "ram://toto");
        let test_source_config = SourceConfig::for_test("test-source", SourceParams::void());
        index_metadata.add_source(test_source_config).unwrap();
        mock_metastore
            .expect_list_indexes_metadata()
            .return_once(|_| {
                Ok(
                    ListIndexesMetadataResponse::try_from_indexes_metadata(vec![index_metadata])
                        .unwrap(),
                )
            });

        mock_metastore
            .expect_toggle_source()
            .times(1)
            .return_once(|toggle_source_request| {
                assert_eq!(toggle_source_request.index_uid, "test-index:0");
                assert_eq!(toggle_source_request.source_id, "test-source");
                Ok(EmptyResponse {})
            });
        mock_metastore
            .expect_toggle_source()
            .times(1)
            .return_once(|toggle_source_request| {
                assert_eq!(toggle_source_request.index_uid, "test-index:0");
                assert_eq!(toggle_source_request.source_id, "test-source");
                assert!(!toggle_source_request.enable);
                Ok(EmptyResponse {})
            });

        let replication_factor = 1;

        let (control_plane_mailbox, _control_plane_handle) = ControlPlane::spawn(
            &universe,
            cluster_id,
            self_node_id,
            indexer_pool,
            ingester_pool,
            MetastoreServiceClient::from(mock_metastore),
            replication_factor,
        );

        let enabling_source_req = ToggleSourceRequest {
            index_uid: "test-index:0".to_string(),
            source_id: "test-source".to_string(),
            enable: true,
        };
        control_plane_mailbox
            .ask_for_res(enabling_source_req)
            .await
            .unwrap();

        let disabling_source_req = ToggleSourceRequest {
            index_uid: "test-index:0".to_string(),
            source_id: "test-source".to_string(),
            enable: false,
        };
        control_plane_mailbox
            .ask_for_res(disabling_source_req)
            .await
            .unwrap();

        universe.assert_quit().await;
    }

    #[tokio::test]
    async fn test_control_plane_delete_source() {
        let universe = Universe::with_accelerated_time();

        let cluster_id = "test-cluster".to_string();
        let self_node_id: NodeId = "test-node".into();
        let indexer_pool = IndexerPool::default();
        let ingester_pool = IngesterPool::default();

        let mut mock_metastore = MetastoreServiceClient::mock();
        mock_metastore
            .expect_delete_source()
            .withf(|delete_source_request| {
                assert_eq!(delete_source_request.index_uid, "test-index:0");
                assert_eq!(delete_source_request.source_id, "test-source");
                true
            })
            .returning(|_| Ok(EmptyResponse {}));
        mock_metastore
            .expect_list_indexes_metadata()
            .returning(|_| {
                Ok(ListIndexesMetadataResponse::try_from_indexes_metadata(Vec::new()).unwrap())
            });
        let replication_factor = 1;

        let (control_plane_mailbox, _control_plane_handle) = ControlPlane::spawn(
            &universe,
            cluster_id,
            self_node_id,
            indexer_pool,
            ingester_pool,
            MetastoreServiceClient::from(mock_metastore),
            replication_factor,
        );
        let delete_source_request = DeleteSourceRequest {
            index_uid: "test-index:0".to_string(),
            source_id: "test-source".to_string(),
        };
        control_plane_mailbox
            .ask_for_res(delete_source_request)
            .await
            .unwrap();

        // TODO: Test that delete index event is properly sent to ingest controller.

        universe.assert_quit().await;
    }

    #[tokio::test]
    async fn test_control_plane_get_or_create_open_shards() {
        let universe = Universe::with_accelerated_time();

        let cluster_id = "test-cluster".to_string();
        let self_node_id: NodeId = "test-node".into();
        let indexer_pool = IndexerPool::default();

        let ingester_pool = IngesterPool::default();

        let mut mock_metastore = MetastoreServiceClient::mock();
        mock_metastore
            .expect_list_indexes_metadata()
            .returning(|_| {
                let mut index_metadata = IndexMetadata::for_test("test-index", "ram:///test-index");
                let mut source_config = SourceConfig::ingest_v2_default();
                source_config.enabled = true;
                index_metadata.add_source(source_config).unwrap();
                Ok(
                    ListIndexesMetadataResponse::try_from_indexes_metadata(vec![index_metadata])
                        .unwrap(),
                )
            });
        mock_metastore.expect_list_shards().returning(|request| {
            assert_eq!(request.subrequests.len(), 1);

            let subrequest = &request.subrequests[0];
            assert_eq!(subrequest.index_uid, "test-index:0");
            assert_eq!(subrequest.source_id, INGEST_V2_SOURCE_ID);

            let subresponses = vec![ListShardsSubresponse {
                index_uid: "test-index:0".to_string(),
                source_id: INGEST_V2_SOURCE_ID.to_string(),
                shards: vec![Shard {
                    index_uid: "test-index:0".to_string(),
                    source_id: INGEST_V2_SOURCE_ID.to_string(),
                    shard_id: Some(ShardId::from(1)),
                    shard_state: ShardState::Open as i32,
                    ..Default::default()
                }],
            }];
            let response = ListShardsResponse { subresponses };
            Ok(response)
        });
        let replication_factor = 1;

        let (control_plane_mailbox, _control_plane_handle) = ControlPlane::spawn(
            &universe,
            cluster_id,
            self_node_id,
            indexer_pool,
            ingester_pool,
            MetastoreServiceClient::from(mock_metastore),
            replication_factor,
        );
        let get_open_shards_request = GetOrCreateOpenShardsRequest {
            subrequests: vec![GetOrCreateOpenShardsSubrequest {
                subrequest_id: 0,
                index_id: "test-index".to_string(),
                source_id: INGEST_V2_SOURCE_ID.to_string(),
            }],
            closed_shards: Vec::new(),
            unavailable_leaders: Vec::new(),
        };
        let get_open_shards_response = control_plane_mailbox
            .ask_for_res(get_open_shards_request)
            .await
            .unwrap();
        assert_eq!(get_open_shards_response.successes.len(), 1);
        assert_eq!(get_open_shards_response.failures.len(), 0);

        let subresponse = &get_open_shards_response.successes[0];
        assert_eq!(subresponse.index_uid, "test-index:0");
        assert_eq!(subresponse.source_id, INGEST_V2_SOURCE_ID);
        assert_eq!(subresponse.open_shards.len(), 1);
        assert_eq!(subresponse.open_shards[0].shard_id(), ShardId::from(1));

        universe.assert_quit().await;
    }

    #[tokio::test]
    async fn test_control_plane_supervision_reload_from_metastore() {
        let universe = Universe::default();
        let node_id = NodeId::new("test_node".to_string());
        let indexer_pool = IndexerPool::default();
        let ingester_pool = IngesterPool::default();
        let mut mock_metastore = MetastoreServiceClient::mock();

        let mut index_0 = IndexMetadata::for_test("test-index-0", "ram:///test-index-0");
        let source = SourceConfig::ingest_v2_default();
        index_0.add_source(source.clone()).unwrap();

        mock_metastore
            .expect_list_indexes_metadata()
            .times(2) // 1 for the first initialization, 1 after the respawn of the control plane.
            .returning(|list_indexes_request: ListIndexesMetadataRequest| {
                assert_eq!(list_indexes_request, ListIndexesMetadataRequest::all());
                Ok(ListIndexesMetadataResponse::empty())
            });
        mock_metastore.expect_list_shards().return_once(
            |_list_shards_request: ListShardsRequest| {
                let list_shards_resp = ListShardsResponse {
                    subresponses: Vec::new(),
                };
                Ok(list_shards_resp)
            },
        );
        let index_uid = IndexUid::new_with_random_ulid("test-index");
        let index_uid_string = index_uid.to_string();
        mock_metastore.expect_create_index().times(1).return_once(
            |_create_index_request: CreateIndexRequest| {
                Ok(CreateIndexResponse {
                    index_uid: index_uid_string,
                })
            },
        );
        mock_metastore.expect_create_index().times(1).return_once(
            |create_index_request: CreateIndexRequest| {
                Err(MetastoreError::AlreadyExists(EntityKind::Index {
                    index_id: create_index_request
                        .deserialize_index_config()
                        .unwrap()
                        .index_id,
                }))
            },
        );
        mock_metastore.expect_create_index().times(1).return_once(
            |_create_index_request: CreateIndexRequest| {
                Err(MetastoreError::Connection {
                    message: "Fake connection error.".to_string(),
                })
            },
        );

        let (control_plane_mailbox, control_plane_handle) = ControlPlane::spawn(
            &universe,
            "cluster".to_string(),
            node_id,
            indexer_pool,
            ingester_pool,
            MetastoreServiceClient::from(mock_metastore),
            1,
        );

        let index_config = IndexConfig::for_test("test-index", "ram:///test-index");
        let create_index_request = CreateIndexRequest {
            index_config_json: serde_json::to_string(&index_config).unwrap(),
        };

        // A happy path: we simply create the index.
        assert!(control_plane_mailbox
            .ask_for_res(create_index_request.clone())
            .await
            .is_ok());

        // Now let's see what happens if we attempt to create the same index a second time.
        let control_plane_error: ControlPlaneError = control_plane_mailbox
            .ask(create_index_request.clone())
            .await
            .unwrap()
            .unwrap_err();

        // That kind of error clearly indicates that the transaction has failed.
        // The control plane does not need to be restarted.
        assert!(
            matches!(control_plane_error, ControlPlaneError::Metastore(MetastoreError::AlreadyExists(entity)) if entity == EntityKind::Index { index_id: "test-index".to_string() })
        );

        control_plane_mailbox.ask(Observe).await.unwrap();

        assert_eq!(
            control_plane_handle
                .process_pending_and_observe()
                .await
                .metrics,
            SupervisorMetrics {
                num_panics: 0,
                num_errors: 0,
                num_kills: 0
            }
        );

        // Now let's see what happens with a grayer type of error.
        let control_plane_error: AskError<ControlPlaneError> = control_plane_mailbox
            .ask_for_res(create_index_request)
            .await
            .unwrap_err();
        assert!(matches!(control_plane_error, AskError::ProcessMessageError));

        // This time, the control plane is restarted.
        control_plane_mailbox.ask(Observe).await.unwrap();
        assert_eq!(
            control_plane_handle
                .process_pending_and_observe()
                .await
                .metrics,
            SupervisorMetrics {
                num_panics: 0,
                num_errors: 1,
                num_kills: 0
            }
        );

        universe.assert_quit().await;
    }

    #[tokio::test]
    async fn test_delete_shard_on_eof() {
        quickwit_common::setup_logging_for_tests();
        let universe = Universe::default();
        let node_id = NodeId::new("control-plane-node".to_string());
        let indexer_pool = IndexerPool::default();
        let (client_mailbox, client_inbox) = universe.create_test_mailbox();
        let client = IndexingServiceClient::from_mailbox::<IndexingService>(client_mailbox);
        let indexer_node_info = IndexerNodeInfo {
            client,
            indexing_tasks: Vec::new(),
            indexing_capacity: CpuCapacity::from_cpu_millis(4_000),
        };
        indexer_pool.insert("indexer-node-1".to_string(), indexer_node_info);
        let ingester_pool = IngesterPool::default();
        let mut mock_metastore = MetastoreServiceClient::mock();

        let mut index_0 = IndexMetadata::for_test("test-index-0", "ram:///test-index-0");
        let mut source = SourceConfig::ingest_v2_default();
        source.enabled = true;
        index_0.add_source(source.clone()).unwrap();

        let index_0_clone = index_0.clone();
        mock_metastore.expect_list_indexes_metadata().return_once(
            move |list_indexes_request: ListIndexesMetadataRequest| {
                assert_eq!(list_indexes_request, ListIndexesMetadataRequest::all());
                Ok(ListIndexesMetadataResponse::try_from_indexes_metadata(vec![
                    index_0_clone.clone()
                ])
                .unwrap())
            },
        );
        let index_uid_clone = index_0.index_uid.to_string();
        mock_metastore.expect_delete_shards().return_once(
            move |delete_shards_request: DeleteShardsRequest| {
                assert!(!delete_shards_request.force);
                assert_eq!(delete_shards_request.subrequests.len(), 1);
                let subrequest = &delete_shards_request.subrequests[0];
                assert_eq!(subrequest.index_uid, index_uid_clone);
                assert_eq!(subrequest.source_id, INGEST_V2_SOURCE_ID);
                assert_eq!(subrequest.shard_ids, [ShardId::from(17)]);
                Ok(DeleteShardsResponse {})
            },
        );

        let mut shard = Shard {
            index_uid: index_0.index_uid.to_string(),
            source_id: INGEST_V2_SOURCE_ID.to_string(),
            shard_id: Some(ShardId::from(17)),
            leader_id: "test_node".to_string(),
            ..Default::default()
        };
        shard.set_shard_state(ShardState::Open);

        let index_uid_clone = index_0.index_uid.clone();
        mock_metastore.expect_list_shards().return_once(
            move |_list_shards_request: ListShardsRequest| {
                let list_shards_resp = ListShardsResponse {
                    subresponses: vec![ListShardsSubresponse {
                        index_uid: index_uid_clone.to_string(),
                        source_id: INGEST_V2_SOURCE_ID.to_string(),
                        shards: vec![shard],
                    }],
                };
                Ok(list_shards_resp)
            },
        );

        let (control_plane_mailbox, _control_plane_handle) = ControlPlane::spawn(
            &universe,
            "cluster".to_string(),
            node_id,
            indexer_pool,
            ingester_pool,
            MetastoreServiceClient::from(mock_metastore),
            1,
        );
        let source_uid = SourceUid {
            index_uid: index_0.index_uid.clone(),
            source_id: INGEST_V2_SOURCE_ID.to_string(),
        };

        // This update should not triggeer anything in the control plane.
        control_plane_mailbox
            .ask(ShardPositionsUpdate {
                source_uid: source_uid.clone(),
                shard_positions: vec![(ShardId::from(17), Position::offset(1_000u64))],
            })
            .await
            .unwrap();

        let control_plane_obs: ControlPlaneObservableState =
            control_plane_mailbox.ask(Observe).await.unwrap();
        let last_applied_physical_plan = control_plane_obs
            .indexing_scheduler
            .last_applied_physical_plan
            .unwrap();
        let indexing_tasks = last_applied_physical_plan
            .indexing_tasks_per_indexer()
            .get("indexer-node-1")
            .unwrap();
        assert_eq!(indexing_tasks.len(), 1);
        assert_eq!(indexing_tasks[0].shard_ids, [ShardId::from(17)]);
        let _ = client_inbox.drain_for_test();

        // This update should trigger the deletion of the shard and a new indexing plan.
        control_plane_mailbox
            .ask(ShardPositionsUpdate {
                source_uid,
                shard_positions: vec![(ShardId::from(17), Position::eof(1_000u64))],
            })
            .await
            .unwrap();

        let control_plane_obs: ControlPlaneObservableState =
            control_plane_mailbox.ask(Observe).await.unwrap();
        let last_applied_physical_plan = control_plane_obs
            .indexing_scheduler
            .last_applied_physical_plan
            .unwrap();
        let indexing_tasks = last_applied_physical_plan
            .indexing_tasks_per_indexer()
            .get("indexer-node-1")
            .unwrap();
        assert!(indexing_tasks.is_empty());

        let results: Vec<ApplyIndexingPlanRequest> =
            client_inbox.drain_for_test_typed::<ApplyIndexingPlanRequest>();
        assert_eq!(results.len(), 1);
        assert!(results[0].indexing_tasks.is_empty());

        universe.assert_quit().await;
    }

    #[tokio::test]
    async fn test_delete_non_existing_shard() {
        quickwit_common::setup_logging_for_tests();
        let universe = Universe::default();
        let node_id = NodeId::new("control-plane-node".to_string());
        let indexer_pool = IndexerPool::default();
        let (client_mailbox, _client_inbox) = universe.create_test_mailbox();
        let client = IndexingServiceClient::from_mailbox::<IndexingService>(client_mailbox);
        let indexer_node_info = IndexerNodeInfo {
            client,
            indexing_tasks: Vec::new(),
            indexing_capacity: CpuCapacity::from_cpu_millis(4_000),
        };
        indexer_pool.insert("indexer-node-1".to_string(), indexer_node_info);
        let ingester_pool = IngesterPool::default();
        let mut mock_metastore = MetastoreServiceClient::mock();

        let mut index_0 = IndexMetadata::for_test("test-index-0", "ram:///test-index-0");
        let mut source = SourceConfig::ingest_v2_default();
        source.enabled = true;
        index_0.add_source(source.clone()).unwrap();

        let index_0_clone = index_0.clone();
        mock_metastore.expect_list_indexes_metadata().return_once(
            move |list_indexes_request: ListIndexesMetadataRequest| {
                assert_eq!(list_indexes_request, ListIndexesMetadataRequest::all());
                Ok(ListIndexesMetadataResponse::try_from_indexes_metadata(vec![
                    index_0_clone.clone()
                ])
                .unwrap())
            },
        );
        let index_uid_clone = index_0.index_uid.to_string();
        mock_metastore.expect_delete_shards().return_once(
            move |delete_shards_request: DeleteShardsRequest| {
                assert!(!delete_shards_request.force);
                assert_eq!(delete_shards_request.subrequests.len(), 1);
                let subrequest = &delete_shards_request.subrequests[0];
                assert_eq!(subrequest.index_uid, index_uid_clone);
                assert_eq!(subrequest.source_id, INGEST_V2_SOURCE_ID);
                assert_eq!(subrequest.shard_ids, [ShardId::from(17)]);
                Ok(DeleteShardsResponse {})
            },
        );

        let index_uid_clone = index_0.index_uid.clone();
        mock_metastore.expect_list_shards().return_once(
            move |_list_shards_request: ListShardsRequest| {
                let list_shards_resp = ListShardsResponse {
                    subresponses: vec![ListShardsSubresponse {
                        index_uid: index_uid_clone.to_string(),
                        source_id: INGEST_V2_SOURCE_ID.to_string(),
                        shards: vec![],
                    }],
                };
                Ok(list_shards_resp)
            },
        );

        let (control_plane_mailbox, _control_plane_handle) = ControlPlane::spawn(
            &universe,
            "cluster".to_string(),
            node_id,
            indexer_pool,
            ingester_pool,
            MetastoreServiceClient::from(mock_metastore),
            1,
        );
        let source_uid = SourceUid {
            index_uid: index_0.index_uid.clone(),
            source_id: INGEST_V2_SOURCE_ID.to_string(),
        };

        // This update should not triggeer anything in the control plane.
        control_plane_mailbox
            .ask(ShardPositionsUpdate {
                source_uid: source_uid.clone(),
                shard_positions: vec![(ShardId::from(17), Position::eof(1_000u64))],
            })
            .await
            .unwrap();

        universe.assert_quit().await;
    }

    #[tokio::test]
    async fn test_delete_index() {
        quickwit_common::setup_logging_for_tests();
        let universe = Universe::default();
        let node_id = NodeId::new("control-plane-node".to_string());
        let indexer_pool = IndexerPool::default();

        let ingester_pool = IngesterPool::default();
        let mut ingester_mock = IngesterServiceClient::mock();
        let mut seq = Sequence::new();

        let mut index_0 = IndexMetadata::for_test("test-index-0", "ram:///test-index-0");
        let mut source = SourceConfig::ingest_v2_default();
        source.enabled = true;
        index_0.add_source(source.clone()).unwrap();

        let index_uid_clone = index_0.index_uid.clone();
        let index_0_clone = index_0.clone();

        let mut mock_metastore = MetastoreServiceClient::mock();
        mock_metastore
            .expect_list_indexes_metadata()
            .times(1)
            .in_sequence(&mut seq)
            .returning(move |list_indexes_request: ListIndexesMetadataRequest| {
                assert_eq!(list_indexes_request, ListIndexesMetadataRequest::all());
                Ok(ListIndexesMetadataResponse::try_from_indexes_metadata(vec![
                    index_0_clone.clone()
                ])
                .unwrap())
            });
        mock_metastore
            .expect_list_shards()
            .times(1)
            .in_sequence(&mut seq)
            .returning(move |_list_shards_request: ListShardsRequest| {
                let list_shards_resp = ListShardsResponse {
                    subresponses: vec![ListShardsSubresponse {
                        index_uid: index_uid_clone.to_string(),
                        source_id: INGEST_V2_SOURCE_ID.to_string(),
                        shards: vec![Shard {
                            index_uid: index_uid_clone.to_string(),
                            source_id: source.source_id.to_string(),
                            shard_id: Some(ShardId::from(15)),
                            leader_id: "node1".to_string(),
                            follower_id: None,
                            shard_state: ShardState::Open as i32,
                            publish_position_inclusive: None,
                            publish_token: None,
                        }],
                    }],
                };
                Ok(list_shards_resp)
            });

        ingester_mock
            .expect_retain_shards()
            .times(1)
            .in_sequence(&mut seq)
            .returning(|request| {
                assert_eq!(request.retain_shards_for_sources.len(), 1);
                assert_eq!(
                    request.retain_shards_for_sources[0].shard_ids,
                    [ShardId::from(15)]
                );
                Ok(RetainShardsResponse {})
            });

        let index_uid_clone = index_0.index_uid.clone();
        mock_metastore
            .expect_delete_index()
            .times(1)
            .in_sequence(&mut seq)
            .returning(move |delete_index_request: DeleteIndexRequest| {
                assert_eq!(delete_index_request.index_uid, index_uid_clone.to_string());
                Ok(EmptyResponse {})
            });
        ingester_mock
            .expect_retain_shards()
            .times(1)
            .in_sequence(&mut seq)
            .returning(|mut request| {
                assert_eq!(request.retain_shards_for_sources.len(), 1);
                let retain_shards_for_source = request.retain_shards_for_sources.pop().unwrap();
                assert!(&retain_shards_for_source.shard_ids.is_empty());
                Ok(RetainShardsResponse {})
            });
        ingester_pool.insert("node1".into(), ingester_mock.into());

        let (control_plane_mailbox, _control_plane_handle) = ControlPlane::spawn(
            &universe,
            "cluster".to_string(),
            node_id,
            indexer_pool,
            ingester_pool,
            MetastoreServiceClient::from(mock_metastore),
            1,
        );
        // This update should not trigger anything in the control plane.
        control_plane_mailbox
            .ask(DeleteIndexRequest {
                index_uid: index_0.index_uid.to_string(),
            })
            .await
            .unwrap()
            .unwrap();

        universe.assert_quit().await;
    }
    #[tokio::test]
    async fn test_delete_source() {
        quickwit_common::setup_logging_for_tests();
        let universe = Universe::default();
        let node_id = NodeId::new("control-plane-node".to_string());
        let indexer_pool = IndexerPool::default();

        let ingester_pool = IngesterPool::default();
        let mut ingester_mock = IngesterServiceClient::mock();
        ingester_mock
            .expect_retain_shards()
            .times(2)
            .returning(|request| {
                assert_eq!(request.retain_shards_for_sources.len(), 1);
                assert_eq!(
                    request.retain_shards_for_sources[0].shard_ids,
                    [ShardId::from(15)]
                );
                Ok(RetainShardsResponse {})
            });
        ingester_pool.insert("node1".into(), ingester_mock.into());

        let mut index_0 = IndexMetadata::for_test("test-index-0", "ram:///test-index-0");
        let index_uid_clone = index_0.index_uid.clone();

        let mut mock_metastore = MetastoreServiceClient::mock();
        mock_metastore.expect_delete_source().return_once(
            move |delete_source_request: DeleteSourceRequest| {
                assert_eq!(delete_source_request.index_uid, index_uid_clone.to_string());
                assert_eq!(&delete_source_request.source_id, INGEST_V2_SOURCE_ID);
                Ok(EmptyResponse {})
            },
        );

        let mut source = SourceConfig::ingest_v2_default();
        source.enabled = true;
        index_0.add_source(source.clone()).unwrap();

        let index_0_clone = index_0.clone();
        mock_metastore.expect_list_indexes_metadata().return_once(
            move |list_indexes_request: ListIndexesMetadataRequest| {
                assert_eq!(list_indexes_request, ListIndexesMetadataRequest::all());
                Ok(ListIndexesMetadataResponse::try_from_indexes_metadata(vec![
                    index_0_clone.clone()
                ])
                .unwrap())
            },
        );

        let index_uid_clone = index_0.index_uid.clone();
        mock_metastore.expect_list_shards().return_once(
            move |_list_shards_request: ListShardsRequest| {
                let list_shards_resp = ListShardsResponse {
                    subresponses: vec![ListShardsSubresponse {
                        index_uid: index_uid_clone.to_string(),
                        source_id: INGEST_V2_SOURCE_ID.to_string(),
                        shards: vec![Shard {
                            index_uid: index_uid_clone.to_string(),
                            source_id: source.source_id.to_string(),
                            shard_id: Some(ShardId::from(15)),
                            leader_id: "node1".to_string(),
                            follower_id: None,
                            shard_state: ShardState::Open as i32,
                            publish_position_inclusive: None,
                            publish_token: None,
                        }],
                    }],
                };
                Ok(list_shards_resp)
            },
        );

        let (control_plane_mailbox, _control_plane_handle) = ControlPlane::spawn(
            &universe,
            "cluster".to_string(),
            node_id,
            indexer_pool,
            ingester_pool,
            MetastoreServiceClient::from(mock_metastore),
            1,
        );
        // This update should not trigger anything in the control plane.
        control_plane_mailbox
            .ask(DeleteSourceRequest {
                index_uid: index_0.index_uid.to_string(),
                source_id: INGEST_V2_SOURCE_ID.to_string(),
            })
            .await
            .unwrap()
            .unwrap();

        universe.assert_quit().await;
    }
}
