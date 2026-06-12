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

use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::fmt::Formatter;
use std::num::NonZeroUsize;
use std::time::Duration;

use anyhow::Context;
use async_trait::async_trait;
use futures::stream::FuturesUnordered;
use futures::{Future, StreamExt};
use itertools::Itertools;
use quickwit_actors::{
    Actor, ActorContext, ActorExitStatus, ActorHandle, DeferableReplyHandler, Handler, Mailbox,
    Supervisor, Universe, WeakMailbox,
};
use quickwit_cluster::{
    ClusterChange, ClusterChangeStream, ClusterChangeStreamFactory, ClusterNode,
};
use quickwit_common::pretty::PrettyDisplay;
use quickwit_common::pubsub::EventSubscriber;
use quickwit_common::uri::Uri;
use quickwit_common::{Progress, shared_consts};
use quickwit_config::{ClusterConfig, IndexConfig, IndexTemplate, SourceConfig};
use quickwit_ingest::{IngesterPool, LocalShardsUpdate};
use quickwit_metastore::{CreateIndexRequestExt, CreateIndexResponseExt, IndexMetadataResponseExt};
use quickwit_proto::control_plane::{
    AdviseResetShardsRequest, AdviseResetShardsResponse, ControlPlaneError, ControlPlaneResult,
    DisableMaintenanceModeRequest, DisableMaintenanceModeResponse, EnableMaintenanceModeRequest,
    EnableMaintenanceModeResponse, GetMaintenanceModeRequest, GetMaintenanceModeResponse,
    GetOrCreateOpenShardsRequest, GetOrCreateOpenShardsResponse, GetOrCreateOpenShardsSubrequest,
    SwapIndexingPipelinesRequest, SwapIndexingPipelinesResponse,
};
use quickwit_proto::indexing::ShardPositionsUpdate;
use quickwit_proto::metastore::{
    AddSourceRequest, CreateIndexRequest, CreateIndexResponse, DeleteIndexRequest,
    DeleteShardsRequest, DeleteSourceRequest, EmptyResponse, FindIndexTemplateMatchesRequest,
    IndexMetadataResponse, IndexTemplateMatch, MetastoreError, MetastoreResult, MetastoreService,
    MetastoreServiceClient, PruneShardsRequest, ToggleSourceRequest, UpdateIndexRequest,
    UpdateSourceRequest, serde_utils,
};
use quickwit_proto::types::{IndexId, IndexUid, NodeId, ShardId, SourceId, SourceUid};
use serde::Serialize;
use serde_json::{Value as JsonValue, json};
use tokio::sync::watch;
use tracing::{Level, debug, enabled, error, info};

use crate::IndexerPool;
use crate::cooldown_map::{CooldownMap, CooldownStatus};
use crate::debouncer::Debouncer;
use crate::indexing_scheduler::{IndexingScheduler, IndexingSchedulerState};
use crate::ingest::IngestController;
use crate::ingest::ingest_controller::{IngestControllerStats, RebalanceShardsCallback};
use crate::maintenance::{MaintenanceState, MetastoreKvPersistence, serialize_frozen_plan};
use crate::model::ControlPlaneModel;

/// Interval between two controls (or checks) of the desired plan VS running plan.
pub(crate) const CONTROL_PLAN_LOOP_INTERVAL: Duration = if cfg!(any(test, feature = "testsuite")) {
    Duration::from_millis(100)
} else {
    Duration::from_secs(5)
};

/// Minimum period between two identical shard pruning operations.
const PRUNE_SHARDS_DEFAULT_COOLDOWN_PERIOD: Duration = Duration::from_secs(120);

/// Minimum period between two rebuild plan operations.
const REBUILD_PLAN_COOLDOWN_PERIOD: Duration = Duration::from_secs(2);

#[derive(Debug)]
struct ControlPlanLoop;

#[derive(Debug, Default, Clone, Copy)]
struct RebuildPlan;

pub struct ControlPlane {
    cluster_config: ClusterConfig,
    cluster_change_stream_opt: Option<ClusterChangeStream>,
    // The control plane state is split into to independent functions, that we naturally isolated
    // code wise and state wise.
    //
    // - The indexing scheduler is in charge of managing indexers: it decides which indexer should
    // index which source/shards.
    // - the ingest controller is in charge of managing ingesters: it opens and closes shards on
    // the different ingesters.
    indexing_scheduler: IndexingScheduler,
    ingest_controller: IngestController,
    metastore: MetastoreServiceClient,
    model: ControlPlaneModel,
    prune_shard_cooldown: CooldownMap<(IndexId, SourceId)>,
    rebuild_plan_debouncer: Debouncer,
    readiness_tx: watch::Sender<bool>,
    // Disables the control loop. This is useful for unit testing.
    disable_control_loop: bool,
    /// Maintenance mode state. When active the indexing plan is frozen (not
    /// rebuilt on topology changes).
    maintenance: MaintenanceState,
    /// Persistence backend for maintenance mode state (frozen plan + metadata).
    maintenance_persistence: MetastoreKvPersistence,
}

impl fmt::Debug for ControlPlane {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        f.debug_struct("ControlPlane").finish()
    }
}

impl ControlPlane {
    pub fn spawn(
        universe: &Universe,
        cluster_config: ClusterConfig,
        self_node_id: NodeId,
        cluster_change_stream_factory: impl ClusterChangeStreamFactory,
        indexer_pool: IndexerPool,
        ingester_pool: IngesterPool,
        metastore: MetastoreServiceClient,
    ) -> (
        Mailbox<Self>,
        ActorHandle<Supervisor<Self>>,
        watch::Receiver<bool>,
    ) {
        let disable_control_loop = false;
        let maintenance_persistence = MetastoreKvPersistence::new(metastore.clone());
        Self::spawn_inner(
            universe,
            cluster_config,
            self_node_id,
            cluster_change_stream_factory,
            indexer_pool,
            ingester_pool,
            metastore,
            disable_control_loop,
            maintenance_persistence,
        )
    }

    #[allow(clippy::too_many_arguments)]
    fn spawn_inner(
        universe: &Universe,
        cluster_config: ClusterConfig,
        self_node_id: NodeId,
        cluster_change_stream_factory: impl ClusterChangeStreamFactory,
        indexer_pool: IndexerPool,
        ingester_pool: IngesterPool,
        metastore: MetastoreServiceClient,
        disable_control_loop: bool,
        maintenance_persistence: MetastoreKvPersistence,
    ) -> (
        Mailbox<Self>,
        ActorHandle<Supervisor<Self>>,
        watch::Receiver<bool>,
    ) {
        info!("starting control plane");

        let (readiness_tx, readiness_rx) = watch::channel(false);
        let (control_plane_mailbox, control_plane_handle) =
            universe.spawn_builder().supervise_fn(move || {
                let cluster_id = cluster_config.cluster_id.clone();
                let replication_factor = cluster_config.replication_factor;
                let shard_throughput_limit_mib: f32 = cluster_config.shard_throughput_limit.as_u64()
                    as f32
                    / shared_consts::MIB as f32;
                let indexing_scheduler =
                    IndexingScheduler::new(cluster_id, self_node_id.clone(), indexer_pool.clone());
                let ingest_controller = IngestController::new(
                    metastore.clone(),
                    ingester_pool.clone(),
                    replication_factor,
                    shard_throughput_limit_mib,
                    cluster_config.shard_scale_up_factor,
                );

                let readiness_tx = readiness_tx.clone();
                let _ = readiness_tx.send(false);

                ControlPlane {
                    cluster_config: cluster_config.clone(),
                    cluster_change_stream_opt: Some(cluster_change_stream_factory.create()),
                    indexing_scheduler,
                    ingest_controller,
                    metastore: metastore.clone(),
                    model: Default::default(),
                    prune_shard_cooldown: CooldownMap::new(NonZeroUsize::new(1024).unwrap()),
                    rebuild_plan_debouncer: Debouncer::new(REBUILD_PLAN_COOLDOWN_PERIOD),
                    readiness_tx,
                    disable_control_loop,
                    maintenance: MaintenanceState::default(),
                    maintenance_persistence: maintenance_persistence.clone(),
                }
            });
        (control_plane_mailbox, control_plane_handle, readiness_rx)
    }
}

#[derive(Debug, Clone, Serialize, Default)]
pub struct ControlPlaneObservableState {
    pub indexing_scheduler: IndexingSchedulerState,
    pub ingest_controller: IngestControllerStats,
    pub num_indexes: usize,
    pub num_sources: usize,
    pub readiness: bool,
    pub maintenance_mode: bool,
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
            ingest_controller: self.ingest_controller.stats,
            num_indexes: self.model.num_indexes(),
            num_sources: self.model.num_sources(),
            readiness: *self.readiness_tx.borrow(),
            maintenance_mode: self.maintenance.is_active(),
        }
    }

    async fn initialize(&mut self, ctx: &ActorContext<Self>) -> Result<(), ActorExitStatus> {
        crate::metrics::CONTROL_PLANE_METRICS.restart_total.inc();

        self.model
            .load_from_metastore(&mut self.metastore, ctx.progress())
            .await
            .context("failed to initialize control plane model")?;

        self.load_maintenance_state_from_persistence().await;

        if self.maintenance.is_active() {
            // In maintenance mode: restore the frozen plan without triggering a rebuild.
            info!(
                enabled_at = self.maintenance.enabled_at().unwrap_or_default(),
                "control plane starting in maintenance mode: indexing plan is frozen"
            );
        } else {
            let _rebuild_plan_waiter = self.rebuild_plan_debounced(ctx);
        }

        self.ingest_controller.sync_with_all_ingesters(&self.model);

        ctx.schedule_self_msg(CONTROL_PLAN_LOOP_INTERVAL, ControlPlanLoop);

        let weak_mailbox = ctx.mailbox().downgrade();
        let cluster_change_stream = self
            .cluster_change_stream_opt
            .take()
            .expect("`initialize` should be called only once");
        spawn_watch_indexers_task(weak_mailbox, cluster_change_stream);
        let _ = self.readiness_tx.send(true);
        Ok(())
    }
}

impl ControlPlane {
    /// Loads maintenance state from the persistence backend.
    /// Called during `initialize()`.
    async fn load_maintenance_state_from_persistence(&mut self) {
        match self.maintenance_persistence.load().await {
            Some(persisted) => {
                self.maintenance.load_from_metadata(persisted.metadata);
                if self.maintenance.is_active() {
                    crate::metrics::CONTROL_PLANE_METRICS
                        .maintenance_mode
                        .set(1);
                    let num_indexers = persisted.frozen_plan.num_indexers();
                    let num_pipelines: usize = persisted
                        .frozen_plan
                        .indexing_tasks_per_indexer()
                        .values()
                        .map(|tasks| tasks.len())
                        .sum();
                    info!(
                        num_indexers,
                        num_pipelines, "restored frozen indexing plan from persistence"
                    );
                    self.indexing_scheduler
                        .load_frozen_plan(persisted.frozen_plan);
                }
            }
            None => {
                // No maintenance state persisted — normal operation.
            }
        }
    }

    async fn auto_create_indexes(
        &mut self,
        subrequests: &[GetOrCreateOpenShardsSubrequest],
        progress: &Progress,
    ) -> MetastoreResult<()> {
        if !self.cluster_config.auto_create_indexes {
            return Ok(());
        }
        let mut index_ids = Vec::new();

        for subrequest in subrequests {
            if self.model.index_uid(&subrequest.index_id).is_none() {
                index_ids.push(subrequest.index_id.clone());
            }
        }
        if index_ids.is_empty() {
            return Ok(());
        }
        let find_index_template_matches_request = FindIndexTemplateMatchesRequest { index_ids };
        let find_index_template_matches_response = progress
            .protect_future(
                self.metastore
                    .find_index_template_matches(find_index_template_matches_request),
            )
            .await?;

        let mut create_index_futures = FuturesUnordered::new();

        for index_template_match in find_index_template_matches_response.matches {
            // TODO: It's a bit brutal to fail the entire operation if applying a single index
            // template fails. We should return a partial failure instead for the subrequest. I
            // want to do so in an upcoming refactor where the `GetOrCreateOpenShardsRequest` will
            // be processed in multiple steps in a dedicated workbench.
            let index_config = apply_index_template_match(
                index_template_match,
                &self.cluster_config.default_index_root_uri,
            )?;
            // We disable ingest V1 for index templates.
            let source_configs = [SourceConfig::ingest_v2(), SourceConfig::cli()];

            let create_index_request = CreateIndexRequest::try_from_index_and_source_configs(
                &index_config,
                &source_configs,
            )?;
            let create_index_future = {
                let metastore = self.metastore.clone();
                async move { metastore.create_index(create_index_request).await }
            };
            create_index_futures.push(create_index_future);
        }
        while let Some(create_index_response_result) =
            progress.protect_future(create_index_futures.next()).await
        {
            // Same here.
            let create_index_response = create_index_response_result?;
            let index_metadata = create_index_response.deserialize_index_metadata()?;
            self.model.add_index(index_metadata);
        }
        Ok(())
    }

    /// Deletes a set of shards from the metastore and the control plane model.
    ///
    /// If the shards were already absent this operation is considered successful.
    async fn delete_shards(
        &mut self,
        source_uid: &SourceUid,
        shard_ids: &[ShardId],
        progress: &Progress,
    ) -> anyhow::Result<()> {
        debug!(
            index_uid=%source_uid.index_uid,
            source_id=%source_uid.source_id,
            shard_ids=%shard_ids.pretty_display(),
            "deleting shards"
        );
        let delete_shards_request = DeleteShardsRequest {
            index_uid: Some(source_uid.index_uid.clone()),
            source_id: source_uid.source_id.clone(),
            shard_ids: shard_ids.to_vec(),
            force: false,
        };
        // We use a tiny bit different strategy here than for other handlers
        // All metastore errors end up fail/respawn the control plane.
        //
        // This is because deleting shards is done in reaction to an event
        // and we do not really have the freedom to return an error to a caller like for other
        // calls: there is no caller.
        progress
            .protect_future(self.metastore.delete_shards(delete_shards_request))
            .await
            .context("failed to delete shards from metastore")?;

        self.model.delete_shards(source_uid, shard_ids);

        info!(
            index_uid=%source_uid.index_uid,
            source_id=%source_uid.source_id,
            shard_ids=%shard_ids.pretty_display(),
            "deleted shards"
        );
        Ok(())
    }

    fn debug_info(&self) -> JsonValue {
        let physical_indexing_plan: Vec<JsonValue> = self
            .indexing_scheduler
            .observable_state()
            .current_targeted_physical_plan
            .map(|plan| {
                plan.indexing_tasks_per_indexer()
                    .iter()
                    .map(|(node_id, tasks)| {
                        json!({
                            "node_id": node_id.clone(),
                            "tasks": tasks.clone(),
                        })
                    })
                    .collect()
            })
            .unwrap_or_default();

        let mut per_index_and_leader_shards_json: BTreeMap<
            IndexUid,
            BTreeMap<String, Vec<JsonValue>>,
        > = BTreeMap::new();

        for (source_uid, shard_entries) in self.model.all_shards_with_source() {
            for shard_entry in shard_entries {
                let shard_json = json!({
                    "index_uid": source_uid.index_uid,
                    "source_id": source_uid.source_id,
                    "shard_id": shard_entry.shard_id,
                    "shard_state": shard_entry.shard_state().as_json_str_name(),
                    "leader_id": shard_entry.leader_id,
                    "follower_id": shard_entry.follower_id,
                    "publish_position_inclusive": shard_entry.publish_position_inclusive(),
                });
                per_index_and_leader_shards_json
                    .entry(source_uid.index_uid.clone())
                    .or_default()
                    .entry(shard_entry.leader_id.clone())
                    .or_default()
                    .push(shard_json);
            }
        }
        json!({
            "physical_indexing_plan": physical_indexing_plan,
            "shard_table": per_index_and_leader_shards_json,
        })
    }

    /// Rebuilds the indexing plan.
    ///
    /// This method includes some debouncing logic. Every call will be followed by a cooldown
    /// period.
    ///
    /// This method returns a future that can be awaited to ensure that the relevant rebuild plan
    /// operation has been executed.
    fn rebuild_plan_debounced(
        &mut self,
        ctx: &ActorContext<Self>,
    ) -> impl Future<Output = ()> + use<> {
        let next_rebuild_waiter = self
            .indexing_scheduler
            .next_rebuild_tracker
            .next_rebuild_waiter();
        self.rebuild_plan_debouncer
            .self_send_with_cooldown::<RebuildPlan>(ctx);
        next_rebuild_waiter
    }
}

#[async_trait]
impl Handler<RebuildPlan> for ControlPlane {
    type Reply = ();

    async fn handle(
        &mut self,
        _message: RebuildPlan,
        _ctx: &ActorContext<Self>,
    ) -> Result<(), ActorExitStatus> {
        self.indexing_scheduler
            .rebuild_plan(&self.model, self.maintenance.is_active());
        Ok(())
    }
}

#[async_trait]
impl Handler<ShardPositionsUpdate> for ControlPlane {
    type Reply = ();

    async fn handle(
        &mut self,
        shard_positions_update: ShardPositionsUpdate,
        ctx: &ActorContext<Self>,
    ) -> Result<(), ActorExitStatus> {
        if enabled!(Level::DEBUG) {
            let pretty_positions: Vec<String> = shard_positions_update
                .updated_shard_positions
                .iter()
                .map(|(shard_id, position)| format!("{shard_id}:{}", position.pretty_display()))
                .sorted()
                .collect();

            debug!(
                index_uid=%shard_positions_update.source_uid.index_uid,
                source_id=%shard_positions_update.source_uid.source_id,
                positions=%pretty_positions.as_slice().pretty_display(),
                "received shard positions update"
            );
        }
        let Some(shard_entries) = self
            .model
            .get_shards_for_source_mut(&shard_positions_update.source_uid)
        else {
            // The source no longer exists.
            return Ok(());
        };

        let mut shard_ids_to_close = Vec::new();
        for (shard_id, position) in shard_positions_update.updated_shard_positions {
            if let Some(shard) = shard_entries.get_mut(&shard_id) {
                shard.publish_position_inclusive =
                    Some(shard.publish_position_inclusive().max(position.clone()));
                if position.is_eof() {
                    // identify shards that have reached EOF but have not yet been removed.
                    info!(
                        index_uid=%shard_positions_update.source_uid.index_uid,
                        source_id=%shard_positions_update.source_uid.source_id,
                        %shard_id,
                        position=%position.pretty_display(),
                        "received shard eof via gossip"
                    );
                    shard_ids_to_close.push(shard_id);
                }
            }
        }
        if shard_ids_to_close.is_empty() {
            return Ok(());
        }
        self.delete_shards(
            &shard_positions_update.source_uid,
            &shard_ids_to_close,
            ctx.progress(),
        )
        .await?;
        let _rebuild_plan_waiter = self.rebuild_plan_debounced(ctx);
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
        if self.disable_control_loop {
            return Ok(());
        }
        let is_maintenance = self.maintenance.is_active();
        if let Err(metastore_error) = self
            .ingest_controller
            .rebalance_shards(
                &mut self.model,
                ctx.mailbox(),
                ctx.progress(),
                is_maintenance,
            )
            .await
        {
            return convert_metastore_error::<()>(metastore_error).map(|_| ());
        }
        self.indexing_scheduler
            .control_running_plan(&self.model, is_maintenance);
        ctx.schedule_self_msg(CONTROL_PLAN_LOOP_INTERVAL, ControlPlanLoop);
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
    let is_transaction_certainly_aborted = metastore_error.is_transaction_certainly_aborted();
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
        error!(error=?metastore_error, transaction_outcome="maybe-executed", "metastore error");
        crate::metrics::CONTROL_PLANE_METRICS
            .metastore_error_maybe_executed
            .inc();
        Err(ActorExitStatus::from(anyhow::anyhow!(metastore_error)))
    }
}

// This handler is a metastore call proxied through the control plane: we must first forward the
// request to the metastore, and then act on the event.
#[async_trait]
impl DeferableReplyHandler<CreateIndexRequest> for ControlPlane {
    type Reply = ControlPlaneResult<CreateIndexResponse>;

    async fn handle_message(
        &mut self,
        request: CreateIndexRequest,
        reply: impl FnOnce(Self::Reply) + Send + Sync + 'static,
        ctx: &ActorContext<Self>,
    ) -> Result<(), ActorExitStatus> {
        debug!("creating index");

        let response = match ctx
            .protect_future(self.metastore.create_index(request))
            .await
        {
            Ok(response) => response,
            Err(metastore_error) => {
                reply(convert_metastore_error(metastore_error)?);
                return Ok(());
            }
        };
        let index_metadata = match response.deserialize_index_metadata() {
            Ok(index_metadata) => index_metadata,
            Err(serde_error) => {
                error!(error=?serde_error, "failed to deserialize index metadata");
                return Err(ActorExitStatus::from(anyhow::anyhow!(serde_error)));
            }
        };
        let index_uid = index_metadata.index_uid.clone();

        // Now, create index can also add sources to support creating indexes automatically from
        // index and source config templates.
        let should_rebuild_plan =
            !index_metadata.sources.is_empty() && !self.maintenance.is_active();
        self.model.add_index(index_metadata);

        if should_rebuild_plan {
            let rebuild_plan_notifier = self.rebuild_plan_debounced(ctx);
            tokio::task::spawn(async move {
                rebuild_plan_notifier.await;
                reply(Ok(response));
            });
        } else {
            reply(Ok(response));
        }
        info!(%index_uid, "created index");
        Ok(())
    }
}

// This handler is a metastore call proxied through the control plane: we must first forward the
// request to the metastore, and then act on the event.
#[async_trait]
impl Handler<UpdateIndexRequest> for ControlPlane {
    type Reply = ControlPlaneResult<IndexMetadataResponse>;

    async fn handle(
        &mut self,
        request: UpdateIndexRequest,
        ctx: &ActorContext<Self>,
    ) -> Result<Self::Reply, ActorExitStatus> {
        let index_uid: IndexUid = request.index_uid().clone();
        debug!(%index_uid, "updating index");

        let response = match ctx
            .protect_future(self.metastore.update_index(request))
            .await
        {
            Ok(response) => response,
            Err(metastore_error) => {
                return convert_metastore_error(metastore_error);
            }
        };
        let index_metadata = match response.deserialize_index_metadata() {
            Ok(index_metadata) => index_metadata,
            Err(serde_error) => {
                error!(error=?serde_error, "failed to deserialize index metadata");
                return Err(ActorExitStatus::from(anyhow::anyhow!(serde_error)));
            }
        };
        if self
            .model
            .update_index_config(&index_uid, index_metadata.index_config)?
            && !self.maintenance.is_active()
        {
            let _rebuild_plan_notifier = self.rebuild_plan_debounced(ctx);
        }
        info!(%index_uid, "updated index");
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
        ctx: &ActorContext<Self>,
    ) -> Result<Self::Reply, ActorExitStatus> {
        let index_uid: IndexUid = request.index_uid().clone();
        debug!(%index_uid, "deleting index");

        if let Err(metastore_error) = ctx
            .protect_future(self.metastore.delete_index(request))
            .await
        {
            return convert_metastore_error(metastore_error);
        };
        let ingester_needing_resync: BTreeSet<NodeId> = self
            .model
            .list_shards_for_index(&index_uid)
            .flat_map(|shard_entry| shard_entry.ingesters())
            .map(|node_id_ref| node_id_ref.to_owned())
            .collect();

        self.model.delete_index(&index_uid);

        self.ingest_controller
            .sync_with_ingesters(&ingester_needing_resync, &self.model);

        // TODO: Refine the event. Notify index will have the effect to reload the entire state from
        // the metastore. We should update the state of the control plane.
        if !self.maintenance.is_active() {
            let _rebuild_plan_waiter = self.rebuild_plan_debounced(ctx);
        }

        info!(%index_uid, "deleted index");
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
        ctx: &ActorContext<Self>,
    ) -> Result<Self::Reply, ActorExitStatus> {
        let index_uid: IndexUid = request.index_uid().clone();
        let source_config: SourceConfig =
            match serde_utils::from_json_str(&request.source_config_json) {
                Ok(source_config) => source_config,
                Err(error) => {
                    error!(%error, "failed to deserialize source config");
                    return Ok(Err(ControlPlaneError::from(error)));
                }
            };
        let source_id = source_config.source_id.clone();
        debug!(%index_uid, source_id, "adding source");

        if let Err(error) = ctx.protect_future(self.metastore.add_source(request)).await {
            return Ok(Err(ControlPlaneError::from(error)));
        };
        self.model
            .add_source(&index_uid, source_config)
            .context("failed to add source")?;

        info!(%index_uid, source_id, "added source");

        // TODO: Refine the event. Notify index will have the effect to reload the entire state from
        // the metastore. We should update the state of the control plane.
        if !self.maintenance.is_active() {
            let _rebuild_plan_waiter = self.rebuild_plan_debounced(ctx);
        }

        let response = EmptyResponse {};
        Ok(Ok(response))
    }
}

#[async_trait]
impl Handler<UpdateSourceRequest> for ControlPlane {
    type Reply = ControlPlaneResult<EmptyResponse>;

    async fn handle(
        &mut self,
        request: UpdateSourceRequest,
        ctx: &ActorContext<Self>,
    ) -> Result<Self::Reply, ActorExitStatus> {
        let index_uid: IndexUid = request.index_uid().clone();
        let source_config: SourceConfig =
            match serde_utils::from_json_str(&request.source_config_json) {
                Ok(source_config) => source_config,
                Err(error) => {
                    error!(%error, "failed to deserialize source config");
                    return Ok(Err(ControlPlaneError::from(error)));
                }
            };
        let source_id = source_config.source_id.clone();
        debug!(%index_uid, source_id, "updating source");

        if let Err(error) = ctx
            .protect_future(self.metastore.update_source(request))
            .await
        {
            return Ok(Err(ControlPlaneError::from(error)));
        };
        self.model
            .update_source(&index_uid, source_config)
            .context("failed to add source")?;

        // TODO: Refine the event. Notify index will have the effect to reload the entire state from
        // the metastore. We should update the state of the control plane.
        if !self.maintenance.is_active() {
            let _rebuild_plan_waiter = self.rebuild_plan_debounced(ctx);
        }

        info!(%index_uid, source_id, "updated source");
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
        ctx: &ActorContext<Self>,
    ) -> Result<Self::Reply, ActorExitStatus> {
        let index_uid: IndexUid = request.index_uid().clone();
        let source_id = request.source_id.clone();
        let enable = request.enable;
        debug!(%index_uid, source_id, enable, "toggling source");

        if let Err(error) = ctx
            .protect_future(self.metastore.toggle_source(request))
            .await
        {
            return Ok(Err(ControlPlaneError::from(error)));
        };

        let mutation_occurred = self
            .model
            .toggle_source(&index_uid, &source_id, enable)
            .context("failed to toggle source")?;

        if mutation_occurred && !self.maintenance.is_active() {
            let _rebuild_plan_waiter = self.rebuild_plan_debounced(ctx);
        }
        info!(%index_uid, source_id, enabled=enable, "toggled source");
        let response = EmptyResponse {};
        Ok(Ok(response))
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
        ctx: &ActorContext<Self>,
    ) -> Result<ControlPlaneResult<EmptyResponse>, ActorExitStatus> {
        let index_uid: IndexUid = request.index_uid().clone();
        let source_id = request.source_id.clone();
        debug!(%index_uid, source_id, "deleting source");

        let source_uid = SourceUid {
            index_uid: index_uid.clone(),
            source_id: source_id.clone(),
        };

        if let Err(metastore_error) = ctx
            .protect_future(self.metastore.delete_source(request))
            .await
        {
            // TODO If the metastore fails returns an error but somehow succeed deleting the source,
            // the control plane will restart and the shards will be remaining on the ingesters.
            //
            // This is tracked in #4274
            return convert_metastore_error(metastore_error);
        };

        let ingesters_needing_resync: BTreeSet<NodeId> =
            if let Some(shard_entries) = self.model.get_shards_for_source(&source_uid) {
                shard_entries
                    .values()
                    .flat_map(|shard_entry| shard_entry.ingesters())
                    .map(|node_id_ref| node_id_ref.to_owned())
                    .collect()
            } else {
                BTreeSet::new()
            };

        self.ingest_controller
            .sync_with_ingesters(&ingesters_needing_resync, &self.model);

        self.model.delete_source(&source_uid);
        if !self.maintenance.is_active() {
            let _rebuild_plan_waiter = self.rebuild_plan_debounced(ctx);
        }

        info!(
            index_uid=%source_uid.index_uid,
            source_id=%source_uid.source_id,
            "deleted source"
        );
        let response = EmptyResponse {};
        Ok(Ok(response))
    }
}

#[async_trait]
impl Handler<PruneShardsRequest> for ControlPlane {
    type Reply = ControlPlaneResult<EmptyResponse>;

    async fn handle(
        &mut self,
        request: PruneShardsRequest,
        _ctx: &ActorContext<Self>,
    ) -> Result<ControlPlaneResult<EmptyResponse>, ActorExitStatus> {
        let interval = request
            .interval_secs
            .map(|interval_secs| Duration::from_secs(interval_secs as u64))
            .unwrap_or_else(|| PRUNE_SHARDS_DEFAULT_COOLDOWN_PERIOD);

        // A very basic debounce is enough here, missing one call to the pruning API is fine
        let status = self.prune_shard_cooldown.update(
            (
                request.index_uid().index_id.clone(),
                request.source_id.clone(),
            ),
            interval,
        );
        if let CooldownStatus::Ready = status
            && let Err(metastore_error) = self.metastore.prune_shards(request).await
        {
            return convert_metastore_error(metastore_error);
        };
        // Return ok regardless of whether the call was successful or debounced
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
        // In maintenance mode, block auto-create indexes but still allow shard routing
        // for existing sources (ingest must continue).
        if !self.maintenance.is_active()
            && let Err(metastore_error) = self
                .auto_create_indexes(&request.subrequests, ctx.progress())
                .await
        {
            return convert_metastore_error(metastore_error);
        }
        match self
            .ingest_controller
            .get_or_create_open_shards(request, &mut self.model, ctx.progress())
            .await
        {
            Ok(response) => {
                let _rebuild_plan_waiter = self.rebuild_plan_debounced(ctx);
                Ok(Ok(response))
            }
            Err(metastore_error) => convert_metastore_error(metastore_error),
        }
    }
}

// This is neither a proxied call nor a metastore callback.
#[async_trait]
impl Handler<AdviseResetShardsRequest> for ControlPlane {
    type Reply = ControlPlaneResult<AdviseResetShardsResponse>;

    async fn handle(
        &mut self,
        request: AdviseResetShardsRequest,
        _ctx: &ActorContext<Self>,
    ) -> Result<Self::Reply, ActorExitStatus> {
        let response = self
            .ingest_controller
            .advise_reset_shards(request, &self.model);
        Ok(Ok(response))
    }
}

#[async_trait]
impl Handler<SwapIndexingPipelinesRequest> for ControlPlane {
    type Reply = ControlPlaneResult<SwapIndexingPipelinesResponse>;

    async fn handle(
        &mut self,
        request: SwapIndexingPipelinesRequest,
        _ctx: &ActorContext<Self>,
    ) -> Result<Self::Reply, ActorExitStatus> {
        let response = self.indexing_scheduler.swap_pipelines(request);
        Ok(response)
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
        if self.maintenance.is_active() {
            // In maintenance mode: skip shard scaling to avoid changing the plan.
            debug!("maintenance mode: ignoring local shards update (scaling frozen)");
            return Ok(Ok(()));
        }
        if let Err(metastore_error) = self
            .ingest_controller
            .handle_local_shards_update(local_shards_update, &mut self.model, ctx.progress())
            .await
        {
            return convert_metastore_error(metastore_error);
        }
        let _rebuild_plan_waiter = self.rebuild_plan_debounced(ctx);
        Ok(Ok(()))
    }
}

#[derive(Debug)]
pub struct GetDebugInfo;

#[async_trait]
impl Handler<GetDebugInfo> for ControlPlane {
    type Reply = JsonValue;

    async fn handle(
        &mut self,
        _: GetDebugInfo,
        _ctx: &ActorContext<Self>,
    ) -> Result<Self::Reply, ActorExitStatus> {
        Ok(self.debug_info())
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
        if let Some(control_plane_mailbox) = self.0.upgrade()
            && let Err(error) = control_plane_mailbox
                .send_message(local_shards_update)
                .await
        {
            error!(%error, "failed to forward local shards update to control plane");
        }
    }
}

#[async_trait]
impl EventSubscriber<ShardPositionsUpdate> for ControlPlaneEventSubscriber {
    async fn handle_event(&mut self, shard_positions_update: ShardPositionsUpdate) {
        if let Some(control_plane_mailbox) = self.0.upgrade()
            && let Err(error) = control_plane_mailbox
                .send_message(shard_positions_update)
                .await
        {
            error!(%error, "failed to forward shard positions update to control plane");
        }
    }
}

fn apply_index_template_match(
    index_template_match: IndexTemplateMatch,
    default_index_root_uri: &Uri,
) -> MetastoreResult<IndexConfig> {
    let index_template: IndexTemplate =
        serde_utils::from_json_str(&index_template_match.index_template_json)?;
    let index_config = index_template
        .apply_template(index_template_match.index_id, default_index_root_uri)
        .map_err(|error| MetastoreError::Internal {
            message: "failed to apply index template".to_string(),
            cause: error.to_string(),
        })?;
    Ok(index_config)
}

/// The indexer joined the cluster.
#[derive(Debug)]
struct IndexerJoined(ClusterNode);

#[async_trait]
impl Handler<IndexerJoined> for ControlPlane {
    type Reply = ();

    async fn handle(
        &mut self,
        message: IndexerJoined,
        ctx: &ActorContext<Self>,
    ) -> Result<Self::Reply, ActorExitStatus> {
        let is_maintenance = self.maintenance.is_active();
        if is_maintenance {
            info!(
                "indexer `{}` joined the cluster during maintenance mode",
                message.0.node_id
            );
        } else {
            info!(
                "indexer `{}` joined the cluster: rebalancing shards and rebuilding indexing plan",
                message.0.node_id
            );
        }

        // TODO: Update shard table.
        if let Err(metastore_error) = self
            .ingest_controller
            .rebalance_shards(
                &mut self.model,
                ctx.mailbox(),
                ctx.progress(),
                is_maintenance,
            )
            .await
        {
            return convert_metastore_error::<()>(metastore_error).map(|_| ());
        }
        self.indexing_scheduler
            .rebuild_plan(&self.model, is_maintenance);
        Ok(())
    }
}

/// The indexer left the cluster.
#[derive(Debug)]
struct IndexerLeft(ClusterNode);

#[async_trait]
impl Handler<IndexerLeft> for ControlPlane {
    type Reply = ();

    async fn handle(
        &mut self,
        message: IndexerLeft,
        ctx: &ActorContext<Self>,
    ) -> Result<Self::Reply, ActorExitStatus> {
        let is_maintenance = self.maintenance.is_active();
        if is_maintenance {
            info!(
                "indexer `{}` left the cluster during maintenance mode",
                message.0.node_id
            );
            return Ok(());
        } else {
            info!(
                "indexer `{}` left the cluster: rebalancing shards and rebuilding indexing plan",
                message.0.node_id
            );
        }
        // TODO: Update shard table.
        if let Err(metastore_error) = self
            .ingest_controller
            .rebalance_shards(
                &mut self.model,
                ctx.mailbox(),
                ctx.progress(),
                is_maintenance,
            )
            .await
        {
            return convert_metastore_error::<()>(metastore_error).map(|_| ());
        }
        self.indexing_scheduler
            .rebuild_plan(&self.model, is_maintenance);
        Ok(())
    }
}

#[async_trait]
impl Handler<RebalanceShardsCallback> for ControlPlane {
    type Reply = ();

    async fn handle(
        &mut self,
        message: RebalanceShardsCallback,
        _ctx: &ActorContext<Self>,
    ) -> Result<Self::Reply, ActorExitStatus> {
        let num_closed_shards = message.closed_shards.len();
        debug!("closing {num_closed_shards} shards after rebalance");

        for closed_shard in message.closed_shards {
            let shard_id = closed_shard.shard_id().clone();
            let source_uid = SourceUid {
                index_uid: closed_shard.index_uid().clone(),
                source_id: closed_shard.source_id,
            };
            self.model.close_shards(&source_uid, &[shard_id]);
        }
        // We drop the rebalance guard explicitly here to put some emphasis on where a the rebalance
        // lock is released.
        drop(message.rebalance_guard);
        Ok(())
    }
}

// -- Maintenance Mode Handlers --

#[async_trait]
impl Handler<EnableMaintenanceModeRequest> for ControlPlane {
    type Reply = ControlPlaneResult<EnableMaintenanceModeResponse>;

    async fn handle(
        &mut self,
        request: EnableMaintenanceModeRequest,
        _ctx: &ActorContext<Self>,
    ) -> Result<Self::Reply, ActorExitStatus> {
        self.handle_enable_maintenance(request).await
    }
}

#[async_trait]
impl Handler<DisableMaintenanceModeRequest> for ControlPlane {
    type Reply = ControlPlaneResult<DisableMaintenanceModeResponse>;

    async fn handle(
        &mut self,
        _request: DisableMaintenanceModeRequest,
        _ctx: &ActorContext<Self>,
    ) -> Result<Self::Reply, ActorExitStatus> {
        self.handle_disable_maintenance().await
    }
}

#[async_trait]
impl Handler<GetMaintenanceModeRequest> for ControlPlane {
    type Reply = ControlPlaneResult<GetMaintenanceModeResponse>;

    async fn handle(
        &mut self,
        _request: GetMaintenanceModeRequest,
        _ctx: &ActorContext<Self>,
    ) -> Result<Self::Reply, ActorExitStatus> {
        self.handle_get_maintenance()
    }
}

impl ControlPlane {
    async fn handle_enable_maintenance(
        &mut self,
        _request: EnableMaintenanceModeRequest,
    ) -> Result<ControlPlaneResult<EnableMaintenanceModeResponse>, ActorExitStatus> {
        if self.maintenance.is_active() {
            return Ok(Err(ControlPlaneError::Internal(
                "maintenance mode is already enabled".to_string(),
            )));
        }

        // Freeze the current plan.
        let frozen_plan = self
            .indexing_scheduler
            .observable_state()
            .current_targeted_physical_plan
            .unwrap_or_else(|| crate::indexing_plan::PhysicalIndexingPlan::with_indexer_ids(&[]));

        let frozen_plan_json = match serialize_frozen_plan(&frozen_plan) {
            Ok(json) => json,
            Err(err) => {
                return Ok(Err(ControlPlaneError::Internal(format!(
                    "failed to serialize frozen plan: {err}"
                ))));
            }
        };

        // Build the metadata (with RFC 3339 datetime).
        let metadata = crate::maintenance::MaintenanceModeMetadata::new_now();

        // Persist to durable storage BEFORE enabling in-memory state.
        // This ensures that on restart, the control plane will find the persisted state
        // even if it crashes right after this point.
        if let Err(err) = self
            .maintenance_persistence
            .save(&metadata, &frozen_plan)
            .await
        {
            return Ok(Err(ControlPlaneError::Internal(format!(
                "failed to persist maintenance state: {err}"
            ))));
        }

        // Only now enable in-memory state (persistence succeeded).
        self.maintenance.load_from_metadata(metadata);
        crate::metrics::CONTROL_PLANE_METRICS
            .maintenance_mode
            .set(1);

        info!(
            num_indexers = frozen_plan.num_indexers(),
            "maintenance mode enabled: indexing plan frozen"
        );

        Ok(Ok(EnableMaintenanceModeResponse { frozen_plan_json }))
    }

    async fn handle_disable_maintenance(
        &mut self,
    ) -> Result<ControlPlaneResult<DisableMaintenanceModeResponse>, ActorExitStatus> {
        if !self.maintenance.is_active() {
            return Ok(Err(ControlPlaneError::Internal(
                "maintenance mode is not currently enabled".to_string(),
            )));
        }

        // Clear persisted state BEFORE disabling in-memory.
        // This ensures that on restart, the control plane will NOT reload maintenance mode
        // even if it crashes right after this point.
        if let Err(err) = self.maintenance_persistence.clear().await {
            return Ok(Err(ControlPlaneError::Internal(format!(
                "failed to clear persisted maintenance state: {err}"
            ))));
        }

        // Only now disable in-memory state (persistence clear succeeded).
        self.maintenance.disable();
        crate::metrics::CONTROL_PLANE_METRICS
            .maintenance_mode
            .set(0);

        // Trigger a full plan rebuild to reconcile the cluster.
        info!("maintenance mode disabled: triggering full indexing plan rebuild");
        self.indexing_scheduler.rebuild_plan(&self.model, false);

        Ok(Ok(DisableMaintenanceModeResponse {}))
    }

    fn handle_get_maintenance(
        &self,
    ) -> Result<ControlPlaneResult<GetMaintenanceModeResponse>, ActorExitStatus> {
        let is_maintenance_mode = self.maintenance.is_active();
        let enabled_at = self.maintenance.enabled_at();

        Ok(Ok(GetMaintenanceModeResponse {
            is_maintenance_mode,
            enabled_at,
        }))
    }
}

fn spawn_watch_indexers_task(
    weak_mailbox: WeakMailbox<ControlPlane>,
    cluster_change_stream: ClusterChangeStream,
) {
    tokio::spawn(watcher_indexers(weak_mailbox, cluster_change_stream));
}

async fn watcher_indexers(
    weak_mailbox: WeakMailbox<ControlPlane>,
    mut cluster_change_stream: ClusterChangeStream,
) {
    while let Some(cluster_change) = cluster_change_stream.next().await {
        let Some(mailbox) = weak_mailbox.upgrade() else {
            return;
        };
        match cluster_change {
            ClusterChange::Add(node) => {
                if node.is_indexer()
                    && let Err(error) = mailbox.send_message(IndexerJoined(node)).await
                {
                    error!(%error, "failed to forward `IndexerJoined` event to control plane");
                }
            }
            ClusterChange::Remove(node) => {
                if node.is_indexer()
                    && let Err(error) = mailbox.send_message(IndexerLeft(node)).await
                {
                    error!(%error, "failed to forward `IndexerLeft` event to control plane");
                }
            }
            ClusterChange::Update(_) => {
                // We are not interested in updates (yet).
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::num::NonZero;
    use std::sync::Arc;

    use futures::FutureExt;
    use mockall::Sequence;
    use quickwit_actors::{AskError, Observe, SupervisorMetrics};
    use quickwit_cluster::ClusterChangeStreamFactoryForTest;
    use quickwit_common::test_utils::wait_until_predicate;
    use quickwit_config::{
        CLI_SOURCE_ID, INGEST_V2_SOURCE_ID, IndexConfig, KafkaSourceParams, SourceParams,
    };
    use quickwit_indexing::IndexingService;
    use quickwit_metastore::{
        CreateIndexRequestExt, IndexMetadata, ListIndexesMetadataResponseExt,
    };
    use quickwit_proto::control_plane::{
        GetOrCreateOpenShardsFailureReason, GetOrCreateOpenShardsSubrequest,
        SwapIndexingPipelinesEntry,
    };
    use quickwit_proto::indexing::{
        ApplyIndexingPlanRequest, ApplyIndexingPlanResponse, CpuCapacity, IndexingServiceClient,
        MockIndexingService,
    };
    use quickwit_proto::ingest::ingester::{
        IngesterServiceClient, InitShardSuccess, InitShardsResponse, MockIngesterService,
        RetainShardsResponse,
    };
    use quickwit_proto::ingest::{Shard, ShardPKey, ShardState};
    use quickwit_proto::metastore::{
        DeleteShardsResponse, EmptyResponse, EntityKind, FindIndexTemplateMatchesResponse,
        GetKvResponse, ListIndexesMetadataRequest, ListIndexesMetadataResponse, ListShardsRequest,
        ListShardsResponse, ListShardsSubresponse, MetastoreError, MockMetastoreService,
        OpenShardSubresponse, OpenShardsResponse, SourceType,
    };
    use quickwit_proto::types::{DocMappingUid, Position};
    use tokio::sync::Mutex;

    use super::*;
    use crate::IndexerNodeInfo;
    use crate::indexing_plan::PhysicalIndexingPlan;
    use crate::maintenance::MetastoreKvPersistence;

    fn setup_disabled_maintenance(mock_metastore: &mut MockMetastoreService) {
        mock_metastore
            .expect_get_kv()
            .returning(|_| Ok(GetKvResponse { value: None }));
    }

    fn setup_maintenance_enable(mock_metastore: &mut MockMetastoreService) {
        mock_metastore
            .expect_get_kv()
            .return_once(|_| Ok(GetKvResponse { value: None }));
        mock_metastore
            .expect_set_kv()
            .return_once(|_| Ok(EmptyResponse {}));
    }

    async fn observe_current_plan(
        control_plane_handle: &ActorHandle<Supervisor<ControlPlane>>,
    ) -> Option<PhysicalIndexingPlan> {
        control_plane_handle
            .observe()
            .await
            .state_opt
            .as_ref()?
            .indexing_scheduler
            .current_targeted_physical_plan
            .clone()
    }

    #[must_use]
    fn add_test_indexer_with_mailbox(
        universe: &Universe,
        indexer_pool: &IndexerPool,
        node_id: NodeId,
    ) -> quickwit_actors::Inbox<IndexingService> {
        let (client_mailbox, client_inbox) = universe.create_test_mailbox();
        let client = IndexingServiceClient::from_mailbox::<IndexingService>(client_mailbox);
        let indexer_info = IndexerNodeInfo {
            node_id: node_id.clone(),
            generation_id: 0,
            client,
            indexing_tasks: Vec::new(),
            indexing_capacity: CpuCapacity::from_cpu_millis(4_000),
        };
        indexer_pool.insert(node_id, indexer_info);
        client_inbox
    }

    #[tokio::test]
    async fn test_maintenance_mode_allows_create_index_without_rebuild() {
        let universe = Universe::with_accelerated_time();

        let indexer_pool = IndexerPool::default();

        // Add one indexer to the pool
        let node_1: NodeId = NodeId::from_str("test-node-1");
        let _indexing_inbox_1 =
            add_test_indexer_with_mailbox(&universe, &indexer_pool, node_1.clone());

        let ingester_pool = IngesterPool::default();

        let index_uid: IndexUid = IndexUid::for_test("test-index", 0);
        let index_uid_clone = index_uid.clone();
        let mut mock_metastore = MockMetastoreService::new();
        setup_maintenance_enable(&mut mock_metastore);
        mock_metastore
            .expect_list_indexes_metadata()
            .returning(|_| Ok(ListIndexesMetadataResponse::for_test(Vec::new())));
        mock_metastore
            .expect_create_index()
            .return_once(move |req| {
                // re-serialize the received requested config
                let index_config = req.deserialize_index_config().unwrap();
                let source_configs = req.deserialize_source_configs().unwrap();
                let mut index_metadata = IndexMetadata::new(index_config);
                index_metadata.index_uid = index_uid_clone.clone();
                for source_config in source_configs {
                    index_metadata.add_source(source_config).unwrap();
                }
                let index_metadata_json = serde_json::to_string(&index_metadata).unwrap();
                Ok(CreateIndexResponse {
                    index_uid: Some(index_uid_clone),
                    index_metadata_json,
                })
            });

        let cluster_config = ClusterConfig::for_test();
        let cluster_change_stream_factory = ClusterChangeStreamFactoryForTest::default();
        let (control_plane_mailbox, control_plane_handle, _readiness_rx) = ControlPlane::spawn(
            &universe,
            cluster_config,
            node_1.clone(),
            cluster_change_stream_factory,
            indexer_pool.clone(),
            ingester_pool,
            MetastoreServiceClient::from_mock(mock_metastore),
        );

        // Wait for a first (empty) plan to be calculated.
        wait_until_predicate(
            || observe_current_plan(&control_plane_handle).map(|plan| plan.is_some()),
            Duration::from_secs(5),
            Duration::from_millis(100),
        )
        .await
        .unwrap();

        // Enable maintenance mode.
        control_plane_mailbox
            .ask(EnableMaintenanceModeRequest {})
            .await
            .unwrap()
            .unwrap();

        let original_physical_plan = observe_current_plan(&control_plane_handle).await;

        // Create index in maintenance mode
        let index_config = IndexConfig::for_test("test-index", "ram:///test-index");
        let kafka_source = SourceConfig::for_test(
            "kafka-source",
            SourceParams::Kafka(KafkaSourceParams {
                topic: "test-topic".to_string(),
                client_log_level: None,
                enable_backfill_mode: false,
                client_params: json!({}),
            }),
        );
        let create_index_request =
            CreateIndexRequest::try_from_index_and_source_configs(&index_config, &[kafka_source])
                .unwrap();
        let create_result = control_plane_mailbox
            .ask_for_res(create_index_request)
            .await;
        assert!(create_result.is_ok());
        assert_eq!(create_result.unwrap().index_uid(), &index_uid);
        // Check that plan rebuild is skipped
        universe.sleep(Duration::from_secs(60)).await;
        assert_eq!(
            original_physical_plan,
            observe_current_plan(&control_plane_handle).await,
            "physical plan should not change after creating index in maintenance mode"
        );

        // Add another node
        let node_2: NodeId = NodeId::from_str("test-node-2");
        let _indexing_inbox_2 =
            add_test_indexer_with_mailbox(&universe, &indexer_pool, node_2.clone());
        // Check that the rebuild is still skipped
        universe.sleep(Duration::from_secs(60)).await;
        assert_eq!(
            original_physical_plan,
            observe_current_plan(&control_plane_handle).await,
            "physical plan should not change after adding new node in maintenance mode"
        );

        universe.assert_quit().await;
    }

    #[tokio::test]
    async fn test_maintenance_mode_allows_delete_index() {
        let universe = Universe::with_accelerated_time();
        let self_node_id: NodeId = NodeId::from_str("test-node");
        let indexer_pool = IndexerPool::default();
        let ingester_pool = IngesterPool::default();

        let mut mock_metastore = MockMetastoreService::new();
        setup_maintenance_enable(&mut mock_metastore);
        mock_metastore
            .expect_list_indexes_metadata()
            .returning(|_| Ok(ListIndexesMetadataResponse::for_test(Vec::new())));
        mock_metastore
            .expect_delete_index()
            .return_once(|_| Ok(EmptyResponse {}));

        let cluster_config = ClusterConfig::for_test();
        let cluster_change_stream_factory = ClusterChangeStreamFactoryForTest::default();
        let (control_plane_mailbox, _control_plane_handle, _readiness_rx) = ControlPlane::spawn(
            &universe,
            cluster_config,
            self_node_id,
            cluster_change_stream_factory,
            indexer_pool,
            ingester_pool,
            MetastoreServiceClient::from_mock(mock_metastore),
        );

        // Enable maintenance mode.
        control_plane_mailbox
            .ask(EnableMaintenanceModeRequest {})
            .await
            .unwrap()
            .unwrap();

        // Delete index in maintenance mode — should succeed, but plan rebuild is skipped.
        let index_uid = IndexUid::for_test("test-index", 0);
        let delete_index_request = DeleteIndexRequest {
            index_uid: Some(index_uid),
        };
        let delete_result = control_plane_mailbox
            .ask(delete_index_request)
            .await
            .unwrap();
        assert!(delete_result.is_ok());

        universe.assert_quit().await;
    }

    #[tokio::test]
    async fn test_maintenance_mode_allows_add_source() {
        let universe = Universe::with_accelerated_time();
        let self_node_id: NodeId = NodeId::from_str("test-node");
        let indexer_pool = IndexerPool::default();
        let ingester_pool = IngesterPool::default();

        // Pre-load an index with an enabled ingest_v2 source so that
        // `create_or_enable_ingest_v2_sources_if_necessary` does not call `add_source` on
        // startup and consume the mock expectation meant for the test's own call.
        let mut index_metadata = IndexMetadata::for_test("test-index", "ram:///test-index");
        let mut ingest_v2_source = SourceConfig::ingest_v2();
        ingest_v2_source.enabled = true;
        index_metadata.add_source(ingest_v2_source).unwrap();
        let mut mock_metastore = MockMetastoreService::new();
        setup_maintenance_enable(&mut mock_metastore);
        mock_metastore
            .expect_list_indexes_metadata()
            .return_once(move |_| Ok(ListIndexesMetadataResponse::for_test(vec![index_metadata])));
        mock_metastore
            .expect_list_shards()
            .return_once(|_| Ok(ListShardsResponse::default()));
        mock_metastore
            .expect_add_source()
            .return_once(|_| Ok(EmptyResponse {}));

        let cluster_config = ClusterConfig::for_test();
        let cluster_change_stream_factory = ClusterChangeStreamFactoryForTest::default();
        let (control_plane_mailbox, _control_plane_handle, _readiness_rx) = ControlPlane::spawn(
            &universe,
            cluster_config,
            self_node_id,
            cluster_change_stream_factory,
            indexer_pool,
            ingester_pool,
            MetastoreServiceClient::from_mock(mock_metastore),
        );

        // Enable maintenance mode.
        control_plane_mailbox
            .ask(EnableMaintenanceModeRequest {})
            .await
            .unwrap()
            .unwrap();

        // Add source in maintenance mode — should succeed, but plan rebuild is skipped.
        let index_uid = IndexUid::for_test("test-index", 0);
        let source_config = SourceConfig::for_test("test-source", SourceParams::void());
        let add_source_request = AddSourceRequest {
            index_uid: Some(index_uid),
            source_config_json: serde_json::to_string(&source_config).unwrap(),
        };
        let result = control_plane_mailbox.ask(add_source_request).await.unwrap();
        assert!(result.is_ok());

        universe.assert_quit().await;
    }

    #[tokio::test]
    async fn test_maintenance_mode_enable_disable_cycle() {
        let universe = Universe::with_accelerated_time();
        let self_node_id: NodeId = NodeId::from_str("test-node");
        let indexer_pool = IndexerPool::default();
        let ingester_pool = IngesterPool::default();

        let mut mock_metastore = MockMetastoreService::new();
        mock_metastore
            .expect_get_kv()
            .returning(|_| Ok(GetKvResponse { value: None }));
        mock_metastore
            .expect_set_kv()
            .returning(|_| Ok(EmptyResponse {}));
        mock_metastore
            .expect_delete_kv()
            .returning(|_| Ok(EmptyResponse {}));
        mock_metastore
            .expect_list_indexes_metadata()
            .returning(|_| Ok(ListIndexesMetadataResponse::for_test(Vec::new())));

        let cluster_config = ClusterConfig::for_test();
        let cluster_change_stream_factory = ClusterChangeStreamFactoryForTest::default();
        let (control_plane_mailbox, _control_plane_handle, _readiness_rx) = ControlPlane::spawn(
            &universe,
            cluster_config,
            self_node_id,
            cluster_change_stream_factory,
            indexer_pool,
            ingester_pool,
            MetastoreServiceClient::from_mock(mock_metastore),
        );

        // Initially not in maintenance mode.
        let status = control_plane_mailbox
            .ask(GetMaintenanceModeRequest {})
            .await
            .unwrap()
            .unwrap();
        assert!(!status.is_maintenance_mode);

        // Enable.
        let enable_resp = control_plane_mailbox
            .ask(EnableMaintenanceModeRequest {})
            .await
            .unwrap()
            .unwrap();
        assert!(!enable_resp.frozen_plan_json.is_empty());

        // Check status.
        let status = control_plane_mailbox
            .ask(GetMaintenanceModeRequest {})
            .await
            .unwrap()
            .unwrap();
        assert!(status.is_maintenance_mode);
        assert!(status.enabled_at.is_some());

        // Enable again — should fail.
        let double_enable = control_plane_mailbox
            .ask(EnableMaintenanceModeRequest {})
            .await
            .unwrap();
        assert!(double_enable.is_err());

        // Disable.
        let disable_resp = control_plane_mailbox
            .ask(DisableMaintenanceModeRequest {})
            .await
            .unwrap();
        assert!(disable_resp.is_ok());

        // Check status again.
        let status = control_plane_mailbox
            .ask(GetMaintenanceModeRequest {})
            .await
            .unwrap()
            .unwrap();
        assert!(!status.is_maintenance_mode);

        // Disable again — should fail.
        let double_disable = control_plane_mailbox
            .ask(DisableMaintenanceModeRequest {})
            .await
            .unwrap();
        assert!(double_disable.is_err());

        universe.assert_quit().await;
    }

    #[tokio::test]
    async fn test_maintenance_mode_observable_state() {
        let universe = Universe::with_accelerated_time();
        let self_node_id: NodeId = NodeId::from_str("test-node");
        let indexer_pool = IndexerPool::default();
        let ingester_pool = IngesterPool::default();

        let mut mock_metastore = MockMetastoreService::new();
        setup_maintenance_enable(&mut mock_metastore);
        mock_metastore
            .expect_list_indexes_metadata()
            .returning(|_| Ok(ListIndexesMetadataResponse::for_test(Vec::new())));

        let cluster_config = ClusterConfig::for_test();
        let cluster_change_stream_factory = ClusterChangeStreamFactoryForTest::default();
        let (control_plane_mailbox, control_plane_handle, _readiness_rx) = ControlPlane::spawn(
            &universe,
            cluster_config,
            self_node_id,
            cluster_change_stream_factory,
            indexer_pool,
            ingester_pool,
            MetastoreServiceClient::from_mock(mock_metastore),
        );

        // Observe initial state.
        let obs = control_plane_handle.process_pending_and_observe().await;
        let state = obs.state_opt.as_ref().unwrap();
        assert!(!state.maintenance_mode);

        // Enable maintenance mode.
        control_plane_mailbox
            .ask(EnableMaintenanceModeRequest {})
            .await
            .unwrap()
            .unwrap();

        // Give the supervisor time to observe the inner actor's updated state.
        universe.sleep(Duration::from_secs(1)).await;

        let obs = control_plane_handle.process_pending_and_observe().await;
        let state = obs.state_opt.as_ref().unwrap();
        assert!(state.maintenance_mode);

        universe.assert_quit().await;
    }

    #[tokio::test]
    async fn test_maintenance_mode_allows_toggle_source() {
        let universe = Universe::with_accelerated_time();
        let self_node_id: NodeId = NodeId::from_str("test-node");
        let indexer_pool = IndexerPool::default();
        let ingester_pool = IngesterPool::default();

        // Pre-load an index with the test source and an enabled ingest_v2 source so that
        // `create_or_enable_ingest_v2_sources_if_necessary` does not call `add_source` on
        // startup and trigger unexpected mock calls.
        let mut index_metadata = IndexMetadata::for_test("test-index", "ram:///test-index");
        let test_source_config = SourceConfig::for_test("test-source", SourceParams::void());
        index_metadata.add_source(test_source_config).unwrap();
        let mut ingest_v2_source = SourceConfig::ingest_v2();
        ingest_v2_source.enabled = true;
        index_metadata.add_source(ingest_v2_source).unwrap();

        let mut mock_metastore = MockMetastoreService::new();
        setup_maintenance_enable(&mut mock_metastore);
        mock_metastore
            .expect_list_indexes_metadata()
            .return_once(move |_| Ok(ListIndexesMetadataResponse::for_test(vec![index_metadata])));
        mock_metastore
            .expect_list_shards()
            .return_once(|_| Ok(ListShardsResponse::default()));
        mock_metastore
            .expect_toggle_source()
            .return_once(|_| Ok(EmptyResponse {}));

        let cluster_config = ClusterConfig::for_test();
        let cluster_change_stream_factory = ClusterChangeStreamFactoryForTest::default();
        let (control_plane_mailbox, _control_plane_handle, _readiness_rx) = ControlPlane::spawn(
            &universe,
            cluster_config,
            self_node_id,
            cluster_change_stream_factory,
            indexer_pool,
            ingester_pool,
            MetastoreServiceClient::from_mock(mock_metastore),
        );

        // Enable maintenance mode.
        control_plane_mailbox
            .ask(EnableMaintenanceModeRequest {})
            .await
            .unwrap()
            .unwrap();

        // Toggle source in maintenance mode — should succeed, but plan rebuild is skipped.
        let index_uid = IndexUid::for_test("test-index", 0);
        let toggle_request = ToggleSourceRequest {
            index_uid: Some(index_uid),
            source_id: "test-source".to_string(),
            enable: false,
        };
        let result = control_plane_mailbox.ask(toggle_request).await.unwrap();
        assert!(result.is_ok());

        universe.assert_quit().await;
    }

    #[tokio::test]
    async fn test_maintenance_mode_allows_get_or_create_open_shards() {
        // In maintenance mode, GetOrCreateOpenShards should still work for existing sources
        // (ingest must continue), but auto_create_indexes is skipped.
        let universe = Universe::with_accelerated_time();
        let self_node_id: NodeId = NodeId::from_str("test-node");
        let indexer_pool = IndexerPool::default();
        let ingester_pool = IngesterPool::default();

        let mut mock_metastore = MockMetastoreService::new();
        setup_maintenance_enable(&mut mock_metastore);
        mock_metastore
            .expect_list_indexes_metadata()
            .returning(|_| Ok(ListIndexesMetadataResponse::for_test(Vec::new())));
        // Note: no expect_find_index_template_matches — if auto_create was NOT skipped,
        // this would panic due to unexpected call.

        let cluster_config = ClusterConfig::for_test();
        let cluster_change_stream_factory = ClusterChangeStreamFactoryForTest::default();
        let (control_plane_mailbox, _control_plane_handle, _readiness_rx) = ControlPlane::spawn(
            &universe,
            cluster_config,
            self_node_id,
            cluster_change_stream_factory,
            indexer_pool,
            ingester_pool,
            MetastoreServiceClient::from_mock(mock_metastore),
        );

        // Enable maintenance mode.
        control_plane_mailbox
            .ask(EnableMaintenanceModeRequest {})
            .await
            .unwrap()
            .unwrap();

        // Send a GetOrCreateOpenShards with a nonexistent index.
        // In maintenance, auto_create is skipped, so the index won't be found.
        // The ingest controller will report a failure for unknown indexes, which is expected.
        let request = GetOrCreateOpenShardsRequest {
            subrequests: vec![GetOrCreateOpenShardsSubrequest {
                subrequest_id: 0,
                index_id: "nonexistent-index".to_string(),
                source_id: "source".to_string(),
            }],
            closed_shards: Vec::new(),
            unavailable_leaders: Vec::new(),
        };
        let result = control_plane_mailbox.ask(request).await.unwrap();
        // The request should succeed at the handler level.
        // It may fail internally because the index doesn't exist, but that's expected.
        match result {
            Ok(response) => {
                // The response should contain a failure for the unknown index.
                assert!(!response.failures.is_empty());
                assert_eq!(
                    response.failures[0].reason(),
                    GetOrCreateOpenShardsFailureReason::IndexNotFound
                );
            }
            Err(_err) => {
                // Any internal error is acceptable here (index not found, etc.).
            }
        }

        universe.assert_quit().await;
    }

    #[tokio::test]
    async fn test_control_plane_create_index() {
        let universe = Universe::with_accelerated_time();
        let self_node_id: NodeId = NodeId::from_str("test-node");
        let indexer_pool = IndexerPool::default();
        let ingester_pool = IngesterPool::default();

        let mut mock_metastore = MockMetastoreService::new();
        setup_disabled_maintenance(&mut mock_metastore);
        let index_uid: IndexUid = IndexUid::for_test("test-index", 0);
        let index_uid_clone = index_uid.clone();
        mock_metastore
            .expect_create_index()
            .withf(|create_index_request| {
                let index_config: IndexConfig =
                    create_index_request.deserialize_index_config().unwrap();
                assert_eq!(index_config.index_id, "test-index");
                assert_eq!(index_config.index_uri, "ram:///test-index");
                true
            })
            .returning(move |_| {
                let index_metadata = IndexMetadata::for_test("test-index", "ram:///test-index");
                let index_metadata_json = serde_json::to_string(&index_metadata).unwrap();
                let response = CreateIndexResponse {
                    index_uid: Some(index_uid_clone.clone()),
                    index_metadata_json,
                };
                Ok(response)
            });
        mock_metastore
            .expect_list_indexes_metadata()
            .returning(|_| Ok(ListIndexesMetadataResponse::for_test(Vec::new())));
        let cluster_config = ClusterConfig::for_test();
        let cluster_change_stream_factory = ClusterChangeStreamFactoryForTest::default();
        let (control_plane_mailbox, _control_plane_handle, _readiness_rx) = ControlPlane::spawn(
            &universe,
            cluster_config,
            self_node_id,
            cluster_change_stream_factory,
            indexer_pool,
            ingester_pool,
            MetastoreServiceClient::from_mock(mock_metastore),
        );
        let index_config = IndexConfig::for_test("test-index", "ram:///test-index");
        let create_index_request =
            CreateIndexRequest::try_from_index_config(&index_config).unwrap();
        let create_index_response = control_plane_mailbox
            .ask_for_res(create_index_request)
            .await
            .unwrap();
        assert_eq!(create_index_response.index_uid(), &index_uid);

        // TODO: Test that create index event is properly sent to ingest controller.

        universe.assert_quit().await;
    }

    #[tokio::test]
    async fn test_control_plane_delete_index() {
        let universe = Universe::with_accelerated_time();
        let self_node_id: NodeId = NodeId::from_str("test-node");
        let indexer_pool = IndexerPool::default();
        let ingester_pool = IngesterPool::default();

        let index_uid: IndexUid = IndexUid::for_test("test-index", 0);
        let mut mock_metastore = MockMetastoreService::new();
        setup_disabled_maintenance(&mut mock_metastore);
        let index_uid_clone = index_uid.clone();
        mock_metastore
            .expect_delete_index()
            .withf(move |delete_index_request| delete_index_request.index_uid() == &index_uid_clone)
            .returning(|_| Ok(EmptyResponse {}));
        mock_metastore
            .expect_list_indexes_metadata()
            .returning(|_| Ok(ListIndexesMetadataResponse::for_test(Vec::new())));

        let cluster_config = ClusterConfig::for_test();
        let cluster_change_stream_factory = ClusterChangeStreamFactoryForTest::default();
        let (control_plane_mailbox, _control_plane_handle, _readiness_rx) = ControlPlane::spawn(
            &universe,
            cluster_config,
            self_node_id,
            cluster_change_stream_factory,
            indexer_pool,
            ingester_pool,
            MetastoreServiceClient::from_mock(mock_metastore),
        );
        let delete_index_request = DeleteIndexRequest {
            index_uid: Some(index_uid),
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
        let self_node_id: NodeId = NodeId::from_str("test-node");
        let indexer_pool = IndexerPool::default();
        let ingester_pool = IngesterPool::default();

        let mut index_metadata = IndexMetadata::for_test("test-index", "ram://test");
        index_metadata
            .add_source(SourceConfig::ingest_v2())
            .unwrap();

        let mut mock_metastore = MockMetastoreService::new();
        setup_disabled_maintenance(&mut mock_metastore);
        mock_metastore
            .expect_add_source()
            .withf(|add_source_request| {
                let source_config: SourceConfig =
                    serde_json::from_str(&add_source_request.source_config_json).unwrap();
                assert_eq!(source_config.source_id, "test-source");
                assert_eq!(source_config.source_type(), SourceType::Void);
                true
            })
            .return_once(|_| Ok(EmptyResponse {}));
        // the list_indexes_metadata and list_shards calls are made when the control plane starts
        mock_metastore
            .expect_list_indexes_metadata()
            .return_once(move |_| {
                Ok(ListIndexesMetadataResponse::for_test(vec![
                    index_metadata.clone(),
                ]))
            });
        mock_metastore
            .expect_list_shards()
            .return_once(move |_| Ok(ListShardsResponse::default()));

        let cluster_config = ClusterConfig::for_test();
        let cluster_change_stream_factory = ClusterChangeStreamFactoryForTest::default();
        let (control_plane_mailbox, _control_plane_handle, _readiness_rx) = ControlPlane::spawn(
            &universe,
            cluster_config,
            self_node_id,
            cluster_change_stream_factory,
            indexer_pool,
            ingester_pool,
            MetastoreServiceClient::from_mock(mock_metastore),
        );
        let index_uid: IndexUid = IndexUid::for_test("test-index", 0);
        let source_config = SourceConfig::for_test("test-source", SourceParams::void());
        let add_source_request = AddSourceRequest {
            index_uid: Some(index_uid),
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
    async fn test_control_plane_update_source() {
        let universe = Universe::with_accelerated_time();
        let pipelines_after_update = 3;
        let self_node_id: NodeId = NodeId::from_str("test-node");
        let indexer_pool = IndexerPool::default();
        let mut mock_indexer = MockIndexingService::new();
        // call when starting the cp
        mock_indexer
            .expect_apply_indexing_plan()
            .withf(|request| request.indexing_tasks.len() == 1)
            .return_once(|_| Ok(ApplyIndexingPlanResponse {}));
        // call after the update (3 tasks because 3 pipelines)
        mock_indexer
            .expect_apply_indexing_plan()
            .withf(move |request| request.indexing_tasks.len() == pipelines_after_update)
            .return_once(|_| Ok(ApplyIndexingPlanResponse {}));
        let indexer = IndexingServiceClient::from_mock(mock_indexer);
        let indexer_info = IndexerNodeInfo {
            node_id: self_node_id.clone(),
            generation_id: 0,
            client: indexer,
            indexing_tasks: Vec::new(),
            indexing_capacity: CpuCapacity::from_cpu_millis(1_000),
        };
        indexer_pool.insert(self_node_id.clone(), indexer_info);

        let ingester_pool = IngesterPool::default();

        let mut index_metadata = IndexMetadata::for_test("test-index", "ram://tata");
        index_metadata
            .add_source(SourceConfig::ingest_v2())
            .unwrap();

        let mut test_source_config = SourceConfig::for_test(
            "test-source",
            SourceParams::Kafka(KafkaSourceParams {
                topic: "test-topic".to_string(),
                client_log_level: None,
                enable_backfill_mode: false,
                client_params: json!({}),
            }),
        );
        index_metadata
            .add_source(test_source_config.clone())
            .unwrap();

        let mut mock_metastore = MockMetastoreService::new();
        setup_disabled_maintenance(&mut mock_metastore);
        mock_metastore
            .expect_update_source()
            .withf(move |update_source_request| {
                let source_config: SourceConfig =
                    serde_json::from_str(&update_source_request.source_config_json).unwrap();
                assert_eq!(source_config.source_id, "test-source");
                assert_eq!(source_config.source_type(), SourceType::Kafka);
                assert_eq!(
                    source_config.num_pipelines,
                    NonZero::new(pipelines_after_update).unwrap()
                );
                true
            })
            .return_once(|_| Ok(EmptyResponse {}));
        // the list_indexes_metadata and list_shards calls are made when the control plane starts
        mock_metastore
            .expect_list_indexes_metadata()
            .return_once(move |_| {
                Ok(ListIndexesMetadataResponse::for_test(vec![
                    index_metadata.clone(),
                ]))
            });
        mock_metastore
            .expect_list_shards()
            .return_once(move |_| Ok(ListShardsResponse::default()));

        let cluster_config = ClusterConfig::for_test();
        let cluster_change_stream_factory = ClusterChangeStreamFactoryForTest::default();
        let (control_plane_mailbox, _control_plane_handle, _readiness_rx) = ControlPlane::spawn(
            &universe,
            cluster_config,
            self_node_id,
            cluster_change_stream_factory,
            indexer_pool,
            ingester_pool,
            MetastoreServiceClient::from_mock(mock_metastore),
        );
        let index_uid: IndexUid = IndexUid::for_test("test-index", 0);
        test_source_config.num_pipelines = NonZero::new(pipelines_after_update).unwrap();
        let update_source_request = UpdateSourceRequest {
            index_uid: Some(index_uid),
            source_config_json: serde_json::to_string(&test_source_config).unwrap(),
        };
        control_plane_mailbox
            .ask_for_res(update_source_request)
            .await
            .unwrap();

        universe.assert_quit().await;
    }

    #[tokio::test]
    async fn test_control_plane_toggle_source() {
        let universe = Universe::with_accelerated_time();
        let self_node_id: NodeId = NodeId::from_str("test-node");
        let indexer_pool = IndexerPool::default();
        let ingester_pool = IngesterPool::default();

        let mut index_metadata = IndexMetadata::for_test("test-index", "ram://toto");
        index_metadata
            .add_source(SourceConfig::ingest_v2())
            .unwrap();

        let test_source_config = SourceConfig::for_test("test-source", SourceParams::void());
        index_metadata.add_source(test_source_config).unwrap();

        let mut mock_metastore = MockMetastoreService::new();
        setup_disabled_maintenance(&mut mock_metastore);
        mock_metastore
            .expect_list_indexes_metadata()
            .return_once(|_| Ok(ListIndexesMetadataResponse::for_test(vec![index_metadata])));
        mock_metastore
            .expect_list_shards()
            .return_once(move |_| Ok(ListShardsResponse::default()));

        let index_uid = IndexUid::for_test("test-index", 0);
        let index_uid_clone = index_uid.clone();
        mock_metastore
            .expect_toggle_source()
            .times(1)
            .return_once(move |toggle_source_request| {
                assert_eq!(toggle_source_request.index_uid(), &index_uid_clone);
                assert_eq!(toggle_source_request.source_id, "test-source");
                Ok(EmptyResponse {})
            });
        let index_uid_clone = index_uid.clone();
        mock_metastore
            .expect_toggle_source()
            .times(1)
            .return_once(move |toggle_source_request| {
                assert_eq!(toggle_source_request.index_uid(), &index_uid_clone);
                assert_eq!(toggle_source_request.source_id, "test-source");
                assert!(!toggle_source_request.enable);
                Ok(EmptyResponse {})
            });

        let cluster_config = ClusterConfig::for_test();
        let cluster_change_stream_factory = ClusterChangeStreamFactoryForTest::default();
        let (control_plane_mailbox, _control_plane_handle, _readiness_rx) = ControlPlane::spawn(
            &universe,
            cluster_config,
            self_node_id,
            cluster_change_stream_factory,
            indexer_pool,
            ingester_pool,
            MetastoreServiceClient::from_mock(mock_metastore),
        );
        let enable_source_request = ToggleSourceRequest {
            index_uid: Some(index_uid.clone()),
            source_id: "test-source".to_string(),
            enable: true,
        };
        control_plane_mailbox
            .ask_for_res(enable_source_request)
            .await
            .unwrap();

        let disable_source_request = ToggleSourceRequest {
            index_uid: Some(index_uid),
            source_id: "test-source".to_string(),
            enable: false,
        };
        control_plane_mailbox
            .ask_for_res(disable_source_request)
            .await
            .unwrap();

        universe.assert_quit().await;
    }

    #[tokio::test]
    async fn test_control_plane_delete_source() {
        let universe = Universe::with_accelerated_time();
        let self_node_id: NodeId = NodeId::from_str("test-node");
        let indexer_pool = IndexerPool::default();
        let ingester_pool = IngesterPool::default();

        let mut mock_metastore = MockMetastoreService::new();
        setup_disabled_maintenance(&mut mock_metastore);
        let index_uid: IndexUid = IndexUid::for_test("test-index", 0);
        let index_uid_clone = index_uid.clone();
        mock_metastore
            .expect_delete_source()
            .withf(move |delete_source_request| {
                assert_eq!(delete_source_request.index_uid(), &index_uid_clone);
                assert_eq!(delete_source_request.source_id, "test-source");
                true
            })
            .returning(|_| Ok(EmptyResponse {}));
        mock_metastore
            .expect_list_indexes_metadata()
            .returning(|_| Ok(ListIndexesMetadataResponse::for_test(Vec::new())));

        let cluster_config = ClusterConfig::for_test();
        let cluster_change_stream_factory = ClusterChangeStreamFactoryForTest::default();
        let (control_plane_mailbox, _control_plane_handle, _readiness_rx) = ControlPlane::spawn(
            &universe,
            cluster_config,
            self_node_id,
            cluster_change_stream_factory,
            indexer_pool,
            ingester_pool,
            MetastoreServiceClient::from_mock(mock_metastore),
        );
        let delete_source_request = DeleteSourceRequest {
            index_uid: Some(index_uid),
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
        let self_node_id: NodeId = NodeId::from_str("test-node");
        let indexer_pool = IndexerPool::default();

        let ingester_pool = IngesterPool::default();

        let mut mock_metastore = MockMetastoreService::new();
        setup_disabled_maintenance(&mut mock_metastore);
        let index_uid: IndexUid = IndexUid::for_test("test-index", 0);
        mock_metastore
            .expect_list_indexes_metadata()
            .returning(|_| {
                let mut index_metadata = IndexMetadata::for_test("test-index", "ram:///test-index");
                let mut source_config = SourceConfig::ingest_v2();
                source_config.enabled = true;
                index_metadata.add_source(source_config).unwrap();
                Ok(ListIndexesMetadataResponse::for_test(vec![index_metadata]))
            });
        let index_uid_clone = index_uid.clone();
        mock_metastore
            .expect_list_shards()
            .returning(move |request| {
                assert_eq!(request.subrequests.len(), 1);

                let subrequest = &request.subrequests[0];
                assert_eq!(subrequest.index_uid(), &index_uid_clone);
                assert_eq!(subrequest.source_id, INGEST_V2_SOURCE_ID);

                let subresponses = vec![ListShardsSubresponse {
                    index_uid: Some(index_uid_clone.clone()),
                    source_id: INGEST_V2_SOURCE_ID.to_string(),
                    shards: vec![Shard {
                        index_uid: Some(index_uid_clone.clone()),
                        source_id: INGEST_V2_SOURCE_ID.to_string(),
                        shard_id: Some(ShardId::from(1)),
                        shard_state: ShardState::Open as i32,
                        ..Default::default()
                    }],
                }];
                let response = ListShardsResponse { subresponses };
                Ok(response)
            });

        let cluster_config = ClusterConfig::for_test();
        let cluster_change_stream_factory = ClusterChangeStreamFactoryForTest::default();
        let (control_plane_mailbox, _control_plane_handle, _readiness_rx) = ControlPlane::spawn(
            &universe,
            cluster_config,
            self_node_id,
            cluster_change_stream_factory,
            indexer_pool,
            ingester_pool,
            MetastoreServiceClient::from_mock(mock_metastore),
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
        assert_eq!(subresponse.index_uid(), &index_uid);
        assert_eq!(subresponse.source_id, INGEST_V2_SOURCE_ID);
        assert_eq!(subresponse.open_shards.len(), 1);
        assert_eq!(subresponse.open_shards[0].shard_id(), ShardId::from(1));

        universe.assert_quit().await;
    }

    #[tokio::test]
    async fn test_control_plane_supervision_reload_from_metastore() {
        let universe = Universe::default();
        let node_id = NodeId::from_str("test_node");
        let indexer_pool = IndexerPool::default();
        let ingester_pool = IngesterPool::default();
        let mut mock_metastore = MockMetastoreService::new();
        setup_disabled_maintenance(&mut mock_metastore);

        let mut index_0 = IndexMetadata::for_test("test-index-0", "ram:///test-index-0");
        let source = SourceConfig::ingest_v2();
        index_0.add_source(source.clone()).unwrap();

        mock_metastore
            .expect_list_indexes_metadata()
            .times(2) // 1 for the first initialization, 1 after the respawn of the control plane.
            .returning(|list_indexes_request: ListIndexesMetadataRequest| {
                assert_eq!(list_indexes_request, ListIndexesMetadataRequest::all());
                Ok(ListIndexesMetadataResponse::for_test(Vec::new()))
            });
        mock_metastore.expect_list_shards().return_once(
            |_list_shards_request: ListShardsRequest| {
                let list_shards_resp = ListShardsResponse {
                    subresponses: Vec::new(),
                };
                Ok(list_shards_resp)
            },
        );
        let index_metadata = IndexMetadata::for_test("test-index", "ram:///test-index");
        let index_metadata_json = serde_json::to_string(&index_metadata).unwrap();

        mock_metastore.expect_create_index().times(1).return_once(
            |_create_index_request: CreateIndexRequest| {
                Ok(CreateIndexResponse {
                    index_uid: index_metadata.index_uid.into(),
                    index_metadata_json,
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

        let cluster_config = ClusterConfig::for_test();
        let cluster_change_stream_factory = ClusterChangeStreamFactoryForTest::default();
        let (control_plane_mailbox, control_plane_handle, mut readiness_rx) = ControlPlane::spawn(
            &universe,
            cluster_config,
            node_id,
            cluster_change_stream_factory,
            indexer_pool,
            ingester_pool,
            MetastoreServiceClient::from_mock(mock_metastore),
        );
        tokio::time::timeout(
            Duration::from_secs(5),
            readiness_rx.wait_for(|readiness| *readiness),
        )
        .await
        .unwrap()
        .unwrap();

        let index_config = IndexConfig::for_test("test-index", "ram:///test-index");
        let create_index_request =
            CreateIndexRequest::try_from_index_config(&index_config).unwrap();

        // A happy path: we simply create the index.
        control_plane_mailbox
            .ask_for_res(create_index_request.clone())
            .await
            .unwrap();

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
        let universe = Universe::with_accelerated_time();
        let node_id = NodeId::from_str("test-control-plane");
        let indexer_pool = IndexerPool::default();
        let client_inbox = add_test_indexer_with_mailbox(
            &universe,
            &indexer_pool,
            NodeId::from_str("test-indexer"),
        );
        let ingester_pool = IngesterPool::default();
        let mut mock_metastore = MockMetastoreService::new();
        setup_disabled_maintenance(&mut mock_metastore);

        let mut index_0 = IndexMetadata::for_test("test-index-0", "ram:///test-index-0");
        let mut source = SourceConfig::ingest_v2();
        source.enabled = true;
        index_0.add_source(source.clone()).unwrap();

        let index_0_clone = index_0.clone();
        mock_metastore.expect_list_indexes_metadata().return_once(
            move |list_indexes_request: ListIndexesMetadataRequest| {
                assert_eq!(list_indexes_request, ListIndexesMetadataRequest::all());
                Ok(ListIndexesMetadataResponse::for_test(vec![
                    index_0_clone.clone(),
                ]))
            },
        );
        let index_uid_clone = index_0.index_uid.clone();
        mock_metastore.expect_delete_shards().return_once(
            move |delete_shards_request: DeleteShardsRequest| {
                assert_eq!(delete_shards_request.index_uid(), &index_uid_clone);
                assert_eq!(delete_shards_request.source_id, INGEST_V2_SOURCE_ID);
                assert_eq!(delete_shards_request.shard_ids, [ShardId::from(17)]);
                assert!(!delete_shards_request.force);

                let response = DeleteShardsResponse {
                    index_uid: delete_shards_request.index_uid,
                    source_id: delete_shards_request.source_id,
                    successes: delete_shards_request.shard_ids,
                    failures: Vec::new(),
                };
                Ok(response)
            },
        );

        let mut shard = Shard {
            index_uid: Some(index_0.index_uid.clone()),
            source_id: INGEST_V2_SOURCE_ID.to_string(),
            shard_id: Some(ShardId::from(17)),
            leader_id: "test-ingester".to_string(),
            publish_position_inclusive: Some(Position::Beginning),
            ..Default::default()
        };
        shard.set_shard_state(ShardState::Open);

        let index_uid_clone = index_0.index_uid.clone();
        mock_metastore.expect_list_shards().return_once(
            move |_list_shards_request: ListShardsRequest| {
                let list_shards_resp = ListShardsResponse {
                    subresponses: vec![ListShardsSubresponse {
                        index_uid: Some(index_uid_clone),
                        source_id: INGEST_V2_SOURCE_ID.to_string(),
                        shards: vec![shard],
                    }],
                };
                Ok(list_shards_resp)
            },
        );

        let cluster_config = ClusterConfig::for_test();
        let cluster_change_stream_factory = ClusterChangeStreamFactoryForTest::default();
        let (control_plane_mailbox, _control_plane_handle, _readiness_rx) = ControlPlane::spawn(
            &universe,
            cluster_config,
            node_id,
            cluster_change_stream_factory,
            indexer_pool,
            ingester_pool,
            MetastoreServiceClient::from_mock(mock_metastore),
        );
        let source_uid = SourceUid {
            index_uid: index_0.index_uid.clone(),
            source_id: INGEST_V2_SOURCE_ID.to_string(),
        };

        // This update should not triggeer anything in the control plane.
        control_plane_mailbox
            .ask(ShardPositionsUpdate {
                source_uid: source_uid.clone(),
                updated_shard_positions: vec![(ShardId::from(17), Position::offset(1_000u64))],
            })
            .await
            .unwrap();

        let control_plane_obs: ControlPlaneObservableState =
            control_plane_mailbox.ask(Observe).await.unwrap();
        let last_applied_physical_plan = control_plane_obs
            .indexing_scheduler
            .current_targeted_physical_plan
            .unwrap();
        let indexing_tasks = last_applied_physical_plan
            .indexing_tasks_per_indexer()
            .get("test-indexer")
            .unwrap();
        assert_eq!(indexing_tasks.len(), 1);
        assert_eq!(indexing_tasks[0].shard_ids, [ShardId::from(17)]);

        let control_plane_debug_info = control_plane_mailbox.ask(GetDebugInfo).await.unwrap();
        let shard = &control_plane_debug_info["shard_table"]
            ["test-index-0:00000000000000000000000000"]["test-ingester"][0];
        assert_eq!(shard["shard_id"], "00000000000000000017");
        assert_eq!(shard["publish_position_inclusive"], "00000000000000001000");

        let _ = client_inbox.drain_for_test();

        universe.sleep(Duration::from_secs(30)).await;
        // This update should trigger the deletion of the shard and a new indexing plan.
        control_plane_mailbox
            .ask(ShardPositionsUpdate {
                source_uid,
                updated_shard_positions: vec![(ShardId::from(17), Position::eof(1_000u64))],
            })
            .await
            .unwrap();

        let control_plane_obs: ControlPlaneObservableState =
            control_plane_mailbox.ask(Observe).await.unwrap();
        let last_applied_physical_plan = control_plane_obs
            .indexing_scheduler
            .current_targeted_physical_plan
            .unwrap();
        let indexing_tasks = last_applied_physical_plan
            .indexing_tasks_per_indexer()
            .get("test-indexer")
            .unwrap();
        assert!(indexing_tasks.is_empty());

        let apply_plan_requests = client_inbox.drain_for_test_typed::<ApplyIndexingPlanRequest>();
        let last_apply_plan_request = apply_plan_requests.last().unwrap();
        assert!(last_apply_plan_request.indexing_tasks.is_empty());

        universe.assert_quit().await;
    }

    #[tokio::test]
    async fn test_fill_shard_table_position_from_metastore_on_startup() {
        let universe = Universe::with_accelerated_time();
        let node_id = NodeId::from_str("test-control-plane");
        let indexer_pool = IndexerPool::default();
        let _indexing_inbox = add_test_indexer_with_mailbox(
            &universe,
            &indexer_pool,
            NodeId::from_str("test-indexer"),
        );
        let ingester_pool = IngesterPool::default();
        let mut mock_metastore = MockMetastoreService::new();
        setup_disabled_maintenance(&mut mock_metastore);

        let mut index_metadata = IndexMetadata::for_test("test-index", "ram:///test-index");
        let mut source_config = SourceConfig::ingest_v2();
        source_config.enabled = true;
        index_metadata.add_source(source_config.clone()).unwrap();

        let index_metadata_clone = index_metadata.clone();
        mock_metastore.expect_list_indexes_metadata().return_once(
            move |list_indexes_request: ListIndexesMetadataRequest| {
                assert_eq!(list_indexes_request, ListIndexesMetadataRequest::all());
                Ok(ListIndexesMetadataResponse::for_test(vec![
                    index_metadata_clone,
                ]))
            },
        );

        let mut shard = Shard {
            index_uid: Some(index_metadata.index_uid.clone()),
            source_id: INGEST_V2_SOURCE_ID.to_string(),
            shard_id: Some(ShardId::from(17)),
            leader_id: "test-ingester".to_string(),
            publish_position_inclusive: Some(Position::Offset(1234u64.into())),
            ..Default::default()
        };
        shard.set_shard_state(ShardState::Open);

        let index_uid_clone = index_metadata.index_uid.clone();
        mock_metastore.expect_list_shards().return_once(
            move |_list_shards_request: ListShardsRequest| {
                let list_shards_resp = ListShardsResponse {
                    subresponses: vec![ListShardsSubresponse {
                        index_uid: Some(index_uid_clone),
                        source_id: INGEST_V2_SOURCE_ID.to_string(),
                        shards: vec![shard],
                    }],
                };
                Ok(list_shards_resp)
            },
        );

        let cluster_config = ClusterConfig::for_test();
        let cluster_change_stream_factory = ClusterChangeStreamFactoryForTest::default();
        let (control_plane_mailbox, _control_plane_handle, _readiness_rx) = ControlPlane::spawn(
            &universe,
            cluster_config,
            node_id,
            cluster_change_stream_factory,
            indexer_pool,
            ingester_pool,
            MetastoreServiceClient::from_mock(mock_metastore),
        );
        let control_plane_debug_info = control_plane_mailbox.ask(GetDebugInfo).await.unwrap();
        let shard = &control_plane_debug_info["shard_table"]
            ["test-index:00000000000000000000000000"]["test-ingester"][0];
        assert_eq!(shard["shard_id"], "00000000000000000017");
        assert_eq!(shard["publish_position_inclusive"], "00000000000000001234");

        universe.assert_quit().await;
    }

    #[tokio::test]
    async fn test_delete_non_existing_shard() {
        quickwit_common::setup_logging_for_tests();
        let universe = Universe::default();
        let node_id = NodeId::from_str("test-control-plane");
        let indexer_pool = IndexerPool::default();
        let _indexing_inbox = add_test_indexer_with_mailbox(
            &universe,
            &indexer_pool,
            NodeId::from_str("test-indexer"),
        );
        let ingester_pool = IngesterPool::default();
        let mut mock_metastore = MockMetastoreService::new();
        setup_disabled_maintenance(&mut mock_metastore);

        let mut index_0 = IndexMetadata::for_test("test-index-0", "ram:///test-index-0");
        let mut source = SourceConfig::ingest_v2();
        source.enabled = true;
        index_0.add_source(source.clone()).unwrap();

        let index_0_clone = index_0.clone();
        mock_metastore.expect_list_indexes_metadata().return_once(
            move |list_indexes_request: ListIndexesMetadataRequest| {
                assert_eq!(list_indexes_request, ListIndexesMetadataRequest::all());
                Ok(ListIndexesMetadataResponse::for_test(vec![
                    index_0_clone.clone(),
                ]))
            },
        );
        let index_uid_clone = index_0.index_uid.clone();
        mock_metastore.expect_delete_shards().return_once(
            move |delete_shards_request: DeleteShardsRequest| {
                assert_eq!(delete_shards_request.index_uid(), &index_uid_clone);
                assert_eq!(delete_shards_request.source_id, INGEST_V2_SOURCE_ID);
                assert_eq!(delete_shards_request.shard_ids, [ShardId::from(17)]);
                assert!(!delete_shards_request.force);

                let response = DeleteShardsResponse {
                    index_uid: delete_shards_request.index_uid,
                    source_id: delete_shards_request.source_id,
                    successes: delete_shards_request.shard_ids,
                    failures: Vec::new(),
                };
                Ok(response)
            },
        );

        let index_uid_clone = index_0.index_uid.clone();
        mock_metastore.expect_list_shards().return_once(
            move |_list_shards_request: ListShardsRequest| {
                let list_shards_resp = ListShardsResponse {
                    subresponses: vec![ListShardsSubresponse {
                        index_uid: Some(index_uid_clone),
                        source_id: INGEST_V2_SOURCE_ID.to_string(),
                        shards: Vec::new(),
                    }],
                };
                Ok(list_shards_resp)
            },
        );

        let cluster_config = ClusterConfig::for_test();
        let cluster_change_stream_factory = ClusterChangeStreamFactoryForTest::default();
        let (control_plane_mailbox, _control_plane_handle, _readiness_rx) = ControlPlane::spawn(
            &universe,
            cluster_config,
            node_id,
            cluster_change_stream_factory,
            indexer_pool,
            ingester_pool,
            MetastoreServiceClient::from_mock(mock_metastore),
        );
        let source_uid = SourceUid {
            index_uid: index_0.index_uid.clone(),
            source_id: INGEST_V2_SOURCE_ID.to_string(),
        };

        // This update should not triggeer anything in the control plane.
        control_plane_mailbox
            .ask(ShardPositionsUpdate {
                source_uid: source_uid.clone(),
                updated_shard_positions: vec![(ShardId::from(17), Position::eof(1_000u64))],
            })
            .await
            .unwrap();

        universe.assert_quit().await;
    }

    #[tokio::test]
    async fn test_delete_index() {
        quickwit_common::setup_logging_for_tests();
        let universe = Universe::default();
        let node_id = NodeId::from_str("test-control-plane");
        let indexer_pool = IndexerPool::default();

        let ingester_pool = IngesterPool::default();
        let mut mock_ingester = MockIngesterService::new();
        let mut seq = Sequence::new();

        let mut index_0 = IndexMetadata::for_test("test-index-0", "ram:///test-index-0");
        let mut source = SourceConfig::ingest_v2();
        source.enabled = true;
        index_0.add_source(source.clone()).unwrap();

        let index_uid_clone = index_0.index_uid.clone();
        let index_0_clone = index_0.clone();

        let mut mock_metastore = MockMetastoreService::new();
        setup_disabled_maintenance(&mut mock_metastore);
        mock_metastore
            .expect_list_indexes_metadata()
            .times(1)
            .in_sequence(&mut seq)
            .returning(move |list_indexes_request: ListIndexesMetadataRequest| {
                assert_eq!(list_indexes_request, ListIndexesMetadataRequest::all());
                Ok(ListIndexesMetadataResponse::for_test(vec![
                    index_0_clone.clone(),
                ]))
            });
        mock_metastore
            .expect_list_shards()
            .times(1)
            .in_sequence(&mut seq)
            .returning(move |_list_shards_request: ListShardsRequest| {
                let list_shards_resp = ListShardsResponse {
                    subresponses: vec![ListShardsSubresponse {
                        index_uid: Some(index_uid_clone.clone()),
                        source_id: INGEST_V2_SOURCE_ID.to_string(),
                        shards: vec![Shard {
                            index_uid: Some(index_uid_clone.clone()),
                            source_id: source.source_id.to_string(),
                            shard_id: Some(ShardId::from(15)),
                            leader_id: "node1".to_string(),
                            follower_id: None,
                            shard_state: ShardState::Open as i32,
                            doc_mapping_uid: Some(DocMappingUid::default()),
                            publish_position_inclusive: None,
                            publish_token: None,
                            update_timestamp: 1724158996,
                        }],
                    }],
                };
                Ok(list_shards_resp)
            });

        mock_ingester
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
                assert_eq!(delete_index_request.index_uid(), &index_uid_clone);
                Ok(EmptyResponse {})
            });
        mock_ingester
            .expect_retain_shards()
            .times(1)
            .in_sequence(&mut seq)
            .returning(|mut request| {
                assert_eq!(request.retain_shards_for_sources.len(), 1);
                let retain_shards_for_source = request.retain_shards_for_sources.pop().unwrap();
                assert!(&retain_shards_for_source.shard_ids.is_empty());
                Ok(RetainShardsResponse {})
            });
        let ingester = IngesterServiceClient::from_mock(mock_ingester);
        ingester_pool.insert(NodeId::from_str("node1"), ingester);

        let cluster_config = ClusterConfig::for_test();
        let cluster_change_stream_factory = ClusterChangeStreamFactoryForTest::default();
        let (control_plane_mailbox, _control_plane_handle, _readiness_rx) = ControlPlane::spawn(
            &universe,
            cluster_config,
            node_id,
            cluster_change_stream_factory,
            indexer_pool,
            ingester_pool,
            MetastoreServiceClient::from_mock(mock_metastore),
        );
        // This update should not trigger anything in the control plane.
        control_plane_mailbox
            .ask(DeleteIndexRequest {
                index_uid: Some(index_0.index_uid),
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
        let node_id = NodeId::from_str("test-control-plane");
        let indexer_pool = IndexerPool::default();

        let ingester_pool = IngesterPool::default();
        let mut mock_ingester = MockIngesterService::new();
        mock_ingester
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
        let ingester = IngesterServiceClient::from_mock(mock_ingester);
        ingester_pool.insert(NodeId::from_str("node1"), ingester);

        let mut index_0 = IndexMetadata::for_test("test-index-0", "ram:///test-index-0");
        let index_uid_clone = index_0.index_uid.clone();

        let mut mock_metastore = MockMetastoreService::new();
        setup_disabled_maintenance(&mut mock_metastore);
        mock_metastore.expect_delete_source().return_once(
            move |delete_source_request: DeleteSourceRequest| {
                assert_eq!(delete_source_request.index_uid(), &index_uid_clone);
                assert_eq!(&delete_source_request.source_id, INGEST_V2_SOURCE_ID);
                Ok(EmptyResponse {})
            },
        );

        let mut source = SourceConfig::ingest_v2();
        source.enabled = true;
        index_0.add_source(source.clone()).unwrap();

        let index_0_clone = index_0.clone();
        mock_metastore.expect_list_indexes_metadata().return_once(
            move |list_indexes_request: ListIndexesMetadataRequest| {
                assert_eq!(list_indexes_request, ListIndexesMetadataRequest::all());
                Ok(ListIndexesMetadataResponse::for_test(vec![
                    index_0_clone.clone(),
                ]))
            },
        );

        let index_uid_clone = index_0.index_uid.clone();
        mock_metastore.expect_list_shards().return_once(
            move |_list_shards_request: ListShardsRequest| {
                let list_shards_resp = ListShardsResponse {
                    subresponses: vec![ListShardsSubresponse {
                        index_uid: Some(index_uid_clone.clone()),
                        source_id: INGEST_V2_SOURCE_ID.to_string(),
                        shards: vec![Shard {
                            index_uid: Some(index_uid_clone),
                            source_id: source.source_id.to_string(),
                            shard_id: Some(ShardId::from(15)),
                            leader_id: "node1".to_string(),
                            follower_id: None,
                            shard_state: ShardState::Open as i32,
                            doc_mapping_uid: Some(DocMappingUid::default()),
                            publish_position_inclusive: None,
                            publish_token: None,
                            update_timestamp: 1724158996,
                        }],
                    }],
                };
                Ok(list_shards_resp)
            },
        );
        let cluster_config = ClusterConfig::for_test();
        let cluster_change_stream_factory = ClusterChangeStreamFactoryForTest::default();
        let (control_plane_mailbox, _control_plane_handle, _readiness_rx) = ControlPlane::spawn(
            &universe,
            cluster_config,
            node_id,
            cluster_change_stream_factory,
            indexer_pool,
            ingester_pool,
            MetastoreServiceClient::from_mock(mock_metastore),
        );
        // This update should not trigger anything in the control plane.
        control_plane_mailbox
            .ask(DeleteSourceRequest {
                index_uid: Some(index_0.index_uid),
                source_id: INGEST_V2_SOURCE_ID.to_string(),
            })
            .await
            .unwrap()
            .unwrap();

        universe.assert_quit().await;
    }

    #[tokio::test]
    async fn test_auto_create_indexes_on_get_or_create_open_shards_request() {
        let universe = Universe::default();

        let mut cluster_config = ClusterConfig::for_test();
        cluster_config.auto_create_indexes = true;

        let node_id = NodeId::from_str("test-node");
        let cluster_change_stream_factory = ClusterChangeStreamFactoryForTest::default();
        let indexer_pool = IndexerPool::default();
        let ingester_pool = IngesterPool::default();

        let mut mock_metastore = MockMetastoreService::new();
        setup_disabled_maintenance(&mut mock_metastore);

        mock_metastore
            .expect_list_indexes_metadata()
            .return_once(|_| Ok(ListIndexesMetadataResponse::for_test(Vec::new())));

        mock_metastore
            .expect_find_index_template_matches()
            .return_once(|request| {
                assert_eq!(request.index_ids, ["test-index-foo"]);

                let index_template =
                    IndexTemplate::for_test("test-template-foo", &["test-index-foo*"], 100);
                let index_template_json = serde_json::to_string(&index_template).unwrap();

                Ok(FindIndexTemplateMatchesResponse {
                    matches: vec![IndexTemplateMatch {
                        template_id: "test-template-foo".to_string(),
                        index_id: "test-index-foo".to_string(),
                        index_template_json,
                    }],
                })
            });

        mock_metastore.expect_create_index().return_once(|request| {
            let index_config = request.deserialize_index_config().unwrap();
            assert_eq!(index_config.index_id, "test-index-foo");
            assert_eq!(index_config.index_uri, "ram:///indexes/test-index-foo");

            let source_configs = request.deserialize_source_configs().unwrap();
            assert_eq!(source_configs.len(), 2);
            // assert_eq!(source_configs[0].source_id, INGEST_API_SOURCE_ID);
            assert_eq!(source_configs[0].source_id, INGEST_V2_SOURCE_ID);
            assert_eq!(source_configs[1].source_id, CLI_SOURCE_ID);

            let index_uid = IndexUid::for_test("test-index-foo", 0);
            let mut index_metadata = IndexMetadata::new_with_index_uid(index_uid, index_config);

            for source_config in source_configs {
                index_metadata.add_source(source_config).unwrap();
            }
            let index_metadata_json = serde_json::to_string(&index_metadata).unwrap();

            Ok(CreateIndexResponse {
                index_uid: index_metadata.index_uid.into(),
                index_metadata_json,
            })
        });

        let (control_plane_mailbox, _control_plane_handle, _readiness_rx) = ControlPlane::spawn(
            &universe,
            cluster_config,
            node_id,
            cluster_change_stream_factory,
            indexer_pool,
            ingester_pool,
            MetastoreServiceClient::from_mock(mock_metastore),
        );

        let response = control_plane_mailbox
            .ask(GetOrCreateOpenShardsRequest {
                subrequests: vec![GetOrCreateOpenShardsSubrequest {
                    subrequest_id: 0,
                    index_id: "test-index-foo".to_string(),
                    source_id: INGEST_V2_SOURCE_ID.to_string(),
                }],
                closed_shards: Vec::new(),
                unavailable_leaders: Vec::new(),
            })
            .await
            .unwrap()
            .unwrap();
        assert!(response.successes.is_empty());
        assert_eq!(response.failures.len(), 1);
        assert!(matches!(
            response.failures[0].reason(),
            GetOrCreateOpenShardsFailureReason::NoIngestersAvailable
        ));

        let control_plane_state = control_plane_mailbox.ask(Observe).await.unwrap();
        assert_eq!(control_plane_state.num_indexes, 1);
        assert_eq!(control_plane_state.num_sources, 1);

        universe.assert_quit().await;
    }

    #[tokio::test]
    async fn test_watch_indexers() {
        let universe = Universe::with_accelerated_time();
        let (control_plane_mailbox, control_plane_inbox) = universe.create_test_mailbox();
        let weak_control_plane_mailbox = control_plane_mailbox.downgrade();

        let cluster_change_stream_factory = ClusterChangeStreamFactoryForTest::default();
        let cluster_change_stream = cluster_change_stream_factory.create();
        spawn_watch_indexers_task(weak_control_plane_mailbox, cluster_change_stream);

        let cluster_change_stream_tx = cluster_change_stream_factory.change_stream_tx();

        let metastore_node =
            ClusterNode::for_test("test-metastore", 1337, false, &["metastore"], &[]).await;
        let cluster_change = ClusterChange::Add(metastore_node);
        cluster_change_stream_tx.send(cluster_change).unwrap();

        let indexer_node =
            ClusterNode::for_test("test-indexer", 1515, false, &["indexer"], &[]).await;
        let cluster_change = ClusterChange::Add(indexer_node.clone());
        cluster_change_stream_tx.send(cluster_change).unwrap();

        let cluster_change = ClusterChange::Remove(indexer_node.clone());
        cluster_change_stream_tx.send(cluster_change).unwrap();

        let IndexerJoined(joined) = control_plane_inbox.recv_typed_message().await.unwrap();
        assert_eq!(joined.grpc_advertise_addr.port(), 1516);

        let IndexerLeft(left) = control_plane_inbox.recv_typed_message().await.unwrap();
        assert_eq!(left.grpc_advertise_addr.port(), 1516);

        universe.assert_quit().await;
    }

    #[tokio::test]
    async fn test_control_plane_rebuilds_plan_on_indexer_joins_or_leaves_the_cluster() {
        let universe = Universe::with_accelerated_time();

        let cluster_config = ClusterConfig::for_test();
        let node_id = NodeId::from_str("test-control-plane");
        let cluster_change_stream_factory = ClusterChangeStreamFactoryForTest::default();

        let indexer_pool = IndexerPool::default();
        let ingester_pool = IngesterPool::default();
        let mut mock_metastore = MockMetastoreService::new();
        setup_disabled_maintenance(&mut mock_metastore);
        mock_metastore
            .expect_list_indexes_metadata()
            .return_once(|_| Ok(ListIndexesMetadataResponse::for_test(Vec::new())));
        let metastore = MetastoreServiceClient::from_mock(mock_metastore);

        // Create mock maintenance persistence metastore
        let mut mock_persistence_metastore = MockMetastoreService::new();
        mock_persistence_metastore
            .expect_get_kv()
            .returning(|_| Ok(GetKvResponse { value: None }));
        mock_persistence_metastore
            .expect_set_kv()
            .returning(|_| Ok(EmptyResponse {}));
        mock_persistence_metastore
            .expect_delete_kv()
            .returning(|_| Ok(EmptyResponse {}));
        let maintenance_persistence = MetastoreKvPersistence::new(
            MetastoreServiceClient::from_mock(mock_persistence_metastore),
        );

        let disable_control_loop = true;
        let (_control_plane_mailbox, control_plane_handle, _readiness_rx) =
            ControlPlane::spawn_inner(
                &universe,
                cluster_config,
                node_id,
                cluster_change_stream_factory.clone(),
                indexer_pool.clone(),
                ingester_pool,
                metastore,
                disable_control_loop,
                maintenance_persistence,
            );
        let cluster_change_stream_tx = cluster_change_stream_factory.change_stream_tx();
        let indexer_node =
            ClusterNode::for_test("test-indexer", 1515, false, &["indexer"], &[]).await;
        let cluster_change = ClusterChange::Add(indexer_node.clone());
        cluster_change_stream_tx.send(cluster_change).unwrap();

        universe.sleep(Duration::from_secs(10)).await;

        let ingest_controller_stats = control_plane_handle
            .process_pending_and_observe()
            .await
            .state_opt
            .as_ref()
            .unwrap()
            .ingest_controller;
        assert_eq!(ingest_controller_stats.num_rebalance_shards_ops, 1);

        let cluster_change = ClusterChange::Remove(indexer_node);
        cluster_change_stream_tx.send(cluster_change).unwrap();

        universe.sleep(Duration::from_secs(10)).await;

        let ingest_controller_stats = control_plane_handle
            .process_pending_and_observe()
            .await
            .state_opt
            .as_ref()
            .unwrap()
            .ingest_controller;
        assert_eq!(ingest_controller_stats.num_rebalance_shards_ops, 2);

        universe.assert_quit().await;
    }

    #[tokio::test]
    async fn test_control_plane_handles_rebalance_shards_callback() {
        let universe = Universe::with_accelerated_time();

        let cluster_config = ClusterConfig::for_test();
        let node_id = NodeId::from_str("test-control-plane");
        let cluster_change_stream_factory = ClusterChangeStreamFactoryForTest::default();

        let indexer_pool = IndexerPool::default();
        let ingester_pool = IngesterPool::default();
        let ingester_id = NodeId::from_str("test-ingester");
        let mut mock_ingester = MockIngesterService::new();
        mock_ingester
            .expect_retain_shards()
            .return_once(|_| Ok(RetainShardsResponse {}));
        mock_ingester.expect_init_shards().return_once(|request| {
            let shard = request.subrequests[0].shard().clone();
            let response = InitShardsResponse {
                successes: vec![InitShardSuccess {
                    subrequest_id: 0,
                    shard: Some(shard),
                }],
                failures: Vec::new(),
            };
            Ok(response)
        });
        let ingester = IngesterServiceClient::from_mock(mock_ingester);
        ingester_pool.insert(ingester_id, ingester);

        let mut mock_metastore = MockMetastoreService::new();
        setup_disabled_maintenance(&mut mock_metastore);
        mock_metastore
            .expect_list_indexes_metadata()
            .return_once(|_| Ok(ListIndexesMetadataResponse::for_test(Vec::new())));

        mock_metastore.expect_create_index().return_once(|request| {
            let index_config = request.deserialize_index_config().unwrap();
            let source_configs = request.deserialize_source_configs().unwrap();
            let mut index_metadata = IndexMetadata::new_with_index_uid(
                IndexUid::for_test(&index_config.index_id, 0),
                index_config,
            );
            for source_config in source_configs {
                index_metadata.add_source(source_config).unwrap();
            }
            let index_metadata_json = serde_json::to_string(&index_metadata).unwrap();
            let response = CreateIndexResponse {
                index_uid: Some(IndexUid::for_test("test-index", 0u128)),
                index_metadata_json,
            };
            Ok(response)
        });
        mock_metastore.expect_open_shards().return_once(|_| {
            let response = OpenShardsResponse {
                subresponses: vec![OpenShardSubresponse {
                    subrequest_id: 0,
                    open_shard: Some(Shard {
                        index_uid: Some(IndexUid::for_test("test-index", 0u128)),
                        source_id: INGEST_V2_SOURCE_ID.to_string(),
                        shard_id: Some(ShardId::from(0u64)),
                        leader_id: "test-ingester".to_string(),
                        follower_id: None,
                        shard_state: ShardState::Open as i32,
                        doc_mapping_uid: Some(DocMappingUid::default()),
                        publish_position_inclusive: Some(Position::Beginning),
                        publish_token: None,
                        update_timestamp: 1724158996,
                    }),
                }],
            };
            Ok(response)
        });
        let metastore = MetastoreServiceClient::from_mock(mock_metastore);
        let (control_plane_mailbox, _control_plane_handle, _readiness_rx) = ControlPlane::spawn(
            &universe,
            cluster_config,
            node_id,
            cluster_change_stream_factory.clone(),
            indexer_pool.clone(),
            ingester_pool,
            metastore,
        );
        let index_config = IndexConfig::for_test("test-index", "ram:///test-index");
        let mut source_config = SourceConfig::ingest_v2();
        source_config.enabled = true;

        let create_index_request =
            CreateIndexRequest::try_from_index_and_source_configs(&index_config, &[source_config])
                .unwrap();
        control_plane_mailbox
            .ask(create_index_request)
            .await
            .unwrap()
            .unwrap();

        let get_or_create_open_shards_request = GetOrCreateOpenShardsRequest {
            subrequests: vec![GetOrCreateOpenShardsSubrequest {
                subrequest_id: 0,
                index_id: "test-index".to_string(),
                source_id: INGEST_V2_SOURCE_ID.to_string(),
            }],
            closed_shards: Vec::new(),
            unavailable_leaders: Vec::new(),
        };
        control_plane_mailbox
            .ask(get_or_create_open_shards_request)
            .await
            .unwrap()
            .unwrap();

        let closed_shards = vec![
            ShardPKey {
                index_uid: Some(IndexUid::for_test("test-index", 0u128)),
                source_id: INGEST_V2_SOURCE_ID.to_string(),
                shard_id: Some(ShardId::from(0u64)),
            },
            ShardPKey {
                index_uid: Some(IndexUid::for_test("test-index", 0u128)),
                source_id: INGEST_V2_SOURCE_ID.to_string(),
                shard_id: Some(ShardId::from(1u64)),
            },
        ];
        let rebalance_lock = Arc::new(Mutex::new(()));
        let rebalance_guard = rebalance_lock.clone().lock_owned().await;
        let callback = RebalanceShardsCallback {
            closed_shards,
            rebalance_guard,
        };
        control_plane_mailbox.ask(callback).await.unwrap();

        let control_plane_debug_info = control_plane_mailbox.ask(GetDebugInfo).await.unwrap();
        let shard = &control_plane_debug_info["shard_table"]
            ["test-index:00000000000000000000000000"]["test-ingester"][0];
        assert_eq!(shard["shard_id"], "00000000000000000000");
        assert_eq!(shard["shard_state"], "closed");

        universe.assert_quit().await;
    }

    #[tokio::test]
    async fn test_control_plane_swap_pipelines_applied_on_next_control_loop() {
        let universe = Universe::default();
        let node_id = NodeId::from_str("test-control-plane");
        let cluster_change_stream_factory = ClusterChangeStreamFactoryForTest::default();
        let indexer_pool = IndexerPool::default();
        let ingester_pool = IngesterPool::default();

        // Two mock indexers that accept unlimited apply_indexing_plan calls.
        let mut mock_indexer_1 = MockIndexingService::new();
        mock_indexer_1
            .expect_apply_indexing_plan()
            .returning(|_| Ok(ApplyIndexingPlanResponse {}));
        let mut mock_indexer_2 = MockIndexingService::new();
        mock_indexer_2
            .expect_apply_indexing_plan()
            .returning(|_| Ok(ApplyIndexingPlanResponse {}));

        indexer_pool.insert(
            NodeId::from_str("indexer-1"),
            IndexerNodeInfo {
                node_id: NodeId::from_str("indexer-1"),
                generation_id: 0,
                client: IndexingServiceClient::from_mock(mock_indexer_1),
                indexing_tasks: Vec::new(),
                indexing_capacity: CpuCapacity::from_cpu_millis(4_000),
            },
        );
        indexer_pool.insert(
            NodeId::from_str("indexer-2"),
            IndexerNodeInfo {
                node_id: NodeId::from_str("indexer-2"),
                generation_id: 0,
                client: IndexingServiceClient::from_mock(mock_indexer_2),
                indexing_tasks: Vec::new(),
                indexing_capacity: CpuCapacity::from_cpu_millis(4_000),
            },
        );

        // Two indexes, each with a single-pipeline Kafka source and an ingest-v2 source
        // (so that `create_or_enable_ingest_v2_sources_if_necessary` does not call `add_source`).
        let mut index_a = IndexMetadata::for_test("index-a", "ram:///index-a");
        index_a
            .add_source(SourceConfig::for_test(
                "kafka-source",
                SourceParams::Kafka(KafkaSourceParams {
                    topic: "topic-a".to_string(),
                    client_log_level: None,
                    enable_backfill_mode: false,
                    client_params: json!({}),
                }),
            ))
            .unwrap();
        index_a.add_source(SourceConfig::ingest_v2()).unwrap();

        let mut index_b = IndexMetadata::for_test("index-b", "ram:///index-b");
        index_b
            .add_source(SourceConfig::for_test(
                "kafka-source",
                SourceParams::Kafka(KafkaSourceParams {
                    topic: "topic-b".to_string(),
                    client_log_level: None,
                    enable_backfill_mode: false,
                    client_params: json!({}),
                }),
            ))
            .unwrap();
        index_b.add_source(SourceConfig::ingest_v2()).unwrap();

        let mut mock_metastore = MockMetastoreService::new();
        setup_disabled_maintenance(&mut mock_metastore);
        mock_metastore
            .expect_list_indexes_metadata()
            .return_once(move |_| {
                Ok(ListIndexesMetadataResponse::for_test(vec![
                    index_a, index_b,
                ]))
            });
        mock_metastore
            .expect_list_shards()
            .return_once(|_| Ok(ListShardsResponse::default()));

        // Create mock maintenance persistence metastore
        let mut mock_persistence_metastore = MockMetastoreService::new();
        mock_persistence_metastore
            .expect_get_kv()
            .returning(|_| Ok(GetKvResponse { value: None }));
        mock_persistence_metastore
            .expect_set_kv()
            .returning(|_| Ok(EmptyResponse {}));
        mock_persistence_metastore
            .expect_delete_kv()
            .returning(|_| Ok(EmptyResponse {}));
        let maintenance_persistence = MetastoreKvPersistence::new(
            MetastoreServiceClient::from_mock(mock_persistence_metastore),
        );

        let cluster_config = ClusterConfig::for_test();
        let (control_plane_mailbox, _control_plane_handle, _readiness_rx) =
            ControlPlane::spawn_inner(
                &universe,
                cluster_config,
                node_id,
                cluster_change_stream_factory,
                indexer_pool,
                ingester_pool,
                MetastoreServiceClient::from_mock(mock_metastore),
                false, // keep the control loop enabled
                maintenance_persistence,
            );

        // ── Wait for the initial plan to be built ──────────────────────────
        // Use `mailbox.ask(Observe)` to get state directly from the inner
        // actor (the supervisor handle only returns a cached snapshot that may
        // lag behind).
        let initial_state = {
            let mut state = None;
            for _ in 0..100 {
                tokio::time::sleep(Duration::from_millis(50)).await;
                let obs: ControlPlaneObservableState =
                    control_plane_mailbox.ask(Observe).await.unwrap();
                if obs
                    .indexing_scheduler
                    .current_targeted_physical_plan
                    .is_some()
                {
                    state = Some(obs);
                    break;
                }
            }
            state.expect("initial plan should have been built")
        };

        let initial_plan = initial_state
            .indexing_scheduler
            .current_targeted_physical_plan
            .as_ref()
            .unwrap();

        // Each indexer should have exactly 1 task (4000 mcpu capacity, 3200 mcpu per pipeline).
        let i1_tasks = initial_plan.indexer("indexer-1").unwrap();
        let i2_tasks = initial_plan.indexer("indexer-2").unwrap();
        assert_eq!(i1_tasks.len(), 1);
        assert_eq!(i2_tasks.len(), 1);

        let idx_on_1 = i1_tasks[0].index_uid().index_id.clone();
        let idx_on_2 = i2_tasks[0].index_uid().index_id.clone();
        assert_ne!(idx_on_1, idx_on_2);

        let num_schedule_before = initial_state.indexing_scheduler.num_schedule_indexing_plan;

        // ── Swap pipelines ─────────────────────────────────────────────────
        let response: SwapIndexingPipelinesResponse = control_plane_mailbox
            .ask(SwapIndexingPipelinesRequest {
                swaps: vec![SwapIndexingPipelinesEntry {
                    left_node_id: "indexer-1".to_string(),
                    left_index_id: idx_on_1.clone(),
                    right_node_id: "indexer-2".to_string(),
                    right_index_id: Some(idx_on_2.clone()),
                }],
            })
            .await
            .unwrap()
            .unwrap();
        assert!(response.results[0].success, "swap must succeed");

        // Immediately after the swap, the targeted plan should reflect it.
        let after_swap: ControlPlaneObservableState =
            control_plane_mailbox.ask(Observe).await.unwrap();
        let plan = after_swap
            .indexing_scheduler
            .current_targeted_physical_plan
            .as_ref()
            .unwrap();
        assert_eq!(
            plan.indexer("indexer-1").unwrap()[0].index_uid().index_id,
            idx_on_2,
            "indexer-1 should now have the index that was on indexer-2"
        );
        assert_eq!(
            plan.indexer("indexer-2").unwrap()[0].index_uid().index_id,
            idx_on_1,
            "indexer-2 should now have the index that was on indexer-1"
        );

        let num_applied_after_swap = after_swap
            .indexing_scheduler
            .num_applied_physical_indexing_plan;

        // ── Wait for the control loop to re-apply the (swapped) plan ───────
        // `control_running_plan` has a MIN_DURATION_BETWEEN_SCHEDULING cooldown
        // (50 ms in tests). The control loop interval is 100 ms. We poll until
        // the apply counter increases.
        let mut reapplied = false;
        for _ in 0..40 {
            tokio::time::sleep(Duration::from_millis(100)).await;
            let obs: ControlPlaneObservableState =
                control_plane_mailbox.ask(Observe).await.unwrap();
            if obs.indexing_scheduler.num_applied_physical_indexing_plan > num_applied_after_swap {
                reapplied = true;
                break;
            }
        }
        assert!(
            reapplied,
            "the control loop should have re-applied the plan"
        );

        // ── Verify the swapped plan is still in place after re-apply ───────
        let final_state: ControlPlaneObservableState =
            control_plane_mailbox.ask(Observe).await.unwrap();
        let final_plan = final_state
            .indexing_scheduler
            .current_targeted_physical_plan
            .as_ref()
            .unwrap();

        assert_eq!(
            final_plan.indexer("indexer-1").unwrap()[0]
                .index_uid()
                .index_id,
            idx_on_2,
            "after control loop re-apply, indexer-1 should still have the swapped index"
        );
        assert_eq!(
            final_plan.indexer("indexer-2").unwrap()[0]
                .index_uid()
                .index_id,
            idx_on_1,
            "after control loop re-apply, indexer-2 should still have the swapped index"
        );

        // No rebuild should have happened; only re-applies of the existing plan.
        assert_eq!(
            final_state.indexing_scheduler.num_schedule_indexing_plan, num_schedule_before,
            "no rebuild should have happened after the swap – the control loop should only \
             re-apply the existing plan"
        );

        universe.assert_quit().await;
    }

    #[tokio::test]
    async fn test_control_plane_get_debug_info() {
        let universe = Universe::with_accelerated_time();

        let cluster_config = ClusterConfig::for_test();
        let node_id = NodeId::from_str("test-control-plane");
        let cluster_change_stream_factory = ClusterChangeStreamFactoryForTest::default();

        let indexer_pool = IndexerPool::default();
        let ingester_id = NodeId::from_str("test-ingester");

        let mut mock_indexer = MockIndexingService::new();
        mock_indexer
            .expect_apply_indexing_plan()
            .return_once(|_| Ok(ApplyIndexingPlanResponse {}));
        let indexer = IndexingServiceClient::from_mock(mock_indexer);

        let indexer_info = IndexerNodeInfo {
            node_id: ingester_id.clone(),
            generation_id: 0,
            client: indexer,
            indexing_tasks: Vec::new(),
            indexing_capacity: CpuCapacity::from_cpu_millis(1_000),
        };
        indexer_pool.insert(ingester_id.clone(), indexer_info);

        let ingester_pool = IngesterPool::default();
        let mut mock_ingester = MockIngesterService::new();
        mock_ingester
            .expect_retain_shards()
            .return_once(|_| Ok(RetainShardsResponse {}));
        mock_ingester.expect_init_shards().return_once(|request| {
            let shard = request.subrequests[0].shard().clone();
            let response = InitShardsResponse {
                successes: vec![InitShardSuccess {
                    subrequest_id: 0,
                    shard: Some(shard),
                }],
                failures: Vec::new(),
            };
            Ok(response)
        });
        let ingester = IngesterServiceClient::from_mock(mock_ingester);
        ingester_pool.insert(ingester_id, ingester);

        let mut mock_metastore = MockMetastoreService::new();
        setup_disabled_maintenance(&mut mock_metastore);
        mock_metastore
            .expect_list_indexes_metadata()
            .return_once(|_| Ok(ListIndexesMetadataResponse::for_test(Vec::new())));

        mock_metastore.expect_create_index().return_once(|request| {
            let index_config = request.deserialize_index_config().unwrap();
            let source_configs = request.deserialize_source_configs().unwrap();
            let mut index_metadata = IndexMetadata::new_with_index_uid(
                IndexUid::for_test(&index_config.index_id, 0),
                index_config,
            );
            for source_config in source_configs {
                index_metadata.add_source(source_config).unwrap();
            }
            let index_metadata_json = serde_json::to_string(&index_metadata).unwrap();
            let response = CreateIndexResponse {
                index_uid: Some(IndexUid::for_test("test-index", 0u128)),
                index_metadata_json,
            };
            Ok(response)
        });
        mock_metastore.expect_open_shards().return_once(|_| {
            let response = OpenShardsResponse {
                subresponses: vec![OpenShardSubresponse {
                    subrequest_id: 0,
                    open_shard: Some(Shard {
                        index_uid: Some(IndexUid::for_test("test-index", 0u128)),
                        source_id: INGEST_V2_SOURCE_ID.to_string(),
                        shard_id: Some(ShardId::from(0u64)),
                        leader_id: "test-ingester".to_string(),
                        follower_id: None,
                        shard_state: ShardState::Open as i32,
                        doc_mapping_uid: Some(DocMappingUid::default()),
                        publish_position_inclusive: Some(Position::Beginning),
                        publish_token: None,
                        update_timestamp: 1724158996,
                    }),
                }],
            };
            Ok(response)
        });
        let metastore = MetastoreServiceClient::from_mock(mock_metastore);
        let (control_plane_mailbox, _control_plane_handle, _readiness_rx) = ControlPlane::spawn(
            &universe,
            cluster_config,
            node_id,
            cluster_change_stream_factory.clone(),
            indexer_pool.clone(),
            ingester_pool,
            metastore,
        );
        let index_config = IndexConfig::for_test("test-index", "ram:///test-index");
        let mut source_config = SourceConfig::ingest_v2();
        source_config.enabled = true;

        let create_index_request =
            CreateIndexRequest::try_from_index_and_source_configs(&index_config, &[source_config])
                .unwrap();
        control_plane_mailbox
            .ask(create_index_request)
            .await
            .unwrap()
            .unwrap();

        let get_or_create_open_shards_request = GetOrCreateOpenShardsRequest {
            subrequests: vec![GetOrCreateOpenShardsSubrequest {
                subrequest_id: 0,
                index_id: "test-index".to_string(),
                source_id: INGEST_V2_SOURCE_ID.to_string(),
            }],
            closed_shards: Vec::new(),
            unavailable_leaders: Vec::new(),
        };
        control_plane_mailbox
            .ask(get_or_create_open_shards_request)
            .await
            .unwrap()
            .unwrap();

        let control_plane_debug_info = control_plane_mailbox.ask(GetDebugInfo).await.unwrap();

        assert_eq!(
            control_plane_debug_info["physical_indexing_plan"][0]["node_id"],
            "test-ingester"
        );
        let shard = &control_plane_debug_info["shard_table"]
            ["test-index:00000000000000000000000000"]["test-ingester"][0];
        assert_eq!(shard["index_uid"], "test-index:00000000000000000000000000");
        assert_eq!(shard["source_id"], INGEST_V2_SOURCE_ID);
        assert_eq!(shard["shard_id"], "00000000000000000000");
        assert_eq!(shard["shard_state"], "open");
        assert_eq!(shard["leader_id"], "test-ingester");
        assert_eq!(shard["follower_id"], JsonValue::Null);
        assert_eq!(
            shard["publish_position_inclusive"],
            json!(Position::Beginning)
        );

        universe.assert_quit().await;
    }
}
