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

use std::collections::hash_map::Entry;
use std::collections::{BTreeSet, HashMap};
use std::fmt;
use std::fmt::Formatter;
use std::time::{Duration, Instant};

use anyhow::Context;
use async_trait::async_trait;
use futures::stream::FuturesUnordered;
use futures::{Future, StreamExt};
use quickwit_actors::{
    Actor, ActorContext, ActorExitStatus, ActorHandle, DeferableReplyHandler, Handler, Mailbox,
    Supervisor, Universe, WeakMailbox,
};
use quickwit_cluster::{
    ClusterChange, ClusterChangeStream, ClusterChangeStreamFactory, ClusterNode,
};
use quickwit_common::pubsub::EventSubscriber;
use quickwit_common::uri::Uri;
use quickwit_common::{shared_consts, Progress};
use quickwit_config::service::QuickwitService;
use quickwit_config::{ClusterConfig, IndexConfig, IndexTemplate, SourceConfig};
use quickwit_ingest::{IngesterPool, LocalShardsUpdate};
use quickwit_metastore::{CreateIndexRequestExt, CreateIndexResponseExt, IndexMetadataResponseExt};
use quickwit_proto::control_plane::{
    AdviseResetShardsRequest, AdviseResetShardsResponse, ControlPlaneError, ControlPlaneResult,
    GetOrCreateOpenShardsRequest, GetOrCreateOpenShardsResponse, GetOrCreateOpenShardsSubrequest,
};
use quickwit_proto::indexing::ShardPositionsUpdate;
use quickwit_proto::metastore::{
    serde_utils, AddSourceRequest, CreateIndexRequest, CreateIndexResponse, DeleteIndexRequest,
    DeleteShardsRequest, DeleteSourceRequest, EmptyResponse, FindIndexTemplateMatchesRequest,
    IndexMetadataResponse, IndexTemplateMatch, MetastoreError, MetastoreResult, MetastoreService,
    MetastoreServiceClient, PruneShardsRequest, ToggleSourceRequest, UpdateIndexRequest,
};
use quickwit_proto::types::{IndexId, IndexUid, NodeId, ShardId, SourceId, SourceUid};
use serde::Serialize;
use serde_json::{json, Value as JsonValue};
use tokio::sync::watch;
use tracing::{debug, error, info};

use crate::debouncer::Debouncer;
use crate::indexing_scheduler::{IndexingScheduler, IndexingSchedulerState};
use crate::ingest::ingest_controller::{IngestControllerStats, RebalanceShardsCallback};
use crate::ingest::IngestController;
use crate::model::ControlPlaneModel;
use crate::IndexerPool;

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
    next_prune_shard_requests: HashMap<(IndexId, SourceId), Instant>,
    rebuild_plan_debouncer: Debouncer,
    readiness_tx: watch::Sender<bool>,
    // Disables the control loop. This is useful for unit testing.
    disable_control_loop: bool,
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
        Self::spawn_inner(
            universe,
            cluster_config,
            self_node_id,
            cluster_change_stream_factory,
            indexer_pool,
            ingester_pool,
            metastore,
            disable_control_loop,
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
                    next_prune_shard_requests: HashMap::new(),
                    rebuild_plan_debouncer: Debouncer::new(REBUILD_PLAN_COOLDOWN_PERIOD),
                    readiness_tx,
                    disable_control_loop,
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
        }
    }

    async fn initialize(&mut self, ctx: &ActorContext<Self>) -> Result<(), ActorExitStatus> {
        crate::metrics::CONTROL_PLANE_METRICS.restart_total.inc();
        self.model
            .load_from_metastore(&mut self.metastore, ctx.progress())
            .await
            .context("failed to initialize control plane model")?;

        let _rebuild_plan_waiter = self.rebuild_plan_debounced(ctx);

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
        info!(shard_ids=?shard_ids, source_uid=?source_uid, "deleting shards");
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
        Ok(())
    }

    fn debug_info(&self) -> JsonValue {
        let physical_indexing_plan: Vec<JsonValue> = self
            .indexing_scheduler
            .observable_state()
            .last_applied_physical_plan
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

        let mut per_index_shards_json: HashMap<IndexUid, Vec<JsonValue>> = HashMap::new();

        for (source_uid, shard_entries) in self.model.all_shards_with_source() {
            let index_uid = source_uid.index_uid.clone();
            let source_id = source_uid.source_id.clone();
            let shards_json = shard_entries.map(|shard_entry| {
                json!({
                    "index_uid": index_uid,
                    "source_id": source_id,
                    "shard_id": shard_entry.shard_id.clone(),
                    "shard_state": shard_entry.shard_state().as_json_str_name(),
                    "leader_id": shard_entry.leader_id.clone(),
                    "follower_id": shard_entry.follower_id.clone(),
                    "publish_position_inclusive": shard_entry.publish_position_inclusive(),
                })
            });
            per_index_shards_json
                .entry(index_uid.clone())
                .or_default()
                .extend(shards_json);
        }
        json!({
            "physical_indexing_plan": physical_indexing_plan,
            "shard_table": per_index_shards_json,
        })
    }

    /// Rebuilds the indexing plan.
    ///
    /// This method includes some debouncing logic. Every call will be followed by a cooldown
    /// period.
    ///
    /// This method returns a future that can be awaited to ensure that the relevant rebuild plan
    /// operation has been executed.
    fn rebuild_plan_debounced(&mut self, ctx: &ActorContext<Self>) -> impl Future<Output = ()> {
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
        self.indexing_scheduler.rebuild_plan(&self.model);
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
        debug!(shard_positions_update=?shard_positions_update, "shard positions update");
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
                    info!(shard_id=%shard_id, position=?position, "received eof shard via gossip");
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
        if let Err(metastore_error) = self
            .ingest_controller
            .rebalance_shards(&mut self.model, ctx.mailbox(), ctx.progress())
            .await
        {
            return convert_metastore_error::<()>(metastore_error).map(|_| ());
        }
        self.indexing_scheduler.control_running_plan(&self.model);
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
        // Now, create index can also add sources to support creating indexes automatically from
        // index and source config templates.
        let should_rebuild_plan = !index_metadata.sources.is_empty();
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
        info!(%index_uid, "deleted index");

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
        let _rebuild_plan_waiter = self.rebuild_plan_debounced(ctx);

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
        let _rebuild_plan_waiter = self.rebuild_plan_debounced(ctx);

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
        info!(%index_uid, source_id, enabled=enable, "toggled source");

        let mutation_occurred = self.model.toggle_source(&index_uid, &source_id, enable)?;

        if mutation_occurred {
            let _rebuild_plan_waiter = self.rebuild_plan_debounced(ctx);
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
        ctx: &ActorContext<Self>,
    ) -> Result<ControlPlaneResult<EmptyResponse>, ActorExitStatus> {
        let index_uid: IndexUid = request.index_uid().clone();
        let source_id = request.source_id.clone();

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

        let _rebuild_plan_waiter = self.rebuild_plan_debounced(ctx);
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
        // A very basic debounce is enough here, missing one call to the pruning API is fine
        let next_prune_shard_request = self.next_prune_shard_requests.entry((
            request.index_uid().index_id.clone(),
            request.source_id.clone(),
        ));
        let interval = request
            .interval
            .map(|interval| Duration::from_secs(interval as u64))
            .unwrap_or_else(|| PRUNE_SHARDS_DEFAULT_COOLDOWN_PERIOD);
        let should_call = match next_prune_shard_request {
            Entry::Vacant(entry) => {
                entry.insert(Instant::now() + interval);
                true
            }
            Entry::Occupied(mut entry) => {
                let deadline = entry.get_mut();
                let now = Instant::now();
                if now >= *deadline {
                    *deadline = now + interval;
                    true
                } else {
                    false
                }
            }
        };
        if should_call {
            if let Err(metastore_error) = self.metastore.prune_shards(request).await {
                return convert_metastore_error(metastore_error);
            };
        }
        // Return ok regardless of whether the call was successful or debounced
        Ok(Ok(EmptyResponse {}))
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
        if let Err(metastore_error) = self
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
impl Handler<LocalShardsUpdate> for ControlPlane {
    type Reply = ControlPlaneResult<()>;

    async fn handle(
        &mut self,
        local_shards_update: LocalShardsUpdate,
        ctx: &ActorContext<Self>,
    ) -> Result<Self::Reply, ActorExitStatus> {
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
        info!(
            "indexer `{}` joined the cluster: rebalancing shards and rebuilding indexing plan",
            message.0.node_id()
        );
        // TODO: Update shard table.
        if let Err(metastore_error) = self
            .ingest_controller
            .rebalance_shards(&mut self.model, ctx.mailbox(), ctx.progress())
            .await
        {
            return convert_metastore_error::<()>(metastore_error).map(|_| ());
        }
        self.indexing_scheduler.rebuild_plan(&self.model);
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
        info!(
            "indexer `{}` left the cluster: rebalancing shards and rebuilding indexing plan",
            message.0.node_id()
        );
        // TODO: Update shard table.
        if let Err(metastore_error) = self
            .ingest_controller
            .rebalance_shards(&mut self.model, ctx.mailbox(), ctx.progress())
            .await
        {
            return convert_metastore_error::<()>(metastore_error).map(|_| ());
        }
        self.indexing_scheduler.rebuild_plan(&self.model);
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
        info!(
            "closing {} shards after rebalance",
            message.closed_shards.len()
        );
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
                if node.enabled_services().contains(&QuickwitService::Indexer) {
                    if let Err(error) = mailbox.send_message(IndexerJoined(node)).await {
                        error!(error=%error, "failed to forward `IndexerJoined` event to control plane");
                    }
                }
            }
            ClusterChange::Remove(node) => {
                if node.enabled_services().contains(&QuickwitService::Indexer) {
                    if let Err(error) = mailbox.send_message(IndexerLeft(node)).await {
                        error!(error=%error, "failed to forward `IndexerLeft` event to control plane");
                    }
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
    use std::sync::Arc;

    use mockall::Sequence;
    use quickwit_actors::{AskError, Observe, SupervisorMetrics};
    use quickwit_cluster::ClusterChangeStreamFactoryForTest;
    use quickwit_config::{IndexConfig, SourceParams, CLI_SOURCE_ID, INGEST_V2_SOURCE_ID};
    use quickwit_indexing::IndexingService;
    use quickwit_metastore::{
        CreateIndexRequestExt, IndexMetadata, ListIndexesMetadataResponseExt,
    };
    use quickwit_proto::control_plane::{
        GetOrCreateOpenShardsFailureReason, GetOrCreateOpenShardsSubrequest,
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
        DeleteShardsResponse, EntityKind, FindIndexTemplateMatchesResponse,
        ListIndexesMetadataRequest, ListIndexesMetadataResponse, ListShardsRequest,
        ListShardsResponse, ListShardsSubresponse, MetastoreError, MockMetastoreService,
        OpenShardSubresponse, OpenShardsResponse, SourceType,
    };
    use quickwit_proto::types::{DocMappingUid, Position};
    use tokio::sync::Mutex;

    use super::*;
    use crate::IndexerNodeInfo;

    #[tokio::test]
    async fn test_control_plane_create_index() {
        let universe = Universe::with_accelerated_time();
        let self_node_id: NodeId = "test-node".into();
        let indexer_pool = IndexerPool::default();
        let ingester_pool = IngesterPool::default();

        let mut mock_metastore = MockMetastoreService::new();
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
        let self_node_id: NodeId = "test-node".into();
        let indexer_pool = IndexerPool::default();
        let ingester_pool = IngesterPool::default();

        let index_uid: IndexUid = IndexUid::for_test("test-index", 0);
        let mut mock_metastore = MockMetastoreService::new();
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
        let self_node_id: NodeId = "test-node".into();
        let indexer_pool = IndexerPool::default();
        let ingester_pool = IngesterPool::default();

        let index_metadata = IndexMetadata::for_test("test-index", "ram://test");
        let index_uid = index_metadata.index_uid.clone();
        let mut mock_metastore = MockMetastoreService::new();
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
                Ok(ListIndexesMetadataResponse::for_test(vec![
                    index_metadata.clone()
                ]))
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
    async fn test_control_plane_toggle_source() {
        let universe = Universe::with_accelerated_time();
        let self_node_id: NodeId = "test-node".into();
        let indexer_pool = IndexerPool::default();
        let ingester_pool = IngesterPool::default();

        let mut mock_metastore = MockMetastoreService::new();
        let mut index_metadata = IndexMetadata::for_test("test-index", "ram://toto");
        let test_source_config = SourceConfig::for_test("test-source", SourceParams::void());
        let index_uid: IndexUid = IndexUid::for_test("test-index", 0);
        index_metadata.add_source(test_source_config).unwrap();
        mock_metastore
            .expect_list_indexes_metadata()
            .return_once(|_| Ok(ListIndexesMetadataResponse::for_test(vec![index_metadata])));

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
        let self_node_id: NodeId = "test-node".into();
        let indexer_pool = IndexerPool::default();
        let ingester_pool = IngesterPool::default();

        let mut mock_metastore = MockMetastoreService::new();
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
        let self_node_id: NodeId = "test-node".into();
        let indexer_pool = IndexerPool::default();

        let ingester_pool = IngesterPool::default();

        let mut mock_metastore = MockMetastoreService::new();
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
        let node_id = NodeId::new("test_node".to_string());
        let indexer_pool = IndexerPool::default();
        let ingester_pool = IngesterPool::default();
        let mut mock_metastore = MockMetastoreService::new();

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
        quickwit_common::setup_logging_for_tests();
        let universe = Universe::with_accelerated_time();
        let node_id = NodeId::new("control-plane-node".to_string());
        let indexer_pool = IndexerPool::default();
        let (client_mailbox, client_inbox) = universe.create_test_mailbox();
        let client = IndexingServiceClient::from_mailbox::<IndexingService>(client_mailbox);
        let indexer_node_info = IndexerNodeInfo {
            node_id: NodeId::new("indexer-node-1".to_string()),
            generation_id: 0,
            client,
            indexing_tasks: Vec::new(),
            indexing_capacity: CpuCapacity::from_cpu_millis(4_000),
        };
        indexer_pool.insert(indexer_node_info.node_id.clone(), indexer_node_info);
        let ingester_pool = IngesterPool::default();
        let mut mock_metastore = MockMetastoreService::new();

        let mut index_0 = IndexMetadata::for_test("test-index-0", "ram:///test-index-0");
        let mut source = SourceConfig::ingest_v2();
        source.enabled = true;
        index_0.add_source(source.clone()).unwrap();

        let index_0_clone = index_0.clone();
        mock_metastore.expect_list_indexes_metadata().return_once(
            move |list_indexes_request: ListIndexesMetadataRequest| {
                assert_eq!(list_indexes_request, ListIndexesMetadataRequest::all());
                Ok(ListIndexesMetadataResponse::for_test(vec![
                    index_0_clone.clone()
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
            leader_id: "test_node".to_string(),
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
            .last_applied_physical_plan
            .unwrap();
        let indexing_tasks = last_applied_physical_plan
            .indexing_tasks_per_indexer()
            .get("indexer-node-1")
            .unwrap();
        assert_eq!(indexing_tasks.len(), 1);
        assert_eq!(indexing_tasks[0].shard_ids, [ShardId::from(17)]);

        let control_plane_debug_info = control_plane_mailbox.ask(GetDebugInfo).await.unwrap();
        let shard =
            &control_plane_debug_info["shard_table"]["test-index-0:00000000000000000000000000"][0];
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
            .last_applied_physical_plan
            .unwrap();
        let indexing_tasks = last_applied_physical_plan
            .indexing_tasks_per_indexer()
            .get("indexer-node-1")
            .unwrap();
        assert!(indexing_tasks.is_empty());

        let apply_plan_requests = client_inbox.drain_for_test_typed::<ApplyIndexingPlanRequest>();
        assert!(!apply_plan_requests.is_empty());

        for apply_plan_request in &apply_plan_requests {
            assert!(apply_plan_request.indexing_tasks.is_empty());
        }

        universe.assert_quit().await;
    }

    #[tokio::test]
    async fn test_fill_shard_table_position_from_metastore_on_startup() {
        let universe = Universe::with_accelerated_time();
        let node_id = NodeId::new("control-plane-node".to_string());
        let indexer_pool = IndexerPool::default();
        let (client_mailbox, _client_inbox) = universe.create_test_mailbox();
        let client = IndexingServiceClient::from_mailbox::<IndexingService>(client_mailbox);
        let indexer_node_info = IndexerNodeInfo {
            node_id: NodeId::new("indexer-node-1".to_string()),
            generation_id: 0,
            client,
            indexing_tasks: Vec::new(),
            indexing_capacity: CpuCapacity::from_cpu_millis(4_000),
        };
        indexer_pool.insert(indexer_node_info.node_id.clone(), indexer_node_info);
        let ingester_pool = IngesterPool::default();
        let mut mock_metastore = MockMetastoreService::new();

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
            leader_id: "test_node".to_string(),
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
        let shard =
            &control_plane_debug_info["shard_table"]["test-index:00000000000000000000000000"][0];
        assert_eq!(shard["shard_id"], "00000000000000000017");
        assert_eq!(shard["publish_position_inclusive"], "00000000000000001234");

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
            node_id: NodeId::new("indexer-node-1".to_string()),
            generation_id: 0,
            client,
            indexing_tasks: Vec::new(),
            indexing_capacity: CpuCapacity::from_cpu_millis(4_000),
        };
        indexer_pool.insert(indexer_node_info.node_id.clone(), indexer_node_info);
        let ingester_pool = IngesterPool::default();
        let mut mock_metastore = MockMetastoreService::new();

        let mut index_0 = IndexMetadata::for_test("test-index-0", "ram:///test-index-0");
        let mut source = SourceConfig::ingest_v2();
        source.enabled = true;
        index_0.add_source(source.clone()).unwrap();

        let index_0_clone = index_0.clone();
        mock_metastore.expect_list_indexes_metadata().return_once(
            move |list_indexes_request: ListIndexesMetadataRequest| {
                assert_eq!(list_indexes_request, ListIndexesMetadataRequest::all());
                Ok(ListIndexesMetadataResponse::for_test(vec![
                    index_0_clone.clone()
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
        let node_id = NodeId::new("control-plane-node".to_string());
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
        mock_metastore
            .expect_list_indexes_metadata()
            .times(1)
            .in_sequence(&mut seq)
            .returning(move |list_indexes_request: ListIndexesMetadataRequest| {
                assert_eq!(list_indexes_request, ListIndexesMetadataRequest::all());
                Ok(ListIndexesMetadataResponse::for_test(vec![
                    index_0_clone.clone()
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
        ingester_pool.insert("node1".into(), ingester);

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
        let node_id = NodeId::new("control-plane-node".to_string());
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
        ingester_pool.insert("node1".into(), ingester);

        let mut index_0 = IndexMetadata::for_test("test-index-0", "ram:///test-index-0");
        let index_uid_clone = index_0.index_uid.clone();

        let mut mock_metastore = MockMetastoreService::new();
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
                    index_0_clone.clone()
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

        let node_id = NodeId::from("test-node");
        let cluster_change_stream_factory = ClusterChangeStreamFactoryForTest::default();
        let indexer_pool = IndexerPool::default();
        let ingester_pool = IngesterPool::default();

        let mut mock_metastore = MockMetastoreService::new();

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
        assert_eq!(joined.grpc_advertise_addr().port(), 1516);

        let IndexerLeft(left) = control_plane_inbox.recv_typed_message().await.unwrap();
        assert_eq!(left.grpc_advertise_addr().port(), 1516);

        universe.assert_quit().await;
    }

    #[tokio::test]
    async fn test_control_plane_rebuilds_plan_on_indexer_joins_or_leaves_the_cluster() {
        let universe = Universe::with_accelerated_time();

        let cluster_config = ClusterConfig::for_test();
        let node_id = NodeId::from("test-control-plane");
        let cluster_change_stream_factory = ClusterChangeStreamFactoryForTest::default();

        let indexer_pool = IndexerPool::default();
        let ingester_pool = IngesterPool::default();
        let mut mock_metastore = MockMetastoreService::new();
        mock_metastore
            .expect_list_indexes_metadata()
            .return_once(|_| Ok(ListIndexesMetadataResponse::for_test(Vec::new())));
        let metastore = MetastoreServiceClient::from_mock(mock_metastore);
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
        let node_id = NodeId::from("test-control-plane");
        let cluster_change_stream_factory = ClusterChangeStreamFactoryForTest::default();

        let indexer_pool = IndexerPool::default();
        let ingester_pool = IngesterPool::default();
        let ingester_id = NodeId::from("test-ingester");
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
        let shard =
            &control_plane_debug_info["shard_table"]["test-index:00000000000000000000000000"][0];
        assert_eq!(shard["shard_id"], "00000000000000000000");
        assert_eq!(shard["shard_state"], "closed");

        universe.assert_quit().await;
    }

    #[tokio::test]
    async fn test_control_plane_get_debug_info() {
        let universe = Universe::with_accelerated_time();

        let cluster_config = ClusterConfig::for_test();
        let node_id = NodeId::from("test-control-plane");
        let cluster_change_stream_factory = ClusterChangeStreamFactoryForTest::default();

        let indexer_pool = IndexerPool::default();
        let ingester_id = NodeId::from("test-ingester");

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
        let shard =
            &control_plane_debug_info["shard_table"]["test-index:00000000000000000000000000"][0];
        assert_eq!(shard["index_uid"], "test-index:00000000000000000000000000");
        assert_eq!(shard["source_id"], INGEST_V2_SOURCE_ID);
        assert_eq!(shard["shard_id"], "00000000000000000000");
        assert_eq!(shard["shard_state"], "open");
        assert_eq!(shard["leader_id"], "test-ingester");
        assert_eq!(shard["follower_id"], JsonValue::Null);
        assert_eq!(
            shard["publish_position_inclusive"],
            Position::Beginning.to_string()
        );

        universe.assert_quit().await;
    }
}
