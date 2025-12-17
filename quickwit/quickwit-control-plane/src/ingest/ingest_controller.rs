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

use std::collections::btree_map::Entry;
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::fmt;
use std::future::Future;
use std::num::NonZeroUsize;
use std::sync::Arc;
use std::time::Duration;

use fnv::FnvHashSet;
use futures::StreamExt;
use futures::stream::FuturesUnordered;
use itertools::{Itertools, MinMaxResult};
use quickwit_actors::Mailbox;
use quickwit_common::Progress;
use quickwit_common::pretty::PrettySample;
use quickwit_ingest::{IngesterPool, LeaderId, LocalShardsUpdate};
use quickwit_proto::control_plane::{
    AdviseResetShardsRequest, AdviseResetShardsResponse, GetOrCreateOpenShardsFailureReason,
    GetOrCreateOpenShardsRequest, GetOrCreateOpenShardsResponse, GetOrCreateOpenShardsSubrequest,
    GetOrCreateOpenShardsSuccess,
};
use quickwit_proto::ingest::ingester::{
    CloseShardsRequest, CloseShardsResponse, IngesterService, InitShardFailure,
    InitShardSubrequest, InitShardsRequest, InitShardsResponse, RetainShardsForSource,
    RetainShardsRequest,
};
use quickwit_proto::ingest::{
    Shard, ShardIdPosition, ShardIdPositions, ShardIds, ShardPKey, ShardState,
};
use quickwit_proto::metastore::{
    MetastoreResult, MetastoreService, MetastoreServiceClient, OpenShardSubrequest,
    OpenShardsRequest, OpenShardsResponse, serde_utils,
};
use quickwit_proto::types::{IndexUid, NodeId, NodeIdRef, Position, ShardId, SourceUid};
use rand::rngs::ThreadRng;
use rand::seq::SliceRandom;
use rand::{Rng, RngCore, thread_rng};
use serde::{Deserialize, Serialize};
use tokio::sync::{Mutex, OwnedMutexGuard};
use tokio::task::JoinHandle;
use tracing::{Level, debug, enabled, error, info, warn};
use ulid::Ulid;

use super::scaling_arbiter::ScalingArbiter;
use crate::control_plane::ControlPlane;
use crate::ingest::wait_handle::WaitHandle;
use crate::model::{ControlPlaneModel, ScalingMode, ShardEntry, ShardStats};

const CLOSE_SHARDS_REQUEST_TIMEOUT: Duration = if cfg!(test) {
    Duration::from_millis(50)
} else {
    Duration::from_secs(3)
};

const INIT_SHARDS_REQUEST_TIMEOUT: Duration = CLOSE_SHARDS_REQUEST_TIMEOUT;

const CLOSE_SHARDS_UPON_REBALANCE_DELAY: Duration = if cfg!(test) {
    Duration::ZERO
} else {
    Duration::from_secs(10)
};

const FIRE_AND_FORGET_TIMEOUT: Duration = Duration::from_secs(3);

/// Spawns a new task to execute the given future,
/// and stops polling it/drops it after a timeout.
///
/// All errors are ignored, and not even logged.
fn fire_and_forget(
    fut: impl Future<Output = ()> + Send + 'static,
    operation: impl std::fmt::Display + Send + 'static,
) {
    tokio::spawn(async move {
        if let Err(_timeout_elapsed) = tokio::time::timeout(FIRE_AND_FORGET_TIMEOUT, fut).await {
            error!(%operation, "timeout elapsed");
        }
    });
}

// Returns a random position of the els `slice`, such that the element in this array is NOT
// `except_el`.
fn pick_position(
    els: &[&NodeIdRef],
    except_el_opt: Option<&NodeIdRef>,
    rng: &mut ThreadRng,
) -> Option<usize> {
    let except_pos_opt =
        except_el_opt.and_then(|except_el| els.iter().position(|el| *el == except_el));
    if let Some(except_pos) = except_pos_opt {
        let pos = rng.gen_range(0..els.len() - 1);
        if pos >= except_pos {
            Some(pos + 1)
        } else {
            Some(pos)
        }
    } else {
        Some(rng.gen_range(0..els.len()))
    }
}

/// Pick a node from the `shard_count_to_node_ids` that is different from `except_node_opt`.
/// We pick in priority nodes with the least number of shards, and we break any tie randomly.
///
/// Once a node has been found, we update the `shard_count_to_node_ids` to reflect the new state.
/// In particular, the ingester node is moved from its previous shard_count level to its new
/// shard_count level. In particular, a shard_count entry that is empty should be removed from the
/// BTreeMap.
fn pick_one<'a>(
    shard_count_to_node_ids: &mut BTreeMap<usize, Vec<&'a NodeIdRef>>,
    except_node_opt: Option<&'a NodeIdRef>,
    rng: &mut ThreadRng,
) -> Option<&'a NodeIdRef> {
    let (&shard_count, _) = shard_count_to_node_ids.iter().find(|(_, node_ids)| {
        let Some(except_node) = except_node_opt else {
            return true;
        };
        if node_ids.len() >= 2 {
            return true;
        }
        let Some(&single_node_id) = node_ids.first() else {
            return false;
        };
        single_node_id != except_node
    })?;
    let mut shard_entry = shard_count_to_node_ids.entry(shard_count);
    let Entry::Occupied(occupied_shard_entry) = &mut shard_entry else {
        panic!();
    };
    let nodes = occupied_shard_entry.get_mut();
    let position = pick_position(nodes, except_node_opt, rng)?;

    let node_id = nodes.swap_remove(position);
    let new_shard_count = shard_count + 1;
    let should_remove_entry = nodes.is_empty();

    if should_remove_entry {
        shard_count_to_node_ids.remove(&shard_count);
    }
    shard_count_to_node_ids
        .entry(new_shard_count)
        .or_default()
        .push(node_id);
    Some(node_id)
}

/// Pick two ingester nodes from `shard_count_to_node_ids` different one from each other.
/// Ingesters with the lower number of shards are preferred.
fn pick_two<'a>(
    shard_count_to_node_ids: &mut BTreeMap<usize, Vec<&'a NodeIdRef>>,
    rng: &mut ThreadRng,
) -> Option<(&'a NodeIdRef, &'a NodeIdRef)> {
    let leader = pick_one(shard_count_to_node_ids, None, rng)?;
    let follower = pick_one(shard_count_to_node_ids, Some(leader), rng)?;
    Some((leader, follower))
}

fn allocate_shards(
    node_id_shard_counts: &HashMap<NodeId, usize>,
    num_shards: usize,
    replication_enabled: bool,
) -> Option<Vec<(&NodeIdRef, Option<&NodeIdRef>)>> {
    let mut shard_count_to_node_ids: BTreeMap<usize, Vec<&NodeIdRef>> = BTreeMap::default();
    for (node_id, &num_shards) in node_id_shard_counts {
        shard_count_to_node_ids
            .entry(num_shards)
            .or_default()
            .push(node_id.as_ref());
    }
    let mut rng = thread_rng();
    let mut shard_allocations: Vec<(&NodeIdRef, Option<&NodeIdRef>)> =
        Vec::with_capacity(num_shards);
    for _ in 0..num_shards {
        if replication_enabled {
            let (leader, follower) = pick_two(&mut shard_count_to_node_ids, &mut rng)?;
            shard_allocations.push((leader, Some(follower)));
        } else {
            let leader = pick_one(&mut shard_count_to_node_ids, None, &mut rng)?;
            shard_allocations.push((leader, None));
        }
    }
    Some(shard_allocations)
}

#[derive(Debug, Default, Clone, Copy, Serialize, Deserialize)]
pub struct IngestControllerStats {
    pub num_rebalance_shards_ops: usize,
}

pub struct IngestController {
    ingester_pool: IngesterPool,
    metastore: MetastoreServiceClient,
    replication_factor: usize,
    // This lock ensures that only one rebalance operation is performed at a time.
    rebalance_lock: Arc<Mutex<()>>,
    pub stats: IngestControllerStats,
    scaling_arbiter: ScalingArbiter,
}

impl fmt::Debug for IngestController {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.debug_struct("IngestController")
            .field("ingester_pool", &self.ingester_pool)
            .field("metastore", &self.metastore)
            .field("replication_factor", &self.replication_factor)
            .finish()
    }
}

/// Updates both the metastore and the control plane.
/// If successful, the control plane is guaranteed to be in sync with the metastore.
/// If an error is returned, the control plane might be out of sync with the metastore.
/// It is up to the client to check the error type and see if the control plane actor should be
/// restarted.
async fn open_shards_on_metastore_and_model(
    open_shard_subrequests: Vec<OpenShardSubrequest>,
    metastore: &mut MetastoreServiceClient,
    model: &mut ControlPlaneModel,
) -> MetastoreResult<OpenShardsResponse> {
    if open_shard_subrequests.is_empty() {
        return Ok(OpenShardsResponse {
            subresponses: Vec::new(),
        });
    }
    let open_shards_request = OpenShardsRequest {
        subrequests: open_shard_subrequests,
    };
    let open_shards_response = metastore.open_shards(open_shards_request).await?;
    for open_shard_subresponse in &open_shards_response.subresponses {
        if let Some(shard) = &open_shard_subresponse.open_shard {
            let shard = shard.clone();
            let index_uid = shard.index_uid().clone();
            let source_id = shard.source_id.clone();
            model.insert_shards(&index_uid, &source_id, vec![shard]);
        }
    }
    Ok(open_shards_response)
}

fn get_open_shard_from_model(
    get_open_shards_subrequest: &GetOrCreateOpenShardsSubrequest,
    model: &ControlPlaneModel,
    unavailable_leaders: &FnvHashSet<NodeId>,
) -> Result<Option<GetOrCreateOpenShardsSuccess>, GetOrCreateOpenShardsFailureReason> {
    let Some(index_uid) = model.index_uid(&get_open_shards_subrequest.index_id) else {
        return Err(GetOrCreateOpenShardsFailureReason::IndexNotFound);
    };
    let Some(open_shard_entries) = model.find_open_shards(
        index_uid,
        &get_open_shards_subrequest.source_id,
        unavailable_leaders,
    ) else {
        return Err(GetOrCreateOpenShardsFailureReason::SourceNotFound);
    };
    if open_shard_entries.is_empty() {
        return Ok(None);
    }
    // We already have open shards. Let's return them.
    let open_shards: Vec<Shard> = open_shard_entries
        .into_iter()
        .map(|shard_entry| shard_entry.shard)
        .collect();
    Ok(Some(GetOrCreateOpenShardsSuccess {
        subrequest_id: get_open_shards_subrequest.subrequest_id,
        index_uid: Some(index_uid.clone()),
        source_id: get_open_shards_subrequest.source_id.clone(),
        open_shards,
    }))
}

impl IngestController {
    pub fn new(
        metastore: MetastoreServiceClient,
        ingester_pool: IngesterPool,
        replication_factor: usize,
        max_shard_ingestion_throughput_mib_per_sec: f32,
        shard_scale_up_factor: f32,
    ) -> Self {
        IngestController {
            metastore,
            ingester_pool,
            replication_factor,
            rebalance_lock: Arc::new(Mutex::new(())),
            stats: IngestControllerStats::default(),
            scaling_arbiter: ScalingArbiter::with_max_shard_ingestion_throughput_mib_per_sec(
                max_shard_ingestion_throughput_mib_per_sec,
                shard_scale_up_factor,
            ),
        }
    }

    /// Sends a retain shard request to the given list of ingesters.
    ///
    /// If the request fails, we just log an error.
    pub(crate) fn sync_with_ingesters(
        &self,
        ingesters: &BTreeSet<NodeId>,
        model: &ControlPlaneModel,
    ) {
        for ingester in ingesters {
            self.sync_with_ingester(ingester, model);
        }
    }

    pub(crate) fn sync_with_all_ingesters(&self, model: &ControlPlaneModel) {
        let ingesters: Vec<NodeId> = self.ingester_pool.keys();
        for ingester in ingesters {
            self.sync_with_ingester(&ingester, model);
        }
    }

    /// Syncs the ingester in a fire and forget manner.
    ///
    /// The returned oneshot is just here for unit test to wait for the operation to terminate.
    fn sync_with_ingester(&self, ingester: &NodeId, model: &ControlPlaneModel) -> WaitHandle {
        info!(ingester = %ingester, "sync_with_ingester");
        let (wait_drop_guard, wait_handle) = WaitHandle::new();
        let Some(ingester_client) = self.ingester_pool.get(ingester) else {
            // TODO: (Maybe) We should mark the ingester as unavailable, and stop advertise its
            // shard to routers.
            warn!("failed to sync with ingester `{ingester}`: not available");
            return wait_handle;
        };
        let mut retain_shards_req = RetainShardsRequest::default();
        for (source_uid, shard_ids) in &*model.list_shards_for_node(ingester) {
            let shards_for_source = RetainShardsForSource {
                index_uid: Some(source_uid.index_uid.clone()),
                source_id: source_uid.source_id.clone(),
                shard_ids: shard_ids.iter().cloned().collect(),
            };
            retain_shards_req
                .retain_shards_for_sources
                .push(shards_for_source);
        }
        info!(ingester = %ingester, "retain shards ingester");
        let operation: String = format!("retain shards `{ingester}`");
        fire_and_forget(
            async move {
                if let Err(retain_shards_err) =
                    ingester_client.retain_shards(retain_shards_req).await
                {
                    error!(%retain_shards_err, "retain shards error");
                }
                // just a way to force moving the drop guard.
                drop(wait_drop_guard);
            },
            operation,
        );
        wait_handle
    }

    fn handle_closed_shards(&self, closed_shards: Vec<ShardIds>, model: &mut ControlPlaneModel) {
        for closed_shard in closed_shards {
            let index_uid: IndexUid = closed_shard.index_uid().clone();
            let source_id = closed_shard.source_id;

            let source_uid = SourceUid {
                index_uid,
                source_id,
            };
            let closed_shard_ids = model.close_shards(&source_uid, &closed_shard.shard_ids);

            if !closed_shard_ids.is_empty() {
                info!(
                    index_id=%source_uid.index_uid.index_id,
                    source_id=%source_uid.source_id,
                    shard_ids=?PrettySample::new(&closed_shard_ids, 5),
                    "closed {} shards reported by router",
                    closed_shard_ids.len()
                );
            }
        }
    }

    pub(crate) async fn handle_local_shards_update(
        &mut self,
        local_shards_update: LocalShardsUpdate,
        model: &mut ControlPlaneModel,
        progress: &Progress,
    ) -> MetastoreResult<()> {
        let shard_stats = model.update_shards(
            &local_shards_update.source_uid,
            &local_shards_update.shard_infos,
        );
        let min_shards = model
            .index_metadata(&local_shards_update.source_uid.index_uid)
            .expect("index should exist")
            .index_config
            .ingest_settings
            .min_shards;

        let Some(scaling_mode) = self.scaling_arbiter.should_scale(shard_stats, min_shards) else {
            return Ok(());
        };
        match scaling_mode {
            ScalingMode::Up(num_shards) => {
                self.try_scale_up_shards(
                    local_shards_update.source_uid,
                    shard_stats,
                    model,
                    progress,
                    num_shards,
                )
                .await?;
            }
            ScalingMode::Down => {
                self.try_scale_down_shards(
                    local_shards_update.source_uid,
                    shard_stats,
                    min_shards,
                    model,
                    progress,
                )
                .await?;
            }
        }

        Ok(())
    }

    /// Finds the open shards that satisfies the [`GetOrCreateOpenShardsRequest`] request sent by an
    /// ingest router. First, the control plane checks its internal shard table to find
    /// candidates. If it does not contain any, the control plane will ask
    /// the metastore to open new shards.
    pub(crate) async fn get_or_create_open_shards(
        &mut self,
        get_open_shards_request: GetOrCreateOpenShardsRequest,
        model: &mut ControlPlaneModel,
        progress: &Progress,
    ) -> MetastoreResult<GetOrCreateOpenShardsResponse> {
        // Closing shards is an operation performed by ingesters,
        // so the control plane is not necessarily aware that they are closed.
        //
        // Routers can report closed shards so that we can update our
        // internal state.
        self.handle_closed_shards(get_open_shards_request.closed_shards, model);

        let num_subrequests = get_open_shards_request.subrequests.len();
        let mut get_or_create_open_shards_successes = Vec::with_capacity(num_subrequests);
        let mut get_or_create_open_shards_failures = Vec::new();

        let mut per_source_num_shards_to_open = HashMap::new();

        let unavailable_leaders: FnvHashSet<NodeId> = get_open_shards_request
            .unavailable_leaders
            .into_iter()
            .map(NodeId::from)
            .collect();

        // We do a first pass to identify the shards that are missing from the model and need to be
        // created.
        for get_open_shards_subrequest in &get_open_shards_request.subrequests {
            if let Ok(None) =
                get_open_shard_from_model(get_open_shards_subrequest, model, &unavailable_leaders)
            {
                // We did not find any open shard in the model, we will have to create one.
                // Let's keep track of all of the source that require new shards, so we can batch
                // create them after this loop.
                let index_uid = model
                    .index_uid(&get_open_shards_subrequest.index_id)
                    .expect("index should exist")
                    .clone();
                let min_shards = model
                    .index_metadata(&index_uid)
                    .expect("index should exist")
                    .index_config
                    .ingest_settings
                    .min_shards
                    .get();
                let source_uid = SourceUid {
                    index_uid,
                    source_id: get_open_shards_subrequest.source_id.clone(),
                };
                per_source_num_shards_to_open.insert(source_uid, min_shards);
            }
        }

        if let Err(metastore_error) = self
            .try_open_shards(
                per_source_num_shards_to_open,
                model,
                &unavailable_leaders,
                progress,
            )
            .await
        {
            // We experienced a metastore error. If this is not certain abort, we need
            // to restart the control plane, to make sure the control plane is not out-of-sync.
            //
            if !metastore_error.is_transaction_certainly_aborted() {
                return Err(metastore_error);
            } else {
                // If not, let's just log something.
                // This is not critical. We will just end up return some failure in the response.
                error!(error=?metastore_error, "failed to open shards on the metastore");
            }
        }
        for get_open_shards_subrequest in get_open_shards_request.subrequests {
            match get_open_shard_from_model(
                &get_open_shards_subrequest,
                model,
                &unavailable_leaders,
            ) {
                Ok(Some(success)) => {
                    get_or_create_open_shards_successes.push(success);
                }
                Ok(None) => {
                    get_or_create_open_shards_failures.push(
                        GetOrCreateOpenShardsFailureReason::NoIngestersAvailable
                            .create_failure(get_open_shards_subrequest),
                    );
                }
                Err(failure_reason) => {
                    get_or_create_open_shards_failures
                        .push(failure_reason.create_failure(get_open_shards_subrequest));
                }
            }
        }
        let response = GetOrCreateOpenShardsResponse {
            successes: get_or_create_open_shards_successes,
            failures: get_or_create_open_shards_failures,
        };
        Ok(response)
    }

    /// Allocates and assigns new shards to ingesters.
    fn allocate_shards(
        &self,
        num_shards_to_allocate: usize,
        unavailable_leaders: &FnvHashSet<NodeId>,
        model: &ControlPlaneModel,
    ) -> Option<Vec<(NodeId, Option<NodeId>)>> {
        // Count of open shards per available ingester node (including the ingester with 0 open
        // shards).
        let mut per_node_num_open_shards: HashMap<NodeId, usize> = self
            .ingester_pool
            .keys()
            .into_iter()
            .filter(|ingester| !unavailable_leaders.contains(ingester))
            .map(|ingester| (ingester, 0))
            .collect();

        let num_ingesters = per_node_num_open_shards.len();

        if num_ingesters == 0 {
            warn!("failed to allocate {num_shards_to_allocate} shards: no ingesters available");
            return None;
        }

        if self.replication_factor > num_ingesters {
            warn!(
                "failed to allocate {num_shards_to_allocate} shards: replication factor is \
                 greater than the number of available ingesters"
            );
            return None;
        }

        for shard in model.all_shards() {
            if shard.is_open() && !unavailable_leaders.contains(&shard.leader_id) {
                for ingest_node in shard.ingesters() {
                    if let Some(shard_count) =
                        per_node_num_open_shards.get_mut(ingest_node.as_str())
                    {
                        *shard_count += 1;
                    } else {
                        // The shard is not present in the `per_node_num_open_shards` map.
                        // This is normal. It just means an ingester is temporarily unavailable,
                        // either from the control plane view (not present in the indexer pool,
                        // because as a result of information from
                        // chitchat), or because it is in the unavailable
                        // leaders map.
                    }
                }
            }
        }

        assert!(self.replication_factor == 1 || self.replication_factor == 2);
        let leader_follower_pairs: Vec<(&NodeIdRef, Option<&NodeIdRef>)> = allocate_shards(
            &per_node_num_open_shards,
            num_shards_to_allocate,
            self.replication_factor == 2,
        )?;
        Some(
            leader_follower_pairs
                .into_iter()
                .map(|(leader_id, follower_id)| {
                    (leader_id.to_owned(), follower_id.map(NodeIdRef::to_owned))
                })
                .collect(),
        )
    }

    /// Calls init shards on the leaders hosting newly opened shards.
    async fn init_shards(
        &self,
        init_shard_subrequests: Vec<InitShardSubrequest>,
        progress: &Progress,
    ) -> InitShardsResponse {
        let mut successes = Vec::with_capacity(init_shard_subrequests.len());
        let mut failures = Vec::new();

        let mut per_leader_shards_to_init: HashMap<String, Vec<InitShardSubrequest>> =
            HashMap::new();

        for init_shard_subrequest in init_shard_subrequests {
            let leader_id = init_shard_subrequest.shard().leader_id.clone();
            per_leader_shards_to_init
                .entry(leader_id)
                .or_default()
                .push(init_shard_subrequest);
        }
        let mut init_shards_futures = FuturesUnordered::new();

        for (leader_id, subrequests) in per_leader_shards_to_init {
            let init_shard_failures: Vec<InitShardFailure> = subrequests
                .iter()
                .map(|subrequest| {
                    let shard = subrequest.shard();

                    InitShardFailure {
                        subrequest_id: subrequest.subrequest_id,
                        index_uid: Some(shard.index_uid().clone()),
                        source_id: shard.source_id.clone(),
                        shard_id: Some(shard.shard_id().clone()),
                    }
                })
                .collect();
            let Some(leader) = self.ingester_pool.get(&leader_id) else {
                warn!("failed to init shards: ingester `{leader_id}` is unavailable");
                failures.extend(init_shard_failures);
                continue;
            };
            let init_shards_request = InitShardsRequest { subrequests };
            let init_shards_future = async move {
                let init_shards_result = tokio::time::timeout(
                    INIT_SHARDS_REQUEST_TIMEOUT,
                    leader.init_shards(init_shards_request),
                )
                .await;
                (leader_id.clone(), init_shards_result, init_shard_failures)
            };
            init_shards_futures.push(init_shards_future);
        }
        while let Some((leader_id, init_shards_result, init_shard_failures)) =
            progress.protect_future(init_shards_futures.next()).await
        {
            match init_shards_result {
                Ok(Ok(init_shards_response)) => {
                    successes.extend(init_shards_response.successes);
                    failures.extend(init_shards_response.failures);
                }
                Ok(Err(error)) => {
                    error!(%error, "failed to init shards on `{leader_id}`");
                    failures.extend(init_shard_failures);
                }
                Err(_elapsed) => {
                    error!("failed to init shards on `{leader_id}`: request timed out");
                    failures.extend(init_shard_failures);
                }
            }
        }
        InitShardsResponse {
            successes,
            failures,
        }
    }

    /// Attempts to increase the number of shards. This operation is rate limited to avoid creating
    /// to many shards in a short period of time. As a result, this method may not create any
    /// shard.
    async fn try_scale_up_shards(
        &mut self,
        source_uid: SourceUid,
        shard_stats: ShardStats,
        model: &mut ControlPlaneModel,
        progress: &Progress,
        num_shards_to_open: usize,
    ) -> MetastoreResult<()> {
        if !model
            .acquire_scaling_permits(&source_uid, ScalingMode::Up(num_shards_to_open))
            .unwrap_or(false)
        {
            return Ok(());
        }
        let new_num_open_shards = shard_stats.num_open_shards + num_shards_to_open;
        let new_shards_per_source: HashMap<SourceUid, usize> =
            HashMap::from_iter([(source_uid.clone(), num_shards_to_open)]);
        let successful_source_uids_res = self
            .try_open_shards(new_shards_per_source, model, &Default::default(), progress)
            .await;

        match successful_source_uids_res {
            Ok(successful_source_uids) => {
                assert!(successful_source_uids.len() <= 1);

                if successful_source_uids.is_empty() {
                    // We did not manage to create the shard.
                    // We can release our permit.
                    model.release_scaling_permits(&source_uid, ScalingMode::Up(num_shards_to_open));
                    warn!(
                        index_uid=%source_uid.index_uid,
                        source_id=%source_uid.source_id,
                        "scaling up number of shards to {new_num_open_shards} failed: shard initialization failure"
                    );
                } else {
                    info!(
                        index_id=%source_uid.index_uid.index_id,
                        source_id=%source_uid.source_id,
                        "successfully scaled up number of shards to {new_num_open_shards}"
                    );
                }
                Ok(())
            }
            Err(metastore_error) => {
                // We did not manage to create the shard.
                // We can release our permit, but we also need to return the error to the caller, in
                // order to restart the control plane actor if necessary.
                warn!(
                    index_id=%source_uid.index_uid.index_id,
                    source_id=%source_uid.source_id,
                    "scaling up number of shards to {new_num_open_shards} failed: {metastore_error:?}"
                );
                model.release_scaling_permits(&source_uid, ScalingMode::Up(num_shards_to_open));
                Err(metastore_error)
            }
        }
    }

    /// Attempts to open shards for different sources
    /// `source_uids` may contain the same source multiple times.
    ///
    /// This function returns the list of sources for which `try_open_shards` was successful.
    ///
    /// As long as no metastore error is returned this function leaves the control plane model
    /// in sync with the metastore.
    ///
    /// Also, this function only updates the control plane model and the metastore after
    /// having successfully initialized a shard (and possibly its replica) on the ingester.
    ///
    /// This function can be partially successful: if init_shards was unsuccessful for some shard,
    /// then the successfully initialized shard will still be record in the metastore/control
    /// plane model.
    ///
    /// The number of successfully open shards is returned.
    async fn try_open_shards(
        &mut self,
        per_source_num_shards_to_open: HashMap<SourceUid, usize>,
        model: &mut ControlPlaneModel,
        unavailable_leaders: &FnvHashSet<NodeId>,
        progress: &Progress,
    ) -> MetastoreResult<HashMap<SourceUid, usize>> {
        let total_num_shards_to_open: usize = per_source_num_shards_to_open.values().sum();

        if total_num_shards_to_open == 0 {
            return Ok(HashMap::new());
        }
        // TODO unavailable leaders
        let Some(leader_follower_pairs) =
            self.allocate_shards(total_num_shards_to_open, unavailable_leaders, model)
        else {
            return Ok(HashMap::new());
        };

        let source_uids_with_multiplicity = per_source_num_shards_to_open
            .iter()
            .flat_map(|(source_uid, count)| std::iter::repeat_n(source_uid, *count));

        let mut init_shard_subrequests: Vec<InitShardSubrequest> = Vec::new();

        for (subrequest_id, (source_uid, (leader_id, follower_id_opt))) in
            source_uids_with_multiplicity
                .zip(leader_follower_pairs)
                .enumerate()
        {
            let shard_id = ShardId::from(Ulid::new());

            let index_metadata = model
                .index_metadata(&source_uid.index_uid)
                .expect("index should exist");
            let has_transform = model
                .source_metadata(source_uid)
                .expect("source should exist")
                .transform_config
                .is_some();
            let validate_docs =
                index_metadata.index_config.ingest_settings.validate_docs && !has_transform;
            let doc_mapping = &index_metadata.index_config.doc_mapping;
            let doc_mapping_uid = doc_mapping.doc_mapping_uid;
            let doc_mapping_json = serde_utils::to_json_str(doc_mapping)?;

            let shard = Shard {
                index_uid: Some(source_uid.index_uid.clone()),
                source_id: source_uid.source_id.clone(),
                shard_id: Some(shard_id),
                leader_id: leader_id.to_string(),
                follower_id: follower_id_opt.as_ref().map(ToString::to_string),
                shard_state: ShardState::Open as i32,
                doc_mapping_uid: Some(doc_mapping_uid),
                publish_position_inclusive: Some(Position::Beginning),
                publish_token: None,
                update_timestamp: 0, // assigned later by the metastore
            };
            let init_shard_subrequest = InitShardSubrequest {
                subrequest_id: subrequest_id as u32,
                shard: Some(shard),
                doc_mapping_json,
                validate_docs,
            };
            init_shard_subrequests.push(init_shard_subrequest);
        }

        // Let's first attempt to initialize these shards.
        let init_shards_response = self.init_shards(init_shard_subrequests, progress).await;

        let open_shard_subrequests = init_shards_response
            .successes
            .into_iter()
            .enumerate()
            .map(|(subrequest_id, init_shard_success)| {
                let shard = init_shard_success.shard();

                OpenShardSubrequest {
                    subrequest_id: subrequest_id as u32,
                    index_uid: shard.index_uid.clone(),
                    source_id: shard.source_id.clone(),
                    shard_id: shard.shard_id.clone(),
                    leader_id: shard.leader_id.clone(),
                    follower_id: shard.follower_id.clone(),
                    doc_mapping_uid: shard.doc_mapping_uid,
                    // Shards are acquired by the ingest sources
                    publish_token: None,
                }
            })
            .collect();

        let open_shards_response = progress
            .protect_future(open_shards_on_metastore_and_model(
                open_shard_subrequests,
                &mut self.metastore,
                model,
            ))
            .await?;

        let mut per_source_num_opened_shards: HashMap<SourceUid, usize> = HashMap::new();

        for open_shard_subresponse in open_shards_response.subresponses {
            let source_uid = open_shard_subresponse.open_shard().source_uid();
            *per_source_num_opened_shards.entry(source_uid).or_default() += 1;
        }

        Ok(per_source_num_opened_shards)
    }

    /// Attempts to decrease the number of shards. This operation is rate limited to avoid closing
    /// shards too aggressively. As a result, this method may not close any shard.
    async fn try_scale_down_shards(
        &self,
        source_uid: SourceUid,
        shard_stats: ShardStats,
        min_shards: NonZeroUsize,
        model: &mut ControlPlaneModel,
        progress: &Progress,
    ) -> MetastoreResult<()> {
        // The scaling arbiter should not suggest scaling down if the number of shards is already
        // below the minimum, but we're just being defensive here.
        if shard_stats.num_open_shards <= min_shards.get() {
            return Ok(());
        }
        if !model
            .acquire_scaling_permits(&source_uid, ScalingMode::Down)
            .unwrap_or(false)
        {
            return Ok(());
        }
        let new_num_open_shards = shard_stats.num_open_shards - 1;

        info!(
            index_id=%source_uid.index_uid.index_id,
            source_id=%source_uid.source_id,
            "scaling down number of shards to {new_num_open_shards}"
        );
        let Some((leader_id, shard_id)) = find_scale_down_candidate(&source_uid, model) else {
            model.release_scaling_permits(&source_uid, ScalingMode::Down);
            return Ok(());
        };
        info!("scaling down shard {shard_id} from {leader_id}");
        let Some(ingester) = self.ingester_pool.get(&leader_id) else {
            model.release_scaling_permits(&source_uid, ScalingMode::Down);
            return Ok(());
        };
        let shard_pkeys = vec![ShardPKey {
            index_uid: Some(source_uid.index_uid.clone()),
            source_id: source_uid.source_id.clone(),
            shard_id: Some(shard_id.clone()),
        }];
        let close_shards_request = CloseShardsRequest { shard_pkeys };

        if let Err(error) = progress
            .protect_future(ingester.close_shards(close_shards_request))
            .await
        {
            warn!("failed to scale down number of shards: {error}");
            model.release_scaling_permits(&source_uid, ScalingMode::Down);
            return Ok(());
        }
        model.close_shards(&source_uid, &[shard_id]);
        Ok(())
    }

    pub(crate) fn advise_reset_shards(
        &self,
        request: AdviseResetShardsRequest,
        model: &ControlPlaneModel,
    ) -> AdviseResetShardsResponse {
        info!(
            "received advise reset shards request from `{}`",
            request.ingester_id
        );
        debug!(shard_ids=?summarize_shard_ids(&request.shard_ids), "advise reset shards");

        let mut shards_to_delete: Vec<ShardIds> = Vec::new();
        let mut shards_to_truncate: Vec<ShardIdPositions> = Vec::new();

        for shard_ids in request.shard_ids {
            let index_uid = shard_ids.index_uid().clone();
            let source_id = shard_ids.source_id.clone();

            let source_uid = SourceUid {
                index_uid,
                source_id,
            };
            let Some(shard_entries) = model.get_shards_for_source(&source_uid) else {
                // The source no longer exists: we can safely delete all the shards.
                shards_to_delete.push(shard_ids);
                continue;
            };
            let mut shard_ids_to_delete = Vec::new();
            let mut shard_positions_to_truncate = Vec::new();

            for shard_id in shard_ids.shard_ids {
                if let Some(shard_entry) = shard_entries.get(&shard_id) {
                    let publish_position_inclusive = shard_entry.publish_position_inclusive();

                    shard_positions_to_truncate.push(ShardIdPosition {
                        shard_id: Some(shard_id),
                        publish_position_inclusive: Some(publish_position_inclusive),
                    });
                } else {
                    shard_ids_to_delete.push(shard_id);
                }
            }
            if !shard_ids_to_delete.is_empty() {
                shards_to_delete.push(ShardIds {
                    index_uid: Some(source_uid.index_uid.clone()),
                    source_id: source_uid.source_id.clone(),
                    shard_ids: shard_ids_to_delete,
                });
            }
            if !shard_positions_to_truncate.is_empty() {
                shards_to_truncate.push(ShardIdPositions {
                    index_uid: Some(source_uid.index_uid),
                    source_id: source_uid.source_id,
                    shard_positions: shard_positions_to_truncate,
                });
            }
        }
        if enabled!(Level::DEBUG) {
            let shards_to_truncate: Vec<(&str, Position)> = shards_to_truncate
                .iter()
                .flat_map(|shard_positions| {
                    shard_positions
                        .shard_positions
                        .iter()
                        .map(|shard_id_position| {
                            (
                                shard_id_position.shard_id().as_str(),
                                shard_id_position.publish_position_inclusive(),
                            )
                        })
                })
                .collect();
            debug!(shard_ids_to_delete=?summarize_shard_ids(&shards_to_delete), shards_to_truncate=?shards_to_truncate, "advise reset shards response");
        }

        AdviseResetShardsResponse {
            shards_to_delete,
            shards_to_truncate,
        }
    }

    /// Rebalances shards from ingesters with too many shards to ingesters with too few shards.
    /// Moving a shard consists of closing the shard on the source ingester and opening a new
    /// one on the target ingester.
    ///
    /// This method is guarded by a lock to ensure that only one rebalance operation is performed at
    /// a time.
    pub(crate) async fn rebalance_shards(
        &mut self,
        model: &mut ControlPlaneModel,
        mailbox: &Mailbox<ControlPlane>,
        progress: &Progress,
    ) -> MetastoreResult<Option<JoinHandle<()>>> {
        let Ok(rebalance_guard) = self.rebalance_lock.clone().try_lock_owned() else {
            debug!("skipping rebalance: another rebalance is already in progress");
            return Ok(None);
        };
        self.stats.num_rebalance_shards_ops += 1;

        let shards_to_rebalance: Vec<Shard> = self.compute_shards_to_rebalance(model);

        crate::metrics::CONTROL_PLANE_METRICS
            .rebalance_shards
            .set(shards_to_rebalance.len() as i64);

        if shards_to_rebalance.is_empty() {
            return Ok(None);
        }
        let mut per_source_num_shards_to_open: HashMap<SourceUid, usize> = HashMap::new();

        for shard in &shards_to_rebalance {
            *per_source_num_shards_to_open
                .entry(shard.source_uid())
                .or_default() += 1;
        }

        let mut per_source_num_opened_shards: HashMap<SourceUid, usize> = self
            .try_open_shards(
                per_source_num_shards_to_open,
                model,
                &Default::default(),
                progress,
            )
            .await
            .inspect_err(|error| {
                error!(%error, "failed to open shards during rebalance");
                crate::metrics::CONTROL_PLANE_METRICS
                    .rebalance_shards
                    .set(0);
            })?;

        let num_opened_shards: usize = per_source_num_opened_shards.values().sum();

        crate::metrics::CONTROL_PLANE_METRICS
            .rebalance_shards
            .set(num_opened_shards as i64);

        for source_uid in per_source_num_opened_shards.keys() {
            // We temporarily disable the ability the scale down the number of shards for
            // the source to avoid closing the shards we just opened.
            model.drain_scaling_permits(source_uid, ScalingMode::Down);
        }

        // Close as many shards as we opened. Because `try_open_shards` might fail partially, we
        // must only close the shards that we successfully opened.
        let mut shards_to_close = Vec::with_capacity(shards_to_rebalance.len());
        for shard in shards_to_rebalance {
            let source_uid = shard.source_uid();
            let Some(num_open_shards) = per_source_num_opened_shards.get_mut(&source_uid) else {
                continue;
            };
            if *num_open_shards == 0 {
                continue;
            };
            *num_open_shards -= 1;
            shards_to_close.push(shard);
        }

        let mailbox_clone = mailbox.clone();

        let close_shards_fut = self.close_shards(shards_to_close);

        let close_shards_and_send_callback_fut = async move {
            // We wait for a few seconds before closing the shards to give the ingesters some time
            // to learn about the ones we just opened via gossip.
            tokio::time::sleep(CLOSE_SHARDS_UPON_REBALANCE_DELAY).await;

            let closed_shards = close_shards_fut.await;

            if closed_shards.is_empty() {
                return;
            }
            let callback = RebalanceShardsCallback {
                closed_shards,
                rebalance_guard,
            };
            let _ = mailbox_clone.send_message(callback).await;
        };

        Ok(Some(tokio::spawn(close_shards_and_send_callback_fut)))
    }

    /// This method just "computes"" the number of shards to move for rebalance.
    /// It does not run any side effect except logging.
    ///
    /// TODO: We consider the number of available (i.e. alive according to chitchat) ingesters for
    /// this computation, but deal with the entire number of shards here.
    /// This could cause problems when dealing with a lot of unavailable ingesters.
    ///
    /// On the other hand it biases thing the "right way":
    /// If we are missing some ingesters, their shards should still be in the model, but they should
    /// be missing from the ingester pool.
    ///
    /// As a result `target_num_open_shards_per_leader` should be inflated.
    ///
    /// TODO: This implementation does not consider replicas.
    fn compute_shards_to_rebalance(&self, model: &ControlPlaneModel) -> Vec<Shard> {
        let ingester_ids: Vec<NodeId> = self.ingester_pool.keys();
        let num_ingesters = ingester_ids.len();

        if num_ingesters == 0 {
            debug!("no ingesters available");
            return Vec::new();
        }
        if num_ingesters < 2 {
            return Vec::new();
        }
        let mut num_open_shards: usize = 0;
        let mut per_leader_open_shards: HashMap<&str, Vec<&Shard>> = HashMap::new();

        for shard in model.all_shards() {
            if shard.is_open() {
                num_open_shards += 1;
                per_leader_open_shards
                    .entry(&shard.leader_id)
                    .or_default()
                    .push(&shard.shard);
            }
        }
        for ingester_id in &ingester_ids {
            per_leader_open_shards
                .entry(ingester_id.as_str())
                .or_default();
        }
        let target_num_open_shards_per_leader = num_open_shards as f32 / num_ingesters as f32;
        let max_num_open_shards_per_leader =
            f32::ceil(target_num_open_shards_per_leader * 1.1) as usize;
        let min_num_open_shards_per_leader =
            f32::floor(target_num_open_shards_per_leader * 0.9) as usize;

        let mut rng = thread_rng();
        let mut per_leader_open_shard_shuffled: Vec<Vec<&Shard>> = per_leader_open_shards
            .into_values()
            .map(|mut shards| {
                shards.shuffle(&mut rng);
                shards
            })
            .collect();

        let mut shards_to_rebalance: Vec<Shard> = Vec::new();

        loop {
            let MinMaxResult::MinMax(min_shards, max_shards) = per_leader_open_shard_shuffled
                .iter_mut()
                .minmax_by_key(|shards| shards.len())
            else {
                break;
            };
            if min_shards.len() < min_num_open_shards_per_leader
                || max_shards.len() > max_num_open_shards_per_leader
            {
                let shard = max_shards.pop().expect("shards should not be empty");
                shards_to_rebalance.push(shard.clone());
                min_shards.push(shard);
            } else {
                break;
            }
        }
        let num_shards_to_rebalance = shards_to_rebalance.len();

        if num_shards_to_rebalance == 0 {
            debug!("no shards to rebalance");
        } else {
            info!(
                num_open_shards,
                num_available_ingesters = num_ingesters,
                min_shards_threshold = min_num_open_shards_per_leader,
                max_shards_threshold = max_num_open_shards_per_leader,
                num_shards_to_rebalance,
                "rebalancing {num_shards_to_rebalance} shards"
            );
        }
        shards_to_rebalance
    }

    fn close_shards(
        &self,
        shards_to_close: Vec<Shard>,
    ) -> impl Future<Output = Vec<ShardPKey>> + Send + 'static {
        let mut per_leader_shards_to_close: HashMap<LeaderId, Vec<ShardPKey>> = HashMap::new();

        for shard in shards_to_close {
            let shard_pkey = ShardPKey {
                index_uid: shard.index_uid,
                source_id: shard.source_id,
                shard_id: shard.shard_id,
            };
            let leader_id = NodeId::from(shard.leader_id);
            per_leader_shards_to_close
                .entry(leader_id)
                .or_default()
                .push(shard_pkey);
        }
        let mut close_shards_futures = FuturesUnordered::new();

        for (leader_id, shard_pkeys) in per_leader_shards_to_close {
            let Some(ingester) = self.ingester_pool.get(&leader_id) else {
                warn!("failed to close shards: ingester `{leader_id}` is unavailable");
                continue;
            };
            let shards_to_close_request = CloseShardsRequest { shard_pkeys };
            let close_shards_future = async move {
                tokio::time::timeout(
                    CLOSE_SHARDS_REQUEST_TIMEOUT,
                    ingester.close_shards(shards_to_close_request),
                )
                .await
            };
            close_shards_futures.push(close_shards_future);
        }
        async move {
            let mut closed_shards = Vec::new();

            while let Some(close_shards_result) = close_shards_futures.next().await {
                match close_shards_result {
                    Ok(Ok(CloseShardsResponse { successes })) => {
                        closed_shards.extend(successes);
                    }
                    Ok(Err(error)) => {
                        error!(%error, "failed to close shards");
                    }
                    Err(_elapsed) => {
                        error!("close shards request timed out");
                    }
                }
            }
            closed_shards
        }
    }
}

fn summarize_shard_ids(shard_ids: &[ShardIds]) -> Vec<&str> {
    shard_ids
        .iter()
        .flat_map(|source_shard_ids| {
            source_shard_ids
                .shard_ids
                .iter()
                .map(|shard_id| shard_id.as_str())
        })
        .collect()
}

/// When rebalancing shards, shards to move are closed some time after new shards are opened.
/// Because we don't want to stall the control plane event loop while waiting for the close shards
/// requests to complete, we use a callback to handle the results of those close shards requests.
#[derive(Debug)]
pub(crate) struct RebalanceShardsCallback {
    pub closed_shards: Vec<ShardPKey>,
    pub rebalance_guard: OwnedMutexGuard<()>,
}

/// Finds a shard on the ingester with the highest number of open
/// shards for this source.
///
/// If multiple shards are hosted on that ingester, the shard with the lowest (oldest)
/// shard ID is chosen.
fn find_scale_down_candidate(
    source_uid: &SourceUid,
    model: &ControlPlaneModel,
) -> Option<(NodeId, ShardId)> {
    let mut per_leader_shard_entries: HashMap<&String, Vec<&ShardEntry>> = HashMap::new();
    let mut rng = thread_rng();

    for shard in model.get_shards_for_source(source_uid)?.values() {
        if shard.is_open() {
            per_leader_shard_entries
                .entry(&shard.leader_id)
                .or_default()
                .push(shard);
        }
    }
    per_leader_shard_entries
        .into_iter()
        // We use a random number to break ties... The HashMap is randomly seeded so this is
        // should not make much difference, but we might want to be as explicit as possible.
        .max_by_key(|(_leader_id, shard_entries)| (shard_entries.len(), rng.next_u32()))
        .map(|(leader_id, shard_entries)| {
            (
                leader_id.clone().into(),
                shard_entries.choose(&mut rng).unwrap().shard_id().clone(),
            )
        })
}

#[cfg(test)]
mod tests {

    use std::collections::BTreeSet;
    use std::str::FromStr;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use itertools::Itertools;
    use quickwit_actors::Universe;
    use quickwit_common::setup_logging_for_tests;
    use quickwit_common::shared_consts::DEFAULT_SHARD_THROUGHPUT_LIMIT;
    use quickwit_common::tower::DelayLayer;
    use quickwit_config::{DocMapping, INGEST_V2_SOURCE_ID, SourceConfig};
    use quickwit_ingest::{RateMibPerSec, ShardInfo};
    use quickwit_metastore::IndexMetadata;
    use quickwit_proto::control_plane::GetOrCreateOpenShardsSubrequest;
    use quickwit_proto::ingest::ingester::{
        CloseShardsResponse, IngesterServiceClient, InitShardSuccess, InitShardsResponse,
        MockIngesterService, RetainShardsResponse,
    };
    use quickwit_proto::ingest::{IngestV2Error, Shard, ShardState};
    use quickwit_proto::metastore::{
        self, MetastoreError, MockMetastoreService, OpenShardSubresponse,
    };
    use quickwit_proto::types::{DocMappingUid, Position, SourceId};

    use super::*;

    const TEST_SHARD_THROUGHPUT_LIMIT_MIB: f32 =
        DEFAULT_SHARD_THROUGHPUT_LIMIT.as_u64() as f32 / quickwit_common::shared_consts::MIB as f32;

    #[tokio::test]
    async fn test_ingest_controller_get_or_create_open_shards() {
        let source_id: &'static str = "test-source";

        let index_id_0 = "test-index-0";
        let mut index_metadata_0 =
            IndexMetadata::for_test(index_id_0, "ram://indexes/test-index-0");
        let index_uid_0 = index_metadata_0.index_uid.clone();

        let doc_mapping_uid_0 = DocMappingUid::random();
        index_metadata_0.index_config.doc_mapping.doc_mapping_uid = doc_mapping_uid_0;

        let index_id_1 = "test-index-1";
        let mut index_metadata_1 =
            IndexMetadata::for_test(index_id_1, "ram://indexes/test-index-1");
        let index_uid_1 = index_metadata_1.index_uid.clone();

        let doc_mapping_uid_1 = DocMappingUid::random();
        index_metadata_1.index_config.doc_mapping.doc_mapping_uid = doc_mapping_uid_1;

        let progress = Progress::default();

        let mut mock_metastore = MockMetastoreService::new();
        mock_metastore.expect_open_shards().once().returning({
            let index_uid_1 = index_uid_1.clone();

            move |request| {
                assert_eq!(request.subrequests.len(), 1);
                assert_eq!(request.subrequests[0].index_uid(), &index_uid_1);
                assert_eq!(request.subrequests[0].source_id, source_id);
                assert_eq!(request.subrequests[0].doc_mapping_uid(), doc_mapping_uid_1);

                let subresponses = vec![metastore::OpenShardSubresponse {
                    subrequest_id: 1,
                    open_shard: Some(Shard {
                        index_uid: index_uid_1.clone().into(),
                        source_id: source_id.to_string(),
                        shard_id: Some(ShardId::from(1)),
                        shard_state: ShardState::Open as i32,
                        leader_id: "test-ingester-2".to_string(),
                        doc_mapping_uid: Some(doc_mapping_uid_1),
                        ..Default::default()
                    }),
                }];
                let response = metastore::OpenShardsResponse { subresponses };
                Ok(response)
            }
        });
        let metastore = MetastoreServiceClient::from_mock(mock_metastore);

        let mock_ingester = MockIngesterService::new();
        let ingester = IngesterServiceClient::from_mock(mock_ingester);

        let ingester_pool = IngesterPool::default();
        ingester_pool.insert(NodeId::from("test-ingester-1"), ingester.clone());

        let mut mock_ingester = MockIngesterService::new();
        let index_uid_1_clone = index_uid_1.clone();
        mock_ingester
            .expect_init_shards()
            .once()
            .returning(move |request| {
                assert_eq!(request.subrequests.len(), 1);

                let subrequest = &request.subrequests[0];

                let shard = subrequest.shard();
                assert_eq!(shard.index_uid(), &index_uid_1_clone);
                assert_eq!(shard.source_id, source_id);
                assert_eq!(shard.leader_id, "test-ingester-2");

                let successes = vec![InitShardSuccess {
                    subrequest_id: request.subrequests[0].subrequest_id,
                    shard: Some(shard.clone()),
                }];
                let response = InitShardsResponse {
                    successes,
                    failures: Vec::new(),
                };
                Ok(response)
            });
        let ingester = IngesterServiceClient::from_mock(mock_ingester);
        ingester_pool.insert(NodeId::from("test-ingester-2"), ingester.clone());

        let replication_factor = 2;
        let mut controller = IngestController::new(
            metastore,
            ingester_pool.clone(),
            replication_factor,
            TEST_SHARD_THROUGHPUT_LIMIT_MIB,
            1.001,
        );

        let mut model = ControlPlaneModel::default();
        model.add_index(index_metadata_0.clone());
        model.add_index(index_metadata_1.clone());

        let mut source_config = SourceConfig::ingest_v2();
        source_config.source_id = source_id.to_string();

        model
            .add_source(&index_uid_0, source_config.clone())
            .unwrap();
        model.add_source(&index_uid_1, source_config).unwrap();

        let shards = vec![
            Shard {
                index_uid: index_uid_0.clone().into(),
                source_id: source_id.to_string(),
                shard_id: Some(ShardId::from(1)),
                leader_id: "test-ingester-0".to_string(),
                shard_state: ShardState::Open as i32,
                doc_mapping_uid: Some(doc_mapping_uid_0),
                ..Default::default()
            },
            Shard {
                index_uid: index_uid_0.clone().into(),
                source_id: source_id.to_string(),
                shard_id: Some(ShardId::from(2)),
                leader_id: "test-ingester-1".to_string(),
                shard_state: ShardState::Open as i32,
                doc_mapping_uid: Some(doc_mapping_uid_0),
                ..Default::default()
            },
        ];

        model.insert_shards(&index_uid_0, &source_id.into(), shards);

        let request = GetOrCreateOpenShardsRequest {
            subrequests: Vec::new(),
            closed_shards: Vec::new(),
            unavailable_leaders: Vec::new(),
        };
        let response = controller
            .get_or_create_open_shards(request, &mut model, &progress)
            .await
            .unwrap();

        assert_eq!(response.successes.len(), 0);
        assert_eq!(response.failures.len(), 0);

        let subrequests = vec![
            GetOrCreateOpenShardsSubrequest {
                subrequest_id: 0,
                index_id: "test-index-0".to_string(),
                source_id: source_id.to_string(),
            },
            GetOrCreateOpenShardsSubrequest {
                subrequest_id: 1,
                index_id: "test-index-1".to_string(),
                source_id: source_id.to_string(),
            },
            GetOrCreateOpenShardsSubrequest {
                subrequest_id: 2,
                index_id: "index-not-found".to_string(),
                source_id: "source-not-found".to_string(),
            },
            GetOrCreateOpenShardsSubrequest {
                subrequest_id: 3,
                index_id: "test-index-0".to_string(),
                source_id: "source-not-found".to_string(),
            },
        ];
        let closed_shards = Vec::new();
        let unavailable_leaders = vec!["test-ingester-0".to_string()];
        let request = GetOrCreateOpenShardsRequest {
            subrequests,
            closed_shards,
            unavailable_leaders,
        };
        let response = controller
            .get_or_create_open_shards(request, &mut model, &progress)
            .await
            .unwrap();

        assert_eq!(response.successes.len(), 2);
        assert_eq!(response.failures.len(), 2);

        let success = &response.successes[0];
        assert_eq!(success.subrequest_id, 0);
        assert_eq!(success.index_uid(), &index_uid_0);
        assert_eq!(success.source_id, source_id);
        assert_eq!(success.open_shards.len(), 1);
        assert_eq!(success.open_shards[0].shard_id(), ShardId::from(2));
        assert_eq!(success.open_shards[0].leader_id, "test-ingester-1");
        assert_eq!(success.open_shards[0].doc_mapping_uid(), doc_mapping_uid_0);

        let success = &response.successes[1];
        assert_eq!(success.subrequest_id, 1);
        assert_eq!(success.index_uid(), &index_uid_1);
        assert_eq!(success.source_id, source_id);
        assert_eq!(success.open_shards.len(), 1);
        assert_eq!(success.open_shards[0].shard_id(), ShardId::from(1));
        assert_eq!(success.open_shards[0].leader_id, "test-ingester-2");
        assert_eq!(success.open_shards[0].doc_mapping_uid(), doc_mapping_uid_1);

        let failure = &response.failures[0];
        assert_eq!(failure.subrequest_id, 2);
        assert_eq!(failure.index_id, "index-not-found");
        assert_eq!(failure.source_id, "source-not-found");
        assert_eq!(
            failure.reason(),
            GetOrCreateOpenShardsFailureReason::IndexNotFound
        );

        let failure = &response.failures[1];
        assert_eq!(failure.subrequest_id, 3);
        assert_eq!(failure.index_id, index_id_0);
        assert_eq!(failure.source_id, "source-not-found");
        assert_eq!(
            failure.reason(),
            GetOrCreateOpenShardsFailureReason::SourceNotFound
        );

        assert_eq!(model.num_shards(), 3);
    }

    #[tokio::test]
    async fn test_ingest_controller_get_or_create_open_shards_metastore_failure() {
        let source_id: &'static str = "test-source";

        let index_id_0 = "test-index-0";
        let index_metadata_0 = IndexMetadata::for_test(index_id_0, "ram://indexes/test-index-0");
        let index_uid_0 = index_metadata_0.index_uid.clone();
        let index_uid_0_clone = index_uid_0.clone();

        let progress = Progress::default();

        let mut mock_metastore = MockMetastoreService::new();
        mock_metastore
            .expect_open_shards()
            .once()
            .returning(move |_| {
                Err(MetastoreError::Internal {
                    message: "this error could be mean anything. transaction success or failure!"
                        .to_string(),
                    cause: "".to_string(),
                })
            });
        let metastore = MetastoreServiceClient::from_mock(mock_metastore);

        let mut mock_ingester = MockIngesterService::new();
        mock_ingester
            .expect_init_shards()
            .once()
            .returning(move |request| {
                assert_eq!(request.subrequests.len(), 1);

                let subrequest = &request.subrequests[0];

                let shard = subrequest.shard();
                assert_eq!(shard.index_uid(), &index_uid_0);
                assert_eq!(shard.source_id, source_id);
                assert_eq!(shard.leader_id, "test-ingester-1");

                let successes = vec![InitShardSuccess {
                    subrequest_id: request.subrequests[0].subrequest_id,
                    shard: Some(shard.clone()),
                }];
                let response = InitShardsResponse {
                    successes,
                    failures: Vec::new(),
                };
                Ok(response)
            });
        let ingester = IngesterServiceClient::from_mock(mock_ingester);

        let ingester_pool = IngesterPool::default();
        ingester_pool.insert(NodeId::from("test-ingester-1"), ingester.clone());

        let replication_factor = 1;
        let mut controller = IngestController::new(
            metastore,
            ingester_pool,
            replication_factor,
            TEST_SHARD_THROUGHPUT_LIMIT_MIB,
            1.001,
        );

        let mut model = ControlPlaneModel::default();
        model.add_index(index_metadata_0.clone());

        let mut source_config = SourceConfig::ingest_v2();
        source_config.source_id = source_id.to_string();

        model
            .add_source(&index_uid_0_clone, source_config.clone())
            .unwrap();

        let subrequests = vec![GetOrCreateOpenShardsSubrequest {
            subrequest_id: 0,
            index_id: "test-index-0".to_string(),
            source_id: source_id.to_string(),
        }];
        let request = GetOrCreateOpenShardsRequest {
            subrequests,
            closed_shards: Vec::new(),
            unavailable_leaders: Vec::new(),
        };

        let metastore_error = controller
            .get_or_create_open_shards(request, &mut model, &progress)
            .await
            .unwrap_err();

        assert!(!metastore_error.is_transaction_certainly_aborted());
    }

    #[tokio::test]
    async fn test_ingest_controller_get_open_shards_handles_closed_shards() {
        let metastore = MetastoreServiceClient::mocked();
        let ingester_pool = IngesterPool::default();
        let replication_factor = 2;

        let mut controller = IngestController::new(
            metastore,
            ingester_pool,
            replication_factor,
            TEST_SHARD_THROUGHPUT_LIMIT_MIB,
            1.001,
        );
        let mut model = ControlPlaneModel::default();

        let index_uid = IndexUid::for_test("test-index-0", 0);
        let source_id: SourceId = "test-source".to_string();

        let shards = vec![Shard {
            shard_id: Some(ShardId::from(1)),
            index_uid: Some(index_uid.clone()),
            source_id: source_id.clone(),
            leader_id: "test-ingester-0".to_string(),
            shard_state: ShardState::Open as i32,
            ..Default::default()
        }];
        model.insert_shards(&index_uid, &source_id, shards);

        let request = GetOrCreateOpenShardsRequest {
            subrequests: Vec::new(),
            closed_shards: vec![ShardIds {
                index_uid: index_uid.clone().into(),
                source_id: source_id.clone(),
                shard_ids: vec![ShardId::from(1), ShardId::from(2)],
            }],
            unavailable_leaders: Vec::new(),
        };
        let progress = Progress::default();

        controller
            .get_or_create_open_shards(request, &mut model, &progress)
            .await
            .unwrap();

        let shard_1 = model
            .all_shards()
            .find(|shard| shard.shard_id() == ShardId::from(1))
            .unwrap();
        assert!(shard_1.is_closed());
    }

    #[test]
    fn test_ingest_controller_allocate_shards() {
        let metastore = MetastoreServiceClient::mocked();
        let ingester_pool = IngesterPool::default();
        let replication_factor = 2;

        let controller = IngestController::new(
            metastore,
            ingester_pool.clone(),
            replication_factor,
            TEST_SHARD_THROUGHPUT_LIMIT_MIB,
            1.001,
        );

        let mut model = ControlPlaneModel::default();

        let leader_follower_pairs_opt =
            controller.allocate_shards(0, &FnvHashSet::default(), &model);
        assert!(leader_follower_pairs_opt.is_none());

        ingester_pool.insert(
            NodeId::from("test-ingester-1"),
            IngesterServiceClient::mocked(),
        );

        let leader_follower_pairs_opt =
            controller.allocate_shards(0, &FnvHashSet::default(), &model);

        // We have only one node so with a replication factor of 2, we can't
        // find any solution.
        assert!(leader_follower_pairs_opt.is_none());

        ingester_pool.insert("test-ingester-2".into(), IngesterServiceClient::mocked());

        let leader_follower_pairs = controller
            .allocate_shards(0, &FnvHashSet::default(), &model)
            .unwrap();

        // We tried to allocate 0 shards, so an empty vec makes sense.
        assert!(leader_follower_pairs.is_empty());

        let leader_follower_pairs = controller
            .allocate_shards(1, &FnvHashSet::default(), &model)
            .unwrap();

        assert_eq!(leader_follower_pairs.len(), 1);

        // The leader follower is picked at random: both ingester have the same number of shards.
        if leader_follower_pairs[0].0 == "test-ingester-1" {
            assert_eq!(
                leader_follower_pairs[0].1,
                Some(NodeId::from("test-ingester-2"))
            );
        } else {
            assert_eq!(leader_follower_pairs[0].0, "test-ingester-2");
            assert_eq!(
                leader_follower_pairs[0].1,
                Some(NodeId::from("test-ingester-1"))
            );
        }

        let leader_follower_pairs = controller
            .allocate_shards(2, &FnvHashSet::default(), &model)
            .unwrap();
        assert_eq!(leader_follower_pairs.len(), 2);

        for leader_follower_pair in leader_follower_pairs {
            if leader_follower_pair.0 == "test-ingester-1" {
                assert_eq!(
                    leader_follower_pair.1,
                    Some(NodeId::from("test-ingester-2"))
                );
            } else {
                assert_eq!(leader_follower_pair.0, "test-ingester-2");
                assert_eq!(
                    leader_follower_pair.1,
                    Some(NodeId::from("test-ingester-1"))
                );
            }
        }

        let leader_follower_pairs = controller
            .allocate_shards(3, &FnvHashSet::default(), &model)
            .unwrap();
        assert_eq!(leader_follower_pairs.len(), 3);
        let index_uid = IndexUid::for_test("test-index", 0);

        let source_id: SourceId = "test-source".to_string();
        let open_shards = vec![Shard {
            index_uid: Some(index_uid.clone()),
            source_id: source_id.clone(),
            shard_id: Some(ShardId::from(1)),
            shard_state: ShardState::Open as i32,
            leader_id: "test-ingester-1".to_string(),
            ..Default::default()
        }];
        model.insert_shards(&index_uid, &source_id, open_shards);

        let leader_follower_pairs = controller
            .allocate_shards(3, &FnvHashSet::default(), &model)
            .unwrap();
        assert_eq!(leader_follower_pairs.len(), 3);
        assert_eq!(leader_follower_pairs[0].0, "test-ingester-2");
        assert_eq!(
            leader_follower_pairs[0].1,
            Some(NodeId::from("test-ingester-1"))
        );

        assert_eq!(leader_follower_pairs[1].0, "test-ingester-2");
        assert_eq!(
            leader_follower_pairs[1].1,
            Some(NodeId::from("test-ingester-1"))
        );

        assert_eq!(leader_follower_pairs[2].0, "test-ingester-2");
        assert_eq!(
            leader_follower_pairs[2].1,
            Some(NodeId::from("test-ingester-1"))
        );

        let open_shards = vec![
            Shard {
                index_uid: Some(index_uid.clone()),
                source_id: source_id.clone(),
                shard_id: Some(ShardId::from(2)),
                shard_state: ShardState::Open as i32,
                leader_id: "test-ingester-1".to_string(),
                ..Default::default()
            },
            Shard {
                index_uid: Some(index_uid.clone()),
                source_id: source_id.clone(),
                shard_id: Some(ShardId::from(3)),
                shard_state: ShardState::Open as i32,
                leader_id: "test-ingester-1".to_string(),
                ..Default::default()
            },
        ];
        model.insert_shards(&index_uid, &source_id, open_shards);

        let leader_follower_pairs = controller
            .allocate_shards(1, &FnvHashSet::default(), &model)
            .unwrap();
        assert_eq!(leader_follower_pairs.len(), 1);
        // Ingester 1 already has two shards, so ingester 2 is picked as leader
        assert_eq!(leader_follower_pairs[0].0, "test-ingester-2");
        assert_eq!(
            leader_follower_pairs[0].1,
            Some(NodeId::from("test-ingester-1"))
        );

        ingester_pool.insert("test-ingester-3".into(), IngesterServiceClient::mocked());
        let unavailable_leaders = FnvHashSet::from_iter([NodeId::from("test-ingester-2")]);
        let leader_follower_pairs = controller
            .allocate_shards(4, &unavailable_leaders, &model)
            .unwrap();
        // Ingester 2 is unavailable. Ingester 1 has open shards. Ingester 3 ends up leader.
        assert_eq!(leader_follower_pairs.len(), 4);
        assert_eq!(leader_follower_pairs[0].0, "test-ingester-3");
        assert_eq!(
            leader_follower_pairs[0].1,
            Some(NodeId::from("test-ingester-1"))
        );

        assert_eq!(leader_follower_pairs[1].0, "test-ingester-3");
        assert_eq!(
            leader_follower_pairs[1].1,
            Some(NodeId::from("test-ingester-1"))
        );

        assert_eq!(leader_follower_pairs[2].0, "test-ingester-3");
        assert_eq!(
            leader_follower_pairs[2].1,
            Some(NodeId::from("test-ingester-1"))
        );

        assert_eq!(leader_follower_pairs[3].0, "test-ingester-3");
        assert_eq!(
            leader_follower_pairs[3].1,
            Some(NodeId::from("test-ingester-1"))
        );
    }

    #[tokio::test]
    async fn test_ingest_controller_init_shards() {
        let metastore = MetastoreServiceClient::mocked();
        let ingester_pool = IngesterPool::default();
        let replication_factor = 1;

        let controller = IngestController::new(
            metastore,
            ingester_pool.clone(),
            replication_factor,
            TEST_SHARD_THROUGHPUT_LIMIT_MIB,
            1.001,
        );

        let ingester_id_0 = NodeId::from("test-ingester-0");
        let mut mock_ingester_0 = MockIngesterService::new();
        mock_ingester_0
            .expect_init_shards()
            .once()
            .returning(|mut request| {
                assert_eq!(request.subrequests.len(), 2);

                request
                    .subrequests
                    .sort_by_key(|subrequest| subrequest.subrequest_id);

                let subrequest_0 = &request.subrequests[0];
                assert_eq!(subrequest_0.subrequest_id, 0);

                let shard_0 = request.subrequests[0].shard();
                assert_eq!(shard_0.index_uid(), &("test-index", 0));
                assert_eq!(shard_0.source_id, "test-source");
                assert_eq!(shard_0.shard_id(), ShardId::from(0));
                assert_eq!(shard_0.leader_id, "test-ingester-0");

                let subrequest_1 = &request.subrequests[1];
                assert_eq!(subrequest_1.subrequest_id, 1);

                let shard_1 = request.subrequests[1].shard();
                assert_eq!(shard_1.index_uid(), &("test-index", 0));
                assert_eq!(shard_1.source_id, "test-source");
                assert_eq!(shard_1.shard_id(), ShardId::from(1));
                assert_eq!(shard_1.leader_id, "test-ingester-0");

                let successes = vec![InitShardSuccess {
                    subrequest_id: 0,
                    shard: Some(shard_0.clone()),
                }];
                let failures = vec![InitShardFailure {
                    subrequest_id: 1,
                    index_uid: shard_1.index_uid.clone(),
                    source_id: shard_1.source_id.clone(),
                    shard_id: shard_1.shard_id.clone(),
                }];
                let response = InitShardsResponse {
                    successes,
                    failures,
                };
                Ok(response)
            });
        let ingester_0 = IngesterServiceClient::from_mock(mock_ingester_0);
        ingester_pool.insert(ingester_id_0, ingester_0);

        let ingester_id_1 = NodeId::from("test-ingester-1");
        let mut mock_ingester_1 = MockIngesterService::new();
        mock_ingester_1
            .expect_init_shards()
            .once()
            .returning(|request| {
                assert_eq!(request.subrequests.len(), 1);

                let subrequest = &request.subrequests[0];
                assert_eq!(subrequest.subrequest_id, 2);

                let shard = request.subrequests[0].shard();
                assert_eq!(shard.index_uid(), &("test-index", 0));
                assert_eq!(shard.source_id, "test-source");
                assert_eq!(shard.shard_id(), ShardId::from(2));
                assert_eq!(shard.leader_id, "test-ingester-1");

                Err(IngestV2Error::Internal("internal error".to_string()))
            });
        let ingester_1 = IngesterServiceClient::from_mock(mock_ingester_1);
        ingester_pool.insert(ingester_id_1, ingester_1);

        let ingester_id_2 = NodeId::from("test-ingester-2");
        let mut mock_ingester_2 = MockIngesterService::new();
        mock_ingester_2.expect_init_shards().never();

        let ingester_2 = IngesterServiceClient::tower()
            .stack_init_shards_layer(DelayLayer::new(INIT_SHARDS_REQUEST_TIMEOUT * 2))
            .build_from_mock(mock_ingester_2);
        ingester_pool.insert(ingester_id_2, ingester_2);

        let init_shards_response = controller
            .init_shards(Vec::new(), &Progress::default())
            .await;
        assert_eq!(init_shards_response.successes.len(), 0);
        assert_eq!(init_shards_response.failures.len(), 0);

        // In this test:
        // - ingester 0 will initialize shard 0 successfully and fail to initialize shard 1;
        // - ingester 1 will return an error;
        // - ingester 2 will time out;
        // - ingester 3 will be unavailable.

        let init_shard_subrequests: Vec<InitShardSubrequest> = vec![
            InitShardSubrequest {
                subrequest_id: 0,
                shard: Some(Shard {
                    index_uid: IndexUid::for_test("test-index", 0).into(),
                    source_id: "test-source".to_string(),
                    shard_id: Some(ShardId::from(0)),
                    leader_id: "test-ingester-0".to_string(),
                    shard_state: ShardState::Open as i32,
                    ..Default::default()
                }),
                doc_mapping_json: "{}".to_string(),
                validate_docs: false,
            },
            InitShardSubrequest {
                subrequest_id: 1,
                shard: Some(Shard {
                    index_uid: IndexUid::for_test("test-index", 0).into(),
                    source_id: "test-source".to_string(),
                    shard_id: Some(ShardId::from(1)),
                    leader_id: "test-ingester-0".to_string(),
                    shard_state: ShardState::Open as i32,
                    ..Default::default()
                }),
                doc_mapping_json: "{}".to_string(),
                validate_docs: false,
            },
            InitShardSubrequest {
                subrequest_id: 2,
                shard: Some(Shard {
                    index_uid: IndexUid::for_test("test-index", 0).into(),
                    source_id: "test-source".to_string(),
                    shard_id: Some(ShardId::from(2)),
                    leader_id: "test-ingester-1".to_string(),
                    shard_state: ShardState::Open as i32,
                    ..Default::default()
                }),
                doc_mapping_json: "{}".to_string(),
                validate_docs: false,
            },
            InitShardSubrequest {
                subrequest_id: 3,
                shard: Some(Shard {
                    index_uid: IndexUid::for_test("test-index", 0).into(),
                    source_id: "test-source".to_string(),
                    shard_id: Some(ShardId::from(3)),
                    leader_id: "test-ingester-2".to_string(),
                    shard_state: ShardState::Open as i32,
                    ..Default::default()
                }),
                doc_mapping_json: "{}".to_string(),
                validate_docs: false,
            },
            InitShardSubrequest {
                subrequest_id: 4,
                shard: Some(Shard {
                    index_uid: IndexUid::for_test("test-index", 0).into(),
                    source_id: "test-source".to_string(),
                    shard_id: Some(ShardId::from(4)),
                    leader_id: "test-ingester-3".to_string(),
                    shard_state: ShardState::Open as i32,
                    ..Default::default()
                }),
                doc_mapping_json: "{}".to_string(),
                validate_docs: false,
            },
        ];
        let init_shards_response = controller
            .init_shards(init_shard_subrequests, &Progress::default())
            .await;
        assert_eq!(init_shards_response.successes.len(), 1);
        assert_eq!(init_shards_response.failures.len(), 4);

        let success = &init_shards_response.successes[0];
        assert_eq!(success.subrequest_id, 0);

        let mut failures = init_shards_response.failures;
        failures.sort_by_key(|failure| failure.subrequest_id);

        assert_eq!(failures[0].subrequest_id, 1);
        assert_eq!(failures[1].subrequest_id, 2);
        assert_eq!(failures[2].subrequest_id, 3);
        assert_eq!(failures[3].subrequest_id, 4);
    }

    #[tokio::test]
    async fn test_ingest_controller_try_open_shards() {
        let doc_mapping_uid = DocMappingUid::random();
        let expected_doc_mapping = doc_mapping_uid;

        let mut mock_metastore = MockMetastoreService::new();
        mock_metastore
            .expect_open_shards()
            .once()
            .returning(move |request| {
                assert_eq!(request.subrequests.len(), 1);

                let subrequest = &request.subrequests[0];
                assert_eq!(subrequest.subrequest_id, 0);

                assert_eq!(subrequest.index_uid(), &("test-index", 0));
                assert_eq!(subrequest.source_id, "test-source");
                assert_eq!(subrequest.leader_id, "test-ingester-1");
                assert_eq!(subrequest.doc_mapping_uid(), expected_doc_mapping);

                let subresponses = vec![metastore::OpenShardSubresponse {
                    subrequest_id: 0,
                    open_shard: Some(Shard {
                        index_uid: Some(IndexUid::for_test("test-index", 0)),
                        source_id: "test-source".to_string(),
                        shard_id: Some(ShardId::from(0)),
                        leader_id: "test-ingester-1".to_string(),
                        shard_state: ShardState::Open as i32,
                        doc_mapping_uid: Some(expected_doc_mapping),
                        ..Default::default()
                    }),
                }];
                let response = metastore::OpenShardsResponse { subresponses };
                Ok(response)
            });
        let metastore = MetastoreServiceClient::from_mock(mock_metastore);
        let ingester_pool = IngesterPool::default();
        let replication_factor = 1;

        let mut controller = IngestController::new(
            metastore,
            ingester_pool.clone(),
            replication_factor,
            TEST_SHARD_THROUGHPUT_LIMIT_MIB,
            1.001,
        );

        let index_uid = IndexUid::for_test("test-index", 0);
        let source_id = "test-source".to_string();

        let source_uid = SourceUid {
            index_uid: index_uid.clone(),
            source_id: source_id.clone(),
        };
        let mut index_metadata = IndexMetadata::for_test("test-index", "ram://indexes/test-index");
        index_metadata.sources.insert(
            source_id.clone(),
            SourceConfig::for_test(&source_id, quickwit_config::SourceParams::void()),
        );

        let doc_mapping_json = format!(
            r#"{{
                "doc_mapping_uid": "{doc_mapping_uid}",
                "field_mappings": [{{
                        "name": "message",
                        "type": "text"
                }}]
            }}"#
        );
        let doc_mapping: DocMapping = serde_json::from_str(&doc_mapping_json).unwrap();
        let expected_doc_mapping = doc_mapping.clone();
        index_metadata.index_config.doc_mapping = doc_mapping;

        let mut model = ControlPlaneModel::default();
        model.add_index(index_metadata);

        let mut mock_ingester = MockIngesterService::new();
        mock_ingester
            .expect_init_shards()
            .once()
            .returning(move |request| {
                assert_eq!(request.subrequests.len(), 1);

                let subrequest = &request.subrequests[0];
                assert_eq!(subrequest.subrequest_id, 0);

                let doc_mapping: DocMapping =
                    serde_json::from_str(&subrequest.doc_mapping_json).unwrap();
                assert_eq!(doc_mapping, expected_doc_mapping);

                let shard = request.subrequests[0].shard();
                assert_eq!(shard.index_uid(), &("test-index", 0));
                assert_eq!(shard.source_id, "test-source");
                assert_eq!(shard.leader_id, "test-ingester-1");
                assert_eq!(shard.doc_mapping_uid(), doc_mapping_uid);

                let successes = vec![InitShardSuccess {
                    subrequest_id: 0,
                    shard: Some(shard.clone()),
                }];
                let response = InitShardsResponse {
                    successes,
                    failures: Vec::new(),
                };
                Ok(response)
            });

        ingester_pool.insert(
            NodeId::from("test-ingester-1"),
            IngesterServiceClient::from_mock(mock_ingester),
        );
        let source_uids: HashMap<SourceUid, usize> = HashMap::from_iter([(source_uid.clone(), 1)]);
        let unavailable_leaders = FnvHashSet::default();
        let progress = Progress::default();

        let per_source_num_opened_shards = controller
            .try_open_shards(source_uids, &mut model, &unavailable_leaders, &progress)
            .await
            .unwrap();

        assert_eq!(per_source_num_opened_shards.len(), 1);
        assert_eq!(*per_source_num_opened_shards.get(&source_uid).unwrap(), 1);
    }

    #[tokio::test]
    async fn test_ingest_controller_handle_local_shards_update() {
        let mut mock_metastore = MockMetastoreService::new();
        mock_metastore
            .expect_open_shards()
            .once()
            .returning(|request| {
                assert_eq!(request.subrequests.len(), 1);
                let subrequest = &request.subrequests[0];

                assert_eq!(subrequest.index_uid(), &IndexUid::for_test("test-index", 0));
                assert_eq!(subrequest.source_id, "test-source");
                assert_eq!(subrequest.leader_id, "test-ingester");

                Err(MetastoreError::InvalidArgument {
                    message: "failed to open shards".to_string(),
                })
            });
        mock_metastore
            .expect_open_shards()
            .once()
            .returning(|request| {
                assert_eq!(request.subrequests.len(), 1);
                let subrequest: &OpenShardSubrequest = &request.subrequests[0];

                assert_eq!(subrequest.index_uid(), &IndexUid::for_test("test-index", 0));
                assert_eq!(subrequest.source_id, "test-source");
                assert_eq!(subrequest.leader_id, "test-ingester");

                let shard = Shard {
                    index_uid: subrequest.index_uid.clone(),
                    source_id: subrequest.source_id.clone(),
                    shard_id: subrequest.shard_id.clone(),
                    shard_state: ShardState::Open as i32,
                    leader_id: subrequest.leader_id.clone(),
                    follower_id: subrequest.follower_id.clone(),
                    doc_mapping_uid: subrequest.doc_mapping_uid,
                    publish_position_inclusive: Some(Position::Beginning),
                    publish_token: None,
                    update_timestamp: 1724158996,
                };
                let response = OpenShardsResponse {
                    subresponses: vec![OpenShardSubresponse {
                        subrequest_id: subrequest.subrequest_id,
                        open_shard: Some(shard),
                    }],
                };
                Ok(response)
            });
        let metastore = MetastoreServiceClient::from_mock(mock_metastore);
        let ingester_pool = IngesterPool::default();
        let replication_factor = 1;

        let mut controller = IngestController::new(
            metastore,
            ingester_pool.clone(),
            replication_factor,
            TEST_SHARD_THROUGHPUT_LIMIT_MIB,
            1.001,
        );

        let index_uid = IndexUid::for_test("test-index", 0);
        let mut index_metadata = IndexMetadata::for_test("test-index", "ram://indexes/test-index");
        let source_id: SourceId = "test-source".to_string();
        index_metadata.sources.insert(
            source_id.clone(),
            SourceConfig::for_test(&source_id, quickwit_config::SourceParams::void()),
        );

        let source_uid = SourceUid {
            index_uid: index_uid.clone(),
            source_id: source_id.clone(),
        };
        let mut model = ControlPlaneModel::default();
        model.add_index(index_metadata);
        let progress = Progress::default();

        let shards = vec![Shard {
            index_uid: Some(index_uid.clone()),
            source_id: source_id.clone(),
            shard_id: Some(ShardId::from(1)),
            leader_id: "test-ingester".to_string(),
            shard_state: ShardState::Open as i32,
            ..Default::default()
        }];
        model.insert_shards(&index_uid, &source_id, shards);
        let shard_entries: Vec<ShardEntry> = model.all_shards().cloned().collect();

        assert_eq!(shard_entries.len(), 1);
        assert_eq!(shard_entries[0].short_term_ingestion_rate, 0);

        // Test update shard ingestion rate but no scale down because num open shards is 1.
        let shard_infos = BTreeSet::from_iter([ShardInfo {
            shard_id: ShardId::from(1),
            shard_state: ShardState::Open,
            short_term_ingestion_rate: RateMibPerSec(1),
            long_term_ingestion_rate: RateMibPerSec(1),
        }]);
        let local_shards_update = LocalShardsUpdate {
            leader_id: "test-ingester".into(),
            source_uid: source_uid.clone(),
            shard_infos,
        };

        controller
            .handle_local_shards_update(local_shards_update, &mut model, &progress)
            .await
            .unwrap();

        let shard_entries: Vec<ShardEntry> = model.all_shards().cloned().collect();
        assert_eq!(shard_entries.len(), 1);
        assert_eq!(shard_entries[0].short_term_ingestion_rate, 1);

        // Test update shard ingestion rate with failing scale down.
        let shards = vec![Shard {
            index_uid: Some(index_uid.clone()),
            source_id: source_id.clone(),
            shard_id: Some(ShardId::from(2)),
            shard_state: ShardState::Open as i32,
            leader_id: "test-ingester".to_string(),
            ..Default::default()
        }];
        model.insert_shards(&index_uid, &source_id, shards);

        let shard_entries: Vec<ShardEntry> = model.all_shards().cloned().collect();
        assert_eq!(shard_entries.len(), 2);

        let mut mock_ingester = MockIngesterService::new();

        let index_uid_clone = index_uid.clone();
        mock_ingester.expect_init_shards().returning(
            move |init_shard_request: InitShardsRequest| {
                assert_eq!(init_shard_request.subrequests.len(), 1);
                let init_shard_subrequest: &InitShardSubrequest =
                    &init_shard_request.subrequests[0];
                assert!(init_shard_subrequest.validate_docs);
                Ok(InitShardsResponse {
                    successes: vec![InitShardSuccess {
                        subrequest_id: init_shard_subrequest.subrequest_id,
                        shard: init_shard_subrequest.shard.clone(),
                    }],
                    failures: Vec::new(),
                })
            },
        );
        mock_ingester
            .expect_close_shards()
            .returning(move |request| {
                assert_eq!(request.shard_pkeys.len(), 1);
                assert_eq!(request.shard_pkeys[0].index_uid(), &index_uid_clone);
                assert_eq!(request.shard_pkeys[0].source_id, "test-source");
                Err(IngestV2Error::Internal(
                    "failed to close shards".to_string(),
                ))
            });
        let ingester = IngesterServiceClient::from_mock(mock_ingester);
        ingester_pool.insert("test-ingester".into(), ingester);

        let shard_infos = BTreeSet::from_iter([
            ShardInfo {
                shard_id: ShardId::from(1),
                shard_state: ShardState::Open,
                short_term_ingestion_rate: RateMibPerSec(1),
                long_term_ingestion_rate: RateMibPerSec(1),
            },
            ShardInfo {
                shard_id: ShardId::from(2),
                shard_state: ShardState::Open,
                short_term_ingestion_rate: RateMibPerSec(1),
                long_term_ingestion_rate: RateMibPerSec(1),
            },
        ]);
        let local_shards_update = LocalShardsUpdate {
            leader_id: "test-ingester".into(),
            source_uid: source_uid.clone(),
            shard_infos,
        };
        controller
            .handle_local_shards_update(local_shards_update, &mut model, &progress)
            .await
            .unwrap();

        // Test update shard ingestion rate with failing scale up.
        let shard_infos = BTreeSet::from_iter([
            ShardInfo {
                shard_id: ShardId::from(1),
                shard_state: ShardState::Open,
                short_term_ingestion_rate: RateMibPerSec(4),
                long_term_ingestion_rate: RateMibPerSec(4),
            },
            ShardInfo {
                shard_id: ShardId::from(2),
                shard_state: ShardState::Open,
                short_term_ingestion_rate: RateMibPerSec(4),
                long_term_ingestion_rate: RateMibPerSec(4),
            },
        ]);
        let local_shards_update = LocalShardsUpdate {
            leader_id: "test-ingester".into(),
            source_uid: source_uid.clone(),
            shard_infos,
        };

        // The first request fails due to an error on the metastore.
        let MetastoreError::InvalidArgument { .. } = controller
            .handle_local_shards_update(local_shards_update.clone(), &mut model, &progress)
            .await
            .unwrap_err()
        else {
            panic!();
        };

        // The second request works!
        controller
            .handle_local_shards_update(local_shards_update, &mut model, &progress)
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn test_ingest_controller_disable_validation_when_vrl() {
        let mut mock_metastore = MockMetastoreService::new();
        mock_metastore
            .expect_open_shards()
            .once()
            .returning(|request| {
                let subrequest: &OpenShardSubrequest = &request.subrequests[0];
                let shard = Shard {
                    index_uid: subrequest.index_uid.clone(),
                    source_id: subrequest.source_id.clone(),
                    shard_id: subrequest.shard_id.clone(),
                    shard_state: ShardState::Open as i32,
                    leader_id: subrequest.leader_id.clone(),
                    follower_id: subrequest.follower_id.clone(),
                    doc_mapping_uid: subrequest.doc_mapping_uid,
                    publish_position_inclusive: Some(Position::Beginning),
                    publish_token: None,
                    update_timestamp: 1724158996,
                };
                let response = OpenShardsResponse {
                    subresponses: vec![OpenShardSubresponse {
                        subrequest_id: subrequest.subrequest_id,
                        open_shard: Some(shard),
                    }],
                };
                Ok(response)
            });
        let metastore = MetastoreServiceClient::from_mock(mock_metastore);
        let ingester_pool = IngesterPool::default();
        let replication_factor = 1;

        let mut controller = IngestController::new(
            metastore,
            ingester_pool.clone(),
            replication_factor,
            TEST_SHARD_THROUGHPUT_LIMIT_MIB,
            1.001,
        );

        let index_uid = IndexUid::for_test("test-index", 0);
        let mut index_metadata = IndexMetadata::for_test("test-index", "ram://indexes/test-index");
        let source_id: SourceId = "test-source".to_string();
        let mut source_config =
            SourceConfig::for_test(&source_id, quickwit_config::SourceParams::void());
        // set a vrl script
        source_config.transform_config =
            Some(quickwit_config::TransformConfig::new("".to_string(), None));
        index_metadata
            .sources
            .insert(source_id.clone(), source_config);

        let source_uid = SourceUid {
            index_uid: index_uid.clone(),
            source_id: source_id.clone(),
        };
        let mut model = ControlPlaneModel::default();
        model.add_index(index_metadata);
        let progress = Progress::default();

        let shards = vec![Shard {
            index_uid: Some(index_uid.clone()),
            source_id: source_id.clone(),
            shard_id: Some(ShardId::from(1)),
            leader_id: "test-ingester".to_string(),
            shard_state: ShardState::Open as i32,
            ..Default::default()
        }];
        model.insert_shards(&index_uid, &source_id, shards);

        let mut mock_ingester = MockIngesterService::new();

        mock_ingester.expect_init_shards().returning(
            move |init_shard_request: InitShardsRequest| {
                assert_eq!(init_shard_request.subrequests.len(), 1);
                let init_shard_subrequest: &InitShardSubrequest =
                    &init_shard_request.subrequests[0];
                // we have vrl, so no validation
                assert!(!init_shard_subrequest.validate_docs);
                Ok(InitShardsResponse {
                    successes: vec![InitShardSuccess {
                        subrequest_id: init_shard_subrequest.subrequest_id,
                        shard: init_shard_subrequest.shard.clone(),
                    }],
                    failures: Vec::new(),
                })
            },
        );

        let ingester = IngesterServiceClient::from_mock(mock_ingester);
        ingester_pool.insert("test-ingester".into(), ingester);

        let shard_infos = BTreeSet::from_iter([ShardInfo {
            shard_id: ShardId::from(1),
            shard_state: ShardState::Open,
            short_term_ingestion_rate: RateMibPerSec(4),
            long_term_ingestion_rate: RateMibPerSec(4),
        }]);
        let local_shards_update = LocalShardsUpdate {
            leader_id: "test-ingester".into(),
            source_uid: source_uid.clone(),
            shard_infos,
        };

        controller
            .handle_local_shards_update(local_shards_update, &mut model, &progress)
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn test_ingest_controller_try_scale_up_shards() {
        let mut mock_metastore = MockMetastoreService::new();

        let index_uid = IndexUid::from_str("test-index:00000000000000000000000000").unwrap();
        let index_uid_clone = index_uid.clone();
        mock_metastore
            .expect_open_shards()
            .once()
            .returning(move |request| {
                assert_eq!(request.subrequests.len(), 1);
                assert_eq!(request.subrequests[0].index_uid(), &index_uid_clone);
                assert_eq!(request.subrequests[0].source_id, INGEST_V2_SOURCE_ID);
                assert_eq!(request.subrequests[0].leader_id, "test-ingester");

                Err(MetastoreError::InvalidArgument {
                    message: "failed to open shards".to_string(),
                })
            });
        let index_uid_clone = index_uid.clone();
        mock_metastore
            .expect_open_shards()
            .returning(move |request| {
                assert_eq!(request.subrequests.len(), 1);
                assert_eq!(request.subrequests[0].index_uid(), &index_uid_clone);
                assert_eq!(request.subrequests[0].source_id, INGEST_V2_SOURCE_ID);
                assert_eq!(request.subrequests[0].leader_id, "test-ingester");

                let subresponses = vec![metastore::OpenShardSubresponse {
                    subrequest_id: 0,
                    open_shard: Some(Shard {
                        index_uid: Some(index_uid.clone()),
                        source_id: INGEST_V2_SOURCE_ID.to_string(),
                        shard_id: Some(ShardId::from(1)),
                        leader_id: "test-ingester".to_string(),
                        shard_state: ShardState::Open as i32,
                        ..Default::default()
                    }),
                }];
                let response = metastore::OpenShardsResponse { subresponses };
                Ok(response)
            });
        let metastore = MetastoreServiceClient::from_mock(mock_metastore);

        let ingester_pool = IngesterPool::default();
        let replication_factor = 1;

        let mut controller = IngestController::new(
            metastore,
            ingester_pool.clone(),
            replication_factor,
            TEST_SHARD_THROUGHPUT_LIMIT_MIB,
            1.001,
        );

        let index_uid = IndexUid::for_test("test-index", 0);
        let source_id: SourceId = INGEST_V2_SOURCE_ID.to_string();

        let source_uid = SourceUid {
            index_uid: index_uid.clone(),
            source_id: source_id.clone(),
        };
        let shard_stats = ShardStats {
            num_open_shards: 2,
            ..Default::default()
        };
        let mut model = ControlPlaneModel::default();
        let index_metadata =
            IndexMetadata::for_test(&index_uid.index_id, "ram://indexes/test-index:0");
        model.add_index(index_metadata);

        let source_config = SourceConfig::ingest_v2();
        model.add_source(&index_uid, source_config).unwrap();

        let progress = Progress::default();

        // Test could not find leader because no ingester in pool
        controller
            .try_scale_up_shards(source_uid.clone(), shard_stats, &mut model, &progress, 1)
            .await
            .unwrap();

        let mut mock_ingester = MockIngesterService::new();

        let index_uid_clone = index_uid.clone();
        mock_ingester
            .expect_init_shards()
            .once()
            .returning(move |request| {
                assert_eq!(request.subrequests.len(), 1);

                let subrequest = &request.subrequests[0];
                assert_eq!(subrequest.subrequest_id, 0);

                let shard = request.subrequests[0].shard();
                assert_eq!(shard.index_uid(), &index_uid_clone);
                assert_eq!(shard.source_id, INGEST_V2_SOURCE_ID);
                assert_eq!(shard.leader_id, "test-ingester");

                Err(IngestV2Error::Internal("failed to init shards".to_string()))
            });
        let index_uid_clone = index_uid.clone();
        mock_ingester
            .expect_init_shards()
            .returning(move |request| {
                assert_eq!(request.subrequests.len(), 1);

                let subrequest = &request.subrequests[0];
                assert_eq!(subrequest.subrequest_id, 0);

                let shard = subrequest.shard();
                assert_eq!(shard.index_uid(), &index_uid_clone);
                assert_eq!(shard.source_id, INGEST_V2_SOURCE_ID);
                assert_eq!(shard.leader_id, "test-ingester");

                let successes = vec![InitShardSuccess {
                    subrequest_id: request.subrequests[0].subrequest_id,
                    shard: Some(shard.clone()),
                }];
                let response = InitShardsResponse {
                    successes,
                    failures: Vec::new(),
                };
                Ok(response)
            });
        let ingester = IngesterServiceClient::from_mock(mock_ingester);
        ingester_pool.insert("test-ingester".into(), ingester);

        // Test failed to open shards.
        controller
            .try_scale_up_shards(source_uid.clone(), shard_stats, &mut model, &progress, 1)
            .await
            .unwrap();
        assert_eq!(model.all_shards().count(), 0);

        // Test failed to init shards.
        controller
            .try_scale_up_shards(source_uid.clone(), shard_stats, &mut model, &progress, 1)
            .await
            .unwrap_err();
        assert_eq!(model.all_shards().count(), 0);

        // Test successfully opened shard.
        controller
            .try_scale_up_shards(source_uid.clone(), shard_stats, &mut model, &progress, 1)
            .await
            .unwrap();
        assert_eq!(
            model.all_shards().filter(|shard| shard.is_open()).count(),
            1
        );
    }

    #[tokio::test]
    async fn test_ingest_controller_try_scale_down_shards() {
        let metastore = MetastoreServiceClient::mocked();
        let ingester_pool = IngesterPool::default();
        let replication_factor = 1;

        let controller = IngestController::new(
            metastore,
            ingester_pool.clone(),
            replication_factor,
            TEST_SHARD_THROUGHPUT_LIMIT_MIB,
            1.001,
        );

        let index_uid = IndexUid::for_test("test-index", 0);
        let source_id: SourceId = "test-source".to_string();

        let source_uid = SourceUid {
            index_uid: index_uid.clone(),
            source_id: source_id.clone(),
        };
        let shard_stats = ShardStats {
            num_open_shards: 2,
            ..Default::default()
        };
        let min_shards = NonZeroUsize::MIN;
        let mut model = ControlPlaneModel::default();
        let progress = Progress::default();

        // Test could not find a scale down candidate.
        controller
            .try_scale_down_shards(
                source_uid.clone(),
                shard_stats,
                min_shards,
                &mut model,
                &progress,
            )
            .await
            .unwrap();

        let shards = vec![Shard {
            shard_id: Some(ShardId::from(1)),
            index_uid: Some(index_uid.clone()),
            source_id: source_id.clone(),
            leader_id: "test-ingester".to_string(),
            shard_state: ShardState::Open as i32,
            ..Default::default()
        }];
        model.insert_shards(&index_uid, &source_id, shards);

        // Test ingester is unavailable.
        controller
            .try_scale_down_shards(
                source_uid.clone(),
                shard_stats,
                min_shards,
                &mut model,
                &progress,
            )
            .await
            .unwrap();

        let mut mock_ingester = MockIngesterService::new();

        let index_uid_clone = index_uid.clone();
        mock_ingester
            .expect_close_shards()
            .once()
            .returning(move |request| {
                assert_eq!(request.shard_pkeys.len(), 1);
                assert_eq!(request.shard_pkeys[0].index_uid(), &index_uid_clone);
                assert_eq!(request.shard_pkeys[0].source_id, "test-source");
                assert_eq!(request.shard_pkeys[0].shard_id(), ShardId::from(1));

                Err(IngestV2Error::Internal(
                    "failed to close shards".to_string(),
                ))
            });
        let index_uid_clone = index_uid.clone();
        mock_ingester
            .expect_close_shards()
            .once()
            .returning(move |request| {
                assert_eq!(request.shard_pkeys.len(), 1);
                assert_eq!(request.shard_pkeys[0].index_uid(), &index_uid_clone);
                assert_eq!(request.shard_pkeys[0].source_id, "test-source");
                assert_eq!(request.shard_pkeys[0].shard_id(), ShardId::from(1));

                let response = CloseShardsResponse {
                    successes: request.shard_pkeys,
                };
                Ok(response)
            });
        let ingester = IngesterServiceClient::from_mock(mock_ingester);
        ingester_pool.insert("test-ingester".into(), ingester);

        // Test failed to close shard.
        controller
            .try_scale_down_shards(
                source_uid.clone(),
                shard_stats,
                min_shards,
                &mut model,
                &progress,
            )
            .await
            .unwrap();
        assert!(model.all_shards().all(|shard| shard.is_open()));

        // Test successfully closed shard.
        controller
            .try_scale_down_shards(
                source_uid.clone(),
                shard_stats,
                min_shards,
                &mut model,
                &progress,
            )
            .await
            .unwrap();
        assert!(model.all_shards().all(|shard| shard.is_closed()));

        let shards = vec![Shard {
            shard_id: Some(ShardId::from(2)),
            index_uid: Some(index_uid.clone()),
            source_id: source_id.clone(),
            leader_id: "test-ingester".to_string(),
            shard_state: ShardState::Open as i32,
            ..Default::default()
        }];
        model.insert_shards(&index_uid, &source_id, shards);

        // Test rate limited.
        controller
            .try_scale_down_shards(
                source_uid.clone(),
                shard_stats,
                min_shards,
                &mut model,
                &progress,
            )
            .await
            .unwrap();
        assert!(model.all_shards().any(|shard| shard.is_open()));
    }

    #[test]
    fn test_find_scale_down_candidate() {
        let index_uid = IndexUid::for_test("test-index", 0);
        let source_id: SourceId = "test-source".to_string();

        let source_uid = SourceUid {
            index_uid: index_uid.clone(),
            source_id: source_id.clone(),
        };
        let mut model = ControlPlaneModel::default();

        assert!(find_scale_down_candidate(&source_uid, &model).is_none());

        let shards = vec![
            Shard {
                index_uid: index_uid.clone().into(),
                source_id: source_id.clone(),
                shard_id: Some(ShardId::from(1)),
                shard_state: ShardState::Open as i32,
                leader_id: "test-ingester-0".to_string(),
                ..Default::default()
            },
            Shard {
                index_uid: index_uid.clone().into(),
                source_id: source_id.clone(),
                shard_id: Some(ShardId::from(2)),
                shard_state: ShardState::Open as i32,
                leader_id: "test-ingester-0".to_string(),
                ..Default::default()
            },
            Shard {
                index_uid: index_uid.clone().into(),
                source_id: source_id.clone(),
                shard_id: Some(ShardId::from(3)),
                shard_state: ShardState::Closed as i32, //< this one is closed
                leader_id: "test-ingester-0".to_string(),
                ..Default::default()
            },
            Shard {
                index_uid: index_uid.clone().into(),
                source_id: source_id.clone(),
                shard_id: Some(ShardId::from(4)),
                shard_state: ShardState::Open as i32,
                leader_id: "test-ingester-1".to_string(),
                ..Default::default()
            },
            Shard {
                index_uid: index_uid.clone().into(),
                source_id: source_id.clone(),
                shard_id: Some(ShardId::from(5)),
                shard_state: ShardState::Open as i32,
                leader_id: "test-ingester-1".to_string(),
                ..Default::default()
            },
            Shard {
                index_uid: index_uid.clone().into(),
                source_id: source_id.clone(),
                shard_id: Some(ShardId::from(6)),
                shard_state: ShardState::Open as i32,
                leader_id: "test-ingester-1".to_string(),
                ..Default::default()
            },
        ];
        // That's 3 open shards on indexer-1, 2 open shard and one closed shard on indexer-0..
        model.insert_shards(&index_uid, &source_id, shards);

        let shard_infos = BTreeSet::from_iter([
            ShardInfo {
                shard_id: ShardId::from(1),
                shard_state: ShardState::Open,
                short_term_ingestion_rate: quickwit_ingest::RateMibPerSec(1),
                long_term_ingestion_rate: quickwit_ingest::RateMibPerSec(1),
            },
            ShardInfo {
                shard_id: ShardId::from(2),
                shard_state: ShardState::Open,
                short_term_ingestion_rate: quickwit_ingest::RateMibPerSec(2),
                long_term_ingestion_rate: quickwit_ingest::RateMibPerSec(2),
            },
            ShardInfo {
                shard_id: ShardId::from(3),
                shard_state: ShardState::Open,
                short_term_ingestion_rate: quickwit_ingest::RateMibPerSec(3),
                long_term_ingestion_rate: quickwit_ingest::RateMibPerSec(3),
            },
            ShardInfo {
                shard_id: ShardId::from(4),
                shard_state: ShardState::Open,
                short_term_ingestion_rate: quickwit_ingest::RateMibPerSec(4),
                long_term_ingestion_rate: quickwit_ingest::RateMibPerSec(4),
            },
            ShardInfo {
                shard_id: ShardId::from(5),
                shard_state: ShardState::Open,
                short_term_ingestion_rate: quickwit_ingest::RateMibPerSec(5),
                long_term_ingestion_rate: quickwit_ingest::RateMibPerSec(5),
            },
            ShardInfo {
                shard_id: ShardId::from(6),
                shard_state: ShardState::Open,
                short_term_ingestion_rate: quickwit_ingest::RateMibPerSec(6),
                long_term_ingestion_rate: quickwit_ingest::RateMibPerSec(6),
            },
        ]);
        model.update_shards(&source_uid, &shard_infos);

        let (leader_id, _shard_id) = find_scale_down_candidate(&source_uid, &model).unwrap();
        // We pick ingester 1 has it has more open shard
        assert_eq!(leader_id, "test-ingester-1");
    }

    #[tokio::test]
    async fn test_sync_with_ingesters() {
        let metastore = MetastoreServiceClient::mocked();
        let ingester_pool = IngesterPool::default();
        let replication_factor = 2;

        let controller = IngestController::new(
            metastore,
            ingester_pool.clone(),
            replication_factor,
            TEST_SHARD_THROUGHPUT_LIMIT_MIB,
            1.001,
        );

        let index_uid = IndexUid::for_test("test-index", 0);
        let source_id: SourceId = "test-source".to_string();
        let mut model = ControlPlaneModel::default();
        let shards = vec![
            Shard {
                index_uid: Some(index_uid.clone()),
                source_id: source_id.clone(),
                shard_id: Some(ShardId::from(1)),
                shard_state: ShardState::Open as i32,
                leader_id: "node-1".to_string(),
                follower_id: Some("node-2".to_string()),
                ..Default::default()
            },
            Shard {
                index_uid: Some(index_uid.clone()),
                source_id: source_id.clone(),
                shard_id: Some(ShardId::from(2)),
                shard_state: ShardState::Open as i32,
                leader_id: "node-2".to_string(),
                follower_id: Some("node-3".to_string()),
                ..Default::default()
            },
            Shard {
                index_uid: Some(index_uid.clone()),
                source_id: source_id.clone(),
                shard_id: Some(ShardId::from(3)),
                shard_state: ShardState::Open as i32,
                leader_id: "node-2".to_string(),
                follower_id: Some("node-1".to_string()),
                ..Default::default()
            },
        ];
        model.insert_shards(&index_uid, &source_id, shards);

        let mut mock_ingester_1 = MockIngesterService::new();
        let mock_ingester_2 = MockIngesterService::new();
        let mock_ingester_3 = MockIngesterService::new();

        let count_calls = Arc::new(AtomicUsize::new(0));
        let count_calls_clone = count_calls.clone();
        mock_ingester_1
            .expect_retain_shards()
            .once()
            .returning(move |request| {
                assert_eq!(request.retain_shards_for_sources.len(), 1);
                assert_eq!(
                    request.retain_shards_for_sources[0].shard_ids,
                    [ShardId::from(1), ShardId::from(3)]
                );
                count_calls_clone.fetch_add(1, Ordering::Release);
                Ok(RetainShardsResponse {})
            });
        ingester_pool.insert(
            "node-1".into(),
            IngesterServiceClient::from_mock(mock_ingester_1),
        );
        ingester_pool.insert(
            "node-2".into(),
            IngesterServiceClient::from_mock(mock_ingester_2),
        );
        ingester_pool.insert(
            "node-3".into(),
            IngesterServiceClient::from_mock(mock_ingester_3),
        );
        let node_id = "node-1".into();
        let wait_handle = controller.sync_with_ingester(&node_id, &model);
        wait_handle.wait().await;
        assert_eq!(count_calls.load(Ordering::Acquire), 1);
    }

    #[tokio::test]
    async fn test_ingest_controller_advise_reset_shards() {
        let metastore = MetastoreServiceClient::mocked();
        let ingester_pool = IngesterPool::default();
        let replication_factor = 2;

        let controller = IngestController::new(
            metastore,
            ingester_pool,
            replication_factor,
            TEST_SHARD_THROUGHPUT_LIMIT_MIB,
            1.001,
        );

        let mut model = ControlPlaneModel::default();

        let index_uid = IndexUid::for_test("test-index", 0);
        let source_id_00: SourceId = "test-source-0".into();
        let source_id_01: SourceId = "test-source-1".into();

        let shards = vec![Shard {
            index_uid: Some(index_uid.clone()),
            source_id: source_id_00.clone(),
            shard_id: Some(ShardId::from(1)),
            shard_state: ShardState::Open as i32,
            publish_position_inclusive: Some(Position::offset(1337u64)),
            ..Default::default()
        }];
        model.insert_shards(&index_uid, &source_id_00, shards);

        let advise_reset_shards_request = AdviseResetShardsRequest {
            ingester_id: "test-ingester".to_string(),
            shard_ids: vec![
                ShardIds {
                    index_uid: Some(index_uid.clone()),
                    source_id: source_id_00.clone(),
                    shard_ids: vec![ShardId::from(1), ShardId::from(2)],
                },
                ShardIds {
                    index_uid: Some(index_uid.clone()),
                    source_id: source_id_01.clone(),
                    shard_ids: vec![ShardId::from(3)],
                },
            ],
        };
        let advise_reset_shards_response =
            controller.advise_reset_shards(advise_reset_shards_request, &model);

        assert_eq!(advise_reset_shards_response.shards_to_delete.len(), 2);

        let shard_to_delete_00 = &advise_reset_shards_response.shards_to_delete[0];
        assert_eq!(shard_to_delete_00.index_uid(), &index_uid);
        assert_eq!(shard_to_delete_00.source_id, source_id_00);
        assert_eq!(shard_to_delete_00.shard_ids.len(), 1);
        assert_eq!(shard_to_delete_00.shard_ids[0], ShardId::from(2));

        let shard_to_delete_01 = &advise_reset_shards_response.shards_to_delete[1];
        assert_eq!(shard_to_delete_01.index_uid(), &index_uid);
        assert_eq!(shard_to_delete_01.source_id, source_id_01);
        assert_eq!(shard_to_delete_01.shard_ids.len(), 1);
        assert_eq!(shard_to_delete_01.shard_ids[0], ShardId::from(3));

        assert_eq!(advise_reset_shards_response.shards_to_truncate.len(), 1);

        let shard_to_truncate = &advise_reset_shards_response.shards_to_truncate[0];
        assert_eq!(shard_to_truncate.index_uid(), &index_uid);
        assert_eq!(shard_to_truncate.source_id, source_id_00);
        assert_eq!(shard_to_truncate.shard_positions.len(), 1);
        assert_eq!(
            shard_to_truncate.shard_positions[0].shard_id(),
            ShardId::from(1)
        );
        assert_eq!(
            shard_to_truncate.shard_positions[0].publish_position_inclusive(),
            Position::offset(1337u64)
        );
    }

    #[tokio::test]
    async fn test_ingest_controller_close_shards() {
        let metastore = MetastoreServiceClient::mocked();
        let ingester_pool = IngesterPool::default();
        let replication_factor = 1;
        let controller = IngestController::new(
            metastore,
            ingester_pool.clone(),
            replication_factor,
            TEST_SHARD_THROUGHPUT_LIMIT_MIB,
            1.001,
        );

        let closed_shards = controller.close_shards(Vec::new()).await;
        assert_eq!(closed_shards.len(), 0);

        let ingester_id_0 = NodeId::from("test-ingester-0");
        let mut mock_ingester_0 = MockIngesterService::new();
        mock_ingester_0
            .expect_close_shards()
            .once()
            .returning(|mut request| {
                assert_eq!(request.shard_pkeys.len(), 2);

                request
                    .shard_pkeys
                    .sort_by(|left, right| left.shard_id().cmp(right.shard_id()));

                let shard_0 = &request.shard_pkeys[0];
                assert_eq!(shard_0.index_uid(), &IndexUid::for_test("test-index", 0));
                assert_eq!(shard_0.source_id, "test-source");
                assert_eq!(shard_0.shard_id(), ShardId::from(0));

                let shard_1 = &request.shard_pkeys[1];
                assert_eq!(shard_1.index_uid(), &IndexUid::for_test("test-index", 0));
                assert_eq!(shard_1.source_id, "test-source");
                assert_eq!(shard_1.shard_id(), ShardId::from(1));

                let response = CloseShardsResponse {
                    successes: vec![shard_0.clone()],
                };
                Ok(response)
            });
        let ingester_0 = IngesterServiceClient::from_mock(mock_ingester_0);
        ingester_pool.insert(ingester_id_0.clone(), ingester_0);

        let ingester_id_1 = NodeId::from("test-ingester-1");
        let mut mock_ingester_1 = MockIngesterService::new();
        mock_ingester_1
            .expect_close_shards()
            .once()
            .returning(|request| {
                assert_eq!(request.shard_pkeys.len(), 1);

                let shard = &request.shard_pkeys[0];
                assert_eq!(shard.index_uid(), &IndexUid::for_test("test-index", 0));
                assert_eq!(shard.source_id, "test-source");
                assert_eq!(shard.shard_id(), ShardId::from(2));

                Err(IngestV2Error::Internal("internal error".to_string()))
            });
        let ingester_1 = IngesterServiceClient::from_mock(mock_ingester_1);
        ingester_pool.insert(ingester_id_1.clone(), ingester_1);

        let ingester_id_2 = NodeId::from("test-ingester-2");
        let mut mock_ingester_2 = MockIngesterService::new();
        mock_ingester_2.expect_close_shards().never();

        let ingester_2 = IngesterServiceClient::tower()
            .stack_close_shards_layer(DelayLayer::new(CLOSE_SHARDS_REQUEST_TIMEOUT * 2))
            .build_from_mock(mock_ingester_2);
        ingester_pool.insert(ingester_id_2.clone(), ingester_2);

        // In this test:
        // - ingester 0 will close shard 0 successfully and fail to close shard 1;
        // - ingester 1 will return an error;
        // - ingester 2 will time out;
        // - ingester 3 will be unavailable.

        let shards_to_close = vec![
            Shard {
                index_uid: Some(IndexUid::for_test("test-index", 0)),
                source_id: "test-source".to_string(),
                shard_id: Some(ShardId::from(0)),
                leader_id: ingester_id_0.to_string(),
                ..Default::default()
            },
            Shard {
                index_uid: Some(IndexUid::for_test("test-index", 0)),
                source_id: "test-source".to_string(),
                shard_id: Some(ShardId::from(1)),
                leader_id: ingester_id_0.to_string(),
                ..Default::default()
            },
            Shard {
                index_uid: Some(IndexUid::for_test("test-index", 0)),
                source_id: "test-source".to_string(),
                shard_id: Some(ShardId::from(2)),
                leader_id: ingester_id_1.to_string(),
                ..Default::default()
            },
            Shard {
                index_uid: Some(IndexUid::for_test("test-index", 0)),
                source_id: "test-source".to_string(),
                shard_id: Some(ShardId::from(3)),
                leader_id: ingester_id_2.to_string(),
                ..Default::default()
            },
            Shard {
                index_uid: Some(IndexUid::for_test("test-index", 0)),
                source_id: "test-source".to_string(),
                shard_id: Some(ShardId::from(4)),
                leader_id: "test-ingester-3".to_string(),
                ..Default::default()
            },
        ];
        let closed_shards = controller.close_shards(shards_to_close).await;
        assert_eq!(closed_shards.len(), 1);

        let closed_shard = &closed_shards[0];
        assert_eq!(closed_shard.index_uid(), &("test-index", 0));
        assert_eq!(closed_shard.source_id, "test-source");
        assert_eq!(closed_shard.shard_id(), ShardId::from(0));
    }

    #[tokio::test]
    async fn test_ingest_controller_rebalance_shards() {
        setup_logging_for_tests();

        let mut mock_metastore = MockMetastoreService::new();
        mock_metastore.expect_open_shards().return_once(|request| {
            assert_eq!(request.subrequests.len(), 1);

            let subrequest_0 = &request.subrequests[0];
            assert_eq!(subrequest_0.subrequest_id, 0);
            assert_eq!(subrequest_0.index_uid(), &("test-index", 0));
            assert_eq!(subrequest_0.source_id, INGEST_V2_SOURCE_ID.to_string());
            assert_eq!(subrequest_0.leader_id, "test-ingester-1");
            assert!(subrequest_0.follower_id.is_none());

            let subresponses = vec![metastore::OpenShardSubresponse {
                subrequest_id: 0,
                open_shard: Some(Shard {
                    index_uid: Some(IndexUid::for_test("test-index", 0)),
                    source_id: INGEST_V2_SOURCE_ID.to_string(),
                    shard_id: subrequest_0.shard_id.clone(),
                    leader_id: "test-ingester-1".to_string(),
                    shard_state: ShardState::Open as i32,
                    ..Default::default()
                }),
            }];
            let response = metastore::OpenShardsResponse { subresponses };
            Ok(response)
        });
        let metastore = MetastoreServiceClient::from_mock(mock_metastore);
        let ingester_pool = IngesterPool::default();
        let replication_factor = 1;
        let mut controller = IngestController::new(
            metastore,
            ingester_pool.clone(),
            replication_factor,
            TEST_SHARD_THROUGHPUT_LIMIT_MIB,
            1.001,
        );

        let mut model = ControlPlaneModel::default();

        let universe = Universe::with_accelerated_time();
        let (control_plane_mailbox, control_plane_inbox) = universe.create_test_mailbox();
        let progress = Progress::default();

        let close_shards_task_opt = controller
            .rebalance_shards(&mut model, &control_plane_mailbox, &progress)
            .await
            .unwrap();
        assert!(close_shards_task_opt.is_none());

        let index_metadata = IndexMetadata::for_test("test-index", "ram://indexes/test-index");
        let index_uid = index_metadata.index_uid.clone();
        model.add_index(index_metadata);

        let source_config = SourceConfig::ingest_v2();
        model.add_source(&index_uid, source_config).unwrap();

        // In this test, ingester 0 hosts 5 shards but there are two ingesters in the cluster.
        // `rebalance_shards` will attempt to move 2 shards to ingester 1. However, it will fail to
        // init one shard, so only one shard will be actually moved.

        let open_shards = vec![
            Shard {
                index_uid: Some(index_uid.clone()),
                source_id: INGEST_V2_SOURCE_ID.to_string(),
                shard_id: Some(ShardId::from(0)),
                leader_id: "test-ingester-0".to_string(),
                shard_state: ShardState::Open as i32,
                ..Default::default()
            },
            Shard {
                index_uid: Some(index_uid.clone()),
                source_id: INGEST_V2_SOURCE_ID.to_string(),
                shard_id: Some(ShardId::from(1)),
                leader_id: "test-ingester-0".to_string(),
                shard_state: ShardState::Open as i32,
                ..Default::default()
            },
            Shard {
                index_uid: Some(index_uid.clone()),
                source_id: INGEST_V2_SOURCE_ID.to_string(),
                shard_id: Some(ShardId::from(2)),
                leader_id: "test-ingester-0".to_string(),
                shard_state: ShardState::Open as i32,
                ..Default::default()
            },
            Shard {
                index_uid: Some(index_uid.clone()),
                source_id: INGEST_V2_SOURCE_ID.to_string(),
                shard_id: Some(ShardId::from(3)),
                leader_id: "test-ingester-0".to_string(),
                shard_state: ShardState::Open as i32,
                ..Default::default()
            },
            Shard {
                index_uid: Some(index_uid.clone()),
                source_id: INGEST_V2_SOURCE_ID.to_string(),
                shard_id: Some(ShardId::from(4)),
                leader_id: "test-ingester-0".to_string(),
                shard_state: ShardState::Open as i32,
                ..Default::default()
            },
        ];
        model.insert_shards(&index_uid, &INGEST_V2_SOURCE_ID.to_string(), open_shards);

        let ingester_id_0 = NodeId::from("test-ingester-0");
        let mut mock_ingester_0 = MockIngesterService::new();
        mock_ingester_0
            .expect_close_shards()
            .once()
            .returning(|request| {
                assert_eq!(request.shard_pkeys.len(), 1);

                let shard = &request.shard_pkeys[0];
                assert_eq!(shard.index_uid(), &("test-index", 0));
                assert_eq!(shard.source_id, INGEST_V2_SOURCE_ID);
                // assert_eq!(shard.shard_id(), ShardId::from(2));

                let response = CloseShardsResponse {
                    successes: vec![shard.clone()],
                };
                Ok(response)
            });
        let ingester_0 = IngesterServiceClient::from_mock(mock_ingester_0);
        ingester_pool.insert(ingester_id_0.clone(), ingester_0);

        let ingester_id_1 = NodeId::from("test-ingester-1");
        let mut mock_ingester_1 = MockIngesterService::new();
        mock_ingester_1.expect_init_shards().return_once(|request| {
            assert_eq!(request.subrequests.len(), 2);

            let subrequest_0 = &request.subrequests[0];
            assert_eq!(subrequest_0.subrequest_id, 0);

            let shard_0 = request.subrequests[0].shard();
            assert_eq!(shard_0.index_uid(), &("test-index", 0));
            assert_eq!(shard_0.source_id, INGEST_V2_SOURCE_ID.to_string());
            assert_eq!(shard_0.leader_id, "test-ingester-1");
            assert!(shard_0.follower_id.is_none());

            let subrequest_1 = &request.subrequests[1];
            assert_eq!(subrequest_1.subrequest_id, 1);

            let shard_1 = request.subrequests[0].shard();
            assert_eq!(shard_1.index_uid(), &("test-index", 0));
            assert_eq!(shard_1.source_id, INGEST_V2_SOURCE_ID.to_string());
            assert_eq!(shard_1.leader_id, "test-ingester-1");
            assert!(shard_1.follower_id.is_none());

            let successes = vec![InitShardSuccess {
                subrequest_id: request.subrequests[0].subrequest_id,
                shard: Some(shard_0.clone()),
            }];
            let failures = vec![InitShardFailure {
                subrequest_id: request.subrequests[1].subrequest_id,
                index_uid: Some(IndexUid::for_test("test-index", 0)),
                source_id: INGEST_V2_SOURCE_ID.to_string(),
                shard_id: Some(shard_1.shard_id().clone()),
            }];
            let response = InitShardsResponse {
                successes,
                failures,
            };
            Ok(response)
        });
        let ingester_1 = IngesterServiceClient::from_mock(mock_ingester_1);
        ingester_pool.insert(ingester_id_1.clone(), ingester_1);

        let close_shards_task = controller
            .rebalance_shards(&mut model, &control_plane_mailbox, &progress)
            .await
            .unwrap()
            .unwrap();

        tokio::time::timeout(CLOSE_SHARDS_REQUEST_TIMEOUT * 2, close_shards_task)
            .await
            .unwrap()
            .unwrap();

        let callbacks: Vec<RebalanceShardsCallback> = control_plane_inbox.drain_for_test_typed();
        assert_eq!(callbacks.len(), 1);

        let callback = &callbacks[0];
        assert_eq!(callback.closed_shards.len(), 1);
    }

    // #[track_caller]
    fn test_allocate_shards_aux_aux(
        shard_counts_map: &HashMap<NodeId, usize>,
        num_shards: usize,
        replication_enabled: bool,
    ) {
        let shard_allocations_opt =
            super::allocate_shards(shard_counts_map, num_shards, replication_enabled);
        if num_shards == 0 {
            assert_eq!(shard_allocations_opt, Some(Vec::new()));
            return;
        }
        let num_nodes_required = if replication_enabled { 2 } else { 1 };
        if shard_counts_map.len() < num_nodes_required {
            assert!(shard_allocations_opt.is_none());
            return;
        }
        let shard_allocations = shard_allocations_opt.unwrap();
        let mut total_counts: HashMap<&NodeIdRef, usize> = HashMap::default();
        assert_eq!(shard_allocations.len(), num_shards);
        if num_shards == 0 {
            return;
        }
        for (leader, follower_opt) in shard_allocations {
            assert_eq!(follower_opt.is_some(), replication_enabled);
            *total_counts.entry(leader).or_default() += 1;
            if let Some(follower) = follower_opt {
                *total_counts.entry(follower).or_default() += 1;
                assert_ne!(follower, leader);
            }
        }
        for (shard, count) in shard_counts_map {
            if let Some(shard_count) = total_counts.get_mut(shard.as_ref()) {
                *shard_count += *count;
            }
        }
        let (min, max) = total_counts
            .values()
            .copied()
            .minmax()
            .into_option()
            .unwrap();
        if !replication_enabled {
            // If replication is enabled, we can end up being forced to not spread shards as evenly
            // as we would wish. For instance, if there are only two nodes initially
            // unbalanced.
            assert!(min + 1 >= max);
        } else {
            let (previous_min, previous_max) = shard_counts_map
                .values()
                .copied()
                .minmax()
                .into_option()
                .unwrap();
            // The algorithm is supposed to reduce the variance.
            // Of course sometimes it is not possible. For instance for 3 nodes that are
            // perfectly balanced to begin with, if we as for a single shard.
            assert!((previous_max - previous_min).max(1) >= (max - min));
        }
    }

    fn test_allocate_shards_aux(shard_counts: &[usize]) {
        let mut shard_counts_map: HashMap<NodeId, usize> = HashMap::new();
        let shards: Vec<String> = (0..shard_counts.len())
            .map(|i| format!("shard-{i}"))
            .collect();
        for (shard, &shard_count) in shards.into_iter().zip(shard_counts.iter()) {
            shard_counts_map.insert(NodeId::from(shard), shard_count);
        }
        for i in 0..10 {
            test_allocate_shards_aux_aux(&shard_counts_map, i, false);
            test_allocate_shards_aux_aux(&shard_counts_map, i, true);
        }
    }

    use proptest::prelude::*;

    proptest! {
        #[test]
        fn test_proptest_allocate_shards(shard_counts in proptest::collection::vec(0..10usize, 0..10usize)) {
            test_allocate_shards_aux(&shard_counts);
        }
    }

    #[test]
    fn test_allocate_shards_prop_test() {
        test_allocate_shards_aux(&[]);
        test_allocate_shards_aux(&[1]);
        test_allocate_shards_aux(&[1, 1]);
        test_allocate_shards_aux(&[1, 2]);
        test_allocate_shards_aux(&[1, 4]);
        test_allocate_shards_aux(&[2, 3, 2]);
        test_allocate_shards_aux(&[2, 4, 6]);
        test_allocate_shards_aux(&[2, 3, 10]);
    }

    #[test]
    fn test_allocate_shards_prop_test_bug() {
        test_allocate_shards_aux(&[7, 7, 7]);
    }

    #[test]
    fn test_pick_one() {
        let mut shard_counts = BTreeMap::default();
        shard_counts.insert(
            1,
            vec![NodeIdRef::from_str("node1"), NodeIdRef::from_str("node2")],
        );
        let mut rng = rand::thread_rng();
        let node = pick_one(
            &mut shard_counts,
            Some(NodeIdRef::from_str("node2")),
            &mut rng,
        )
        .unwrap();
        assert_eq!(node.as_str(), "node1");
        assert_eq!(shard_counts.len(), 2);
        assert_eq!(
            &shard_counts.get(&1).unwrap()[..],
            &[NodeIdRef::from_str("node2")]
        );
        assert_eq!(
            &shard_counts.get(&2).unwrap()[..],
            &[NodeIdRef::from_str("node1")]
        );
        let node = pick_one(&mut shard_counts, None, &mut rng).unwrap();
        assert_eq!(node.as_str(), "node2");
        assert_eq!(shard_counts.len(), 1);
        assert_eq!(
            &shard_counts.get(&2).unwrap()[..],
            &[NodeIdRef::from_str("node1"), NodeIdRef::from_str("node2")]
        );
    }

    fn test_compute_shards_to_rebalance_aux(shard_allocation: &[usize]) {
        let index_id = "test-index";
        let index_metadata = IndexMetadata::for_test(index_id, "ram://indexes/test-index");
        let index_uid = index_metadata.index_uid.clone();
        let source_id: SourceId = "test-source".to_string();

        let mut model = ControlPlaneModel::default();
        model.add_index(index_metadata.clone());

        let mut source_config = SourceConfig::ingest_v2();
        source_config.source_id = source_id.to_string();
        model.add_source(&index_uid, source_config).unwrap();

        let ingester_pool = IngesterPool::default();
        let mock_ingester = MockIngesterService::new();
        let ingester_client = IngesterServiceClient::from_mock(mock_ingester);

        let ingester_ids: Vec<String> = (0..shard_allocation.len())
            .map(|i| format!("test-ingester-{}", i))
            .collect();

        for ingester_id in &ingester_ids {
            ingester_pool.insert(NodeId::from(ingester_id.clone()), ingester_client.clone());
        }
        let mut shards = Vec::new();

        for (ingester_idx, &num_shards) in shard_allocation.iter().enumerate() {
            for _ in 0..num_shards {
                let shard_id = shards.len() as u64;
                let shard = Shard {
                    index_uid: Some(index_uid.clone()),
                    source_id: source_id.to_string(),
                    shard_id: Some(ShardId::from(shard_id)),
                    leader_id: ingester_ids[ingester_idx].clone(),
                    shard_state: ShardState::Open as i32,
                    ..Default::default()
                };
                shards.push(shard);
            }
        }
        model.insert_shards(&index_uid, &source_id, shards.clone());

        let controller = IngestController::new(
            MetastoreServiceClient::mocked(),
            ingester_pool.clone(),
            2, // replication_factor
            TEST_SHARD_THROUGHPUT_LIMIT_MIB,
            1.001,
        );
        let shards_to_rebalance = controller.compute_shards_to_rebalance(&model);

        let source_uid = SourceUid {
            index_uid: index_uid.clone(),
            source_id: source_id.clone(),
        };
        let shard_ids_to_rebalance: Vec<ShardId> = shards_to_rebalance
            .iter()
            .flat_map(|shard| shard.shard_id.clone())
            .collect();

        let closed_shard_ids = model.close_shards(&source_uid, &shard_ids_to_rebalance);
        assert_eq!(closed_shard_ids.len(), shards_to_rebalance.len());

        let mut per_ingester_num_shards: HashMap<&str, usize> = HashMap::new();

        for shard in model.all_shards() {
            if shard.is_open() {
                *per_ingester_num_shards.entry(&shard.leader_id).or_default() += 1;
            }
        }
        for ingester_id in &ingester_ids {
            per_ingester_num_shards
                .entry(ingester_id.as_str())
                .or_default();
        }
        let mut per_ingester_num_shards_sorted: BTreeSet<(usize, &str)> = per_ingester_num_shards
            .into_iter()
            .map(|(ingester_id, num_shards)| (num_shards, ingester_id))
            .collect();
        let mut opened_shards: Vec<Shard> = Vec::new();
        let mut shard_id = shards.len() as u64;

        for _ in 0..shards_to_rebalance.len() {
            let (num_shards, ingester_id) = per_ingester_num_shards_sorted.pop_first().unwrap();
            let opened_shard = Shard {
                index_uid: Some(index_uid.clone()),
                source_id: source_id.to_string(),
                shard_id: Some(ShardId::from(shard_id)),
                leader_id: ingester_id.to_string(),
                shard_state: ShardState::Open as i32,
                ..Default::default()
            };
            per_ingester_num_shards_sorted.insert((num_shards + 1, ingester_id));
            opened_shards.push(opened_shard);
            shard_id += 1;
        }
        let num_open_shards: usize = per_ingester_num_shards_sorted
            .iter()
            .map(|(num_shards, _)| num_shards)
            .sum();
        let target_num_open_shards_per_leader = num_open_shards as f32 / ingester_ids.len() as f32;
        let max_num_open_shards_per_leader =
            f32::ceil(target_num_open_shards_per_leader * 1.1) as usize;
        let min_num_open_shards_per_leader =
            f32::floor(target_num_open_shards_per_leader * 0.9) as usize;
        assert!(
            per_ingester_num_shards_sorted
                .iter()
                .all(
                    |(num_shards, _)| *num_shards >= min_num_open_shards_per_leader
                        && *num_shards <= max_num_open_shards_per_leader
                )
        );

        // Test stability of the algorithm
        model.insert_shards(&index_uid, &source_id, opened_shards);

        let shards_to_rebalance = controller.compute_shards_to_rebalance(&model);
        assert!(shards_to_rebalance.is_empty());
    }

    proptest! {
        #[test]
        fn test_compute_shards_to_rebalance_proptest(
            shard_allocation in proptest::collection::vec(0..13usize, 0..13usize),
        ) {
            test_compute_shards_to_rebalance_aux(&shard_allocation);
        }
    }

    #[test]
    fn test_compute_shards_to_rebalance() {
        test_compute_shards_to_rebalance_aux(&[]);
        test_compute_shards_to_rebalance_aux(&[0]);
        test_compute_shards_to_rebalance_aux(&[1]);
        test_compute_shards_to_rebalance_aux(&[0, 1]);
    }
}
