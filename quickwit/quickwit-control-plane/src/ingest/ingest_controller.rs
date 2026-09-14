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

use std::collections::{BTreeSet, HashMap, HashSet};
use std::fmt;
use std::future::Future;
use std::num::NonZeroUsize;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use fnv::FnvHashSet;
use futures::StreamExt;
use futures::stream::FuturesUnordered;
use itertools::{Itertools as _, MinMaxResult};
use quickwit_actors::Mailbox;
use quickwit_common::Progress;
use quickwit_common::pretty::PrettySample;
use quickwit_ingest::{IngesterPool, LocalShardsUpdate};
use quickwit_proto::control_plane::{
    AdviseResetShardsRequest, AdviseResetShardsResponse, GetOrCreateOpenShardsFailureReason,
    GetOrCreateOpenShardsRequest, GetOrCreateOpenShardsResponse, GetOrCreateOpenShardsSubrequest,
    GetOrCreateOpenShardsSuccess,
};
use quickwit_proto::ingest::ingester::{
    CloseShardsRequest, CloseShardsResponse, IngesterService, IngesterStatus, InitShardFailure,
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
use quickwit_proto::types::{IndexUid, NodeId, Position, ShardId, SourceUid};
use rand::prelude::IndexedRandom;
use rand::rngs::ThreadRng;
use rand::seq::SliceRandom;
use rand::{Rng, rng};
use serde::{Deserialize, Serialize};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use tracing::{Level, debug, enabled, error, info, instrument, warn};
use ulid::Ulid;

use super::scaling_arbiter::ScalingArbiter;
use crate::control_plane::ControlPlane;
use crate::ingest::wait_handle::WaitHandle;
use crate::metrics::REBALANCE_SHARDS;
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

type Zone = String;

type SourceShardCount = HashMap<SourceUid, usize>;

fn total_shards(source_shard_counts: &SourceShardCount) -> usize {
    source_shard_counts.values().sum()
}

/// In some cases, it could be advantageous to prefer to place shards in a specific zone (such as
/// when rebalancing the shards of a decommissioned ingester). Otherwise, the default is to spread
/// new shards evenly.
enum ShardPlacement {
    Balanced(SourceShardCount),
    Zoned(HashMap<Option<Zone>, SourceShardCount>),
}

struct EligibleIngester {
    node_id: NodeId,
    zone: Option<Zone>,
    num_open_shards: AtomicUsize,
}

/// Find the globally minimally loaded ingester. Break ties with the requested zone, if provided.
fn pick_least_loaded<'a>(
    eligible_ingesters: &'a [EligibleIngester],
    requested_zone: Option<&Zone>,
    rng: &mut ThreadRng,
) -> Option<&'a EligibleIngester> {
    let min_load = eligible_ingesters
        .iter()
        .map(|ingester| ingester.num_open_shards.load(Ordering::Relaxed))
        .min()?;
    let minima: Vec<&EligibleIngester> = eligible_ingesters
        .iter()
        .filter(|ingester| ingester.num_open_shards.load(Ordering::Relaxed) == min_load)
        .collect();
    let same_zone_minima: Vec<&EligibleIngester> = minima
        .iter()
        .copied()
        .filter(|ingester| requested_zone.is_some() && ingester.zone.as_ref() == requested_zone)
        .collect();
    let candidates = if !same_zone_minima.is_empty() {
        same_zone_minima
    } else {
        minima
    };
    candidates.choose(rng).copied()
}

fn all_ingesters_advertise_availability_zone(ingesters: &IngesterPool) -> bool {
    ingesters
        .keys_values()
        .iter()
        .all(|(_node_id, ingester)| ingester.availability_zone.is_some())
}

fn eligible_ingesters(
    ingester_pool: &IngesterPool,
    unavailable_ingesters: &FnvHashSet<NodeId>,
    model: &ControlPlaneModel,
    zonal_placement_enabled: bool,
) -> Vec<EligibleIngester> {
    let mut num_open_shards_by_ingester_id: HashMap<String, usize> = HashMap::new();
    for shard in model.all_shards() {
        if shard.is_open() {
            *num_open_shards_by_ingester_id
                .entry(shard.ingester_id.clone())
                .or_default() += 1;
        }
    }
    ingester_pool
        .keys_values()
        .into_iter()
        .filter(|(id, ingester)| ingester.status.is_ready() && !unavailable_ingesters.contains(id))
        .map(|(node_id, ingester)| EligibleIngester {
            num_open_shards: AtomicUsize::new(
                num_open_shards_by_ingester_id
                    .get(node_id.as_str())
                    .copied()
                    .unwrap_or(0),
            ),
            node_id,
            // If zonal placement is disabled, every ingester's zone is set to None, creating one
            // global "zonal" group.
            zone: ingester
                .availability_zone
                .filter(|_| zonal_placement_enabled),
        })
        .collect()
}

fn allocate_shards(
    eligible_ingesters: &[EligibleIngester],
    requested_zone: Option<Zone>,
    num_shards: usize,
) -> Option<Vec<NodeId>> {
    if eligible_ingesters.is_empty() {
        return None;
    }
    let mut rng = rng();
    let mut ingester_ids = Vec::with_capacity(num_shards);
    for _ in 0..num_shards {
        let picked = pick_least_loaded(eligible_ingesters, requested_zone.as_ref(), &mut rng)
            .expect("eligible ingesters non-empty");
        picked.num_open_shards.fetch_add(1, Ordering::Relaxed);
        ingester_ids.push(picked.node_id.clone());
    }
    Some(ingester_ids)
}

fn distribute_shards_across_zones(
    num_to_open: usize,
    zones: &HashSet<Zone>,
) -> HashMap<Option<Zone>, usize> {
    if num_to_open == 0 {
        return HashMap::new();
    }
    if zones.is_empty() {
        return HashMap::from([(None, num_to_open)]);
    }
    let mut shuffled: Vec<&Zone> = zones.iter().collect();
    shuffled.shuffle(&mut rng());
    shuffled
        .iter()
        .cycle()
        .take(num_to_open)
        .map(|zone| Some((*zone).clone()))
        .counts()
}

/// For each source's requested count, group shards to open by zone.
fn balance_shards_to_open_across_zones(
    source_shard_counts: SourceShardCount,
    eligible_ingesters: &[EligibleIngester],
) -> HashMap<Option<Zone>, SourceShardCount> {
    let zones: HashSet<Zone> = eligible_ingesters
        .iter()
        .filter_map(|ingester| ingester.zone.clone())
        .collect();
    let mut num_shards_by_source_by_zone: HashMap<Option<Zone>, SourceShardCount> = HashMap::new();
    for (source_uid, num_shards) in source_shard_counts {
        // Number of shards to open for this source in each zone.
        for (zone, count) in distribute_shards_across_zones(num_shards, &zones) {
            num_shards_by_source_by_zone
                .entry(zone)
                .or_default()
                .insert(source_uid.clone(), count);
        }
    }
    num_shards_by_source_by_zone
}

/// Matches successful replacement opens to shards that are still hosted by live ingesters.
///
/// The ingester pool can change while replacements are being opened. A shard whose ingester has
/// disappeared from the pool can no longer be closed directly, so it is deliberately left for the
/// control plane's self-healing mechanisms instead of turning normal cluster churn into a panic.
fn match_shards_to_close(
    ingester_pool: &IngesterPool,
    opened_by_zone: &HashMap<Option<Zone>, SourceShardCount>,
    shards_to_rebalance: &mut Vec<Shard>,
) -> Vec<Shard> {
    let mut shards_to_close: Vec<Shard> = Vec::new();
    for (requested_zone, opened_by_source) in opened_by_zone {
        for (source_uid, &num_opened) in opened_by_source {
            let mut num_matched = 0;
            for _ in 0..num_opened {
                let Some(position) = shards_to_rebalance.iter().position(|shard| {
                    shard.source_uid() == *source_uid
                        && ingester_pool
                            .get(shard.ingester_id.as_str())
                            .and_then(|ingester| ingester.availability_zone)
                            == *requested_zone
                }) else {
                    break;
                };
                shards_to_close.push(shards_to_rebalance.swap_remove(position));
                num_matched += 1;
            }
            if num_matched < num_opened {
                warn!(
                    index_uid = %source_uid.index_uid,
                    source_id = %source_uid.source_id,
                    ?requested_zone,
                    num_opened,
                    num_matched,
                    "could not match every replacement shard to a live predecessor"
                );
            }
        }
    }
    shards_to_close
}

#[derive(Debug, Default, Clone, Copy, Serialize, Deserialize)]
pub struct IngestControllerStats {
    pub num_rebalance_shards_ops: usize,
}

pub struct IngestController {
    pub(crate) ingester_pool: IngesterPool,
    pub(crate) stats: IngestControllerStats,
    metastore: MetastoreServiceClient,
    // This semaphore ensures that only one rebalance operation is performed at a time.
    rebalance_semaphore: Arc<Semaphore>,
    scaling_arbiter: ScalingArbiter,
}

impl fmt::Debug for IngestController {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.debug_struct("IngestController")
            .field("ingester_pool", &self.ingester_pool)
            .field("metastore", &self.metastore)
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

/// Returns `true` if the ingester is available, i.e. in the ingester pool and ready to serve
/// requests.
fn is_ingester_available_and_ready(ingester_pool: &IngesterPool, ingester_id: &str) -> bool {
    ingester_pool
        .get(ingester_id)
        .map(|ingester| ingester.status.is_ready())
        .unwrap_or(false)
}

fn get_open_shard_from_model(
    get_open_shards_subrequest: &GetOrCreateOpenShardsSubrequest,
    model: &ControlPlaneModel,
    ingester_pool: &IngesterPool,
    unavailable_ingesters: &FnvHashSet<NodeId>,
) -> Result<Option<GetOrCreateOpenShardsSuccess>, GetOrCreateOpenShardsFailureReason> {
    let Some(index_uid) = model.index_uid(&get_open_shards_subrequest.index_id) else {
        return Err(GetOrCreateOpenShardsFailureReason::IndexNotFound);
    };
    let Some(open_shard_entries) = model.find_open_shards(
        index_uid,
        &get_open_shards_subrequest.source_id,
        unavailable_ingesters,
    ) else {
        return Err(GetOrCreateOpenShardsFailureReason::SourceNotFound);
    };
    let open_shards: Vec<Shard> = open_shard_entries
        .into_iter()
        .filter(|shard_entry| {
            is_ingester_available_and_ready(ingester_pool, shard_entry.ingester_id.as_str())
        })
        .map(|shard_entry| shard_entry.shard)
        .collect();
    if open_shards.is_empty() {
        return Ok(None);
    }
    let success = GetOrCreateOpenShardsSuccess {
        subrequest_id: get_open_shards_subrequest.subrequest_id,
        index_uid: Some(index_uid.clone()),
        source_id: get_open_shards_subrequest.source_id.clone(),
        open_shards,
    };
    Ok(Some(success))
}

impl IngestController {
    pub fn new(
        metastore: MetastoreServiceClient,
        ingester_pool: IngesterPool,
        max_shard_ingestion_throughput_mib_per_sec: f32,
        shard_scale_up_factor: f32,
    ) -> Self {
        IngestController {
            metastore,
            ingester_pool,
            rebalance_semaphore: Arc::new(Semaphore::new(1)),
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
    fn sync_with_ingester(&self, ingester_id: &NodeId, model: &ControlPlaneModel) -> WaitHandle {
        info!(ingester = %ingester_id, "sync_with_ingester");
        let (wait_drop_guard, wait_handle) = WaitHandle::new();
        let Some(ingester) = self.ingester_pool.get(ingester_id) else {
            // TODO: (Maybe) We should mark the ingester as unavailable, and stop advertise its
            // shard to routers.
            warn!("failed to sync with ingester `{ingester_id}`: not available");
            return wait_handle;
        };
        let mut retain_shards_req = RetainShardsRequest::default();
        for (source_uid, shard_ids) in &*model.list_shards_for_node(ingester_id) {
            let shards_for_source = RetainShardsForSource {
                index_uid: Some(source_uid.index_uid.clone()),
                source_id: source_uid.source_id.clone(),
                shard_ids: shard_ids.iter().cloned().collect(),
            };
            retain_shards_req
                .retain_shards_for_sources
                .push(shards_for_source);
        }
        info!(%ingester_id, "retain shards ingester");
        let operation: String = format!("retain shards `{ingester_id}`");
        fire_and_forget(
            async move {
                if let Err(retain_shards_err) =
                    ingester.client.retain_shards(retain_shards_req).await
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

        let mut num_shards_to_open_by_source = SourceShardCount::new();

        let unavailable_ingesters: FnvHashSet<NodeId> = get_open_shards_request
            .unavailable_ingesters
            .into_iter()
            .map(|id| NodeId::from_str(&id))
            .collect();

        // We do a first pass to identify the shards that are missing from the model and need to be
        // created.
        for get_open_shards_subrequest in &get_open_shards_request.subrequests {
            if let Ok(None) = get_open_shard_from_model(
                get_open_shards_subrequest,
                model,
                &self.ingester_pool,
                &unavailable_ingesters,
            ) {
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
                num_shards_to_open_by_source.insert(source_uid, min_shards);
            }
        }

        if let Err(metastore_error) = self
            .try_open_shards(
                ShardPlacement::Balanced(num_shards_to_open_by_source),
                model,
                &unavailable_ingesters,
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
                &self.ingester_pool,
                &unavailable_ingesters,
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

    /// Calls init shards on the ingesters hosting newly opened shards.
    async fn init_shards(
        &self,
        init_shard_subrequests: Vec<InitShardSubrequest>,
        progress: &Progress,
    ) -> InitShardsResponse {
        let mut successes = Vec::with_capacity(init_shard_subrequests.len());
        let mut failures = Vec::new();

        let mut shards_to_init_by_ingester_id: HashMap<NodeId, Vec<InitShardSubrequest>> =
            HashMap::new();

        for init_shard_subrequest in init_shard_subrequests {
            let ingester_id = NodeId::from_str(&init_shard_subrequest.shard().ingester_id);
            shards_to_init_by_ingester_id
                .entry(ingester_id)
                .or_default()
                .push(init_shard_subrequest);
        }
        let mut init_shards_futures = FuturesUnordered::new();

        for (ingester_id, subrequests) in shards_to_init_by_ingester_id {
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
            let Some(ingester) = self.ingester_pool.get(ingester_id.as_str()) else {
                warn!("failed to init shards: ingester `{ingester_id}` is unavailable");
                failures.extend(init_shard_failures);
                continue;
            };
            let init_shards_request = InitShardsRequest { subrequests };
            let init_shards_future = async move {
                let init_shards_result = tokio::time::timeout(
                    INIT_SHARDS_REQUEST_TIMEOUT,
                    ingester.client.init_shards(init_shards_request),
                )
                .await;
                (ingester_id.clone(), init_shards_result, init_shard_failures)
            };
            init_shards_futures.push(init_shards_future);
        }
        while let Some((ingester_id, init_shards_result, init_shard_failures)) =
            progress.protect_future(init_shards_futures.next()).await
        {
            match init_shards_result {
                Ok(Ok(init_shards_response)) => {
                    successes.extend(init_shards_response.successes);
                    failures.extend(init_shards_response.failures);
                }
                Ok(Err(error)) => {
                    error!(%error, "failed to init shards on `{ingester_id}`");
                    failures.extend(init_shard_failures);
                }
                Err(_elapsed) => {
                    error!("failed to init shards on `{ingester_id}`: request timed out");
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
        let num_shards_to_open_by_source: SourceShardCount =
            HashMap::from_iter([(source_uid.clone(), num_shards_to_open)]);
        let try_open_shards_result = self
            .try_open_shards(
                ShardPlacement::Balanced(num_shards_to_open_by_source),
                model,
                &Default::default(),
                progress,
            )
            .await;

        match try_open_shards_result {
            Ok(opened_shards) => {
                if opened_shards.is_empty() {
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

    /// Try to open shards given the counts by source and optional requested zones.
    async fn try_open_shards(
        &mut self,
        placement: ShardPlacement,
        model: &mut ControlPlaneModel,
        unavailable_ingesters: &FnvHashSet<NodeId>,
        progress: &Progress,
    ) -> MetastoreResult<HashMap<Option<Zone>, SourceShardCount>> {
        // Zonal aware placement is enabled only after every ingester has advertised its
        // zone. If not, every ingester's zone is set to None and the global balancing logic
        // applies.
        let zonal_placement_enabled =
            all_ingesters_advertise_availability_zone(&self.ingester_pool);
        let eligible_ingesters = eligible_ingesters(
            &self.ingester_pool,
            unavailable_ingesters,
            model,
            zonal_placement_enabled,
        );
        if eligible_ingesters.is_empty() {
            warn!("failed to open shards: no ingesters available");
            return Ok(HashMap::new());
        }
        let num_shards_to_open_by_source_by_zone = match placement {
            ShardPlacement::Balanced(num_shards_by_source) => {
                balance_shards_to_open_across_zones(num_shards_by_source, &eligible_ingesters)
            }
            ShardPlacement::Zoned(num_shards_by_source_by_zone) => num_shards_by_source_by_zone,
        };
        self.open_shards(
            num_shards_to_open_by_source_by_zone,
            &eligible_ingesters,
            model,
            progress,
        )
        .await
    }

    /// Iterates the per-zone groups, calling [`Self::try_open_shards_by_zone`] for each. The
    /// per-AZ structure is preserved in the result so callers can match opens back to
    /// their originating (source, AZ) bucket.
    async fn open_shards(
        &mut self,
        num_shards_by_source_by_zone: HashMap<Option<Zone>, SourceShardCount>,
        eligible_ingesters: &[EligibleIngester],
        model: &mut ControlPlaneModel,
        progress: &Progress,
    ) -> MetastoreResult<HashMap<Option<Zone>, SourceShardCount>> {
        let mut opened_by_zone: HashMap<Option<Zone>, SourceShardCount> = HashMap::new();
        for (requested_zone, num_shards_by_source) in num_shards_by_source_by_zone {
            let opened = self
                .try_open_shards_by_zone(
                    num_shards_by_source,
                    requested_zone.clone(),
                    eligible_ingesters,
                    model,
                    progress,
                )
                .await?;
            if !opened.is_empty() {
                opened_by_zone.insert(requested_zone, opened);
            }
        }
        Ok(opened_by_zone)
    }

    /// Attempts to open shards for different sources
    /// The values in `num_shards_to_open_by_source` specify how many shards to open for each
    /// source.
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
    async fn try_open_shards_by_zone(
        &mut self,
        num_shards_to_open_by_source: SourceShardCount,
        requested_zone: Option<Zone>,
        eligible_ingesters: &[EligibleIngester],
        model: &mut ControlPlaneModel,
        progress: &Progress,
    ) -> MetastoreResult<SourceShardCount> {
        let num_shards_to_open = total_shards(&num_shards_to_open_by_source);

        if num_shards_to_open == 0 {
            return Ok(HashMap::new());
        }
        let Some(ingester_ids) =
            allocate_shards(eligible_ingesters, requested_zone, num_shards_to_open)
        else {
            return Ok(HashMap::new());
        };
        let source_uids_with_multiplicity = num_shards_to_open_by_source
            .iter()
            .flat_map(|(source_uid, &num_shards)| std::iter::repeat_n(source_uid, num_shards));

        let mut init_shard_subrequests: Vec<InitShardSubrequest> = Vec::new();

        for (subrequest_id, (source_uid, ingester_id)) in
            source_uids_with_multiplicity.zip(ingester_ids).enumerate()
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
                ingester_id: ingester_id.to_string(),
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
            .map(|init_shard_success| {
                let shard = init_shard_success.shard();

                OpenShardSubrequest {
                    subrequest_id: init_shard_success.subrequest_id,
                    index_uid: shard.index_uid.clone(),
                    source_id: shard.source_id.clone(),
                    shard_id: shard.shard_id.clone(),
                    ingester_id: shard.ingester_id.clone(),
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

        let mut num_opened_shards_by_source: SourceShardCount = HashMap::new();

        for open_shard_subresponse in open_shards_response.subresponses {
            let source_uid = open_shard_subresponse.open_shard().source_uid();
            *num_opened_shards_by_source.entry(source_uid).or_default() += 1;
        }

        Ok(num_opened_shards_by_source)
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
        let Some((ingester_id, shard_id)) = find_scale_down_candidate(&source_uid, model) else {
            model.release_scaling_permits(&source_uid, ScalingMode::Down);
            return Ok(());
        };
        info!("scaling down shard {shard_id} from {ingester_id}");
        let Some(ingester) = self.ingester_pool.get(&ingester_id) else {
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
            .protect_future(ingester.client.close_shards(close_shards_request))
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
    /// Moving a shard consists of opening a new one on the target ingester and closing the shard
    /// on the source ingester. We attempt to open new shards in the zone in which the original was
    /// closed, but fall back to cross-zonal if it creates better global balance.
    ///
    /// This method uses a single semaphore permit to ensure that only one rebalance operation is
    /// performed at a time.
    #[instrument(skip_all)]
    pub(crate) async fn rebalance_shards(
        &mut self,
        model: &mut ControlPlaneModel,
        mailbox: &Mailbox<ControlPlane>,
        progress: &Progress,
    ) -> MetastoreResult<usize> {
        let Ok(rebalance_permit) = self.rebalance_semaphore.clone().try_acquire_owned() else {
            debug!("skipping rebalance: another rebalance is already in progress");
            return Ok(0);
        };
        self.stats.num_rebalance_shards_ops += 1;

        let mut shards_to_rebalance: Vec<Shard> = self.compute_shards_to_rebalance(model);

        REBALANCE_SHARDS.set(shards_to_rebalance.len() as f64);

        if shards_to_rebalance.is_empty() {
            debug!("skipping rebalance: no shards to rebalance");
            return Ok(0);
        }
        let mut replacement_counts_by_zone: HashMap<Option<Zone>, SourceShardCount> =
            HashMap::new();
        for shard in &shards_to_rebalance {
            let zone = self
                .ingester_pool
                .get(shard.ingester_id.as_str())
                .and_then(|ingester| ingester.availability_zone);
            *replacement_counts_by_zone
                .entry(zone)
                .or_default()
                .entry(shard.source_uid())
                .or_default() += 1;
        }

        let opened_by_zone = self
            .try_open_shards(
                ShardPlacement::Zoned(replacement_counts_by_zone),
                model,
                &Default::default(),
                progress,
            )
            .await
            .inspect_err(|error| {
                error!(%error, "failed to open shards during rebalance");
                REBALANCE_SHARDS.set(0.0);
            })?;

        // For every shard we successfully opened, close an equivalent from the zone we requested
        // it in. The preferred zone is not necessarily the zone in which the replacement landed.
        // Pool membership may have changed while opening replacements, so matching is best effort.
        let num_opened_shards: usize = opened_by_zone.values().map(total_shards).sum();
        let shards_to_close = match_shards_to_close(
            &self.ingester_pool,
            &opened_by_zone,
            &mut shards_to_rebalance,
        );

        REBALANCE_SHARDS.set(num_opened_shards as f64);

        for source_uid in opened_by_zone
            .values()
            .flat_map(|opened_by_source| opened_by_source.keys())
            .unique()
        {
            // We temporarily disable the ability the scale down the number of shards for
            // the source to avoid closing the shards we just opened.
            model.drain_scaling_permits(source_uid, ScalingMode::Down);
        }
        let close_shards_fut = self.close_shards(shards_to_close);
        let mailbox_clone = mailbox.clone();

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
                rebalance_permit,
            };
            let _ = mailbox_clone.send_message(callback).await;
        };
        tokio::spawn(close_shards_and_send_callback_fut);

        if num_opened_shards > 0 {
            info!("rebalance opened {num_opened_shards} new shards");
        }
        Ok(num_opened_shards)
    }

    /// Computes shards that need to be rebalanced.
    ///
    /// This function identifies which shards should be moved to achieve a balance across available
    /// ingesters.
    /// It does not mutate any state. It just identifies the list of shards
    /// that need to be rebalanced.
    ///
    /// Unfortunately, we cannot move shards that are on unavailable ingesters.
    /// The closing operation can only be done by the ingester of that shard.
    /// For these reason, we exclude these shards from the rebalance process.
    fn compute_shards_to_rebalance(&self, model: &ControlPlaneModel) -> Vec<Shard> {
        let mut shards_by_ready_ingester_id: HashMap<NodeId, Vec<&Shard>> = HashMap::new();
        let mut retiring_ingesters: HashSet<NodeId> = HashSet::new();

        for (ingester_id, ingester) in self.ingester_pool.keys_values() {
            if ingester.status.is_ready() {
                shards_by_ready_ingester_id.insert(ingester_id, Vec::new());
            } else if ingester.status == IngesterStatus::Retiring {
                retiring_ingesters.insert(ingester_id);
            }
        }

        let mut shards_to_rebalance: Vec<Shard> = Vec::new();
        let mut num_ready_shards: usize = 0;

        for shard in model.all_shards() {
            if !shard.is_open() {
                continue;
            }
            if let Some(shards) = shards_by_ready_ingester_id.get_mut(shard.ingester_id.as_str()) {
                // Shards on ready ingesters participate in the balancing logic.
                num_ready_shards += 1;
                shards.push(&shard.shard);
            } else if retiring_ingesters.contains(shard.ingester_id.as_str()) {
                // All open shards on retiring ingesters must be rebalanced.
                shards_to_rebalance.push(shard.shard.clone());
            }
        }

        let num_retiring_shards = shards_to_rebalance.len();
        let num_ready_ingesters = shards_by_ready_ingester_id.len();

        let mut rng = rng();
        let mut shuffled_open_shards_by_ingester: Vec<Vec<&Shard>> = shards_by_ready_ingester_id
            .into_values()
            .map(|mut shards| {
                shards.shuffle(&mut rng);
                shards
            })
            .collect();

        // This is more of a loop-loop, but since we know it should exit before
        // `num_ready_shards`, we defensively use a for-loop.
        for _ in 0..num_ready_shards {
            let MinMaxResult::MinMax(min_shards, max_shards) = shuffled_open_shards_by_ingester
                .iter_mut()
                .minmax_by_key(|shards| shards.len())
            else {
                // There are less than 2 ingesters.
                // Nothing to do here.
                break;
            };

            // We leave a tolerance of 1/10 between the min and max number of shards per ingester
            const TOLERANCE_INV_RATIO: usize = 10;
            if max_shards.len()
                < min_shards.len() + min_shards.len().div_ceil(TOLERANCE_INV_RATIO).max(2)
            {
                break;
            }

            let shard = max_shards.pop().expect("shards should not be empty");
            shards_to_rebalance.push(shard.clone());
            min_shards.push(shard);
        }

        if shards_to_rebalance.is_empty() {
            debug!("no shards to rebalance");
        } else {
            info!(
                num_ready_shards,
                num_ready_ingesters,
                num_retiring_shards,
                num_shards_to_rebalance = shards_to_rebalance.len(),
                "rebalancing shards"
            );
        }
        shards_to_rebalance
    }

    /// Attempts to close the list of shards passed as argument.
    ///
    /// If ingesters are not available, the shards are not closed.
    fn close_shards(
        &self,
        shards_to_close: Vec<Shard>,
    ) -> impl Future<Output = Vec<ShardPKey>> + Send + 'static {
        let mut shards_to_close_by_ingester_id: HashMap<NodeId, Vec<ShardPKey>> = HashMap::new();

        for shard in shards_to_close {
            let shard_pkey = ShardPKey {
                index_uid: shard.index_uid,
                source_id: shard.source_id,
                shard_id: shard.shard_id,
            };
            let ingester_id = NodeId::from_str(&shard.ingester_id);
            shards_to_close_by_ingester_id
                .entry(ingester_id)
                .or_default()
                .push(shard_pkey);
        }
        let mut close_shards_futures = FuturesUnordered::new();

        for (ingester_id, shard_pkeys) in shards_to_close_by_ingester_id {
            let Some(ingester) = self.ingester_pool.get(&ingester_id) else {
                warn!("failed to close shards: ingester `{ingester_id}` is unavailable");
                continue;
            };
            let shards_to_close_request = CloseShardsRequest { shard_pkeys };
            let close_shards_future = async move {
                tokio::time::timeout(
                    CLOSE_SHARDS_REQUEST_TIMEOUT,
                    ingester.client.close_shards(shards_to_close_request),
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
    pub rebalance_permit: OwnedSemaphorePermit,
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
    let mut shard_entries_by_ingester_id: HashMap<NodeId, Vec<&ShardEntry>> = HashMap::new();
    let mut rng = rng();

    for shard in model.get_shards_for_source(source_uid)?.values() {
        if shard.is_open() {
            shard_entries_by_ingester_id
                .entry(NodeId::from_str(&shard.ingester_id))
                .or_default()
                .push(shard);
        }
    }
    shard_entries_by_ingester_id
        .into_iter()
        // We use a random number to break ties... The HashMap is randomly seeded so this is
        // should not make much difference, but we might want to be as explicit as possible.
        .max_by_key(|(_ingester_id, shard_entries)| (shard_entries.len(), rng.next_u32()))
        .map(|(ingester_id, shard_entries)| {
            (
                ingester_id,
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
    use quickwit_ingest::{IngesterPoolEntry, RateMibPerSec, ShardInfo};
    use quickwit_metastore::IndexMetadata;
    use quickwit_proto::control_plane::GetOrCreateOpenShardsSubrequest;
    use quickwit_proto::ingest::ingester::{
        CloseShardsResponse, IngesterServiceClient, IngesterStatus, InitShardSuccess,
        InitShardsResponse, MockIngesterService, RetainShardsResponse,
    };
    use quickwit_proto::ingest::{IngestV2Error, Shard, ShardState};
    use quickwit_proto::metastore::{
        self, MetastoreError, MockMetastoreService, OpenShardSubresponse,
    };
    use quickwit_proto::types::{DocMappingUid, Position, SourceId};

    use super::*;

    const TEST_SHARD_THROUGHPUT_LIMIT_MIB: f32 =
        DEFAULT_SHARD_THROUGHPUT_LIMIT.as_u64() as f32 / quickwit_common::shared_consts::MIB as f32;

    fn ingester_pool_entry(
        status: IngesterStatus,
        availability_zone: Option<&str>,
    ) -> IngesterPoolEntry {
        IngesterPoolEntry {
            client: IngesterServiceClient::mocked(),
            status,
            availability_zone: availability_zone.map(str::to_string),
        }
    }

    fn eligible_ingester(
        node_id: &str,
        zone: Option<&str>,
        num_open_shards: usize,
    ) -> EligibleIngester {
        EligibleIngester {
            node_id: NodeId::from_str(node_id),
            zone: zone.map(str::to_string),
            num_open_shards: AtomicUsize::new(num_open_shards),
        }
    }

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
                        ingester_id: "test-ingester-2".to_string(),
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
        let client = IngesterServiceClient::from_mock(mock_ingester);

        let ingester_pool = IngesterPool::default();
        ingester_pool.insert(
            NodeId::from_str("test-ingester-1"),
            IngesterPoolEntry::ready_with_client(client.clone()),
        );

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
                assert_eq!(shard.ingester_id, "test-ingester-2");

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
        ingester_pool.insert(
            NodeId::from_str("test-ingester-2"),
            IngesterPoolEntry::ready_with_client(ingester.clone()),
        );

        let mut controller = IngestController::new(
            metastore,
            ingester_pool.clone(),
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
                ingester_id: "test-ingester-0".to_string(),
                shard_state: ShardState::Open as i32,
                doc_mapping_uid: Some(doc_mapping_uid_0),
                ..Default::default()
            },
            Shard {
                index_uid: index_uid_0.clone().into(),
                source_id: source_id.to_string(),
                shard_id: Some(ShardId::from(2)),
                ingester_id: "test-ingester-1".to_string(),
                shard_state: ShardState::Open as i32,
                doc_mapping_uid: Some(doc_mapping_uid_0),
                ..Default::default()
            },
        ];

        model.insert_shards(&index_uid_0, &source_id.into(), shards);

        let request = GetOrCreateOpenShardsRequest {
            subrequests: Vec::new(),
            closed_shards: Vec::new(),
            unavailable_ingesters: Vec::new(),
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
        let unavailable_ingesters = vec!["test-ingester-0".to_string()];
        let request = GetOrCreateOpenShardsRequest {
            subrequests,
            closed_shards,
            unavailable_ingesters,
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
        assert_eq!(success.open_shards[0].ingester_id, "test-ingester-1");
        assert_eq!(success.open_shards[0].doc_mapping_uid(), doc_mapping_uid_0);

        let success = &response.successes[1];
        assert_eq!(success.subrequest_id, 1);
        assert_eq!(success.index_uid(), &index_uid_1);
        assert_eq!(success.source_id, source_id);
        assert_eq!(success.open_shards.len(), 1);
        assert_eq!(success.open_shards[0].shard_id(), ShardId::from(1));
        assert_eq!(success.open_shards[0].ingester_id, "test-ingester-2");
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
                assert_eq!(shard.ingester_id, "test-ingester-1");

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
        let client = IngesterServiceClient::from_mock(mock_ingester);

        let ingester_pool = IngesterPool::default();
        ingester_pool.insert(
            NodeId::from_str("test-ingester-1"),
            IngesterPoolEntry::ready_with_client(client.clone()),
        );

        let mut controller = IngestController::new(
            metastore,
            ingester_pool,
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
            unavailable_ingesters: Vec::new(),
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

        let mut controller = IngestController::new(
            metastore,
            ingester_pool,
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
            ingester_id: "test-ingester-0".to_string(),
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
            unavailable_ingesters: Vec::new(),
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
    fn test_get_open_shard_from_model_excludes_ingesters_that_are_not_available_and_ready() {
        let index_id = "test-index-0";
        let index_uid = IndexUid::for_test(index_id, 0);
        let source_id: SourceId = "test-source".to_string();

        let mut model = ControlPlaneModel::default();
        let mut index_metadata = IndexMetadata::for_test(index_id, "ram://indexes/test-index-0");
        let mut source_config = SourceConfig::ingest_v2();
        source_config.source_id = source_id.clone();
        index_metadata.add_source(source_config).unwrap();
        model.add_index(index_metadata);

        let shards = vec![Shard {
            shard_id: Some(ShardId::from(1)),
            index_uid: Some(index_uid.clone()),
            source_id: source_id.clone(),
            ingester_id: "test-ingester-0".to_string(),
            shard_state: ShardState::Open as i32,
            ..Default::default()
        }];
        model.insert_shards(&index_uid, &source_id, shards);

        let subrequest = GetOrCreateOpenShardsSubrequest {
            subrequest_id: 0,
            index_id: index_id.to_string(),
            source_id: source_id.clone(),
        };
        let unavailable_ingesters = FnvHashSet::default();

        // The ingester holding the only open shard is missing from the pool: the shard is excluded
        // and the control plane will have to create a new one.
        let ingester_pool = IngesterPool::default();
        let open_shard_opt =
            get_open_shard_from_model(&subrequest, &model, &ingester_pool, &unavailable_ingesters)
                .unwrap();
        assert!(open_shard_opt.is_none());

        // The ingester is in the pool but not ready (retiring): the shard is still excluded.
        ingester_pool.insert(
            NodeId::from_str("test-ingester-0"),
            IngesterPoolEntry {
                client: IngesterServiceClient::mocked(),
                status: IngesterStatus::Retiring,
                availability_zone: None,
            },
        );
        let open_shard_opt =
            get_open_shard_from_model(&subrequest, &model, &ingester_pool, &unavailable_ingesters)
                .unwrap();
        assert!(open_shard_opt.is_none());

        // The ingester is in the pool and ready: the shard is returned.
        ingester_pool.insert(
            NodeId::from_str("test-ingester-0"),
            IngesterPoolEntry::ready_with_client(IngesterServiceClient::mocked()),
        );
        let success =
            get_open_shard_from_model(&subrequest, &model, &ingester_pool, &unavailable_ingesters)
                .unwrap()
                .unwrap();
        assert_eq!(success.open_shards.len(), 1);
        assert_eq!(success.open_shards[0].shard_id(), ShardId::from(1));
        assert_eq!(success.open_shards[0].ingester_id, "test-ingester-0");
    }

    #[test]
    fn test_eligible_ingesters_preserve_zones_when_all_ingesters_are_zoned() {
        let ingester_pool = IngesterPool::default();
        let ingester_id_0 = NodeId::from_str("test-ingester-0");
        let ingester_id_1 = NodeId::from_str("test-ingester-1");
        let ingester_id_2 = NodeId::from_str("test-ingester-2");
        ingester_pool.insert(
            ingester_id_0.clone(),
            ingester_pool_entry(IngesterStatus::Ready, Some("az-a")),
        );
        ingester_pool.insert(
            ingester_id_1.clone(),
            ingester_pool_entry(IngesterStatus::Ready, Some("az-b")),
        );
        ingester_pool.insert(
            ingester_id_2.clone(),
            ingester_pool_entry(IngesterStatus::Ready, Some("az-c")),
        );

        let zonal_placement_enabled = all_ingesters_advertise_availability_zone(&ingester_pool);
        let eligible_ingesters = eligible_ingesters(
            &ingester_pool,
            &FnvHashSet::default(),
            &ControlPlaneModel::default(),
            zonal_placement_enabled,
        );
        let zones_by_ingester: HashMap<NodeId, Option<Zone>> = eligible_ingesters
            .into_iter()
            .map(|ingester| (ingester.node_id, ingester.zone))
            .collect();

        assert_eq!(zones_by_ingester[&ingester_id_0].as_deref(), Some("az-a"));
        assert_eq!(zones_by_ingester[&ingester_id_1].as_deref(), Some("az-b"));
        assert_eq!(zones_by_ingester[&ingester_id_2].as_deref(), Some("az-c"));
    }

    #[test]
    fn test_eligible_ingesters_mask_zones_when_one_ready_ingester_is_unzoned() {
        let ingester_pool = IngesterPool::default();
        ingester_pool.insert(
            NodeId::from_str("test-ingester-0"),
            ingester_pool_entry(IngesterStatus::Ready, Some("az-a")),
        );
        ingester_pool.insert(
            NodeId::from_str("test-ingester-1"),
            ingester_pool_entry(IngesterStatus::Ready, Some("az-b")),
        );
        ingester_pool.insert(
            NodeId::from_str("test-ingester-2"),
            ingester_pool_entry(IngesterStatus::Ready, Some("az-c")),
        );
        ingester_pool.insert(
            NodeId::from_str("test-ingester-unzoned"),
            ingester_pool_entry(IngesterStatus::Ready, None),
        );

        let zonal_placement_enabled = all_ingesters_advertise_availability_zone(&ingester_pool);
        let eligible_ingesters = eligible_ingesters(
            &ingester_pool,
            &FnvHashSet::default(),
            &ControlPlaneModel::default(),
            zonal_placement_enabled,
        );

        assert_eq!(eligible_ingesters.len(), 4);
        assert!(
            eligible_ingesters
                .iter()
                .all(|ingester| ingester.zone.is_none())
        );
    }

    #[test]
    fn test_eligible_ingesters_mask_zones_for_unzoned_non_ready_ingester() {
        let ingester_pool = IngesterPool::default();
        let ready_ingester_id = NodeId::from_str("test-ingester-0");
        ingester_pool.insert(
            ready_ingester_id.clone(),
            ingester_pool_entry(IngesterStatus::Ready, Some("az-a")),
        );
        ingester_pool.insert(
            NodeId::from_str("test-ingester-1"),
            ingester_pool_entry(IngesterStatus::Ready, Some("az-b")),
        );
        ingester_pool.insert(
            NodeId::from_str("test-ingester-2"),
            ingester_pool_entry(IngesterStatus::Ready, Some("az-c")),
        );
        ingester_pool.insert(
            NodeId::from_str("test-ingester-unzoned"),
            ingester_pool_entry(IngesterStatus::Retiring, None),
        );

        let zonal_placement_enabled = all_ingesters_advertise_availability_zone(&ingester_pool);
        let eligible_ingesters = eligible_ingesters(
            &ingester_pool,
            &FnvHashSet::default(),
            &ControlPlaneModel::default(),
            zonal_placement_enabled,
        );

        assert_eq!(eligible_ingesters.len(), 3);
        assert!(
            eligible_ingesters
                .iter()
                .any(|ingester| ingester.node_id == ready_ingester_id)
        );
        assert!(
            eligible_ingesters
                .iter()
                .all(|ingester| ingester.zone.is_none())
        );
    }

    #[test]
    fn test_eligible_ingesters_and_allocate_shards() {
        let ingester_pool = IngesterPool::default();
        ingester_pool.insert(
            NodeId::from_str("test-ingester-1"),
            ingester_pool_entry(IngesterStatus::Ready, Some("az-a")),
        );
        ingester_pool.insert(
            NodeId::from_str("test-ingester-2"),
            ingester_pool_entry(IngesterStatus::Ready, Some("az-b")),
        );
        ingester_pool.insert(
            NodeId::from_str("test-ingester-3"),
            ingester_pool_entry(IngesterStatus::Ready, Some("az-c")),
        );

        let index_uid = IndexUid::for_test("test-index", 0);
        let source_id: SourceId = "test-source".to_string();
        let shards = vec![
            Shard {
                index_uid: Some(index_uid.clone()),
                source_id: source_id.clone(),
                shard_id: Some(ShardId::from(1)),
                shard_state: ShardState::Open as i32,
                ingester_id: "test-ingester-1".to_string(),
                ..Default::default()
            },
            Shard {
                index_uid: Some(index_uid.clone()),
                source_id: source_id.clone(),
                shard_id: Some(ShardId::from(2)),
                shard_state: ShardState::Open as i32,
                ingester_id: "test-ingester-1".to_string(),
                ..Default::default()
            },
            Shard {
                index_uid: Some(index_uid.clone()),
                source_id: source_id.clone(),
                shard_id: Some(ShardId::from(3)),
                shard_state: ShardState::Closed as i32,
                ingester_id: "test-ingester-3".to_string(),
                ..Default::default()
            },
        ];
        let mut model = ControlPlaneModel::default();
        model.insert_shards(&index_uid, &source_id, shards);

        let unavailable_ingesters = FnvHashSet::from_iter([NodeId::from_str("test-ingester-2")]);
        let eligible_ingesters =
            eligible_ingesters(&ingester_pool, &unavailable_ingesters, &model, true);
        assert_eq!(eligible_ingesters.len(), 2);
        let initial_loads: HashMap<&str, usize> = eligible_ingesters
            .iter()
            .map(|ingester| {
                (
                    ingester.node_id.as_str(),
                    ingester.num_open_shards.load(Ordering::Relaxed),
                )
            })
            .collect();
        assert_eq!(initial_loads.get("test-ingester-1"), Some(&2));
        assert_eq!(initial_loads.get("test-ingester-3"), Some(&0));

        assert!(allocate_shards(&[], None, 0).is_none());
        assert_eq!(
            allocate_shards(&eligible_ingesters, None, 0),
            Some(Vec::new())
        );
        let ingester_ids = allocate_shards(&eligible_ingesters, None, 4).unwrap();

        // Ingester 2 is unavailable. Ingester 1 already has 2 open shards, ingester 3 has none, so
        // shards are allocated to balance the load between ingester 1 and ingester 3: ingester 3
        // ends up with 3 more shards and ingester 1 with 1 more.
        assert_eq!(ingester_ids.len(), 4);
        let mut num_shards_by_ingester_id: HashMap<&str, usize> = HashMap::new();
        for ingester_id in &ingester_ids {
            *num_shards_by_ingester_id
                .entry(ingester_id.as_str())
                .or_default() += 1;
        }
        assert_eq!(num_shards_by_ingester_id.get("test-ingester-1"), Some(&1));
        assert_eq!(num_shards_by_ingester_id.get("test-ingester-3"), Some(&3));
    }

    #[tokio::test]
    async fn test_ingest_controller_init_shards() {
        let metastore = MetastoreServiceClient::mocked();
        let ingester_pool = IngesterPool::default();

        let controller = IngestController::new(
            metastore,
            ingester_pool.clone(),
            TEST_SHARD_THROUGHPUT_LIMIT_MIB,
            1.001,
        );

        let ingester_id_0 = NodeId::from_str("test-ingester-0");
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
                assert_eq!(shard_0.ingester_id, "test-ingester-0");

                let subrequest_1 = &request.subrequests[1];
                assert_eq!(subrequest_1.subrequest_id, 1);

                let shard_1 = request.subrequests[1].shard();
                assert_eq!(shard_1.index_uid(), &("test-index", 0));
                assert_eq!(shard_1.source_id, "test-source");
                assert_eq!(shard_1.shard_id(), ShardId::from(1));
                assert_eq!(shard_1.ingester_id, "test-ingester-0");

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
        ingester_pool.insert(
            ingester_id_0,
            IngesterPoolEntry::ready_with_client(ingester_0),
        );

        let ingester_id_1 = NodeId::from_str("test-ingester-1");
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
                assert_eq!(shard.ingester_id, "test-ingester-1");

                Err(IngestV2Error::Internal("internal error".to_string()))
            });
        let ingester_1 =
            IngesterPoolEntry::ready_with_client(IngesterServiceClient::from_mock(mock_ingester_1));
        ingester_pool.insert(ingester_id_1, ingester_1);

        let ingester_id_2 = NodeId::from_str("test-ingester-2");
        let mut mock_ingester_2 = MockIngesterService::new();
        mock_ingester_2.expect_init_shards().never();

        let client_2 = IngesterServiceClient::tower()
            .stack_init_shards_layer(DelayLayer::new(INIT_SHARDS_REQUEST_TIMEOUT * 2))
            .build_from_mock(mock_ingester_2);
        ingester_pool.insert(
            ingester_id_2,
            IngesterPoolEntry::ready_with_client(client_2),
        );

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
                    ingester_id: "test-ingester-0".to_string(),
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
                    ingester_id: "test-ingester-0".to_string(),
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
                    ingester_id: "test-ingester-1".to_string(),
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
                    ingester_id: "test-ingester-2".to_string(),
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
                    ingester_id: "test-ingester-3".to_string(),
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
                assert_eq!(subrequest.ingester_id, "test-ingester-1");
                assert_eq!(subrequest.doc_mapping_uid(), expected_doc_mapping);

                let subresponses = vec![metastore::OpenShardSubresponse {
                    subrequest_id: 0,
                    open_shard: Some(Shard {
                        index_uid: Some(IndexUid::for_test("test-index", 0)),
                        source_id: "test-source".to_string(),
                        shard_id: Some(ShardId::from(0)),
                        ingester_id: "test-ingester-1".to_string(),
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

        let mut controller = IngestController::new(
            metastore,
            ingester_pool.clone(),
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
                assert_eq!(shard.ingester_id, "test-ingester-1");
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
            NodeId::from_str("test-ingester-1"),
            IngesterPoolEntry::ready_with_client(IngesterServiceClient::from_mock(mock_ingester)),
        );
        let num_shards_to_open_by_source: SourceShardCount =
            HashMap::from_iter([(source_uid.clone(), 1)]);
        let unavailable_ingesters = FnvHashSet::default();
        let progress = Progress::default();

        let opened_by_zone = controller
            .try_open_shards(
                ShardPlacement::Balanced(num_shards_to_open_by_source),
                &mut model,
                &unavailable_ingesters,
                &progress,
            )
            .await
            .unwrap();

        assert_eq!(opened_by_zone.len(), 1);
        assert_eq!(opened_by_zone[&None][&source_uid], 1);
    }

    #[tokio::test]
    async fn test_try_open_shards_spreads_across_three_zones() {
        let index_uid = IndexUid::for_test("test-index", 0);
        let source_id = INGEST_V2_SOURCE_ID.to_string();
        let source_uid = SourceUid {
            index_uid: index_uid.clone(),
            source_id: source_id.clone(),
        };
        let mut model = ControlPlaneModel::default();
        model.add_index(IndexMetadata::for_test("test-index", "ram://test-index"));
        model
            .add_source(&index_uid, SourceConfig::ingest_v2())
            .unwrap();

        let mut mock_ingester = MockIngesterService::new();
        mock_ingester
            .expect_init_shards()
            .times(3)
            .returning(|request| {
                assert_eq!(request.subrequests.len(), 1);
                let subrequest = &request.subrequests[0];
                Ok(InitShardsResponse {
                    successes: vec![InitShardSuccess {
                        subrequest_id: subrequest.subrequest_id,
                        shard: subrequest.shard.clone(),
                    }],
                    failures: Vec::new(),
                })
            });
        let ingester_client = IngesterServiceClient::from_mock(mock_ingester);
        let ingester_pool = IngesterPool::default();
        for (ingester_id, zone) in [
            ("ingester-a", "az-a"),
            ("ingester-b", "az-b"),
            ("ingester-c", "az-c"),
        ] {
            ingester_pool.insert(
                NodeId::from_str(ingester_id),
                IngesterPoolEntry {
                    client: ingester_client.clone(),
                    status: IngesterStatus::Ready,
                    availability_zone: Some(zone.to_string()),
                },
            );
        }

        let mut mock_metastore = MockMetastoreService::new();
        mock_metastore
            .expect_open_shards()
            .times(3)
            .returning(|request| {
                assert_eq!(request.subrequests.len(), 1);
                let subrequest = &request.subrequests[0];
                Ok(OpenShardsResponse {
                    subresponses: vec![OpenShardSubresponse {
                        subrequest_id: subrequest.subrequest_id,
                        open_shard: Some(Shard {
                            index_uid: subrequest.index_uid.clone(),
                            source_id: subrequest.source_id.clone(),
                            shard_id: subrequest.shard_id.clone(),
                            ingester_id: subrequest.ingester_id.clone(),
                            shard_state: ShardState::Open as i32,
                            doc_mapping_uid: subrequest.doc_mapping_uid,
                            ..Default::default()
                        }),
                    }],
                })
            });
        let mut controller = IngestController::new(
            MetastoreServiceClient::from_mock(mock_metastore),
            ingester_pool,
            TEST_SHARD_THROUGHPUT_LIMIT_MIB,
            1.001,
        );

        let opened_by_zone = controller
            .try_open_shards(
                ShardPlacement::Balanced(HashMap::from([(source_uid.clone(), 3)])),
                &mut model,
                &FnvHashSet::default(),
                &Progress::default(),
            )
            .await
            .unwrap();

        assert_eq!(opened_by_zone.len(), 3);
        for zone in ["az-a", "az-b", "az-c"] {
            assert_eq!(opened_by_zone[&Some(zone.to_string())][&source_uid], 1);
        }
        let ingester_ids: HashSet<&str> = model
            .all_shards()
            .map(|shard| shard.ingester_id.as_str())
            .collect();
        assert_eq!(
            ingester_ids,
            HashSet::from(["ingester-a", "ingester-b", "ingester-c"])
        );
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
                assert_eq!(subrequest.ingester_id, "test-ingester");

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
                assert_eq!(subrequest.ingester_id, "test-ingester");

                let shard = Shard {
                    index_uid: subrequest.index_uid.clone(),
                    source_id: subrequest.source_id.clone(),
                    shard_id: subrequest.shard_id.clone(),
                    shard_state: ShardState::Open as i32,
                    ingester_id: subrequest.ingester_id.clone(),
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

        let mut controller = IngestController::new(
            metastore,
            ingester_pool.clone(),
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
            ingester_id: "test-ingester".to_string(),
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
            ingester_id: NodeId::from_str("test-ingester"),
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
            ingester_id: "test-ingester".to_string(),
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
        let ingester =
            IngesterPoolEntry::ready_with_client(IngesterServiceClient::from_mock(mock_ingester));
        ingester_pool.insert(NodeId::from_str("test-ingester"), ingester);

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
            ingester_id: NodeId::from_str("test-ingester"),
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
            ingester_id: NodeId::from_str("test-ingester"),
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
                    ingester_id: subrequest.ingester_id.clone(),
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

        let mut controller = IngestController::new(
            metastore,
            ingester_pool.clone(),
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
            ingester_id: "test-ingester".to_string(),
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

        let ingester =
            IngesterPoolEntry::ready_with_client(IngesterServiceClient::from_mock(mock_ingester));
        ingester_pool.insert(NodeId::from_str("test-ingester"), ingester);

        let shard_infos = BTreeSet::from_iter([ShardInfo {
            shard_id: ShardId::from(1),
            shard_state: ShardState::Open,
            short_term_ingestion_rate: RateMibPerSec(4),
            long_term_ingestion_rate: RateMibPerSec(4),
        }]);
        let local_shards_update = LocalShardsUpdate {
            ingester_id: NodeId::from_str("test-ingester"),
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
                assert_eq!(request.subrequests[0].ingester_id, "test-ingester");

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
                assert_eq!(request.subrequests[0].ingester_id, "test-ingester");

                let subresponses = vec![metastore::OpenShardSubresponse {
                    subrequest_id: 0,
                    open_shard: Some(Shard {
                        index_uid: Some(index_uid.clone()),
                        source_id: INGEST_V2_SOURCE_ID.to_string(),
                        shard_id: Some(ShardId::from(1)),
                        ingester_id: "test-ingester".to_string(),
                        shard_state: ShardState::Open as i32,
                        ..Default::default()
                    }),
                }];
                let response = metastore::OpenShardsResponse { subresponses };
                Ok(response)
            });
        let metastore = MetastoreServiceClient::from_mock(mock_metastore);

        let ingester_pool = IngesterPool::default();

        let mut controller = IngestController::new(
            metastore,
            ingester_pool.clone(),
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

        // Test could not find ingester because no ingester in pool
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
                assert_eq!(shard.ingester_id, "test-ingester");

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
                assert_eq!(shard.ingester_id, "test-ingester");

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
        let ingester =
            IngesterPoolEntry::ready_with_client(IngesterServiceClient::from_mock(mock_ingester));
        ingester_pool.insert(NodeId::from_str("test-ingester"), ingester);

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

        let controller = IngestController::new(
            metastore,
            ingester_pool.clone(),
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
            ingester_id: "test-ingester".to_string(),
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
        let ingester =
            IngesterPoolEntry::ready_with_client(IngesterServiceClient::from_mock(mock_ingester));
        ingester_pool.insert(NodeId::from_str("test-ingester"), ingester);

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
            ingester_id: "test-ingester".to_string(),
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
                ingester_id: "test-ingester-0".to_string(),
                ..Default::default()
            },
            Shard {
                index_uid: index_uid.clone().into(),
                source_id: source_id.clone(),
                shard_id: Some(ShardId::from(2)),
                shard_state: ShardState::Open as i32,
                ingester_id: "test-ingester-0".to_string(),
                ..Default::default()
            },
            Shard {
                index_uid: index_uid.clone().into(),
                source_id: source_id.clone(),
                shard_id: Some(ShardId::from(3)),
                shard_state: ShardState::Closed as i32, //< this one is closed
                ingester_id: "test-ingester-0".to_string(),
                ..Default::default()
            },
            Shard {
                index_uid: index_uid.clone().into(),
                source_id: source_id.clone(),
                shard_id: Some(ShardId::from(4)),
                shard_state: ShardState::Open as i32,
                ingester_id: "test-ingester-1".to_string(),
                ..Default::default()
            },
            Shard {
                index_uid: index_uid.clone().into(),
                source_id: source_id.clone(),
                shard_id: Some(ShardId::from(5)),
                shard_state: ShardState::Open as i32,
                ingester_id: "test-ingester-1".to_string(),
                ..Default::default()
            },
            Shard {
                index_uid: index_uid.clone().into(),
                source_id: source_id.clone(),
                shard_id: Some(ShardId::from(6)),
                shard_state: ShardState::Open as i32,
                ingester_id: "test-ingester-1".to_string(),
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

        let (ingester_id, _shard_id) = find_scale_down_candidate(&source_uid, &model).unwrap();
        // We pick ingester 1 has it has more open shard
        assert_eq!(ingester_id, "test-ingester-1");
    }

    #[tokio::test]
    async fn test_sync_with_ingesters() {
        let metastore = MetastoreServiceClient::mocked();
        let ingester_pool = IngesterPool::default();

        let controller = IngestController::new(
            metastore,
            ingester_pool.clone(),
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
                ingester_id: "node-1".to_string(),
                ..Default::default()
            },
            Shard {
                index_uid: Some(index_uid.clone()),
                source_id: source_id.clone(),
                shard_id: Some(ShardId::from(2)),
                shard_state: ShardState::Open as i32,
                ingester_id: "node-2".to_string(),
                ..Default::default()
            },
            Shard {
                index_uid: Some(index_uid.clone()),
                source_id: source_id.clone(),
                shard_id: Some(ShardId::from(3)),
                shard_state: ShardState::Open as i32,
                ingester_id: "node-2".to_string(),
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
                    [ShardId::from(1)]
                );
                count_calls_clone.fetch_add(1, Ordering::Release);
                Ok(RetainShardsResponse {})
            });
        ingester_pool.insert(
            NodeId::from_str("node-1"),
            IngesterPoolEntry::ready_with_client(IngesterServiceClient::from_mock(mock_ingester_1)),
        );
        ingester_pool.insert(
            NodeId::from_str("node-2"),
            IngesterPoolEntry::ready_with_client(IngesterServiceClient::from_mock(mock_ingester_2)),
        );
        ingester_pool.insert(
            NodeId::from_str("node-3"),
            IngesterPoolEntry::ready_with_client(IngesterServiceClient::from_mock(mock_ingester_3)),
        );
        let ingester_id = NodeId::from_str("node-1");
        let wait_handle = controller.sync_with_ingester(&ingester_id, &model);
        wait_handle.wait().await;
        assert_eq!(count_calls.load(Ordering::Acquire), 1);
    }

    #[tokio::test]
    async fn test_ingest_controller_advise_reset_shards() {
        let metastore = MetastoreServiceClient::mocked();
        let ingester_pool = IngesterPool::default();

        let controller = IngestController::new(
            metastore,
            ingester_pool,
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
        let controller = IngestController::new(
            metastore,
            ingester_pool.clone(),
            TEST_SHARD_THROUGHPUT_LIMIT_MIB,
            1.001,
        );

        let closed_shards = controller.close_shards(Vec::new()).await;
        assert_eq!(closed_shards.len(), 0);

        let ingester_id_0 = NodeId::from_str("test-ingester-0");
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
        ingester_pool.insert(
            ingester_id_0.clone(),
            IngesterPoolEntry::ready_with_client(ingester_0),
        );

        let ingester_id_1 = NodeId::from_str("test-ingester-1");
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
        ingester_pool.insert(
            ingester_id_1.clone(),
            IngesterPoolEntry::ready_with_client(ingester_1),
        );

        let ingester_id_2 = NodeId::from_str("test-ingester-2");
        let mut mock_ingester_2 = MockIngesterService::new();
        mock_ingester_2.expect_close_shards().never();

        let client_2 = IngesterServiceClient::tower()
            .stack_close_shards_layer(DelayLayer::new(CLOSE_SHARDS_REQUEST_TIMEOUT * 2))
            .build_from_mock(mock_ingester_2);
        ingester_pool.insert(
            ingester_id_2.clone(),
            IngesterPoolEntry::ready_with_client(client_2),
        );

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
                ingester_id: ingester_id_0.to_string(),
                ..Default::default()
            },
            Shard {
                index_uid: Some(IndexUid::for_test("test-index", 0)),
                source_id: "test-source".to_string(),
                shard_id: Some(ShardId::from(1)),
                ingester_id: ingester_id_0.to_string(),
                ..Default::default()
            },
            Shard {
                index_uid: Some(IndexUid::for_test("test-index", 0)),
                source_id: "test-source".to_string(),
                shard_id: Some(ShardId::from(2)),
                ingester_id: ingester_id_1.to_string(),
                ..Default::default()
            },
            Shard {
                index_uid: Some(IndexUid::for_test("test-index", 0)),
                source_id: "test-source".to_string(),
                shard_id: Some(ShardId::from(3)),
                ingester_id: ingester_id_2.to_string(),
                ..Default::default()
            },
            Shard {
                index_uid: Some(IndexUid::for_test("test-index", 0)),
                source_id: "test-source".to_string(),
                shard_id: Some(ShardId::from(4)),
                ingester_id: "test-ingester-3".to_string(),
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
            assert_eq!(subrequest_0.ingester_id, "test-ingester-1");

            let subresponses = vec![metastore::OpenShardSubresponse {
                subrequest_id: 0,
                open_shard: Some(Shard {
                    index_uid: Some(IndexUid::for_test("test-index", 0)),
                    source_id: INGEST_V2_SOURCE_ID.to_string(),
                    shard_id: subrequest_0.shard_id.clone(),
                    ingester_id: "test-ingester-1".to_string(),
                    shard_state: ShardState::Open as i32,
                    ..Default::default()
                }),
            }];
            let response = metastore::OpenShardsResponse { subresponses };
            Ok(response)
        });
        let metastore = MetastoreServiceClient::from_mock(mock_metastore);
        let ingester_pool = IngesterPool::default();
        let mut controller = IngestController::new(
            metastore,
            ingester_pool.clone(),
            TEST_SHARD_THROUGHPUT_LIMIT_MIB,
            1.001,
        );

        let mut model = ControlPlaneModel::default();

        let universe = Universe::with_accelerated_time();
        let (control_plane_mailbox, control_plane_inbox) = universe.create_test_mailbox();
        let progress = Progress::default();

        let num_opened_shards = controller
            .rebalance_shards(&mut model, &control_plane_mailbox, &progress)
            .await
            .unwrap();
        assert_eq!(num_opened_shards, 0);

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
                ingester_id: "test-ingester-0".to_string(),
                shard_state: ShardState::Open as i32,
                ..Default::default()
            },
            Shard {
                index_uid: Some(index_uid.clone()),
                source_id: INGEST_V2_SOURCE_ID.to_string(),
                shard_id: Some(ShardId::from(1)),
                ingester_id: "test-ingester-0".to_string(),
                shard_state: ShardState::Open as i32,
                ..Default::default()
            },
            Shard {
                index_uid: Some(index_uid.clone()),
                source_id: INGEST_V2_SOURCE_ID.to_string(),
                shard_id: Some(ShardId::from(2)),
                ingester_id: "test-ingester-0".to_string(),
                shard_state: ShardState::Open as i32,
                ..Default::default()
            },
            Shard {
                index_uid: Some(index_uid.clone()),
                source_id: INGEST_V2_SOURCE_ID.to_string(),
                shard_id: Some(ShardId::from(3)),
                ingester_id: "test-ingester-0".to_string(),
                shard_state: ShardState::Open as i32,
                ..Default::default()
            },
            Shard {
                index_uid: Some(index_uid.clone()),
                source_id: INGEST_V2_SOURCE_ID.to_string(),
                shard_id: Some(ShardId::from(4)),
                ingester_id: "test-ingester-0".to_string(),
                shard_state: ShardState::Open as i32,
                ..Default::default()
            },
        ];
        model.insert_shards(&index_uid, &INGEST_V2_SOURCE_ID.to_string(), open_shards);

        let ingester_id_0 = NodeId::from_str("test-ingester-0");
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
        ingester_pool.insert(
            ingester_id_0.clone(),
            IngesterPoolEntry::ready_with_client(ingester_0),
        );

        let ingester_id_1 = NodeId::from_str("test-ingester-1");
        let mut mock_ingester_1 = MockIngesterService::new();
        mock_ingester_1.expect_init_shards().return_once(|request| {
            assert_eq!(request.subrequests.len(), 2);

            let subrequest_0 = &request.subrequests[0];
            assert_eq!(subrequest_0.subrequest_id, 0);

            let shard_0 = request.subrequests[0].shard();
            assert_eq!(shard_0.index_uid(), &("test-index", 0));
            assert_eq!(shard_0.source_id, INGEST_V2_SOURCE_ID.to_string());
            assert_eq!(shard_0.ingester_id, "test-ingester-1");

            let subrequest_1 = &request.subrequests[1];
            assert_eq!(subrequest_1.subrequest_id, 1);

            let shard_1 = request.subrequests[1].shard();
            assert_eq!(shard_1.index_uid(), &("test-index", 0));
            assert_eq!(shard_1.source_id, INGEST_V2_SOURCE_ID.to_string());
            assert_eq!(shard_1.ingester_id, "test-ingester-1");

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
        let ingester_1 =
            IngesterPoolEntry::ready_with_client(IngesterServiceClient::from_mock(mock_ingester_1));
        ingester_pool.insert(ingester_id_1.clone(), ingester_1);

        let num_opened_shards = controller
            .rebalance_shards(&mut model, &control_plane_mailbox, &progress)
            .await
            .unwrap();
        assert_eq!(num_opened_shards, 1);

        let callback: RebalanceShardsCallback = tokio::time::timeout(
            CLOSE_SHARDS_REQUEST_TIMEOUT * 2,
            control_plane_inbox.recv_typed_message(),
        )
        .await
        .unwrap()
        .unwrap();
        assert_eq!(callback.closed_shards.len(), 1);

        assert_eq!(controller.rebalance_semaphore.available_permits(), 0);
        let num_opened_shards = controller
            .rebalance_shards(&mut model, &control_plane_mailbox, &progress)
            .await
            .unwrap();
        assert_eq!(num_opened_shards, 0);
        assert_eq!(controller.rebalance_semaphore.available_permits(), 0);

        drop(callback);
        assert_eq!(controller.rebalance_semaphore.available_permits(), 1);

        let mut empty_model = ControlPlaneModel::default();
        let num_opened_shards = controller
            .rebalance_shards(&mut empty_model, &control_plane_mailbox, &progress)
            .await
            .unwrap();
        assert_eq!(num_opened_shards, 0);
        assert_eq!(controller.rebalance_semaphore.available_permits(), 1);
    }

    #[test]
    fn test_match_shards_to_close_is_zonal_and_tolerates_pool_churn() {
        let source_uid = SourceUid {
            index_uid: IndexUid::for_test("test-index", 0),
            source_id: "test-source".to_string(),
        };
        let ingester_pool = IngesterPool::default();
        for (ingester_id, zone) in [
            ("origin-a", "az-a"),
            ("origin-b", "az-b"),
            ("origin-c", "az-c"),
        ] {
            ingester_pool.insert(
                NodeId::from_str(ingester_id),
                ingester_pool_entry(IngesterStatus::Retiring, Some(zone)),
            );
        }
        let make_shard = |shard_id, ingester_id: &str| Shard {
            index_uid: Some(source_uid.index_uid.clone()),
            source_id: source_uid.source_id.clone(),
            shard_id: Some(ShardId::from(shard_id)),
            ingester_id: ingester_id.to_string(),
            shard_state: ShardState::Open as i32,
            ..Default::default()
        };
        // This shard's ingester disappeared after the replacement request was constructed.
        let mut shards_to_rebalance = vec![
            make_shard(1, "departed-origin"),
            make_shard(2, "origin-a"),
            make_shard(3, "origin-b"),
            make_shard(4, "origin-c"),
        ];
        let opened_by_zone = HashMap::from([
            (
                Some("az-a".to_string()),
                HashMap::from([(source_uid.clone(), 2)]),
            ),
            (Some("az-c".to_string()), HashMap::from([(source_uid, 1)])),
        ]);

        let mut shards_to_close =
            match_shards_to_close(&ingester_pool, &opened_by_zone, &mut shards_to_rebalance);
        shards_to_close.sort_by_key(|shard| shard.shard_id().clone());

        assert_eq!(
            shards_to_close
                .iter()
                .map(|shard| shard.shard_id())
                .collect_vec(),
            [ShardId::from(2), ShardId::from(4)]
        );
        assert_eq!(shards_to_rebalance.len(), 2);
    }
    #[track_caller]
    fn assert_allocate_shards_balances_load(initial_num_shards: &[usize], num_shards: usize) {
        let eligible_ingesters: Vec<EligibleIngester> = initial_num_shards
            .iter()
            .enumerate()
            .map(|(index, &num_open_shards)| {
                eligible_ingester(&format!("ingester-{index}"), None, num_open_shards)
            })
            .collect();
        let ingester_ids_opt = allocate_shards(&eligible_ingesters, None, num_shards);
        if initial_num_shards.is_empty() {
            assert!(ingester_ids_opt.is_none());
            return;
        }
        let ingester_ids = ingester_ids_opt.unwrap();
        assert_eq!(ingester_ids.len(), num_shards);
        let mut current_num_shards_by_ingester_id: HashMap<NodeId, usize> = initial_num_shards
            .iter()
            .enumerate()
            .map(|(index, &num_open_shards)| {
                (
                    NodeId::from_str(&format!("ingester-{index}")),
                    num_open_shards,
                )
            })
            .collect();
        for ingester in ingester_ids {
            let min_num_shards = current_num_shards_by_ingester_id
                .values()
                .copied()
                .min()
                .unwrap();
            let num_shards = current_num_shards_by_ingester_id
                .get_mut(&ingester)
                .unwrap();
            assert_eq!(*num_shards, min_num_shards);
            *num_shards += 1;
        }
        for ingester in &eligible_ingesters {
            assert_eq!(
                ingester.num_open_shards.load(Ordering::Relaxed),
                current_num_shards_by_ingester_id[&ingester.node_id]
            );
        }
    }

    fn assert_allocate_shards_for_initial_counts(initial_num_shards: &[usize]) {
        for num_shards in 0..10 {
            assert_allocate_shards_balances_load(initial_num_shards, num_shards);
        }
    }

    use proptest::prelude::*;

    proptest! {
        #[test]
        fn test_proptest_allocate_shards(initial_num_shards in proptest::collection::vec(0..10usize, 0..10usize)) {
            assert_allocate_shards_for_initial_counts(&initial_num_shards);
        }
    }

    #[test]
    fn test_allocate_shards_prop_test() {
        assert_allocate_shards_for_initial_counts(&[]);
        assert_allocate_shards_for_initial_counts(&[1]);
        assert_allocate_shards_for_initial_counts(&[1, 1]);
        assert_allocate_shards_for_initial_counts(&[1, 2]);
        assert_allocate_shards_for_initial_counts(&[1, 4]);
        assert_allocate_shards_for_initial_counts(&[2, 3, 2]);
        assert_allocate_shards_for_initial_counts(&[2, 4, 6]);
        assert_allocate_shards_for_initial_counts(&[2, 3, 10]);
        assert_allocate_shards_for_initial_counts(&[7, 7, 7]);
    }

    #[test]
    fn test_pick_least_loaded_uses_zone_only_to_break_global_ties() {
        let mut rng = rand::rng();
        assert!(pick_least_loaded(&[], None, &mut rng).is_none());

        let tied_ingesters = vec![
            eligible_ingester("ingester-a", Some("az-a"), 1),
            eligible_ingester("ingester-b", Some("az-b"), 1),
            eligible_ingester("ingester-c", Some("az-c"), 1),
        ];
        let az_b = "az-b".to_string();
        let picked = pick_least_loaded(&tied_ingesters, Some(&az_b), &mut rng).unwrap();
        assert_eq!(picked.node_id, "ingester-b");

        let uneven_ingesters = vec![
            eligible_ingester("ingester-a", Some("az-a"), 2),
            eligible_ingester("ingester-b", Some("az-b"), 1),
            eligible_ingester("ingester-c", Some("az-c"), 2),
        ];
        let az_a = "az-a".to_string();
        let picked = pick_least_loaded(&uneven_ingesters, Some(&az_a), &mut rng).unwrap();
        assert_eq!(picked.node_id, "ingester-b");
        let picked = pick_least_loaded(&uneven_ingesters, None, &mut rng).unwrap();
        assert_eq!(picked.node_id, "ingester-b");
    }

    #[test]
    fn test_balance_shards_across_three_distinct_zones() {
        let no_zones = HashSet::new();
        assert!(distribute_shards_across_zones(0, &no_zones).is_empty());
        assert_eq!(
            distribute_shards_across_zones(5, &no_zones),
            HashMap::from([(None, 5)])
        );

        let zones =
            HashSet::from_iter(["az-a".to_string(), "az-b".to_string(), "az-c".to_string()]);
        for (num_shards, expected_num_zones) in [(2, 2), (3, 3), (8, 3)] {
            let distribution = distribute_shards_across_zones(num_shards, &zones);
            assert_eq!(distribution.len(), expected_num_zones);
            assert_eq!(distribution.values().sum::<usize>(), num_shards);
            let min_count = distribution.values().min().unwrap();
            let max_count = distribution.values().max().unwrap();
            assert!(max_count - min_count <= 1);
        }

        let source_uid = SourceUid {
            index_uid: IndexUid::for_test("index", 0),
            source_id: "source".to_string(),
        };
        // Multiple ingesters in az-a must not give that zone more weight than az-b or az-c.
        let eligible_ingesters = vec![
            eligible_ingester("ingester-a-0", Some("az-a"), 0),
            eligible_ingester("ingester-a-1", Some("az-a"), 0),
            eligible_ingester("ingester-b", Some("az-b"), 0),
            eligible_ingester("ingester-c", Some("az-c"), 0),
        ];
        let balanced = balance_shards_to_open_across_zones(
            HashMap::from([(source_uid.clone(), 6)]),
            &eligible_ingesters,
        );
        assert_eq!(balanced.len(), 3);
        assert!(balanced.values().all(|counts| counts[&source_uid] == 2));
    }
    /// Test helper for compute_shards_to_rebalance.
    /// The reason for testing both available and unavailable ingesters with open shards is to
    /// ensure the algorithm holds up when there are open shards
    ///
    /// - `available_ingester_shards`: open shards per available ingester
    /// - `unavailable_ingester_shards`: open shards on unavailable ingesters
    fn test_compute_shards_to_rebalance_aux(
        ready_ingester_shards: &[usize],
        unavailable_ingester_shards: &[usize],
        retiring_ingester_shards: &[usize],
    ) {
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

        let ready_ids: Vec<String> = (0..ready_ingester_shards.len())
            .map(|i| format!("ready-ingester-{}", i))
            .collect();

        for ingester_id in &ready_ids {
            let ingester = IngesterPoolEntry {
                client: ingester_client.clone(),
                status: IngesterStatus::Ready,
                availability_zone: None,
            };
            ingester_pool.insert(NodeId::from_str(ingester_id), ingester);
        }

        let unavailable_ids: Vec<String> = (0..unavailable_ingester_shards.len())
            .map(|i| format!("unavailable-ingester-{}", i))
            .collect();

        let retiring_ids: Vec<String> = (0..retiring_ingester_shards.len())
            .map(|i| format!("retiring-ingester-{}", i))
            .collect();

        for ingester_id in &retiring_ids {
            let ingester = IngesterPoolEntry {
                client: ingester_client.clone(),
                status: IngesterStatus::Retiring,
                availability_zone: None,
            };
            ingester_pool.insert(NodeId::from_str(ingester_id), ingester);
        }

        let mut shards: Vec<Shard> = Vec::new();
        let mut shard_id: u64 = 0;

        for (idx, &num_shards) in ready_ingester_shards.iter().enumerate() {
            for _ in 0..num_shards {
                shards.push(Shard {
                    index_uid: Some(index_uid.clone()),
                    source_id: source_id.clone(),
                    shard_id: Some(ShardId::from(shard_id)),
                    ingester_id: ready_ids[idx].clone(),
                    shard_state: ShardState::Open as i32,
                    ..Default::default()
                });
                shard_id += 1;
            }
        }

        // Shards on unavailable ingesters - these shouldn't affect rebalancing calculations
        for (idx, &num_shards) in unavailable_ingester_shards.iter().enumerate() {
            for _ in 0..num_shards {
                shards.push(Shard {
                    index_uid: Some(index_uid.clone()),
                    source_id: source_id.clone(),
                    shard_id: Some(ShardId::from(shard_id)),
                    ingester_id: unavailable_ids[idx].clone(),
                    shard_state: ShardState::Open as i32,
                    ..Default::default()
                });
                shard_id += 1;
            }
        }

        let num_retiring_shards: usize = retiring_ingester_shards.iter().sum();

        // Shards on retiring ingesters - all of these should be rebalanced
        for (idx, &num_shards) in retiring_ingester_shards.iter().enumerate() {
            for _ in 0..num_shards {
                shards.push(Shard {
                    index_uid: Some(index_uid.clone()),
                    source_id: source_id.clone(),
                    shard_id: Some(ShardId::from(shard_id)),
                    ingester_id: retiring_ids[idx].clone(),
                    shard_state: ShardState::Open as i32,
                    ..Default::default()
                });
                shard_id += 1;
            }
        }

        model.insert_shards(&index_uid, &source_id, shards.clone());

        let controller = IngestController::new(
            MetastoreServiceClient::mocked(),
            ingester_pool.clone(),
            TEST_SHARD_THROUGHPUT_LIMIT_MIB,
            1.001,
        );
        let shards_to_rebalance = controller.compute_shards_to_rebalance(&model);

        // All shards on retiring ingesters must be rebalanced.
        let num_retiring_shards_to_rebalance = shards_to_rebalance
            .iter()
            .filter(|shard| shard.ingester_id.starts_with("retiring-"))
            .count();
        assert_eq!(num_retiring_shards_to_rebalance, num_retiring_shards);

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

        let mut num_shards_by_ready_ingester_id: HashMap<&str, usize> = ready_ids
            .iter()
            .map(|ready_id| (ready_id.as_str(), 0))
            .collect();

        for shard in model.all_shards() {
            if !shard.is_open() {
                continue;
            }
            if let Some(num_shards) =
                num_shards_by_ready_ingester_id.get_mut(shard.ingester_id.as_str())
            {
                *num_shards += 1;
            }
        }

        // Now we move the different shards to ready ingesters (not retiring ones).
        // We can only simulate this if there are ready ingesters to receive shards.
        if !ready_ids.is_empty() {
            let mut sorted_num_shards_by_ingester_id: BTreeSet<(usize, &str)> =
                num_shards_by_ready_ingester_id
                    .into_iter()
                    .map(|(ingester_id, num_shards)| (num_shards, ingester_id))
                    .collect();
            let mut opened_shards: Vec<Shard> = Vec::new();
            for _ in 0..shards_to_rebalance.len() {
                let (num_shards, ingester_id) =
                    sorted_num_shards_by_ingester_id.pop_first().unwrap();
                let opened_shard = Shard {
                    index_uid: Some(index_uid.clone()),
                    source_id: source_id.to_string(),
                    shard_id: Some(ShardId::from(shard_id)),
                    ingester_id: ingester_id.to_string(),
                    shard_state: ShardState::Open as i32,
                    ..Default::default()
                };
                sorted_num_shards_by_ingester_id.insert((num_shards + 1, ingester_id));
                opened_shards.push(opened_shard);
                shard_id += 1;
            }

            if let Some((min_shards, max_shards)) = sorted_num_shards_by_ingester_id
                .iter()
                .map(|(num_shards, _)| num_shards)
                .copied()
                .minmax()
                .into_option()
            {
                assert!(min_shards + min_shards.div_ceil(10).max(2) >= max_shards);
            }

            // Test stability of the algorithm: mark the retiring ingesters as
            // decommissioned, insert the new shards, and verify no further rebalance is
            // needed among the ready ingesters.
            for ingester_id in &retiring_ids {
                let ingester = IngesterPoolEntry {
                    client: ingester_client.clone(),
                    status: IngesterStatus::Decommissioned,
                    availability_zone: None,
                };
                ingester_pool.insert(NodeId::from_str(ingester_id), ingester);
            }
            model.insert_shards(&index_uid, &source_id, opened_shards);

            let shards_to_rebalance = controller.compute_shards_to_rebalance(&model);
            assert!(shards_to_rebalance.is_empty());
        }
    }

    proptest! {
        #[test]
        fn test_compute_shards_to_rebalance_proptest(
            ready_shards in proptest::collection::vec(0..13usize, 0..13usize),
            unavailable_shards in proptest::collection::vec(0..13usize, 0..5usize),
            retiring_shards in proptest::collection::vec(0..5usize, 0..5usize),
        ) {
            test_compute_shards_to_rebalance_aux(&ready_shards, &unavailable_shards, &retiring_shards);
        }
    }

    #[test]
    fn test_compute_shards_to_rebalance() {
        test_compute_shards_to_rebalance_aux(&[], &[], &[]);
        test_compute_shards_to_rebalance_aux(&[0], &[], &[]);
        test_compute_shards_to_rebalance_aux(&[1], &[], &[]);
        test_compute_shards_to_rebalance_aux(&[0, 1], &[], &[]);
        test_compute_shards_to_rebalance_aux(&[0, 1], &[1], &[]);
        test_compute_shards_to_rebalance_aux(&[0, 1, 2], &[3, 4], &[]);
        // Retiring ingesters: all their shards must be rebalanced
        test_compute_shards_to_rebalance_aux(&[1, 1], &[], &[3]);
        test_compute_shards_to_rebalance_aux(&[0, 0, 0], &[], &[5]);
        test_compute_shards_to_rebalance_aux(&[2], &[], &[1, 2]);
    }
}
