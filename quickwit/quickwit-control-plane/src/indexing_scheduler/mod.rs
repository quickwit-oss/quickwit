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

mod change_tracker;
mod scheduling;

use std::cmp::Ordering;
use std::fmt;
use std::num::NonZeroU32;
use std::sync::{Arc, LazyLock};
use std::time::{Duration, Instant};

use fnv::{FnvHashMap, FnvHashSet};
use itertools::Itertools;
use quickwit_common::is_parquet_pipeline_index;
use quickwit_common::pretty::PrettySample;
use quickwit_config::{
    FileSourceParams, SourceParams, disable_ingest_v1, indexing_pipeline_params_fingerprint,
};
use quickwit_proto::indexing::{
    ApplyIndexingPlanRequest, IndexingService, IndexingTask, PIPELINE_FULL_CAPACITY,
    PIPELINE_THROUGHPUT,
};
use quickwit_proto::ingest::ingester::IngesterStatus;
use quickwit_proto::types::NodeId;
use scheduling::{
    Eligibility, IndexerInfo, SourceToSchedule, SourceToScheduleType, is_shard_nearby,
};
use serde::Serialize;
use tracing::{debug, info, warn};
use ulid::Ulid;

use crate::indexing_plan::PhysicalIndexingPlan;
use crate::indexing_scheduler::change_tracker::{NotifyChangeOnDrop, RebuildNotifier};
use crate::indexing_scheduler::scheduling::build_physical_indexing_plan;
use crate::metrics::{APPLY_PLAN_TOTAL, SCHEDULE_TOTAL, ShardLocalityMetrics};
use crate::model::{ControlPlaneModel, ShardEntry, ShardLocations};
use crate::{IndexerPool, IndexerPoolEntry};

const DEFAULT_ENABLE_VARIABLE_SHARD_LOAD: bool = false;

const DEFAULT_ENABLE_LOCALITY_AWARE_SCHEDULING: bool = false;

const DEFAULT_MIN_SHARD_LOCALITY_PERCENT: u32 = 30;

/// Minimum period before being able to rebuild the plan from scratch.
const PLAN_FROM_SCRATCH_COOLDOWN_PERIOD: Duration = Duration::from_mins(30);

pub(crate) const MIN_DURATION_BETWEEN_SCHEDULING: Duration =
    if cfg!(any(test, feature = "testsuite")) {
        Duration::from_millis(50)
    } else {
        Duration::from_secs(30)
    };

pub(crate) const APPLY_INDEXING_PLAN_TIMEOUT: Duration = if cfg!(any(test, feature = "testsuite")) {
    Duration::from_millis(10)
} else {
    Duration::from_secs(2)
};

#[derive(Debug, Clone, Default, Serialize)]
pub struct IndexingSchedulerState {
    pub num_applied_physical_indexing_plan: usize,
    pub num_schedule_indexing_plan: usize,
    pub last_applied_physical_plan: Option<PhysicalIndexingPlan>,
    #[serde(skip)]
    pub last_applied_indexer_statuses: FnvHashMap<NodeId, IngesterStatus>,
    #[serde(skip)]
    pub last_applied_plan_timestamp: Option<Instant>,
    #[serde(skip)]
    pub next_plan_from_scratch_timestamp: Option<Instant>,
}

/// The [`IndexingScheduler`] is responsible for listing indexing tasks and assigning them to
/// indexers.
/// We call this duty `scheduling`. Contrary to what the name suggests, most indexing tasks are
/// ever running. We just borrowed the terminology to Kubernetes.
///
/// Scheduling executes the following steps:
/// 1. Builds a [`PhysicalIndexingPlan`] from the list of logical indexing tasks. See
///    `build_physical_indexing_plan` for the implementation details.
/// 2. Apply the [`PhysicalIndexingPlan`]: for each indexer, the scheduler send the indexing tasks
///    by gRPC. An indexer immediately returns an Ok and apply asynchronously the received plan. Any
///    errors (network) happening in this step are ignored. The scheduler runs a control loop that
///    regularly checks if indexers are effectively running their plans (more details in the next
///    section).
///
/// All events altering the list of indexes and sources are proxied through
/// through the control plane. The control plane model is therefore guaranteed to be up-to-date
/// (at the cost of making the control plane a single point of failure).
///
/// Each change to the model triggers the production of a new `PhysicalIndexingPlan`.
///
/// A `ControlPlanLoop` event is scheduled every `CONTROL_PLAN_LOOP_INTERVAL` and steers
/// the cluster toward the last applied [`PhysicalIndexingPlan`].
///
/// This physical plan is a desired state. Even after that state is reached, it can be altered due
/// to faulty server for instance.
///
/// We then need to detect deviation, possibly recompute the desired `PhysicalIndexingPlan`
/// and steer back the cluster to the right state.
///
/// First to detect deviation, the control plan gathers an eventually consistent view of what is
/// running on the different nodes of the cluster: the `running plan`. This is done via `chitchat`.
///
/// If the list of node ids has changed, the scheduler will retrigger a scheduling.
/// If the indexing tasks do not match, the scheduler will apply again the last applied plan.
/// Concretely, it will send the faulty nodes of the plan they are supposed to follow.
//
/// Finally, in order to give the time for each indexer to run their indexing tasks, the control
/// plane will wait at least `MIN_DURATION_BETWEEN_SCHEDULING` before comparing the desired
/// plan with the running plan.
pub struct IndexingScheduler {
    cluster_id: String,
    self_node_id: NodeId,
    indexer_pool: IndexerPool,
    state: IndexingSchedulerState,
    pub(crate) next_rebuild_tracker: RebuildNotifier,
}

impl fmt::Debug for IndexingScheduler {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.debug_struct("IndexingScheduler")
            .field("cluster_id", &self.cluster_id)
            .field("node_id", &self.self_node_id)
            .field(
                "last_applied_plan_ts",
                &self.state.last_applied_plan_timestamp,
            )
            .finish()
    }
}

fn enable_variable_shard_load() -> bool {
    static IS_SHARD_LOAD_CP_ENABLED: LazyLock<bool> = LazyLock::new(|| {
        if let Some(enable_flag) =
            quickwit_common::get_bool_from_env_opt("QW_ENABLE_VARIABLE_SHARD_LOAD")
        {
            return enable_flag;
        }
        // For backward compatibility, if QW_DISABLE_VARIABLE_SHARD_LOAD is set, we accept this
        // value too.
        if let Some(disable_flag) =
            quickwit_common::get_bool_from_env_opt("QW_DISABLE_VARIABLE_SHARD_LOAD")
        {
            warn!(
                disable = disable_flag,
                "QW_DISABLE_VARIABLE_SHARD_LOAD is deprecated. Please use \
                 QW_ENABLE_VARIABLE_SHARD_LOAD instead. We will use your setting in this version, \
                 but will likely ignore it in future versions."
            );
            return !disable_flag;
        }
        // Defaulting to false
        info!(
            "QW_ENABLE_VARIABLE_SHARD_LOAD not set, defaulting to {}",
            DEFAULT_ENABLE_VARIABLE_SHARD_LOAD
        );
        DEFAULT_ENABLE_VARIABLE_SHARD_LOAD
    });
    *IS_SHARD_LOAD_CP_ENABLED
}

/// Locality aware scheduling is additional constraints on top of the regular indexing scheduler.
/// With this enabled, availability zone will be used (if provided) to preferentially index shards
/// in the same-AZ. It also allows decommissioning indexers to index their own shards to speed up
/// the decommissioning process.
fn enable_locality_aware_scheduling() -> bool {
    static IS_LOCALITY_AWARE_SCHEDULING_ENABLED: LazyLock<bool> = LazyLock::new(|| {
        quickwit_common::get_bool_from_env(
            "QW_ENABLE_LOCALITY_AWARE_SCHEDULING",
            DEFAULT_ENABLE_LOCALITY_AWARE_SCHEDULING,
        )
    });
    *IS_LOCALITY_AWARE_SCHEDULING_ENABLED
}

/// Computes the CPU load associated to a single shard of a given index.
///
/// The array passed contains all of data we have about the shard of the index.
/// This function averages their statistics.
///
/// For the moment, this function only takes in account the measured throughput,
/// and assumes a constant CPU usage of 4 vCPU = 20mb/s.
///
/// It does not take in account the variation that could raise from the different
/// doc mapping / nature of the data, etc.
fn compute_load_per_shard(shard_entries: &[&ShardEntry]) -> NonZeroU32 {
    if enable_variable_shard_load() {
        let num_shards = shard_entries.len().max(1) as u64;
        let average_throughput_per_shard_bytes: u64 = shard_entries
            .iter()
            .map(|shard_entry| shard_entry.long_term_ingestion_rate.0 as u64 * bytesize::MIB)
            .sum::<u64>()
            .div_ceil(num_shards)
            // A shard throughput cannot exceed PIPELINE_THROUGHPUT in the long term (this is
            // enforced by the configuration).
            .min(PIPELINE_THROUGHPUT.as_u64());
        let num_cpu_millis = (PIPELINE_FULL_CAPACITY.cpu_millis() as u64
            * average_throughput_per_shard_bytes)
            / PIPELINE_THROUGHPUT.as_u64();
        const MIN_CPU_LOAD_PER_SHARD: u32 = 50u32;
        NonZeroU32::new((num_cpu_millis as u32).max(MIN_CPU_LOAD_PER_SHARD)).unwrap()
    } else {
        get_default_load_per_shard()
    }
}

fn get_default_load_per_shard() -> NonZeroU32 {
    let default_load_per_shard = quickwit_common::get_from_env_cached!(
        u32,
        "QW_DEFAULT_LOAD_PER_SHARD",
        PIPELINE_FULL_CAPACITY.cpu_millis() / 4,
        false
    );
    NonZeroU32::new(default_load_per_shard).unwrap()
}

fn get_sources_to_schedule(
    model: &ControlPlaneModel,
    disable_ingest_v1: bool,
) -> Vec<SourceToSchedule> {
    if disable_ingest_v1 {
        debug!("skipping scheduling of ingest API sources because ingest v1 is disabled");
    }
    let mut sources = Vec::new();

    for (source_uid, source_config) in model.source_configs() {
        if !source_config.enabled {
            continue;
        }
        let params_fingerprint = model
            .index_metadata(&source_uid.index_uid)
            .map(|index_meta| {
                indexing_pipeline_params_fingerprint(&index_meta.index_config, source_config)
            })
            .unwrap_or_default();
        match source_config.source_params {
            SourceParams::File(FileSourceParams::Filepath(_))
            | SourceParams::IngestCli
            | SourceParams::Stdin
            | SourceParams::Void(_)
            | SourceParams::Vec(_) => { // We don't need to schedule those.
            }

            SourceParams::IngestApi => {
                if disable_ingest_v1 {
                    continue;
                }
                // Metrics indexes should use IngestV2 only, not IngestV1.
                // The ParquetSourceLoader doesn't support IngestV1.
                if is_parquet_pipeline_index(&source_uid.index_uid.index_id) {
                    continue;
                }
                // TODO ingest v1 is scheduled differently
                sources.push(SourceToSchedule {
                    source_uid,
                    source_type: SourceToScheduleType::IngestV1,
                    params_fingerprint,
                });
            }
            SourceParams::Ingest => {
                // Expect: the source should exist since we just read it from `get_source_configs`.
                // Note that we keep all shards, including Closed shards:
                // A closed shards still needs to be indexed.
                let shard_entries: Vec<&ShardEntry> = model
                    .get_shards_for_source(&source_uid)
                    .expect("source should exist")
                    .values()
                    .collect();
                if shard_entries.is_empty() {
                    continue;
                }
                let shard_ids = shard_entries
                    .iter()
                    .map(|shard_entry| shard_entry.shard_id().clone())
                    .collect();
                let load_per_shard = compute_load_per_shard(&shard_entries[..]);
                sources.push(SourceToSchedule {
                    source_uid,
                    source_type: SourceToScheduleType::Sharded {
                        shard_ids,
                        load_per_shard,
                    },
                    params_fingerprint,
                });
            }
            SourceParams::Kafka(_)
            | SourceParams::Kinesis(_)
            | SourceParams::PubSub(_)
            | SourceParams::Pulsar(_)
            | SourceParams::File(FileSourceParams::Notifications(_)) => {
                sources.push(SourceToSchedule {
                    source_uid,
                    source_type: SourceToScheduleType::NonSharded {
                        num_pipelines: source_config.num_pipelines.get() as u32,
                        // FIXME
                        load_per_pipeline: NonZeroU32::new(PIPELINE_FULL_CAPACITY.cpu_millis())
                            .unwrap(),
                    },
                    params_fingerprint,
                });
            }
        }
    }
    sources
}

fn build_indexer_info(indexer: &IndexerPoolEntry, locality_aware: bool) -> IndexerInfo {
    if !locality_aware {
        return IndexerInfo {
            cpu_capacity: indexer.indexing_capacity,
            availability_zone: None,
            eligibility: Eligibility::Any,
        };
    }
    let eligibility = match indexer.ingester_status {
        IngesterStatus::Ready => Eligibility::Any,
        _ => Eligibility::SelfHostedOnly,
    };
    IndexerInfo {
        cpu_capacity: indexer.indexing_capacity,
        availability_zone: indexer.availability_zone.clone(),
        eligibility,
    }
}

fn build_indexer_infos(
    indexers: &[IndexerPoolEntry],
    locality_aware: bool,
) -> FnvHashMap<NodeId, IndexerInfo> {
    let mut indexer_infos: FnvHashMap<NodeId, IndexerInfo> = FnvHashMap::default();
    for indexer in indexers {
        if indexer.indexing_capacity.cpu_millis() == 0 {
            continue;
        }
        let indexer_info = build_indexer_info(indexer, locality_aware);
        indexer_infos.insert(indexer.node_id.clone(), indexer_info);
    }
    indexer_infos
}

fn build_indexer_statuses(indexers: &[IndexerPoolEntry]) -> FnvHashMap<NodeId, IngesterStatus> {
    indexers
        .iter()
        .map(|indexer| (indexer.node_id.clone(), indexer.ingester_status))
        .collect()
}

fn build_indexer_tasks(indexers: &[IndexerPoolEntry]) -> FnvHashMap<NodeId, Vec<IndexingTask>> {
    indexers
        .iter()
        .map(|indexer| (indexer.node_id.clone(), indexer.indexing_tasks.clone()))
        .collect()
}

impl IndexingScheduler {
    pub fn new(cluster_id: String, self_node_id: NodeId, indexer_pool: IndexerPool) -> Self {
        IndexingScheduler {
            cluster_id,
            self_node_id,
            indexer_pool,
            state: IndexingSchedulerState::default(),
            next_rebuild_tracker: RebuildNotifier::default(),
        }
    }

    pub fn observable_state(&self) -> IndexingSchedulerState {
        self.state.clone()
    }

    // Should be called whenever a change in the list of index/shard
    // has happened.
    //
    // Prefer not calling this method directly, and instead call
    // `ControlPlane::rebuild_indexing_plan_debounced`.
    pub(crate) fn rebuild_plan(&mut self, model: &ControlPlaneModel) {
        SCHEDULE_TOTAL.inc();

        let notify_on_drop = self.next_rebuild_tracker.start_rebuild();

        let sources = get_sources_to_schedule(model, disable_ingest_v1());

        let indexers: Vec<IndexerPoolEntry> = self.select_available_indexers_for_scheduling();

        let is_locality_aware = enable_locality_aware_scheduling();

        let indexer_infos: FnvHashMap<NodeId, IndexerInfo> =
            build_indexer_infos(&indexers, is_locality_aware);

        if indexer_infos.is_empty() {
            if !sources.is_empty() {
                warn!("no indexing capacity available, cannot schedule an indexing plan");
            }
            return;
        };

        let shard_locations = model.shard_locations();
        let (new_physical_plan, shard_locality_metrics) = self.build_new_plan(
            &sources,
            &indexer_infos,
            is_locality_aware,
            &shard_locations,
        );
        shard_locality_metrics.publish();

        let indexer_statuses = build_indexer_statuses(&indexers);
        if let Some(last_applied_plan) = &self.state.last_applied_physical_plan {
            let plans_diff = get_indexing_plans_diff(
                last_applied_plan.indexing_tasks_per_indexer(),
                new_physical_plan.indexing_tasks_per_indexer(),
                &self.state.last_applied_indexer_statuses,
                &indexer_statuses,
            );
            // No need to apply the new plan as it is the same as the old one.
            if plans_diff.is_empty() {
                return;
            }
        }
        self.state.last_applied_indexer_statuses = indexer_statuses;
        self.apply_physical_indexing_plan(new_physical_plan, Some(notify_on_drop));
        self.state.num_schedule_indexing_plan += 1;
    }

    /// An indexing plan built incrementally from the previous plan can lose locality over time but
    /// never regain it. Below a certain locality threshold, we try to rebuild the plan from scratch
    /// to improve locality (equivalent to restarting the control plane). We expect a plan built
    /// from scratch to have better locality. There is a long cooldown to prevent churning indexing
    /// too frequently.
    fn build_new_plan(
        &mut self,
        sources: &[SourceToSchedule],
        indexer_infos: &FnvHashMap<NodeId, IndexerInfo>,
        locality_aware: bool,
        shard_locations: &ShardLocations,
    ) -> (PhysicalIndexingPlan, ShardLocalityMetrics) {
        // Build the plan normally, seeded with the existing plan.
        let new_plan_incremental = build_physical_indexing_plan(
            sources,
            indexer_infos,
            locality_aware,
            self.state.last_applied_physical_plan.as_ref(),
            shard_locations,
        );
        let locality_incremental =
            get_shard_locality_metrics(&new_plan_incremental, shard_locations, indexer_infos);
        if locality_incremental.locality_percent() >= min_shard_locality_percent() {
            return (new_plan_incremental, locality_incremental);
        }
        let now = Instant::now();
        if let Some(next_plan_from_scratch_timestamp) = self.state.next_plan_from_scratch_timestamp
            && now < next_plan_from_scratch_timestamp
        {
            // The cooldown for applying a new plan from scratch is still active, so we return the
            // incremental plan.
            return (new_plan_incremental, locality_incremental);
        }
        // Locality on the incremental plan has degraded; let's see if building the plan from
        // scratch will yield a more optimal plan.
        let new_plan_from_scratch = build_physical_indexing_plan(
            sources,
            indexer_infos,
            locality_aware,
            None,
            shard_locations,
        );
        let locality_from_scratch =
            get_shard_locality_metrics(&new_plan_from_scratch, shard_locations, indexer_infos);
        if locality_from_scratch.locality_percent() <= locality_incremental.locality_percent() {
            // The plan from scratch yielded worse locality than the incremental plan, so we apply
            // the incremental plan. We don't really expect this.
            info!(
                "indexing plan rebuilt from scratch had worse locality than plan built \
                 incrementally; returning incremental plan"
            );
            return (new_plan_incremental, locality_incremental);
        }
        info!(
            locality_percent = locality_incremental.locality_percent(),
            locality_percent_from_scratch = locality_from_scratch.locality_percent(),
            "rebuilt the indexing plan from scratch to restore shard locality"
        );
        self.state.next_plan_from_scratch_timestamp = Some(now + PLAN_FROM_SCRATCH_COOLDOWN_PERIOD);
        (new_plan_from_scratch, locality_from_scratch)
    }

    /// Checks if the last applied plan corresponds to the running indexing tasks present in the
    /// chitchat cluster state. If true, do nothing.
    /// - If node IDs differ, schedule a new indexing plan.
    /// - If indexing tasks differ, apply again the last plan.
    pub(crate) fn control_running_plan(&mut self, model: &ControlPlaneModel) {
        let last_applied_plan =
            if let Some(last_applied_plan) = &self.state.last_applied_physical_plan {
                last_applied_plan
            } else {
                // If there is no plan, the node is probably starting and the scheduler did not find
                // indexers yet. In this case, we want to schedule as soon as possible to find new
                // indexers.
                self.rebuild_plan(model);
                return;
            };
        if let Some(last_applied_plan_timestamp) = self.state.last_applied_plan_timestamp
            && Instant::now().duration_since(last_applied_plan_timestamp)
                < MIN_DURATION_BETWEEN_SCHEDULING
        {
            return;
        }
        let indexers: Vec<IndexerPoolEntry> = self.select_available_indexers_for_scheduling();
        let running_indexer_tasks = build_indexer_tasks(&indexers);
        let running_indexer_statuses = build_indexer_statuses(&indexers);

        let indexing_plans_diff = get_indexing_plans_diff(
            &running_indexer_tasks,
            last_applied_plan.indexing_tasks_per_indexer(),
            &running_indexer_statuses,
            &self.state.last_applied_indexer_statuses,
        );
        if !indexing_plans_diff.has_same_nodes() {
            info!(plans_diff=?indexing_plans_diff, "running plan and last applied plan indexers differ: schedule an indexing plan");
            self.rebuild_plan(model);
        } else if !indexing_plans_diff.has_same_tasks() {
            // Some nodes may have not received their tasks, apply it again.
            info!(plans_diff=?indexing_plans_diff, "running tasks and last applied tasks differ: reapply last plan");
            self.apply_physical_indexing_plan(last_applied_plan.clone(), None);
        }
    }

    fn select_available_indexers_for_scheduling(&self) -> Vec<IndexerPoolEntry> {
        if enable_locality_aware_scheduling() {
            return self.select_ready_and_draining_indexers();
        }
        self.select_ready_or_retiring_indexers()
    }

    fn select_ready_and_draining_indexers(&self) -> Vec<IndexerPoolEntry> {
        self.indexer_pool
            .values()
            .into_iter()
            .filter(|indexer| {
                matches!(
                    indexer.ingester_status,
                    IngesterStatus::Ready
                        | IngesterStatus::Retiring
                        | IngesterStatus::Decommissioning
                )
            })
            .collect()
    }

    fn select_ready_or_retiring_indexers(&self) -> Vec<IndexerPoolEntry> {
        let (ready, retiring): (Vec<IndexerPoolEntry>, Vec<IndexerPoolEntry>) = self
            .indexer_pool
            .values()
            .into_iter()
            .filter(|indexer| {
                matches!(
                    indexer.ingester_status,
                    IngesterStatus::Ready | IngesterStatus::Retiring
                )
            })
            .partition(|indexer| indexer.ingester_status == IngesterStatus::Ready);

        if ready.is_empty() {
            // Allow scheduling on retiring indexers to drain shards
            // and avoid decommission timeouts (e.g. single-node cluster).
            warn!(
                "no ready indexer available, falling back to retiring indexers for shard draining"
            );
            retiring
        } else {
            ready
        }
    }

    fn apply_physical_indexing_plan(
        &mut self,
        new_physical_plan: PhysicalIndexingPlan,
        notify_on_drop: Option<Arc<NotifyChangeOnDrop>>,
    ) {
        debug!(new_physical_plan=?new_physical_plan, "apply physical indexing plan");
        APPLY_PLAN_TOTAL.inc();
        // The indexing plan ID is a monotonically increasing time based ID that's used as the
        // publish token for indexers, which ensures indexing plans and shard acquisition are always
        // informed by the most recent plan.
        let indexing_plan_id = Ulid::new().to_string();

        // Retiring and decommissioning indexers still receive the plan so they can gracefully shut
        // down dropped pipelines; other states (initializing, decommissioned, failed) are skipped.
        for indexer in self.indexer_pool.values().into_iter().filter(|indexer| {
            matches!(
                indexer.ingester_status,
                IngesterStatus::Ready | IngesterStatus::Retiring | IngesterStatus::Decommissioning
            )
        }) {
            let indexing_tasks = new_physical_plan
                .indexer(&indexer.node_id)
                .unwrap_or(&[])
                .to_vec();

            // We don't want to block on a slow indexer so we apply this change asynchronously.
            // Retiring/decommissioning indexers are time-bound, so a slow or unreachable
            // draining node can't hold the notify guard. Ready indexers get no timeout.
            let apply_deadline = matches!(
                indexer.ingester_status,
                IngesterStatus::Retiring | IngesterStatus::Decommissioning
            )
            .then_some(APPLY_INDEXING_PLAN_TIMEOUT);

            let notify_on_drop = notify_on_drop.clone();
            let indexing_plan_id = indexing_plan_id.clone();
            tokio::spawn(async move {
                let client = indexer.client.clone();
                let apply_plan_fut = client.apply_indexing_plan(ApplyIndexingPlanRequest {
                    indexing_tasks,
                    indexing_plan_id,
                });
                let apply_result = match apply_deadline {
                    Some(timeout) => tokio::time::timeout(timeout, apply_plan_fut).await,
                    None => Ok(apply_plan_fut.await),
                };
                match apply_result {
                    Ok(Ok(_)) => {}
                    Ok(Err(error)) => {
                        warn!(
                            %error,
                            node_id=%indexer.node_id,
                            generation_id=indexer.generation_id,
                            "failed to apply indexing plan to indexer"
                        );
                    }
                    Err(_elapsed) => {
                        warn!(
                            node_id=%indexer.node_id,
                            generation_id=indexer.generation_id,
                            "timed out applying indexing plan to indexer"
                        );
                    }
                }
                drop(notify_on_drop);
            });
        }
        self.state.num_applied_physical_indexing_plan += 1;
        self.state.last_applied_plan_timestamp = Some(Instant::now());
        self.state.last_applied_physical_plan = Some(new_physical_plan);
    }
}

struct IndexingPlansDiff<'a> {
    pub missing_node_ids: FnvHashSet<&'a NodeId>,
    pub unplanned_node_ids: FnvHashSet<&'a NodeId>,
    pub nodes_with_changed_ingester_status: FnvHashSet<&'a NodeId>,
    pub missing_tasks_by_node_id: FnvHashMap<&'a NodeId, Vec<&'a IndexingTask>>,
    pub unplanned_tasks_by_node_id: FnvHashMap<&'a NodeId, Vec<&'a IndexingTask>>,
}

impl IndexingPlansDiff<'_> {
    pub fn has_same_nodes(&self) -> bool {
        self.missing_node_ids.is_empty()
            && self.unplanned_node_ids.is_empty()
            && self.nodes_with_changed_ingester_status.is_empty()
    }

    pub fn has_same_tasks(&self) -> bool {
        self.missing_tasks_by_node_id
            .values()
            .map(Vec::len)
            .sum::<usize>()
            == 0
            && self
                .unplanned_tasks_by_node_id
                .values()
                .map(Vec::len)
                .sum::<usize>()
                == 0
    }

    pub fn is_empty(&self) -> bool {
        self.has_same_nodes() && self.has_same_tasks()
    }
}

fn min_shard_locality_percent() -> u32 {
    quickwit_common::get_from_env_cached!(
        u32,
        "QW_MIN_SHARD_LOCALITY_PERCENT",
        DEFAULT_MIN_SHARD_LOCALITY_PERCENT,
        false
    )
}

fn get_shard_locality_metrics(
    physical_plan: &PhysicalIndexingPlan,
    shard_locations: &ShardLocations,
    indexer_infos: &FnvHashMap<NodeId, IndexerInfo>,
) -> ShardLocalityMetrics {
    let mut num_local_shards = 0;
    let mut num_nearby_shards = 0;
    let mut num_remote_shards = 0;
    for (indexer, tasks) in physical_plan.indexing_tasks_per_indexer() {
        for task in tasks {
            for shard_id in &task.shard_ids {
                if shard_locations
                    .get_shard_locations(shard_id)
                    .iter()
                    .any(|node| *node == indexer)
                {
                    num_local_shards += 1;
                } else if is_shard_nearby(indexer, shard_id, shard_locations, indexer_infos) {
                    num_nearby_shards += 1;
                } else {
                    num_remote_shards += 1;
                }
            }
        }
    }
    ShardLocalityMetrics {
        num_remote_shards,
        num_nearby_shards,
        num_local_shards,
    }
}

impl fmt::Debug for IndexingPlansDiff<'_> {
    fn fmt(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
        if self.has_same_nodes() && self.has_same_tasks() {
            return write!(formatter, "EmptyIndexingPlansDiff");
        }
        write!(formatter, "IndexingPlansDiff(")?;
        let mut separator = "";
        if !self.missing_node_ids.is_empty() {
            write!(
                formatter,
                "missing_node_ids={:?}",
                PrettySample::new(
                    self.missing_node_ids.iter().map(|node_id| node_id.as_str()),
                    10
                )
            )?;
            separator = ", "
        }
        if !self.unplanned_node_ids.is_empty() {
            write!(
                formatter,
                "{separator}unplanned_node_ids={:?}",
                PrettySample::new(
                    self.unplanned_node_ids
                        .iter()
                        .map(|node_id| node_id.as_str()),
                    10
                )
            )?;
            separator = ", "
        }
        if !self.nodes_with_changed_ingester_status.is_empty() {
            write!(
                formatter,
                "{separator}nodes_with_changed_ingester_status={:?}",
                PrettySample::new(
                    self.nodes_with_changed_ingester_status
                        .iter()
                        .map(|node_id| node_id.as_str()),
                    10
                )
            )?;
            separator = ", "
        }
        if !self.missing_tasks_by_node_id.is_empty() {
            write!(formatter, "{separator}missing_tasks_by_node_id=",)?;
            format_indexing_task_map(formatter, &self.missing_tasks_by_node_id)?;
            separator = ", "
        }
        if !self.unplanned_tasks_by_node_id.is_empty() {
            write!(formatter, "{separator}unplanned_tasks_by_node_id=",)?;
            format_indexing_task_map(formatter, &self.unplanned_tasks_by_node_id)?;
        }
        write!(formatter, ")")
    }
}

fn format_indexing_task_map(
    formatter: &mut std::fmt::Formatter,
    indexing_tasks: &FnvHashMap<&NodeId, Vec<&IndexingTask>>,
) -> std::fmt::Result {
    // we show at most 5 nodes, and aggregate the results for the other.
    // we show at most 10 indexes, but aggregate results after.
    // we always aggregate shard ids
    // we hide pipeline id and incarnation id, they are not very useful in most case, but take a
    // lot of place
    const MAX_NODE: usize = 5;
    const MAX_INDEXES: usize = 10;
    let mut index_displayed = 0;
    write!(formatter, "{{")?;
    let mut indexer_iter = indexing_tasks.iter().enumerate();
    for (i, (index_name, tasks)) in &mut indexer_iter {
        if i != 0 {
            write!(formatter, ", ")?;
        }
        if index_displayed != MAX_INDEXES - 1 {
            write!(formatter, "\"{index_name}\": [")?;
            let mut tasks_iter = tasks.iter().enumerate();
            for (i, task) in &mut tasks_iter {
                if i != 0 {
                    write!(formatter, ", ")?;
                }
                write!(
                    formatter,
                    r#"(index_id: "{}", source_id: "{}", shard_count: {})"#,
                    task.index_uid.as_ref().unwrap().index_id,
                    task.source_id,
                    task.shard_ids.len()
                )?;
                index_displayed += 1;
                if index_displayed == MAX_INDEXES - 1 {
                    let (task_count, shard_count) = tasks_iter.fold((0, 0), |(t, s), (_, task)| {
                        (t + 1, s + task.shard_ids.len())
                    });
                    if task_count > 0 {
                        write!(
                            formatter,
                            " and {task_count} tasks and {shard_count} shards"
                        )?;
                    }
                    break;
                }
            }
            write!(formatter, "]")?;
        } else {
            write!(
                formatter,
                "\"{index_name}\": [with {} tasks and {} shards]",
                tasks.len(),
                tasks.iter().map(|task| task.shard_ids.len()).sum::<usize>()
            )?;
        }
        if i == MAX_NODE - 1 {
            break;
        }
    }
    let (indexer, tasks, shards) = indexer_iter.fold((0, 0, 0), |(i, t, s), (_, (_, task))| {
        (
            i + 1,
            t + task.len(),
            s + task.iter().map(|task| task.shard_ids.len()).sum::<usize>(),
        )
    });
    if indexer > 0 {
        write!(
            formatter,
            " and {indexer} more indexers, handling {tasks} tasks and {shards} shards}}"
        )
    } else {
        write!(formatter, "}}")
    }
}

/// Returns the difference between the `running_plan` retrieved from the chitchat state and
/// the last plan applied by the scheduler.
fn get_indexing_plans_diff<'a>(
    running_plan: &'a FnvHashMap<NodeId, Vec<IndexingTask>>,
    last_applied_plan: &'a FnvHashMap<NodeId, Vec<IndexingTask>>,
    running_ingester_statuses: &'a FnvHashMap<NodeId, IngesterStatus>,
    last_applied_ingester_statuses: &'a FnvHashMap<NodeId, IngesterStatus>,
) -> IndexingPlansDiff<'a> {
    // Nodes diff.
    let running_node_ids: FnvHashSet<&NodeId> = running_plan.keys().collect();
    let planned_node_ids: FnvHashSet<&NodeId> = last_applied_plan.keys().collect();
    let missing_node_ids: FnvHashSet<&NodeId> = planned_node_ids
        .difference(&running_node_ids)
        .copied()
        .collect();
    let unplanned_node_ids: FnvHashSet<&NodeId> = running_node_ids
        .difference(&planned_node_ids)
        .copied()
        .collect();
    // Ingester status diff.
    let running_node_states: FnvHashSet<(&NodeId, IngesterStatus)> = running_ingester_statuses
        .iter()
        .map(|(node_id, ingester_status)| (node_id, *ingester_status))
        .collect();
    let planned_node_states: FnvHashSet<(&NodeId, IngesterStatus)> = last_applied_ingester_statuses
        .iter()
        .map(|(node_id, ingester_status)| (node_id, *ingester_status))
        .collect();
    let nodes_with_changed_ingester_status: FnvHashSet<&NodeId> = running_node_states
        .difference(&planned_node_states)
        .map(|(node_id, _)| *node_id)
        .collect();
    // Tasks diff.
    let mut missing_tasks_by_node_id: FnvHashMap<&NodeId, Vec<&IndexingTask>> =
        FnvHashMap::default();
    let mut unplanned_tasks_by_node_id: FnvHashMap<&NodeId, Vec<&IndexingTask>> =
        FnvHashMap::default();
    for node_id in running_node_ids.iter().chain(planned_node_ids.iter()) {
        let running_tasks = running_plan
            .get(*node_id)
            .map(Vec::as_slice)
            .unwrap_or_else(|| &[]);
        let last_applied_tasks = last_applied_plan
            .get(*node_id)
            .map(Vec::as_slice)
            .unwrap_or_else(|| &[]);
        let (missing_tasks, unplanned_tasks) =
            get_indexing_tasks_diff(running_tasks, last_applied_tasks);
        missing_tasks_by_node_id.insert(*node_id, missing_tasks);
        unplanned_tasks_by_node_id.insert(*node_id, unplanned_tasks);
    }
    IndexingPlansDiff {
        missing_node_ids,
        unplanned_node_ids,
        nodes_with_changed_ingester_status,
        missing_tasks_by_node_id,
        unplanned_tasks_by_node_id,
    }
}

/// Computes the difference between `running_tasks` and `last_applied_tasks` and returns a tuple
/// of `missing_tasks` and `unplanned_tasks`.
/// Note: we need to handle duplicate tasks in each array, so we count them and make the diff.
fn get_indexing_tasks_diff<'a>(
    running_tasks: &'a [IndexingTask],
    last_applied_tasks: &'a [IndexingTask],
) -> (Vec<&'a IndexingTask>, Vec<&'a IndexingTask>) {
    let mut missing_tasks: Vec<&IndexingTask> = Vec::new();
    let mut unplanned_tasks: Vec<&IndexingTask> = Vec::new();
    let grouped_running_tasks: FnvHashMap<&IndexingTask, usize> = running_tasks
        .iter()
        .chunk_by(|&task| task)
        .into_iter()
        .map(|(key, group)| (key, group.count()))
        .collect();
    let grouped_last_applied_tasks: FnvHashMap<&IndexingTask, usize> = last_applied_tasks
        .iter()
        .chunk_by(|&task| task)
        .into_iter()
        .map(|(key, group)| (key, group.count()))
        .collect();
    let all_tasks: FnvHashSet<&IndexingTask> =
        FnvHashSet::from_iter(running_tasks.iter().chain(last_applied_tasks.iter()));
    for task in all_tasks {
        let running_task_count = grouped_running_tasks.get(task).unwrap_or(&0);
        let desired_task_count = grouped_last_applied_tasks.get(task).unwrap_or(&0);
        match running_task_count.cmp(desired_task_count) {
            Ordering::Greater => {
                unplanned_tasks
                    .extend_from_slice(&vec![task; running_task_count - desired_task_count]);
            }
            Ordering::Less => {
                missing_tasks
                    .extend_from_slice(&vec![task; desired_task_count - running_task_count])
            }
            _ => {}
        }
    }

    (missing_tasks, unplanned_tasks)
}

#[cfg(test)]
mod tests {
    use std::num::NonZeroUsize;
    use std::str::FromStr;

    use proptest::{prop_compose, proptest};
    use quickwit_config::{IndexConfig, KafkaSourceParams, SourceConfig, SourceParams};
    use quickwit_metastore::IndexMetadata;
    use quickwit_proto::types::{IndexUid, PipelineUid, ShardId, SourceUid};

    use super::*;
    use crate::indexing_scheduler::scheduling::{
        build_physical_indexing_plan_without_locality, shard_ids_for_indexer,
    };
    use crate::model::ShardLocations;
    #[test]
    fn test_indexing_plans_diff() {
        let index_uid = IndexUid::from_str("index-1:11111111111111111111111111").unwrap();
        let index_uid2 = IndexUid::from_str("index-2:11111111111111111111111111").unwrap();
        let indexer_1 = NodeId::from_str("indexer-1");
        let indexer_2 = NodeId::from_str("indexer-2");
        let indexer_statuses: FnvHashMap<NodeId, IngesterStatus> = FnvHashMap::default();
        {
            let running_plan = FnvHashMap::default();
            let desired_plan = FnvHashMap::default();
            let indexing_plans_diff = get_indexing_plans_diff(
                &running_plan,
                &desired_plan,
                &indexer_statuses,
                &indexer_statuses,
            );
            assert!(indexing_plans_diff.is_empty());
        }
        {
            let mut running_plan = FnvHashMap::default();
            let mut desired_plan = FnvHashMap::default();
            let task_1 = IndexingTask {
                pipeline_uid: Some(PipelineUid::for_test(10u128)),
                index_uid: Some(index_uid.clone()),
                source_id: "source-1".to_string(),
                shard_ids: Vec::new(),
                params_fingerprint: 0,
            };
            let task_1b = IndexingTask {
                pipeline_uid: Some(PipelineUid::for_test(11u128)),
                index_uid: Some(index_uid.clone()),
                source_id: "source-1".to_string(),
                shard_ids: Vec::new(),
                params_fingerprint: 0,
            };
            let task_2 = IndexingTask {
                pipeline_uid: Some(PipelineUid::for_test(20u128)),
                index_uid: Some(index_uid.clone()),
                source_id: "source-2".to_string(),
                shard_ids: Vec::new(),
                params_fingerprint: 0,
            };
            running_plan.insert(
                indexer_1.clone(),
                vec![task_1.clone(), task_1b.clone(), task_2.clone()],
            );
            desired_plan.insert(
                indexer_1.clone(),
                vec![task_2, task_1.clone(), task_1b.clone()],
            );
            let indexing_plans_diff = get_indexing_plans_diff(
                &running_plan,
                &desired_plan,
                &indexer_statuses,
                &indexer_statuses,
            );
            assert!(indexing_plans_diff.is_empty());
        }
        {
            let mut running_plan = FnvHashMap::default();
            let mut desired_plan = FnvHashMap::default();
            let task_1 = IndexingTask {
                pipeline_uid: Some(PipelineUid::for_test(1u128)),
                index_uid: Some(index_uid.clone()),
                source_id: "source-1".to_string(),
                shard_ids: Vec::new(),
                params_fingerprint: 0,
            };
            let task_2 = IndexingTask {
                pipeline_uid: Some(PipelineUid::for_test(2u128)),
                index_uid: Some(index_uid.clone()),
                source_id: "source-2".to_string(),
                shard_ids: Vec::new(),
                params_fingerprint: 0,
            };
            running_plan.insert(indexer_1.clone(), vec![task_1.clone()]);
            desired_plan.insert(indexer_1.clone(), vec![task_2.clone()]);

            let indexing_plans_diff = get_indexing_plans_diff(
                &running_plan,
                &desired_plan,
                &indexer_statuses,
                &indexer_statuses,
            );
            assert!(!indexing_plans_diff.is_empty());
            assert!(indexing_plans_diff.has_same_nodes());
            assert!(!indexing_plans_diff.has_same_tasks());
            assert_eq!(
                indexing_plans_diff.unplanned_tasks_by_node_id,
                FnvHashMap::from_iter([(&indexer_1, vec![&task_1])])
            );
            assert_eq!(
                indexing_plans_diff.missing_tasks_by_node_id,
                FnvHashMap::from_iter([(&indexer_1, vec![&task_2])])
            );
        }
        {
            // Task assigned to indexer-1 in desired plan but another one running.
            let mut running_plan = FnvHashMap::default();
            let mut desired_plan = FnvHashMap::default();
            let task_1 = IndexingTask {
                pipeline_uid: Some(PipelineUid::for_test(1u128)),
                index_uid: Some(index_uid.clone()),
                source_id: "source-1".to_string(),
                shard_ids: Vec::new(),
                params_fingerprint: 0,
            };
            let task_2 = IndexingTask {
                pipeline_uid: Some(PipelineUid::for_test(2u128)),
                index_uid: Some(index_uid2.clone()),
                source_id: "source-2".to_string(),
                shard_ids: Vec::new(),
                params_fingerprint: 0,
            };
            running_plan.insert(indexer_2.clone(), vec![task_2.clone()]);
            desired_plan.insert(indexer_1.clone(), vec![task_1.clone()]);

            let indexing_plans_diff = get_indexing_plans_diff(
                &running_plan,
                &desired_plan,
                &indexer_statuses,
                &indexer_statuses,
            );
            assert!(!indexing_plans_diff.is_empty());
            assert!(!indexing_plans_diff.has_same_nodes());
            assert!(!indexing_plans_diff.has_same_tasks());
            assert_eq!(
                indexing_plans_diff.missing_node_ids,
                FnvHashSet::from_iter([&indexer_1])
            );
            assert_eq!(
                indexing_plans_diff.unplanned_node_ids,
                FnvHashSet::from_iter([&indexer_2])
            );
            assert_eq!(
                indexing_plans_diff.missing_tasks_by_node_id,
                FnvHashMap::from_iter([(&indexer_1, vec![&task_1]), (&indexer_2, Vec::new())])
            );
            assert_eq!(
                indexing_plans_diff.unplanned_tasks_by_node_id,
                FnvHashMap::from_iter([(&indexer_2, vec![&task_2]), (&indexer_1, Vec::new())])
            );
        }
        {
            // Diff with 3 same tasks running but only one on the desired plan.
            let mut running_plan = FnvHashMap::default();
            let mut desired_plan = FnvHashMap::default();
            let task_1a = IndexingTask {
                pipeline_uid: Some(PipelineUid::for_test(10u128)),
                index_uid: Some(index_uid.clone()),
                source_id: "source-1".to_string(),
                shard_ids: Vec::new(),
                params_fingerprint: 0,
            };
            let task_1b = IndexingTask {
                pipeline_uid: Some(PipelineUid::for_test(11u128)),
                index_uid: Some(index_uid.clone()),
                source_id: "source-1".to_string(),
                shard_ids: Vec::new(),
                params_fingerprint: 0,
            };
            let task_1c = IndexingTask {
                pipeline_uid: Some(PipelineUid::for_test(12u128)),
                index_uid: Some(index_uid.clone()),
                source_id: "source-1".to_string(),
                shard_ids: Vec::new(),
                params_fingerprint: 0,
            };
            running_plan.insert(indexer_1.clone(), vec![task_1a.clone()]);
            desired_plan.insert(
                indexer_1.clone(),
                vec![task_1a.clone(), task_1b.clone(), task_1c.clone()],
            );

            let indexing_plans_diff = get_indexing_plans_diff(
                &running_plan,
                &desired_plan,
                &indexer_statuses,
                &indexer_statuses,
            );
            assert!(!indexing_plans_diff.is_empty());
            assert!(indexing_plans_diff.has_same_nodes());
            assert!(!indexing_plans_diff.has_same_tasks());
            assert_eq!(
                indexing_plans_diff.missing_tasks_by_node_id,
                FnvHashMap::from_iter([(&indexer_1, vec![&task_1b, &task_1c])])
            );
        }
        {
            let mut running_plan = FnvHashMap::default();
            let mut desired_plan = FnvHashMap::default();
            let task_1 = IndexingTask {
                pipeline_uid: Some(PipelineUid::for_test(1u128)),
                index_uid: Some(index_uid.clone()),
                source_id: "source-1".to_string(),
                shard_ids: Vec::new(),
                params_fingerprint: 0,
            };
            running_plan.insert(indexer_1.clone(), vec![task_1.clone()]);
            desired_plan.insert(indexer_1.clone(), vec![task_1.clone()]);

            let mut running_statuses = FnvHashMap::default();
            running_statuses.insert(indexer_1.clone(), IngesterStatus::Retiring);
            let mut last_applied_statuses = FnvHashMap::default();
            last_applied_statuses.insert(indexer_1.clone(), IngesterStatus::Ready);

            let indexing_plans_diff = get_indexing_plans_diff(
                &running_plan,
                &desired_plan,
                &running_statuses,
                &last_applied_statuses,
            );
            assert!(!indexing_plans_diff.is_empty());
            assert!(indexing_plans_diff.has_same_tasks());
            assert!(!indexing_plans_diff.has_same_nodes());
            assert_eq!(
                indexing_plans_diff.nodes_with_changed_ingester_status,
                FnvHashSet::from_iter([&indexer_1])
            );

            let mirrored_plans_diff = get_indexing_plans_diff(
                &running_plan,
                &desired_plan,
                &last_applied_statuses,
                &running_statuses,
            );
            assert!(!mirrored_plans_diff.has_same_nodes());
            assert_eq!(
                mirrored_plans_diff.nodes_with_changed_ingester_status,
                FnvHashSet::from_iter([&indexer_1])
            );
        }
    }

    #[test]
    fn test_build_new_plan() {
        let indexer1 = NodeId::from_str("indexer1");
        let indexer2 = NodeId::from_str("indexer2");
        let shard1 = ShardId::from(1);
        let shard2 = ShardId::from(2);
        let source_uid = SourceUid {
            index_uid: IndexUid::for_test("test-index", 0),
            source_id: "test-source".to_string(),
        };
        let sources = vec![SourceToSchedule {
            source_uid: source_uid.clone(),
            source_type: SourceToScheduleType::Sharded {
                shard_ids: vec![shard1.clone(), shard2.clone()],
                load_per_shard: NonZeroU32::new(1_000).unwrap(),
            },
            params_fingerprint: 0,
        }];
        let mut shard_locations = ShardLocations::default();
        shard_locations.add_location(&shard1, &indexer1);
        shard_locations.add_location(&shard2, &indexer2);

        let mut indexer_infos = FnvHashMap::default();
        indexer_infos.insert(indexer1.clone(), IndexerInfo::for_test(mcpu(4_000)));
        indexer_infos.insert(indexer2.clone(), IndexerInfo::for_test(mcpu(4_000)));

        // Each indexer indexes the shard the other one hosts, so nothing is local.
        let swapped_plan = || {
            let indexer_ids = vec![indexer1.clone(), indexer2.clone()];
            let mut plan = PhysicalIndexingPlan::with_indexer_ids(&indexer_ids);
            for (indexer, shard_id) in [(&indexer1, &shard2), (&indexer2, &shard1)] {
                plan.add_indexing_task(
                    indexer,
                    IndexingTask {
                        index_uid: Some(source_uid.index_uid.clone()),
                        source_id: source_uid.source_id.clone(),
                        pipeline_uid: Some(PipelineUid::random()),
                        shard_ids: vec![shard_id.clone()],
                        params_fingerprint: 0,
                    },
                );
            }
            plan
        };

        let mut scheduler = IndexingScheduler::new(
            "test-cluster".to_string(),
            NodeId::from_str("control-plane"),
            IndexerPool::default(),
        );
        let locality_aware = false;

        scheduler.state.last_applied_physical_plan = Some(swapped_plan());
        let (plan, metrics) =
            scheduler.build_new_plan(&sources, &indexer_infos, locality_aware, &shard_locations);
        assert_eq!(metrics.locality_percent(), 100);
        assert_eq!(
            shard_ids_for_indexer(&plan, &indexer1),
            vec![shard1.clone()]
        );

        scheduler.state.last_applied_physical_plan = Some(swapped_plan());
        let (_, metrics_in_cooldown) =
            scheduler.build_new_plan(&sources, &indexer_infos, locality_aware, &shard_locations);
        assert_eq!(metrics_in_cooldown.locality_percent(), 0);

        scheduler.state.next_plan_from_scratch_timestamp = None;
        scheduler.state.last_applied_physical_plan = Some(plan);
        let (_, metrics_above_threshold) =
            scheduler.build_new_plan(&sources, &indexer_infos, locality_aware, &shard_locations);
        assert_eq!(metrics_above_threshold.locality_percent(), 100);
        assert!(scheduler.state.next_plan_from_scratch_timestamp.is_none());
    }

    #[test]
    fn test_get_sources_to_schedule() {
        let mut model = ControlPlaneModel::default();
        let kafka_source_params = KafkaSourceParams {
            topic: "kafka-topic".to_string(),
            client_log_level: None,
            client_params: serde_json::json!({}),
            enable_backfill_mode: false,
        };
        let index_metadata = IndexMetadata::for_test("test-index", "ram:///test-index");
        let index_uid = index_metadata.index_uid.clone();
        model.add_index(index_metadata);
        model
            .add_source(
                &index_uid,
                SourceConfig {
                    source_id: "source_disabled".to_string(),
                    num_pipelines: NonZeroUsize::new(3).unwrap(),
                    enabled: false,
                    source_params: SourceParams::Kafka(kafka_source_params.clone()),
                    transform_config: None,
                    input_format: Default::default(),
                },
            )
            .unwrap();
        model
            .add_source(
                &index_uid,
                SourceConfig {
                    source_id: "source_enabled".to_string(),
                    num_pipelines: NonZeroUsize::new(2).unwrap(),
                    enabled: true,
                    source_params: SourceParams::Kafka(kafka_source_params.clone()),
                    transform_config: None,
                    input_format: Default::default(),
                },
            )
            .unwrap();
        model
            .add_source(
                &index_uid,
                SourceConfig {
                    source_id: "ingest_v1".to_string(),
                    num_pipelines: NonZeroUsize::new(2).unwrap(),
                    enabled: true,
                    // ingest v1
                    source_params: SourceParams::IngestApi,
                    transform_config: None,
                    input_format: Default::default(),
                },
            )
            .unwrap();
        model
            .add_source(
                &index_uid,
                SourceConfig {
                    source_id: "ingest_v2".to_string(),
                    num_pipelines: NonZeroUsize::new(2).unwrap(),
                    enabled: true,
                    // ingest v2
                    source_params: SourceParams::Ingest,
                    transform_config: None,
                    input_format: Default::default(),
                },
            )
            .unwrap();
        // ingest v2 without any open shard is skipped.
        model
            .add_source(
                &index_uid,
                SourceConfig {
                    source_id: "ingest_v2_without_shard".to_string(),
                    num_pipelines: NonZeroUsize::new(2).unwrap(),
                    enabled: true,
                    // ingest v2
                    source_params: SourceParams::Ingest,
                    transform_config: None,
                    input_format: Default::default(),
                },
            )
            .unwrap();
        model
            .add_source(
                &index_uid,
                SourceConfig {
                    source_id: "ingest_cli".to_string(),
                    num_pipelines: NonZeroUsize::new(2).unwrap(),
                    enabled: true,
                    // ingest v1
                    source_params: SourceParams::IngestCli,
                    transform_config: None,
                    input_format: Default::default(),
                },
            )
            .unwrap();
        let shard = Shard {
            index_uid: Some(index_uid.clone()),
            source_id: "ingest_v2".to_string(),
            shard_id: Some(ShardId::from(17)),
            shard_state: ShardState::Open as i32,
            ..Default::default()
        };
        model.insert_shards(&index_uid, &"ingest_v2".to_string(), vec![shard]);

        let disable_ingest_v1 = false;
        let sources: Vec<SourceToSchedule> = get_sources_to_schedule(&model, disable_ingest_v1);
        assert_eq!(sources.len(), 3);

        let disable_ingest_v1 = true;
        let sources: Vec<SourceToSchedule> = get_sources_to_schedule(&model, disable_ingest_v1);
        assert_eq!(sources.len(), 2);

        let contains_any_ingest_v1_source = sources
            .iter()
            .any(|source| matches!(source.source_type, SourceToScheduleType::IngestV1));
        assert!(!contains_any_ingest_v1_source);
    }

    #[test]
    fn test_build_physical_indexing_plan_simple() {
        let source_1 = SourceUid {
            index_uid: IndexUid::for_test("index-1", 0),
            source_id: "source1".to_string(),
        };
        let source_2 = SourceUid {
            index_uid: IndexUid::for_test("index-2", 0),
            source_id: "source2".to_string(),
        };
        let sources = [
            SourceToSchedule {
                source_uid: source_1.clone(),
                source_type: SourceToScheduleType::NonSharded {
                    num_pipelines: 3,
                    load_per_pipeline: NonZeroU32::new(1_000).unwrap(),
                },
                params_fingerprint: 0,
            },
            SourceToSchedule {
                source_uid: source_2.clone(),
                source_type: SourceToScheduleType::NonSharded {
                    num_pipelines: 2,
                    load_per_pipeline: NonZeroU32::new(1_000).unwrap(),
                },
                params_fingerprint: 0,
            },
        ];
        let indexer1 = NodeId::from_str("indexer1");
        let indexer2 = NodeId::from_str("indexer2");
        let mut indexer_infos = FnvHashMap::default();
        indexer_infos.insert(indexer1.clone(), IndexerInfo::for_test(mcpu(3_000)));
        indexer_infos.insert(indexer2.clone(), IndexerInfo::for_test(mcpu(3_000)));
        let shard_locations = ShardLocations::default();
        let physical_plan = build_physical_indexing_plan_without_locality(
            &sources[..],
            &indexer_infos,
            None,
            &shard_locations,
        );
        assert_eq!(physical_plan.indexing_tasks_per_indexer().len(), 2);
        let indexing_tasks_1 = physical_plan.indexer(&indexer1).unwrap();
        assert_eq!(indexing_tasks_1.len(), 2);
        let indexer_2_tasks = physical_plan.indexer(&indexer2).unwrap();
        assert_eq!(indexer_2_tasks.len(), 3);
    }

    #[test]
    fn test_debug_indexing_task_map() {
        let mut map = FnvHashMap::default();
        let task1 = IndexingTask {
            index_uid: Some(IndexUid::for_test("index1", 123)),
            source_id: "my-source".to_string(),
            pipeline_uid: Some(PipelineUid::random()),
            shard_ids: vec!["shard1".into()],
            params_fingerprint: 0,
        };
        let task2 = IndexingTask {
            index_uid: Some(IndexUid::for_test("index2", 123)),
            source_id: "my-source".to_string(),
            pipeline_uid: Some(PipelineUid::random()),
            shard_ids: vec!["shard2".into(), "shard3".into()],
            params_fingerprint: 0,
        };
        let task3 = IndexingTask {
            index_uid: Some(IndexUid::for_test("index3", 123)),
            source_id: "my-source".to_string(),
            pipeline_uid: Some(PipelineUid::random()),
            shard_ids: vec!["shard6".into()],
            params_fingerprint: 0,
        };
        let indexers: Vec<NodeId> = (1..=6)
            .map(|indexer_ord| NodeId::from_str(&format!("indexer{indexer_ord}")))
            .collect();
        // order made to map with the debug for lisibility
        map.insert(&indexers[4], vec![&task2]);
        map.insert(&indexers[3], vec![&task1]);
        map.insert(&indexers[2], vec![&task1, &task3]);
        map.insert(&indexers[1], vec![&task2, &task3, &task1, &task2]);
        map.insert(&indexers[0], vec![&task1, &task2, &task3, &task1]);
        map.insert(&indexers[5], vec![&task1, &task2, &task3]);
        let plan = IndexingPlansDiff {
            missing_node_ids: FnvHashSet::default(),
            unplanned_node_ids: FnvHashSet::default(),
            nodes_with_changed_ingester_status: FnvHashSet::default(),
            missing_tasks_by_node_id: map,
            unplanned_tasks_by_node_id: FnvHashMap::default(),
        };

        let debug = format!("{plan:?}");
        assert_eq!(
            debug,
            r#"IndexingPlansDiff(missing_tasks_by_node_id={"indexer5": [(index_id: "index2", source_id: "my-source", shard_count: 2)], "indexer4": [(index_id: "index1", source_id: "my-source", shard_count: 1)], "indexer3": [(index_id: "index1", source_id: "my-source", shard_count: 1), (index_id: "index3", source_id: "my-source", shard_count: 1)], "indexer2": [(index_id: "index2", source_id: "my-source", shard_count: 2), (index_id: "index3", source_id: "my-source", shard_count: 1), (index_id: "index1", source_id: "my-source", shard_count: 1), (index_id: "index2", source_id: "my-source", shard_count: 2)], "indexer1": [(index_id: "index1", source_id: "my-source", shard_count: 1) and 3 tasks and 4 shards] and 1 more indexers, handling 3 tasks and 4 shards})"#
        );
    }

    proptest! {
        #[test]
        fn test_building_indexing_tasks_and_physical_plan(num_indexers in 1usize..50usize, index_id_sources in proptest::collection::vec(gen_kafka_source(), 1..20)) {
            let index_uids: fnv::FnvHashSet<IndexUid> =
                index_id_sources.iter()
                    .map(|(index_uid, _)| index_uid.clone())
                    .collect();
            let mut model = ControlPlaneModel::default();
            for index_uid in index_uids {
                let index_config = IndexConfig::for_test(&index_uid.index_id, &format!("ram://test/{index_uid}"));
                model.add_index(IndexMetadata::new_with_index_uid(index_uid, index_config));
            }
            for (index_uid, source_config) in &index_id_sources {
                model.add_source(index_uid, source_config.clone()).unwrap();
            }

            let sources: Vec<SourceToSchedule> = get_sources_to_schedule(&model, false);
            let mut indexer_infos = FnvHashMap::default();
            for i in 0..num_indexers {
                let indexer_id = format!("indexer-{i}");
                indexer_infos.insert(NodeId::from_str(&indexer_id), IndexerInfo::for_test(mcpu(4_000)));
            }
            let shard_locations = ShardLocations::default();
            let _physical_indexing_plan = build_physical_indexing_plan_without_locality(&sources, &indexer_infos, None, &shard_locations);
        }
    }

    use quickwit_config::SourceInputFormat;
    use quickwit_proto::indexing::{
        ApplyIndexingPlanResponse, CpuCapacity, IndexingServiceClient, MockIndexingService, mcpu,
    };
    use quickwit_proto::ingest::{Shard, ShardState};

    fn mock_indexer_node_info(node_id: &str, status: IngesterStatus) -> IndexerPoolEntry {
        let mock_indexer = MockIndexingService::new();
        let client = IndexingServiceClient::from_mock(mock_indexer);
        IndexerPoolEntry {
            node_id: NodeId::from_str(node_id),
            generation_id: 0,
            client,
            indexing_tasks: Vec::new(),
            indexing_capacity: CpuCapacity::from_cpu_millis(4_000),
            ingester_status: status,
            availability_zone: None,
        }
    }

    #[test]
    fn test_select_available_indexers_returns_only_ready_when_available() {
        let indexer_pool = IndexerPool::default();
        let ready_indexer = mock_indexer_node_info("indexer-ready-1", IngesterStatus::Ready);
        let ready_indexer_2 = mock_indexer_node_info("indexer-ready-2", IngesterStatus::Ready);
        let retiring_indexer = mock_indexer_node_info("indexer-retiring", IngesterStatus::Retiring);
        indexer_pool.insert(ready_indexer.node_id.clone(), ready_indexer);
        indexer_pool.insert(ready_indexer_2.node_id.clone(), ready_indexer_2);
        indexer_pool.insert(retiring_indexer.node_id.clone(), retiring_indexer);

        let scheduler = IndexingScheduler::new(
            "test-cluster".to_string(),
            NodeId::from_str("control-plane"),
            indexer_pool,
        );
        let selected = scheduler.select_ready_or_retiring_indexers();

        assert_eq!(selected.len(), 2);
        assert!(
            selected
                .iter()
                .all(|i| i.ingester_status == IngesterStatus::Ready)
        );
    }

    #[test]
    fn test_select_available_indexers_falls_back_to_retiring_when_no_ready() {
        let indexer_pool = IndexerPool::default();
        let retiring_1 = mock_indexer_node_info("indexer-retiring-1", IngesterStatus::Retiring);
        let retiring_2 = mock_indexer_node_info("indexer-retiring-2", IngesterStatus::Retiring);
        let decommissioned_1 =
            mock_indexer_node_info("indexer-decommissioned-1", IngesterStatus::Decommissioned);
        let decommissioning_1 =
            mock_indexer_node_info("indexer-decommissioning-1", IngesterStatus::Decommissioning);
        let initializing_1 =
            mock_indexer_node_info("indexer-initializing-1", IngesterStatus::Initializing);

        indexer_pool.insert(retiring_1.node_id.clone(), retiring_1);
        indexer_pool.insert(retiring_2.node_id.clone(), retiring_2);
        indexer_pool.insert(decommissioned_1.node_id.clone(), decommissioned_1);
        indexer_pool.insert(decommissioning_1.node_id.clone(), decommissioning_1);
        indexer_pool.insert(initializing_1.node_id.clone(), initializing_1);

        let scheduler = IndexingScheduler::new(
            "test-cluster".to_string(),
            NodeId::from_str("control-plane"),
            indexer_pool,
        );
        let selected = scheduler.select_ready_or_retiring_indexers();

        assert_eq!(selected.len(), 2);
        assert!(
            selected
                .iter()
                .all(|i| i.ingester_status == IngesterStatus::Retiring)
        );
    }

    #[test]
    fn test_select_available_indexers_returns_empty_when_pool_is_empty() {
        let indexer_pool = IndexerPool::default();
        let scheduler = IndexingScheduler::new(
            "test-cluster".to_string(),
            NodeId::from_str("control-plane"),
            indexer_pool,
        );
        let selected = scheduler.select_ready_or_retiring_indexers();
        assert!(selected.is_empty());
    }

    #[test]
    fn test_select_ready_and_draining_indexers() {
        let indexer_pool = IndexerPool::default();
        let statuses = [
            IngesterStatus::Unspecified,
            IngesterStatus::Initializing,
            IngesterStatus::Ready,
            IngesterStatus::Retiring,
            IngesterStatus::Decommissioning,
            IngesterStatus::Decommissioned,
            IngesterStatus::Failed,
        ];
        for status in statuses {
            let node_id = format!("indexer-{status:?}");
            let indexer = mock_indexer_node_info(&node_id, status);
            indexer_pool.insert(indexer.node_id.clone(), indexer);
        }

        let scheduler = IndexingScheduler::new(
            "test-cluster".to_string(),
            NodeId::from_str("control-plane"),
            indexer_pool,
        );
        let selected = scheduler.select_ready_and_draining_indexers();

        let selected_statuses: FnvHashSet<IngesterStatus> = selected
            .iter()
            .map(|indexer| indexer.ingester_status)
            .collect();
        let expected_statuses = FnvHashSet::from_iter([
            IngesterStatus::Ready,
            IngesterStatus::Retiring,
            IngesterStatus::Decommissioning,
        ]);
        assert_eq!(selected.len(), 3);
        assert_eq!(selected_statuses, expected_statuses);
    }

    #[test]
    fn test_build_indexer_infos_assigns_draining_eligibility() {
        let locality_aware = true;
        {
            let mut ready = mock_indexer_node_info("indexer-ready", IngesterStatus::Ready);
            ready.availability_zone = Some("az-a".to_string());
            let retiring = mock_indexer_node_info("indexer-retiring", IngesterStatus::Retiring);
            let decommissioning =
                mock_indexer_node_info("indexer-decommissioning", IngesterStatus::Decommissioning);
            let indexers = vec![ready, retiring, decommissioning];

            let indexer_infos = build_indexer_infos(&indexers, locality_aware);

            assert_eq!(indexer_infos["indexer-ready"].eligibility, Eligibility::Any);
            assert_eq!(
                indexer_infos["indexer-ready"].availability_zone,
                Some("az-a".to_string())
            );
            assert_eq!(
                indexer_infos["indexer-retiring"].eligibility,
                Eligibility::SelfHostedOnly
            );
            assert_eq!(
                indexer_infos["indexer-decommissioning"].eligibility,
                Eligibility::SelfHostedOnly
            );
        }
        {
            let retiring = mock_indexer_node_info("indexer-retiring", IngesterStatus::Retiring);
            let decommissioning =
                mock_indexer_node_info("indexer-decommissioning", IngesterStatus::Decommissioning);
            let indexers = vec![retiring, decommissioning];

            let indexer_infos = build_indexer_infos(&indexers, locality_aware);

            assert_eq!(
                indexer_infos["indexer-retiring"].eligibility,
                Eligibility::SelfHostedOnly
            );
            assert_eq!(
                indexer_infos["indexer-decommissioning"].eligibility,
                Eligibility::SelfHostedOnly
            );
        }
        {
            let mut ready = mock_indexer_node_info("indexer-ready", IngesterStatus::Ready);
            ready.availability_zone = Some("az-a".to_string());
            let mut retiring = mock_indexer_node_info("indexer-retiring", IngesterStatus::Retiring);
            retiring.availability_zone = Some("az-b".to_string());
            let indexers = vec![ready, retiring];
            let locality_unaware = false;

            let indexer_infos = build_indexer_infos(&indexers, locality_unaware);

            assert_eq!(indexer_infos["indexer-ready"].availability_zone, None);
            assert_eq!(indexer_infos["indexer-retiring"].availability_zone, None);
            assert_eq!(indexer_infos["indexer-ready"].eligibility, Eligibility::Any);
            assert_eq!(
                indexer_infos["indexer-retiring"].eligibility,
                Eligibility::Any
            );
        }
    }

    // Only ready, retiring, and decommissioning indexers receive a plan; indexers in any other
    // state must be skipped entirely. See `apply_physical_indexing_plan`.
    #[tokio::test]
    async fn test_apply_plan_skips_non_eligible_indexers() {
        let indexer_pool = IndexerPool::default();
        let eligible_indexers = [
            asserting_indexer_node_info("indexer-ready", IngesterStatus::Ready, true),
            asserting_indexer_node_info("indexer-retiring", IngesterStatus::Retiring, true),
            asserting_indexer_node_info(
                "indexer-decommissioning",
                IngesterStatus::Decommissioning,
                true,
            ),
        ];
        let skipped_indexers = [
            never_applied_indexer_node_info("indexer-unspecified", IngesterStatus::Unspecified),
            never_applied_indexer_node_info("indexer-initializing", IngesterStatus::Initializing),
            never_applied_indexer_node_info(
                "indexer-decommissioned",
                IngesterStatus::Decommissioned,
            ),
            never_applied_indexer_node_info("indexer-failed", IngesterStatus::Failed),
        ];
        for indexer in eligible_indexers.into_iter().chain(skipped_indexers) {
            indexer_pool.insert(indexer.node_id.clone(), indexer);
        }

        let mut scheduler = IndexingScheduler::new(
            "test-cluster".to_string(),
            NodeId::from_str("control-plane"),
            indexer_pool,
        );
        let physical_plan = PhysicalIndexingPlan::with_indexer_ids(&[]);
        let waiter = scheduler.next_rebuild_tracker.next_rebuild_waiter();
        let notify_on_drop = scheduler.next_rebuild_tracker.start_rebuild();
        scheduler.apply_physical_indexing_plan(physical_plan, Some(notify_on_drop));
        waiter.await;
    }

    // A node the planner dropped from the plan (e.g. a retiring indexer) must still receive an
    // empty plan so it shuts down its now-orphaned pipelines.
    #[tokio::test]
    async fn test_apply_plan_sends_empty_plan_to_dropped_indexer() {
        let indexer_pool = IndexerPool::default();
        let ready_indexer =
            asserting_indexer_node_info("indexer-ready", IngesterStatus::Ready, false);
        // Dropped from the plan (retiring): must receive an empty plan.
        let retiring_indexer =
            asserting_indexer_node_info("indexer-retiring", IngesterStatus::Retiring, true);
        indexer_pool.insert(ready_indexer.node_id.clone(), ready_indexer);
        indexer_pool.insert(retiring_indexer.node_id.clone(), retiring_indexer);

        let mut scheduler = IndexingScheduler::new(
            "test-cluster".to_string(),
            NodeId::from_str("control-plane"),
            indexer_pool,
        );

        let index_uid = IndexUid::from_str("index-1:11111111111111111111111111").unwrap();
        let task = IndexingTask {
            pipeline_uid: Some(PipelineUid::for_test(1u128)),
            index_uid: Some(index_uid),
            source_id: "source-1".to_string(),
            shard_ids: Vec::new(),
            params_fingerprint: 0,
        };
        let mut physical_plan =
            PhysicalIndexingPlan::with_indexer_ids(&[NodeId::from_str("indexer-ready")]);
        physical_plan.add_indexing_task(&NodeId::from_str("indexer-ready"), task);

        // `apply_physical_indexing_plan` dispatches the RPCs on spawned tasks; the rebuild waiter
        // resolves once every spawned task has dropped its `notify_on_drop` clone, i.e. after all
        // `apply_indexing_plan` calls have completed.
        let waiter = scheduler.next_rebuild_tracker.next_rebuild_waiter();
        let notify_on_drop = scheduler.next_rebuild_tracker.start_rebuild();
        scheduler.apply_physical_indexing_plan(physical_plan, Some(notify_on_drop));
        waiter.await;
    }

    // Builds an `IndexerNodeInfo` whose client asserts the exact `ApplyIndexingPlanRequest` it
    // receives (via `withf`) and that it is called exactly once (via `times(1)`, verified on drop).
    fn asserting_indexer_node_info(
        node_id: &str,
        status: IngesterStatus,
        expect_empty_plan: bool,
    ) -> IndexerPoolEntry {
        let mut mock_indexer = MockIndexingService::new();
        mock_indexer
            .expect_apply_indexing_plan()
            .times(1)
            .withf(move |request| request.indexing_tasks.is_empty() == expect_empty_plan)
            .returning(|_| Ok(ApplyIndexingPlanResponse {}));
        let client = IndexingServiceClient::from_mock(mock_indexer);
        IndexerPoolEntry {
            node_id: NodeId::from_str(node_id),
            generation_id: 0,
            client,
            indexing_tasks: Vec::new(),
            indexing_capacity: CpuCapacity::from_cpu_millis(4_000),
            ingester_status: status,
            availability_zone: None,
        }
    }

    // Builds an `IndexerNodeInfo` whose client asserts it is never asked to apply a plan (via
    // `never()`, verified on drop). The shared mock is `Arc`-cloned across the client, so a wrong
    // call from a spawned task is seen when the pool's copy drops on the main thread.
    fn never_applied_indexer_node_info(node_id: &str, status: IngesterStatus) -> IndexerPoolEntry {
        let mut mock_indexer = MockIndexingService::new();
        mock_indexer.expect_apply_indexing_plan().never();
        let client = IndexingServiceClient::from_mock(mock_indexer);
        IndexerPoolEntry {
            node_id: NodeId::from_str(node_id),
            generation_id: 0,
            client,
            indexing_tasks: Vec::new(),
            indexing_capacity: CpuCapacity::from_cpu_millis(4_000),
            ingester_status: status,
            availability_zone: None,
        }
    }

    // An `IndexingService` whose apply RPC never returns, so the spawned apply task can only
    // finish if a timeout cancels it.
    #[derive(Debug)]
    struct HangingIndexingService;

    #[async_trait::async_trait]
    impl IndexingService for HangingIndexingService {
        async fn apply_indexing_plan(
            &self,
            _request: ApplyIndexingPlanRequest,
        ) -> quickwit_proto::indexing::IndexingResult<ApplyIndexingPlanResponse> {
            std::future::pending().await
        }
    }

    fn hanging_indexer_node_info(status: IngesterStatus) -> IndexerPoolEntry {
        let client = IndexingServiceClient::tower().build(HangingIndexingService);
        IndexerPoolEntry {
            node_id: NodeId::from_str("indexer"),
            generation_id: 0,
            client,
            indexing_tasks: Vec::new(),
            indexing_capacity: CpuCapacity::from_cpu_millis(4_000),
            ingester_status: status,
            availability_zone: None,
        }
    }

    // Applies a plan to a single indexer whose apply RPC hangs forever, then reports whether the
    // apply task finished within `observe` — i.e. whether a timeout cancelled it.
    async fn hanging_apply_is_cancelled_within(status: IngesterStatus, observe: Duration) -> bool {
        let indexer_pool = IndexerPool::default();
        let indexer = hanging_indexer_node_info(status);
        indexer_pool.insert(indexer.node_id.clone(), indexer);
        let mut scheduler = IndexingScheduler::new(
            "test-cluster".to_string(),
            NodeId::from_str("control-plane"),
            indexer_pool,
        );
        let physical_plan = PhysicalIndexingPlan::with_indexer_ids(&[]);
        let waiter = scheduler.next_rebuild_tracker.next_rebuild_waiter();
        let notify_on_drop = scheduler.next_rebuild_tracker.start_rebuild();
        scheduler.apply_physical_indexing_plan(physical_plan, Some(notify_on_drop));
        // The waiter resolves only once the spawned apply task drops its `notify_on_drop`, which
        // for a hanging RPC happens only if a timeout fires.
        tokio::time::timeout(observe, waiter).await.is_ok()
    }

    #[tokio::test]
    async fn test_apply_plan_times_out_only_for_draining_indexers() {
        // A ready indexer is unbounded: the hanging apply is never cancelled, so its task never
        // finishes (a wrongly-applied timeout would fire well within 500ms and flip this).
        assert!(
            !hanging_apply_is_cancelled_within(IngesterStatus::Ready, Duration::from_millis(500))
                .await
        );
        // Retiring/decommissioning indexers are bounded, so the hanging apply is cancelled and the
        // task finishes (resolves in ~APPLY_INDEXING_PLAN_TIMEOUT, far within the window).
        assert!(
            hanging_apply_is_cancelled_within(IngesterStatus::Retiring, Duration::from_secs(5))
                .await
        );
        assert!(
            hanging_apply_is_cancelled_within(
                IngesterStatus::Decommissioning,
                Duration::from_secs(5)
            )
            .await
        );
    }

    fn kafka_source_params_for_test() -> SourceParams {
        SourceParams::Kafka(KafkaSourceParams {
            topic: "topic".to_string(),
            client_log_level: None,
            client_params: serde_json::json!({
                "bootstrap.servers": "localhost:9092",
            }),
            enable_backfill_mode: true,
        })
    }

    prop_compose! {
      fn gen_kafka_source()
        (index_idx in 0usize..100usize, num_pipelines in 1usize..51usize) -> (IndexUid, SourceConfig) {
          let index_uid = IndexUid::for_test(&format!("index-id-{index_idx}"), 0 /* this is the index uid */);
          let source_id = quickwit_common::rand::append_random_suffix("kafka-source");
          (index_uid, SourceConfig {
              source_id,
              num_pipelines: NonZeroUsize::new(num_pipelines).unwrap(),
              enabled: true,
              source_params: kafka_source_params_for_test(),
              transform_config: None,
              input_format: SourceInputFormat::Json,
          })
      }
    }
}
