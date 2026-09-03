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

use std::num::NonZeroU32;

use fnv::{FnvHashMap, FnvHashSet};
use quickwit_proto::indexing::CpuCapacity;
use quickwit_proto::types::{IndexUid, NodeId, PipelineUid, ShardId, SourceUid};
use rand::rngs::StdRng;
use rand::{RngExt, SeedableRng};

use super::{
    ChurnTally, Eligibility, IndexerInfo, IndexerSpec, SourceToSchedule, SourceToScheduleType,
    build_physical_indexing_plan, build_physical_indexing_plan_with_seed_choice,
    count_shards_that_moved, indexer_per_shard,
};
use crate::indexing_plan::PhysicalIndexingPlan;
use crate::indexing_scheduler::get_shard_locality_metrics;
use crate::model::ShardLocations;

const NUM_PODS: usize = 16;
const NUM_AZS: usize = 3;
const POD_CPU_CAPACITY: CpuCapacity = CpuCapacity::from_cpu_millis(4_000);
const LOAD_PER_SHARD: u32 = 1_000;
const SEED: u64 = 20_260_819;

/// Twenty five indexes spanning three orders of magnitude, from a single shard to two hundred.
const SOURCE_SHARD_COUNTS: [usize; 25] = [
    200, 150, 100, 80, 60, 50, 40, 30, 25, 20, 16, 12, 10, 8, 6, 5, 4, 3, 3, 2, 2, 2, 1, 1, 1,
];

const NUM_DEPLOYMENTS: usize = 2;
const REBUILDS_PER_POD: usize = 12;
const DRAIN_ROUNDS: usize = 11;
const CLOSED_SHARD_LINGER_ROUNDS: usize = 3;
const REBALANCE_TOLERANCE_INV_RATIO: usize = 10;

const HEADROOM_RATIO: f32 = 1.2;
const MIN_SHARDS_PER_POD_FOR_FEASIBILITY: usize = 5;

#[derive(Clone, Copy, Eq, PartialEq)]
enum PodState {
    Ready,
    Draining,
    Absent,
}

struct ModelShard {
    shard_id: ShardId,
    source_ord: usize,
    host_ord: usize,
    rounds_since_closed: Option<usize>,
}

impl ModelShard {
    fn is_open(&self) -> bool {
        self.rounds_since_closed.is_none()
    }
}

struct DeployWorld {
    indexer_specs: Vec<IndexerSpec>,
    pod_states: Vec<PodState>,
    shards: Vec<ModelShard>,
    next_shard_id: u64,
    index_uid: IndexUid,
}

fn pick_emptiest_ready_pod(
    open_shard_counts: &[usize],
    pod_states: &[PodState],
    rng: &mut StdRng,
) -> usize {
    let mut lowest_count = usize::MAX;
    let mut candidates: Vec<usize> = Vec::new();
    for pod_ord in 0..NUM_PODS {
        if pod_states[pod_ord] != PodState::Ready {
            continue;
        }
        let count = open_shard_counts[pod_ord];
        if count < lowest_count {
            lowest_count = count;
            candidates.clear();
            candidates.push(pod_ord);
        } else if count == lowest_count {
            candidates.push(pod_ord);
        }
    }
    candidates[rng.random_range(0..candidates.len())]
}

impl DeployWorld {
    fn new(rng: &mut StdRng) -> DeployWorld {
        let indexer_specs = (0..NUM_PODS)
            .map(|pod_ord| {
                let availability_zone = format!("az-{}", pod_ord % NUM_AZS);
                let node_id = format!("indexer-{pod_ord}");
                IndexerSpec::new(&node_id, POD_CPU_CAPACITY, Some(&availability_zone))
            })
            .collect();
        let mut world = DeployWorld {
            indexer_specs,
            pod_states: vec![PodState::Ready; NUM_PODS],
            shards: Vec::new(),
            next_shard_id: 0,
            index_uid: IndexUid::for_test("deployment-index", 0),
        };
        for (source_ord, num_shards) in SOURCE_SHARD_COUNTS.into_iter().enumerate() {
            for _ in 0..num_shards {
                let open_shard_counts = world.open_shard_counts();
                let host_ord = pick_emptiest_ready_pod(&open_shard_counts, &world.pod_states, rng);
                world.open_shard(source_ord, host_ord);
            }
        }
        world
    }

    fn open_shard(&mut self, source_ord: usize, host_ord: usize) {
        let shard_id = ShardId::from(self.next_shard_id);
        self.next_shard_id += 1;
        self.shards.push(ModelShard {
            shard_id,
            source_ord,
            host_ord,
            rounds_since_closed: None,
        });
    }

    fn open_shard_counts(&self) -> Vec<usize> {
        let mut open_shard_counts = vec![0usize; NUM_PODS];
        for shard in &self.shards {
            if shard.is_open() {
                open_shard_counts[shard.host_ord] += 1;
            }
        }
        open_shard_counts
    }

    fn hosted_shard_counts(&self) -> Vec<usize> {
        let mut hosted_shard_counts = vec![0usize; NUM_PODS];
        for shard in &self.shards {
            hosted_shard_counts[shard.host_ord] += 1;
        }
        hosted_shard_counts
    }

    fn node_id(&self, pod_ord: usize) -> &NodeId {
        &self.indexer_specs[pod_ord].node_id
    }

    /// Mirrors `IngestController::compute_shards_to_rebalance` followed by `allocate_shards`: every
    /// open shard on a pod that is not ready must move, then open shard counts are levelled across
    /// ready pods within the tolerance. Moving a shard means closing it and opening a new one on
    /// the emptiest ready pod, never re-hosting it.
    fn rebalance(&mut self, rng: &mut StdRng) {
        let mut open_shard_counts = self.open_shard_counts();
        let mut shard_indexes_to_move: Vec<usize> = Vec::new();
        for (shard_index, shard) in self.shards.iter().enumerate() {
            if shard.is_open() && self.pod_states[shard.host_ord] != PodState::Ready {
                shard_indexes_to_move.push(shard_index);
            }
        }
        let mut balancing_counts = open_shard_counts.clone();
        for &shard_index in &shard_indexes_to_move {
            balancing_counts[self.shards[shard_index].host_ord] -= 1;
        }
        let mut moving: FnvHashSet<usize> = shard_indexes_to_move.iter().copied().collect();
        loop {
            let mut min_pod_ord = usize::MAX;
            let mut max_pod_ord = usize::MAX;
            for pod_ord in 0..NUM_PODS {
                if self.pod_states[pod_ord] != PodState::Ready {
                    continue;
                }
                if min_pod_ord == usize::MAX
                    || balancing_counts[pod_ord] < balancing_counts[min_pod_ord]
                {
                    min_pod_ord = pod_ord;
                }
                if max_pod_ord == usize::MAX
                    || balancing_counts[pod_ord] > balancing_counts[max_pod_ord]
                {
                    max_pod_ord = pod_ord;
                }
            }
            if min_pod_ord == usize::MAX || max_pod_ord == usize::MAX {
                break;
            }
            let min_count = balancing_counts[min_pod_ord];
            let tolerance = min_count.div_ceil(REBALANCE_TOLERANCE_INV_RATIO).max(2);
            if balancing_counts[max_pod_ord] < min_count + tolerance {
                break;
            }
            let shard_index_opt = self.shards.iter().enumerate().position(|(index, shard)| {
                shard.is_open() && shard.host_ord == max_pod_ord && !moving.contains(&index)
            });
            let Some(shard_index) = shard_index_opt else {
                break;
            };
            moving.insert(shard_index);
            shard_indexes_to_move.push(shard_index);
            balancing_counts[max_pod_ord] -= 1;
            balancing_counts[min_pod_ord] += 1;
        }
        for shard_index in shard_indexes_to_move {
            let source_ord = self.shards[shard_index].source_ord;
            let host_ord = self.shards[shard_index].host_ord;
            let target_pod_ord = pick_emptiest_ready_pod(&open_shard_counts, &self.pod_states, rng);
            open_shard_counts[target_pod_ord] += 1;
            open_shard_counts[host_ord] -= 1;
            self.open_shard(source_ord, target_pod_ord);
            self.shards[shard_index].rounds_since_closed = Some(0);
        }
    }

    fn age_closed_shards(&mut self) {
        for shard in &mut self.shards {
            if let Some(rounds_since_closed) = shard.rounds_since_closed {
                shard.rounds_since_closed = Some(rounds_since_closed + 1);
            }
        }
        self.shards.retain(|shard| match shard.rounds_since_closed {
            Some(rounds_since_closed) => rounds_since_closed <= CLOSED_SHARD_LINGER_ROUNDS,
            None => true,
        });
    }

    fn sources(&self) -> Vec<SourceToSchedule> {
        let mut shard_ids_per_source: Vec<Vec<ShardId>> =
            vec![Vec::new(); SOURCE_SHARD_COUNTS.len()];
        for shard in &self.shards {
            shard_ids_per_source[shard.source_ord].push(shard.shard_id.clone());
        }
        let mut sources = Vec::new();
        for (source_ord, mut shard_ids) in shard_ids_per_source.into_iter().enumerate() {
            if shard_ids.is_empty() {
                continue;
            }
            shard_ids.sort();
            sources.push(SourceToSchedule {
                source_uid: SourceUid {
                    index_uid: self.index_uid.clone(),
                    source_id: format!("source-{source_ord}"),
                },
                source_type: SourceToScheduleType::Sharded {
                    shard_ids,
                    load_per_shard: NonZeroU32::new(LOAD_PER_SHARD).unwrap(),
                },
                params_fingerprint: 0,
            });
        }
        sources
    }

    fn host_ord_per_shard(&self) -> FnvHashMap<ShardId, usize> {
        self.shards
            .iter()
            .map(|shard| (shard.shard_id.clone(), shard.host_ord))
            .collect()
    }

    fn live_shard_ids(&self) -> FnvHashSet<ShardId> {
        self.shards
            .iter()
            .map(|shard| shard.shard_id.clone())
            .collect()
    }

    fn shard_locations(&self) -> ShardLocations<'_> {
        let mut shard_locations = ShardLocations::default();
        for shard in &self.shards {
            shard_locations
                .add_location(&shard.shard_id, &self.indexer_specs[shard.host_ord].node_id);
        }
        shard_locations
    }

    fn indexer_infos(&self, locality_aware: bool) -> FnvHashMap<NodeId, IndexerInfo> {
        let mut indexer_infos = FnvHashMap::default();
        for (pod_ord, indexer_spec) in self.indexer_specs.iter().enumerate() {
            let indexer_info = match self.pod_states[pod_ord] {
                PodState::Absent => continue,
                PodState::Draining => {
                    if !locality_aware {
                        continue;
                    }
                    indexer_spec.to_indexer_info(Eligibility::SelfHostedOnly)
                }
                PodState::Ready => {
                    if locality_aware {
                        indexer_spec.to_indexer_info(Eligibility::Any)
                    } else {
                        IndexerInfo::for_test(POD_CPU_CAPACITY)
                    }
                }
            };
            indexer_infos.insert(indexer_spec.node_id.clone(), indexer_info);
        }
        indexer_infos
    }
}

fn pipeline_shard_sets(
    plan: &PhysicalIndexingPlan,
) -> FnvHashMap<(NodeId, PipelineUid), Vec<ShardId>> {
    let mut pipeline_shard_sets = FnvHashMap::default();
    for (indexer, tasks) in plan.indexing_tasks_per_indexer() {
        for task in tasks {
            let mut shard_ids = task.shard_ids.clone();
            shard_ids.sort();
            pipeline_shard_sets.insert((indexer.clone(), task.pipeline_uid()), shard_ids);
        }
    }
    pipeline_shard_sets
}

/// A pipeline is reset when it loses a shard that has not finished indexing. Adding shards to a
/// pipeline is done in flight, and dropping a shard that reached EOF only removes it from the
/// assignment, so neither costs anything. See `IngestSource::reset_if_needed`.
/// Splits resets by cause: a live shard that left for another indexer, versus one that stayed on
/// this indexer and was merely re-packed into a different pipeline. Both discard the pipeline's
/// in-flight work; only the first is visible as a moved shard.
fn count_pipeline_resets(
    previous_plan: &PhysicalIndexingPlan,
    plan: &PhysicalIndexingPlan,
    live_shard_ids: &FnvHashSet<ShardId>,
) -> (usize, usize) {
    let shard_sets_before = pipeline_shard_sets(previous_plan);
    let shard_sets_after = pipeline_shard_sets(plan);
    let indexer_per_shard_after = indexer_per_shard(plan);
    let no_shards: Vec<ShardId> = Vec::new();
    let mut num_resets_from_moves = 0;
    let mut num_resets_from_repacking = 0;
    for (pipeline, shard_ids_before) in &shard_sets_before {
        let (indexer, _) = pipeline;
        let shard_ids_after = shard_sets_after.get(pipeline).unwrap_or(&no_shards);
        let mut lost_a_live_shard = false;
        let mut lost_a_live_shard_to_another_indexer = false;
        for shard_id in shard_ids_before {
            if shard_ids_after.contains(shard_id) || !live_shard_ids.contains(shard_id) {
                continue;
            }
            lost_a_live_shard = true;
            match indexer_per_shard_after.get(shard_id) {
                Some(indexer_after) if *indexer_after == indexer => {}
                _ => lost_a_live_shard_to_another_indexer = true,
            }
        }
        if lost_a_live_shard_to_another_indexer {
            num_resets_from_moves += 1;
        } else if lost_a_live_shard {
            num_resets_from_repacking += 1;
        }
    }
    (num_resets_from_moves, num_resets_from_repacking)
}

fn pod_ord_per_node_id(world: &DeployWorld) -> FnvHashMap<NodeId, usize> {
    (0..NUM_PODS)
        .map(|pod_ord| (world.node_id(pod_ord).clone(), pod_ord))
        .collect()
}

fn count_misplaced_shards(
    plan: &PhysicalIndexingPlan,
    host_ord_per_shard: &FnvHashMap<ShardId, usize>,
    pod_ord_per_node_id: &FnvHashMap<NodeId, usize>,
) -> usize {
    let mut num_misplaced = 0;
    for (shard_id, indexer) in indexer_per_shard(plan) {
        let host_ord = host_ord_per_shard[shard_id];
        if pod_ord_per_node_id[indexer] != host_ord {
            num_misplaced += 1;
        }
    }
    num_misplaced
}

fn num_shards_on_pod(plan: &PhysicalIndexingPlan, node_id: &NodeId) -> usize {
    plan.indexer(node_id)
        .unwrap_or(&[])
        .iter()
        .map(|task| task.shard_ids.len())
        .sum()
}

/// A shard lives on exactly one ingester for its whole life: a rebalance closes it and opens a
/// different one rather than re-hosting it. Every zero-churn claim rests on this.
fn assert_hosts_never_change(world: &DeployWorld, host_ord_seen: &mut FnvHashMap<ShardId, usize>) {
    for shard in &world.shards {
        let previous_host_ord = host_ord_seen.insert(shard.shard_id.clone(), shard.host_ord);
        if let Some(previous_host_ord) = previous_host_ord {
            assert_eq!(previous_host_ord, shard.host_ord, "a shard changed host");
        }
    }
}

fn assert_every_shard_indexed_once(world: &DeployWorld, plan: &PhysicalIndexingPlan) {
    let num_shards_in_plan: usize = plan
        .indexing_tasks_per_indexer()
        .values()
        .flatten()
        .map(|task| task.shard_ids.len())
        .sum();
    assert_eq!(num_shards_in_plan, world.shards.len());
    assert_eq!(indexer_per_shard(plan).len(), world.shards.len());
}

/// Perfect locality is feasible exactly when no pod's hosted load exceeds its capacity. Capacity is
/// inflated until the fleet total is `HEADROOM_RATIO` times the total load, so with uniform load
/// per shard and uniform pod capacity the condition reduces to a bound on hosting imbalance.
fn locality_is_feasible(world: &DeployWorld) -> bool {
    let hosted_shard_counts = world.hosted_shard_counts();
    let mut num_pods_in_plan = 0;
    let mut max_hosted_shards = 0;
    for pod_ord in 0..NUM_PODS {
        if world.pod_states[pod_ord] == PodState::Absent {
            assert_eq!(
                hosted_shard_counts[pod_ord], 0,
                "a pod left the cluster still hosting shards, which no plan can index locally"
            );
            continue;
        }
        num_pods_in_plan += 1;
        max_hosted_shards = max_hosted_shards.max(hosted_shard_counts[pod_ord]);
    }
    let mean_hosted_shards = world.shards.len() as f32 / num_pods_in_plan as f32;
    max_hosted_shards as f32 <= HEADROOM_RATIO * mean_hosted_shards
}

/// Replays a StatefulSet rolling update: every pod replaced in turn, highest ordinal first, one at
/// a time, each taking 60s of wall clock which the control plane's 5s loop turns into 12 rebuilds.
/// Ingest keeps the shard population flat, hosted through the same rebalancing rules the ingest
/// controller uses.
///
/// Three plans are built from the same events. The first is what we ship. The second differs only
/// in that the solver starts from an empty solution instead of the previous plan, while the
/// physical conversion still receives the previous plan so pipeline identity survives. The third is
/// the pre-locality behaviour, where draining pods are left out of the plan entirely.
#[test]
fn test_rolling_deployment_churn_and_drain_invariants() {
    let mut rng = StdRng::seed_from_u64(SEED);
    let mut world = DeployWorld::new(&mut rng);
    let pod_ord_per_node_id = pod_ord_per_node_id(&world);
    assert!(
        world.shards.len() / NUM_PODS >= MIN_SHARDS_PER_POD_FOR_FEASIBILITY,
        "below this density the rebalancer's tolerance is a wider imbalance than the capacity \
         headroom, and locality stops being feasible at all"
    );
    let mut host_ord_seen: FnvHashMap<ShardId, usize> = FnvHashMap::default();
    let mut previous_plans: [Option<PhysicalIndexingPlan>; 3] = [None, None, None];
    let mut tallies: [ChurnTally; 3] = [
        ChurnTally::default(),
        ChurnTally::default(),
        ChurnTally::default(),
    ];
    let mut num_resets_from_moves = [0usize; 3];
    let mut num_resets_from_repacking = [0usize; 3];
    let mut final_locality_percent = [0u32; 3];
    let mut final_misplaced = [0usize; 3];
    let mut num_feasible_rounds = 0;

    for pod_index in 0..NUM_DEPLOYMENTS * NUM_PODS {
        let pod_ord = NUM_PODS - 1 - (pod_index % NUM_PODS);
        for round in 0..REBUILDS_PER_POD {
            let draining = round < DRAIN_ROUNDS;
            world.pod_states[pod_ord] = if draining {
                PodState::Draining
            } else {
                PodState::Absent
            };
            world.rebalance(&mut rng);
            world.age_closed_shards();
            assert_hosts_never_change(&world, &mut host_ord_seen);

            let live_shard_ids = world.live_shard_ids();
            let host_ord_per_shard = world.host_ord_per_shard();
            let feasible = locality_is_feasible(&world);
            if feasible {
                num_feasible_rounds += 1;
            }
            let sources = world.sources();
            let shard_locations = world.shard_locations();
            let last_round =
                pod_index == NUM_DEPLOYMENTS * NUM_PODS - 1 && round == REBUILDS_PER_POD - 1;

            for path_ord in 0..3 {
                let locality_aware = path_ord != 2;
                let indexer_infos = world.indexer_infos(locality_aware);
                let plan = if locality_aware {
                    build_physical_indexing_plan_with_seed_choice(
                        &sources,
                        &indexer_infos,
                        previous_plans[path_ord].as_ref(),
                        &shard_locations,
                        path_ord == 0,
                    )
                } else {
                    build_physical_indexing_plan(
                        &sources,
                        &indexer_infos,
                        false,
                        previous_plans[path_ord].as_ref(),
                        &shard_locations,
                    )
                };
                assert_every_shard_indexed_once(&world, &plan);
                let num_misplaced =
                    count_misplaced_shards(&plan, &host_ord_per_shard, &pod_ord_per_node_id);

                if locality_aware {
                    let num_shards_on_draining_pod =
                        num_shards_on_pod(&plan, world.node_id(pod_ord));
                    if draining {
                        for task in plan.indexer(world.node_id(pod_ord)).unwrap_or(&[]) {
                            for shard_id in &task.shard_ids {
                                assert_eq!(
                                    host_ord_per_shard[shard_id], pod_ord,
                                    "a draining pod was assigned a shard it does not host"
                                );
                            }
                        }
                        if round == DRAIN_ROUNDS - 1 {
                            assert_eq!(
                                num_shards_on_draining_pod, 0,
                                "a pod still held work at the moment it left the cluster"
                            );
                        }
                    } else {
                        assert_eq!(num_shards_on_draining_pod, 0);
                    }
                }
                if path_ord == 1 && feasible {
                    assert_eq!(
                        num_misplaced, 0,
                        "locality was feasible for this problem but the plan built from an empty \
                         solution still misplaced shards"
                    );
                }
                if let Some(previous_plan) = &previous_plans[path_ord] {
                    let (num_moved_shards, num_surviving_shards) =
                        count_shards_that_moved(previous_plan, &plan);
                    tallies[path_ord].record(num_moved_shards, num_surviving_shards);
                    let (resets_from_moves, resets_from_repacking) =
                        count_pipeline_resets(previous_plan, &plan, &live_shard_ids);
                    num_resets_from_moves[path_ord] += resets_from_moves;
                    num_resets_from_repacking[path_ord] += resets_from_repacking;
                }
                if last_round {
                    let locality =
                        get_shard_locality_metrics(&plan, &shard_locations, &indexer_infos);
                    final_locality_percent[path_ord] = locality.locality_percent();
                    final_misplaced[path_ord] = num_misplaced;
                }
                previous_plans[path_ord] = Some(plan);
            }
        }
        world.pod_states[pod_ord] = PodState::Ready;
    }

    let num_rebuilds = NUM_DEPLOYMENTS * NUM_PODS * REBUILDS_PER_POD;
    println!(
        "{NUM_DEPLOYMENTS} deployments of {NUM_PODS} pods x {REBUILDS_PER_POD} rebuilds, {} \
         shards, locality feasible in {}/{} rounds",
        world.shards.len(),
        num_feasible_rounds,
        num_rebuilds
    );
    for (path_ord, label) in ["incremental", "empty seed", "legacy"]
        .into_iter()
        .enumerate()
    {
        println!(
            "  {label:12} moved {} ({:.3}%), worst rebuild {:.1}%, resets {} from moves + {} from \
             repacking, final misplaced {} of {} ({}% local or nearby)",
            tallies[path_ord].num_moved_shards,
            tallies[path_ord].moved_percent(),
            tallies[path_ord].max_moved_percent,
            num_resets_from_moves[path_ord],
            num_resets_from_repacking[path_ord],
            final_misplaced[path_ord],
            world.shards.len(),
            final_locality_percent[path_ord],
        );
    }

    assert_eq!(
        tallies[1].num_moved_shards, 0,
        "a plan anchored to shard locations should never move a surviving shard, because a shard \
         never changes host"
    );
    assert_eq!(
        num_resets_from_moves[1], 0,
        "no shard changed indexer, so no pipeline should have lost a live shard to another one"
    );
    assert!(
        tallies[0].moved_percent() <= tallies[2].moved_percent(),
        "the current path moved {:.3}% of shards against the legacy path's {:.3}%",
        tallies[0].moved_percent(),
        tallies[2].moved_percent()
    );
}
