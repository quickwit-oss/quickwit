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

use std::collections::HashSet;
use std::num::NonZeroU32;

use fnv::{FnvHashMap, FnvHashSet};
use quickwit_proto::indexing::CpuCapacity;
use quickwit_proto::types::{IndexUid, PipelineUid, ShardId, SourceUid};
use rand::rngs::StdRng;
use rand::{RngExt, SeedableRng};

use super::{
    Eligibility, IndexerInfo, IndexerSpec, SourceToSchedule, SourceToScheduleType,
    build_physical_indexing_plan, compute_max_num_shards_per_pipeline, shard_ids_for_indexer,
    shard_ids_of_source, total_num_shards,
};
use crate::indexing_plan::PhysicalIndexingPlan;
use crate::indexing_scheduler::get_shard_locality_metrics;
use crate::model::ShardLocations;

const NUM_INDEXERS: usize = 500;
const NUM_SOURCES: usize = 1_000;
const NUM_AZS: usize = 3;
const NUM_DRAINING_INDEXERS: usize = 250;
const MAX_SHARDS_PER_SOURCE: usize = 4_000;
const MAX_TOTAL_SHARDS: usize = 20_000;
const INDEXER_CPU_CAPACITY: CpuCapacity = CpuCapacity::from_cpu_millis(4_000);
const SEED: u64 = 20_260_814;
const LOADS_PER_SHARD: [u32; 8] = [50, 250, 500, 1_000, 1_200, 1_600, 2_000, 3_200];

struct SourceSizeTier {
    num_sources: usize,
    max_shards_per_source: usize,
}

const SOURCE_SIZE_TIERS: [SourceSizeTier; 5] = [
    SourceSizeTier {
        num_sources: 2,
        max_shards_per_source: MAX_SHARDS_PER_SOURCE,
    },
    SourceSizeTier {
        num_sources: 10,
        max_shards_per_source: 500,
    },
    SourceSizeTier {
        num_sources: 50,
        max_shards_per_source: 60,
    },
    SourceSizeTier {
        num_sources: 200,
        max_shards_per_source: 12,
    },
    SourceSizeTier {
        num_sources: 738,
        max_shards_per_source: 1,
    },
];

/// Scheduling at scale: 500 indexers, 3 zones, 1000 sources, ~10k shards, fixed seed.
///
/// These tests attempt to assert that indexing planning is resilient to "scale" conditions,
/// where the assumptions we make for solving the "normal" problem might break down. They attempt
/// to answer the questions
/// * "Does this work well at petabyte+ scale?"
/// * "Does a large drain of indexers across multiple AZs result in a balanced plan?"
/// * "Does the removal of a whole AZ result in a balanced plan?"
///
/// Assertions are on <1% drift overall.
#[test]
fn test_scale_all_indexers_ready() {
    let mut rng = StdRng::seed_from_u64(SEED);
    let indexer_specs = build_indexer_specs();
    let sources = build_sources(&mut rng);
    let shard_locations = build_shard_locations(&sources, &indexer_specs, &mut rng);
    let no_draining_indexers = FnvHashSet::default();
    let indexer_infos = build_indexer_infos(&indexer_specs, &no_draining_indexers);
    let num_shards = total_num_shards(&sources);

    let locality_aware = true;
    let plan = build_physical_indexing_plan(
        &sources,
        &indexer_infos,
        locality_aware,
        None,
        &shard_locations,
    );

    assert_every_shard_scheduled_once(&plan, &sources);
    assert_pipelines_within_limits(&plan, &sources);
    assert_load_is_balanced(&plan, &sources);
    assert_eq!(num_idle_indexers(&plan, &indexer_specs), 0);

    let metrics = get_shard_locality_metrics(&plan, &shard_locations, &indexer_infos);
    println!(
        "{num_shards} shards: {} local, {} nearby, {} remote",
        metrics.num_local_shards, metrics.num_nearby_shards, metrics.num_remote_shards
    );
    assert_eq!(
        metrics.num_local_shards + metrics.num_nearby_shards + metrics.num_remote_shards,
        num_shards
    );
    assert!(
        metrics.num_nearby_shards > 0,
        "no shard overflowed to a same-az peer, so pass two never engaged"
    );
    assert_eq!(
        metrics.num_remote_shards, 0,
        "{} shards crossed an availability zone despite every zone having room",
        metrics.num_remote_shards
    );

    let replanned = build_physical_indexing_plan(
        &sources,
        &indexer_infos,
        locality_aware,
        Some(&plan),
        &shard_locations,
    );
    assert_eq!(plan, replanned);
}

#[test]
fn test_scale_drain_spread_across_azs() {
    let mut rng = StdRng::seed_from_u64(SEED);
    let indexer_specs = build_indexer_specs();
    let sources = build_sources(&mut rng);
    let shard_locations = build_shard_locations(&sources, &indexer_specs, &mut rng);
    let host_per_shard = build_host_per_shard(&sources, &shard_locations);
    let num_shards = total_num_shards(&sources);

    let no_draining_indexers = FnvHashSet::default();
    let ready_indexer_infos = build_indexer_infos(&indexer_specs, &no_draining_indexers);
    let locality_aware = true;
    let baseline_plan = build_physical_indexing_plan(
        &sources,
        &ready_indexer_infos,
        locality_aware,
        None,
        &shard_locations,
    );

    let draining_indexer_ords = spread_draining_indexer_ords();
    assert_eq!(draining_indexer_ords.len(), NUM_DRAINING_INDEXERS);
    let draining_indexer_infos = build_indexer_infos(&indexer_specs, &draining_indexer_ords);
    let drained_plan = build_physical_indexing_plan(
        &sources,
        &draining_indexer_infos,
        locality_aware,
        Some(&baseline_plan),
        &shard_locations,
    );

    assert_every_shard_scheduled_once(&drained_plan, &sources);
    assert_pipelines_within_limits(&drained_plan, &sources);
    assert_load_is_balanced(&drained_plan, &sources);
    assert_draining_indexers_index_only_own_shards(
        &drained_plan,
        &host_per_shard,
        &draining_indexer_infos,
    );

    let num_hosted_on_draining =
        num_shards_hosted_on_draining_indexers(&host_per_shard, &draining_indexer_infos);
    let num_indexed_by_draining =
        num_shards_indexed_by_draining_indexers(&drained_plan, &draining_indexer_infos);
    let metrics = get_shard_locality_metrics(&drained_plan, &shard_locations, &ready_indexer_infos);
    println!(
        "{num_shards} shards: draining indexers host {num_hosted_on_draining} and index \
         {num_indexed_by_draining}; {} local, {} nearby, {} remote",
        metrics.num_local_shards, metrics.num_nearby_shards, metrics.num_remote_shards
    );
    assert!(
        num_indexed_by_draining * 100 >= num_hosted_on_draining * 85,
        "draining indexers index {num_indexed_by_draining} of the {num_hosted_on_draining} shards \
         they host"
    );

    let replanned = build_physical_indexing_plan(
        &sources,
        &draining_indexer_infos,
        locality_aware,
        Some(&drained_plan),
        &shard_locations,
    );
    let num_released_shards = num_hosted_on_draining - num_indexed_by_draining;
    let num_churned_shards = num_shards_with_changed_pipeline(&drained_plan, &replanned);
    println!(
        "{num_churned_shards} of {num_shards} shards changed pipeline on replan, out of \
         {num_released_shards} released by the draining indexers"
    );
    assert!(
        num_churned_shards <= num_released_shards,
        "{num_churned_shards} shards changed pipeline but only {num_released_shards} were released"
    );
    assert_every_shard_scheduled_once(&replanned, &sources);
    assert_locality_of_hosted_shards_is_stable(
        &drained_plan,
        &replanned,
        &shard_locations,
        &ready_indexer_infos,
    );
    assert_draining_indexers_index_only_own_shards(
        &replanned,
        &host_per_shard,
        &draining_indexer_infos,
    );
}

#[test]
fn test_scale_drain_whole_az() {
    let mut rng = StdRng::seed_from_u64(SEED);
    let indexer_specs = build_indexer_specs();
    let sources = build_sources(&mut rng);
    let shard_locations = build_shard_locations(&sources, &indexer_specs, &mut rng);
    let host_per_shard = build_host_per_shard(&sources, &shard_locations);
    let num_shards = total_num_shards(&sources);

    let no_draining_indexers = FnvHashSet::default();
    let ready_indexer_infos = build_indexer_infos(&indexer_specs, &no_draining_indexers);
    let locality_aware = true;
    let baseline_plan = build_physical_indexing_plan(
        &sources,
        &ready_indexer_infos,
        locality_aware,
        None,
        &shard_locations,
    );

    let draining_indexer_ords = whole_az_draining_indexer_ords();
    assert_eq!(draining_indexer_ords.len(), NUM_DRAINING_INDEXERS);
    let draining_indexer_infos = build_indexer_infos(&indexer_specs, &draining_indexer_ords);
    let drained_plan = build_physical_indexing_plan(
        &sources,
        &draining_indexer_infos,
        locality_aware,
        Some(&baseline_plan),
        &shard_locations,
    );

    assert_every_shard_scheduled_once(&drained_plan, &sources);
    assert_pipelines_within_limits(&drained_plan, &sources);
    assert_load_is_balanced(&drained_plan, &sources);
    assert_draining_indexers_index_only_own_shards(
        &drained_plan,
        &host_per_shard,
        &draining_indexer_infos,
    );
    assert_drained_az_spills_across_zones(
        &drained_plan,
        &host_per_shard,
        &draining_indexer_infos,
        "az-0",
    );

    let num_hosted_on_draining =
        num_shards_hosted_on_draining_indexers(&host_per_shard, &draining_indexer_infos);
    let num_indexed_by_draining =
        num_shards_indexed_by_draining_indexers(&drained_plan, &draining_indexer_infos);
    let metrics = get_shard_locality_metrics(&drained_plan, &shard_locations, &ready_indexer_infos);
    println!(
        "{num_shards} shards: draining indexers host {num_hosted_on_draining} and index \
         {num_indexed_by_draining}; {} local, {} nearby, {} remote",
        metrics.num_local_shards, metrics.num_nearby_shards, metrics.num_remote_shards
    );
    assert!(
        num_indexed_by_draining * 100 >= num_hosted_on_draining * 85,
        "draining indexers index {num_indexed_by_draining} of the {num_hosted_on_draining} shards \
         they host"
    );
    assert!(
        metrics.num_remote_shards > 0,
        "the fully drained az has no eligible peer, so its overflow must cross a zone"
    );

    let replanned = build_physical_indexing_plan(
        &sources,
        &draining_indexer_infos,
        locality_aware,
        Some(&drained_plan),
        &shard_locations,
    );
    let num_released_shards = num_hosted_on_draining - num_indexed_by_draining;
    let num_churned_shards = num_shards_with_changed_pipeline(&drained_plan, &replanned);
    println!(
        "{num_churned_shards} of {num_shards} shards changed pipeline on replan, out of \
         {num_released_shards} released by the draining indexers"
    );
    assert!(
        num_churned_shards <= num_released_shards,
        "{num_churned_shards} shards changed pipeline but only {num_released_shards} were released"
    );
    assert_every_shard_scheduled_once(&replanned, &sources);
    assert_locality_of_hosted_shards_is_stable(
        &drained_plan,
        &replanned,
        &shard_locations,
        &ready_indexer_infos,
    );
    assert_draining_indexers_index_only_own_shards(
        &replanned,
        &host_per_shard,
        &draining_indexer_infos,
    );
}

fn build_indexer_specs() -> Vec<IndexerSpec> {
    (0..NUM_INDEXERS)
        .map(|indexer_ord| {
            let node_id = format!("indexer-{indexer_ord}");
            let availability_zone = format!("az-{}", indexer_ord % NUM_AZS);
            IndexerSpec::new(&node_id, INDEXER_CPU_CAPACITY, Some(&availability_zone))
        })
        .collect()
}

fn build_sources(rng: &mut StdRng) -> Vec<SourceToSchedule> {
    let index_uid = IndexUid::for_test("scale-test-index", 0);
    let mut next_shard_id = 0u64;
    let mut sources = Vec::with_capacity(NUM_SOURCES);
    for source_size_tier in &SOURCE_SIZE_TIERS {
        for _ in 0..source_size_tier.num_sources {
            let num_shards = 1 + rng.random_range(0..source_size_tier.max_shards_per_source);
            let load_per_shard = LOADS_PER_SHARD[rng.random_range(0..LOADS_PER_SHARD.len())];
            let shard_ids: Vec<ShardId> = (0..num_shards)
                .map(|_| {
                    let shard_id = ShardId::from(next_shard_id);
                    next_shard_id += 1;
                    shard_id
                })
                .collect();
            let source_uid = SourceUid {
                index_uid: index_uid.clone(),
                source_id: format!("source-{}", sources.len()),
            };
            sources.push(SourceToSchedule {
                source_uid,
                source_type: SourceToScheduleType::Sharded {
                    shard_ids,
                    load_per_shard: NonZeroU32::new(load_per_shard).unwrap(),
                },
                params_fingerprint: 0,
            });
        }
    }
    assert_eq!(sources.len(), NUM_SOURCES);
    assert!(total_num_shards(&sources) <= MAX_TOTAL_SHARDS);
    sources
}

fn build_shard_locations<'a>(
    sources: &'a [SourceToSchedule],
    indexer_specs: &'a [IndexerSpec],
    rng: &mut StdRng,
) -> ShardLocations<'a> {
    let mut shard_locations = ShardLocations::default();
    for source in sources {
        for shard_id in shard_ids_of_source(source) {
            let host_ord = rng.random_range(0..indexer_specs.len());
            shard_locations.add_location(shard_id, &indexer_specs[host_ord].node_id);
        }
    }
    shard_locations
}

fn build_indexer_infos(
    indexer_specs: &[IndexerSpec],
    draining_indexer_ords: &FnvHashSet<usize>,
) -> FnvHashMap<String, IndexerInfo> {
    let mut indexer_infos = FnvHashMap::default();
    for (indexer_ord, indexer_spec) in indexer_specs.iter().enumerate() {
        let eligibility = if draining_indexer_ords.contains(&indexer_ord) {
            Eligibility::SelfHostedOnly
        } else {
            Eligibility::Any
        };
        let indexer_info = indexer_spec.to_indexer_info(eligibility);
        indexer_infos.insert(indexer_spec.node_id.to_string(), indexer_info);
    }
    indexer_infos
}

fn spread_draining_indexer_ords() -> FnvHashSet<usize> {
    (0..NUM_INDEXERS).step_by(2).collect()
}

fn whole_az_draining_indexer_ords() -> FnvHashSet<usize> {
    let mut draining_indexer_ords: FnvHashSet<usize> = (0..NUM_INDEXERS)
        .filter(|indexer_ord| indexer_ord % NUM_AZS == 0)
        .collect();
    let num_remaining_to_drain = NUM_DRAINING_INDEXERS - draining_indexer_ords.len();
    let next_az_indexer_ords = (0..NUM_INDEXERS)
        .filter(|indexer_ord| indexer_ord % NUM_AZS == 1)
        .take(num_remaining_to_drain);
    draining_indexer_ords.extend(next_az_indexer_ords);
    draining_indexer_ords
}

fn assert_every_shard_scheduled_once(plan: &PhysicalIndexingPlan, sources: &[SourceToSchedule]) {
    let mut scheduled_shard_ids: Vec<ShardId> = Vec::new();
    for tasks in plan.indexing_tasks_per_indexer().values() {
        for task in tasks {
            scheduled_shard_ids.extend(task.shard_ids.iter().cloned());
        }
    }
    let unique_scheduled_shard_ids: HashSet<&ShardId> = scheduled_shard_ids.iter().collect();
    let expected_shard_ids: HashSet<&ShardId> =
        sources.iter().flat_map(shard_ids_of_source).collect();
    assert_eq!(
        scheduled_shard_ids.len(),
        expected_shard_ids.len(),
        "scheduled {} shard slots for {} shards",
        scheduled_shard_ids.len(),
        expected_shard_ids.len()
    );
    assert_eq!(unique_scheduled_shard_ids, expected_shard_ids);
}

fn assert_pipelines_within_limits(plan: &PhysicalIndexingPlan, sources: &[SourceToSchedule]) {
    let mut max_shards_per_source: FnvHashMap<&str, usize> = FnvHashMap::default();
    for source in sources {
        let max_num_shards = compute_max_num_shards_per_pipeline(&source.source_type);
        max_shards_per_source.insert(&source.source_uid.source_id, max_num_shards.get() as usize);
    }
    for tasks in plan.indexing_tasks_per_indexer().values() {
        for task in tasks {
            let max_num_shards = max_shards_per_source[task.source_id.as_str()];
            assert!(
                !task.shard_ids.is_empty(),
                "pipeline for {} holds no shards at all",
                task.source_id
            );
            assert!(
                task.shard_ids.len() <= max_num_shards,
                "pipeline for {} holds {} shards, limit is {max_num_shards}",
                task.source_id,
                task.shard_ids.len()
            );
        }
    }
}

fn load_per_indexer(plan: &PhysicalIndexingPlan, sources: &[SourceToSchedule]) -> Vec<u64> {
    let mut load_per_source: FnvHashMap<&str, u64> = FnvHashMap::default();
    for source in sources {
        let SourceToScheduleType::Sharded { load_per_shard, .. } = &source.source_type else {
            continue;
        };
        load_per_source.insert(&source.source_uid.source_id, load_per_shard.get() as u64);
    }
    plan.indexing_tasks_per_indexer()
        .values()
        .map(|tasks| {
            tasks
                .iter()
                .map(|task| {
                    let load_per_shard = load_per_source[task.source_id.as_str()];
                    load_per_shard * task.shard_ids.len() as u64
                })
                .sum()
        })
        .collect()
}

fn num_idle_indexers(plan: &PhysicalIndexingPlan, indexer_specs: &[IndexerSpec]) -> usize {
    indexer_specs
        .iter()
        .filter(|indexer_spec| {
            let node_id = indexer_spec.node_id.as_str();
            shard_ids_for_indexer(plan, node_id).is_empty()
        })
        .count()
}

fn assert_load_is_balanced(plan: &PhysicalIndexingPlan, sources: &[SourceToSchedule]) {
    let loads = load_per_indexer(plan, sources);
    let total_load: u64 = loads.iter().sum();
    let mean_load = total_load / loads.len() as u64;
    let max_load = *loads.iter().max().unwrap();
    assert!(
        max_load * 2 <= mean_load * 3,
        "most loaded indexer holds {max_load} mcpu against a mean of {mean_load} mcpu"
    );
}

fn build_host_per_shard<'a>(
    sources: &'a [SourceToSchedule],
    shard_locations: &ShardLocations,
) -> FnvHashMap<&'a ShardId, String> {
    let mut host_per_shard = FnvHashMap::default();
    for source in sources {
        for shard_id in shard_ids_of_source(source) {
            let Some(host) = shard_locations.get_shard_locations(shard_id).first() else {
                continue;
            };
            host_per_shard.insert(shard_id, host.to_string());
        }
    }
    host_per_shard
}

fn pipeline_per_shard(
    plan: &PhysicalIndexingPlan,
) -> FnvHashMap<&ShardId, (&String, Option<PipelineUid>)> {
    let mut pipeline_per_shard = FnvHashMap::default();
    for (indexer, tasks) in plan.indexing_tasks_per_indexer() {
        for task in tasks {
            for shard_id in &task.shard_ids {
                pipeline_per_shard.insert(shard_id, (indexer, task.pipeline_uid));
            }
        }
    }
    pipeline_per_shard
}

fn num_shards_with_changed_pipeline(
    plan: &PhysicalIndexingPlan,
    replanned: &PhysicalIndexingPlan,
) -> usize {
    let pipeline_per_shard_before = pipeline_per_shard(plan);
    let pipeline_per_shard_after = pipeline_per_shard(replanned);
    pipeline_per_shard_before
        .iter()
        .filter(|(shard_id, pipeline_before)| {
            pipeline_per_shard_after[**shard_id] != **pipeline_before
        })
        .count()
}

fn assert_locality_of_hosted_shards_is_stable(
    plan: &PhysicalIndexingPlan,
    replanned: &PhysicalIndexingPlan,
    shard_locations: &ShardLocations,
    indexer_infos: &FnvHashMap<String, IndexerInfo>,
) {
    let metrics_before = get_shard_locality_metrics(plan, shard_locations, indexer_infos);
    let metrics_after = get_shard_locality_metrics(replanned, shard_locations, indexer_infos);
    assert_eq!(
        metrics_before.num_local_shards, metrics_after.num_local_shards,
        "{} shards were indexed on their host before the replan and {} after",
        metrics_before.num_local_shards, metrics_after.num_local_shards
    );
    let num_displaced_before =
        metrics_before.num_nearby_shards + metrics_before.num_remote_shards;
    let num_displaced_after = metrics_after.num_nearby_shards + metrics_after.num_remote_shards;
    assert_eq!(num_displaced_before, num_displaced_after);
}

fn is_draining(indexer: &str, indexer_infos: &FnvHashMap<String, IndexerInfo>) -> bool {
    indexer_infos[indexer].eligibility == Eligibility::SelfHostedOnly
}

fn assert_draining_indexers_index_only_own_shards(
    plan: &PhysicalIndexingPlan,
    host_per_shard: &FnvHashMap<&ShardId, String>,
    indexer_infos: &FnvHashMap<String, IndexerInfo>,
) {
    for (indexer, tasks) in plan.indexing_tasks_per_indexer() {
        if !is_draining(indexer, indexer_infos) {
            continue;
        }
        for task in tasks {
            for shard_id in &task.shard_ids {
                let host = &host_per_shard[shard_id];
                assert_eq!(
                    host, indexer,
                    "draining indexer {indexer} indexes shard {shard_id:?} hosted on {host}"
                );
            }
        }
    }
}

fn assert_drained_az_spills_across_zones(
    plan: &PhysicalIndexingPlan,
    host_per_shard: &FnvHashMap<&ShardId, String>,
    indexer_infos: &FnvHashMap<String, IndexerInfo>,
    drained_az: &str,
) {
    for (indexer, tasks) in plan.indexing_tasks_per_indexer() {
        for task in tasks {
            for shard_id in &task.shard_ids {
                let host = &host_per_shard[shard_id];
                let host_az = indexer_infos[host.as_str()].availability_zone.as_deref();
                if host_az != Some(drained_az) || host == indexer {
                    continue;
                }
                let indexer_az = indexer_infos[indexer].availability_zone.as_deref();
                assert_ne!(
                    indexer_az,
                    Some(drained_az),
                    "shard {shard_id:?} hosted on {host} moved to {indexer}, still inside the \
                     fully drained {drained_az}"
                );
            }
        }
    }
}

fn num_shards_hosted_on_draining_indexers(
    host_per_shard: &FnvHashMap<&ShardId, String>,
    indexer_infos: &FnvHashMap<String, IndexerInfo>,
) -> usize {
    host_per_shard
        .values()
        .filter(|host| is_draining(host, indexer_infos))
        .count()
}

fn num_shards_indexed_by_draining_indexers(
    plan: &PhysicalIndexingPlan,
    indexer_infos: &FnvHashMap<String, IndexerInfo>,
) -> usize {
    plan.indexing_tasks_per_indexer()
        .iter()
        .filter(|(indexer, _)| is_draining(indexer, indexer_infos))
        .map(|(_, tasks)| tasks.iter().map(|task| task.shard_ids.len()).sum::<usize>())
        .sum()
}
