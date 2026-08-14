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
use quickwit_proto::types::{IndexUid, ShardId, SourceUid};
use rand::rngs::StdRng;
use rand::{RngExt, SeedableRng};

use super::{
    Eligibility, IndexerInfo, IndexerSpec, SourceToSchedule, SourceToScheduleType,
    build_physical_indexing_plan, shard_ids_of_source, total_num_shards,
};
use crate::indexing_plan::PhysicalIndexingPlan;
use crate::model::ShardLocations;

const NUM_INDEXERS: usize = 500;
const NUM_AZS: usize = 3;
const INDEXER_CPU_CAPACITY: CpuCapacity = CpuCapacity::from_cpu_millis(4_000);
const SEED: u64 = 20_260_814;
const LOADS_PER_SHARD: [u32; 8] = [50, 250, 500, 1_000, 1_200, 1_600, 2_000, 3_200];

const CHURN_NUM_SOURCES: usize = 100;
const CHURN_ITERATIONS: usize = 200;
const CHURN_PHASE_LENGTH: usize = 15;
const CHURN_MAX_DRAINING_INDEXERS: usize = NUM_INDEXERS / 10;
const CHURN_SCALE_UP_FACTOR: f32 = 1.5;
const CHURN_MAX_SHARDS: usize = 4_000;

struct ChurnWorld {
    indexer_specs: Vec<IndexerSpec>,
    sources: Vec<SourceToSchedule>,
    host_ord_per_shard: FnvHashMap<ShardId, usize>,
    draining_indexer_ords: FnvHashSet<usize>,
    next_shard_id: u64,
}

impl ChurnWorld {
    fn new(rng: &mut StdRng) -> ChurnWorld {
        let indexer_specs = (0..NUM_INDEXERS)
            .map(|indexer_ord| {
                let availability_zone = format!("az-{}", indexer_ord % NUM_AZS);
                let node_id = format!("indexer-{indexer_ord}");
                IndexerSpec::new(&node_id, INDEXER_CPU_CAPACITY, Some(&availability_zone))
            })
            .collect();
        let index_uid = IndexUid::for_test("churn-index", 0);
        let mut world = ChurnWorld {
            indexer_specs,
            sources: Vec::new(),
            host_ord_per_shard: FnvHashMap::default(),
            draining_indexer_ords: FnvHashSet::default(),
            next_shard_id: 0,
        };
        for source_ord in 0..CHURN_NUM_SOURCES {
            let load_per_shard = LOADS_PER_SHARD[rng.random_range(0..LOADS_PER_SHARD.len())];
            let source_uid = SourceUid {
                index_uid: index_uid.clone(),
                source_id: format!("source-{source_ord}"),
            };
            world.sources.push(SourceToSchedule {
                source_uid,
                source_type: SourceToScheduleType::Sharded {
                    shard_ids: Vec::new(),
                    load_per_shard: NonZeroU32::new(load_per_shard).unwrap(),
                },
                params_fingerprint: 0,
            });
            world.open_shards(source_ord, 1 + rng.random_range(0..8), rng);
        }
        world
    }

    fn open_shards(&mut self, source_ord: usize, num_shards_to_open: usize, rng: &mut StdRng) {
        let mut host_ords = Vec::with_capacity(num_shards_to_open);
        for _ in 0..num_shards_to_open {
            host_ords.push(self.pick_routable_indexer_ord(rng));
        }
        let SourceToScheduleType::Sharded { shard_ids, .. } =
            &mut self.sources[source_ord].source_type
        else {
            return;
        };
        for host_ord in host_ords {
            let shard_id = ShardId::from(self.next_shard_id);
            self.next_shard_id += 1;
            shard_ids.push(shard_id.clone());
            self.host_ord_per_shard.insert(shard_id, host_ord);
        }
    }

    fn pick_routable_indexer_ord(&self, rng: &mut StdRng) -> usize {
        loop {
            let indexer_ord = rng.random_range(0..self.indexer_specs.len());
            if !self.draining_indexer_ords.contains(&indexer_ord) {
                return indexer_ord;
            }
        }
    }

    fn close_shards(&mut self, source_ord: usize, num_shards_to_close: usize) {
        let SourceToScheduleType::Sharded { shard_ids, .. } =
            &mut self.sources[source_ord].source_type
        else {
            return;
        };
        for _ in 0..num_shards_to_close {
            if shard_ids.len() <= 1 {
                return;
            }
            let shard_id = shard_ids.remove(0);
            self.host_ord_per_shard.remove(&shard_id);
        }
    }

    fn close_shards_on_draining_indexers(&mut self, rng: &mut StdRng) {
        for source_ord in 0..self.sources.len() {
            let drained_shard_ids: Vec<ShardId> = shard_ids_of_source(&self.sources[source_ord])
                .iter()
                .filter(|shard_id| {
                    let host_ord = self.host_ord_per_shard[*shard_id];
                    self.draining_indexer_ords.contains(&host_ord)
                })
                .filter(|_| rng.random_range(0..100) < 40)
                .cloned()
                .collect();
            for shard_id in drained_shard_ids {
                let SourceToScheduleType::Sharded { shard_ids, .. } =
                    &mut self.sources[source_ord].source_type
                else {
                    continue;
                };
                if shard_ids.len() <= 1 {
                    break;
                }
                shard_ids.retain(|candidate| candidate != &shard_id);
                self.host_ord_per_shard.remove(&shard_id);
            }
        }
    }

    fn num_hosted_shards(&self, indexer_ord: usize) -> usize {
        self.host_ord_per_shard
            .values()
            .filter(|host_ord| **host_ord == indexer_ord)
            .count()
    }

    fn advance_drain_lifecycle(&mut self, rng: &mut StdRng) {
        let fully_drained: Vec<usize> = self
            .draining_indexer_ords
            .iter()
            .copied()
            .filter(|indexer_ord| self.num_hosted_shards(*indexer_ord) == 0)
            .collect();
        for indexer_ord in fully_drained {
            self.draining_indexer_ords.remove(&indexer_ord);
        }
        while self.draining_indexer_ords.len() < CHURN_MAX_DRAINING_INDEXERS {
            let indexer_ord = rng.random_range(0..self.indexer_specs.len());
            if !self.draining_indexer_ords.insert(indexer_ord) {
                break;
            }
        }
    }

    fn scale_shards(&mut self, growing: bool, rng: &mut StdRng) {
        let num_shards_in_cluster = total_num_shards(&self.sources);
        for source_ord in 0..self.sources.len() {
            if rng.random_range(0..100) >= 25 {
                continue;
            }
            let num_shards = shard_ids_of_source(&self.sources[source_ord]).len();
            if growing && num_shards_in_cluster < CHURN_MAX_SHARDS {
                let target_num_shards =
                    (num_shards as f32 * CHURN_SCALE_UP_FACTOR).ceil() as usize;
                self.open_shards(source_ord, target_num_shards - num_shards, rng);
            } else if !growing {
                let target_num_shards =
                    (num_shards as f32 / CHURN_SCALE_UP_FACTOR).floor().max(1.0) as usize;
                self.close_shards(source_ord, num_shards - target_num_shards);
            }
        }
    }

    fn shard_locations(&self) -> ShardLocations<'_> {
        let mut shard_locations = ShardLocations::default();
        for source in &self.sources {
            for shard_id in shard_ids_of_source(source) {
                let host_ord = self.host_ord_per_shard[shard_id];
                shard_locations.add_location(shard_id, &self.indexer_specs[host_ord].node_id);
            }
        }
        shard_locations
    }

    fn indexer_infos(&self, locality_aware: bool) -> FnvHashMap<String, IndexerInfo> {
        let mut indexer_infos = FnvHashMap::default();
        for (indexer_ord, indexer_spec) in self.indexer_specs.iter().enumerate() {
            let draining = self.draining_indexer_ords.contains(&indexer_ord);
            if !locality_aware {
                if draining {
                    continue;
                }
                let indexer_info = IndexerInfo::for_test(INDEXER_CPU_CAPACITY);
                indexer_infos.insert(indexer_spec.node_id.to_string(), indexer_info);
                continue;
            }
            let eligibility = if draining {
                Eligibility::SelfHostedOnly
            } else {
                Eligibility::Any
            };
            let indexer_info = indexer_spec.to_indexer_info(eligibility);
            indexer_infos.insert(indexer_spec.node_id.to_string(), indexer_info);
        }
        indexer_infos
    }
}

fn indexer_per_shard(plan: &PhysicalIndexingPlan) -> FnvHashMap<&ShardId, &String> {
    let mut indexer_per_shard = FnvHashMap::default();
    for (indexer, tasks) in plan.indexing_tasks_per_indexer() {
        for task in tasks {
            for shard_id in &task.shard_ids {
                indexer_per_shard.insert(shard_id, indexer);
            }
        }
    }
    indexer_per_shard
}

fn count_shards_that_moved(
    plan: &PhysicalIndexingPlan,
    replanned: &PhysicalIndexingPlan,
) -> (usize, usize) {
    let indexer_per_shard_before = indexer_per_shard(plan);
    let indexer_per_shard_after = indexer_per_shard(replanned);
    let mut num_surviving_shards = 0;
    let mut num_moved_shards = 0;
    for (shard_id, indexer_before) in &indexer_per_shard_before {
        let Some(indexer_after) = indexer_per_shard_after.get(*shard_id) else {
            continue;
        };
        num_surviving_shards += 1;
        if indexer_before != indexer_after {
            num_moved_shards += 1;
        }
    }
    (num_moved_shards, num_surviving_shards)
}

#[derive(Default)]
struct ChurnTally {
    num_moved_shards: usize,
    num_surviving_shards: usize,
    max_moved_percent: f32,
}

impl ChurnTally {
    fn record(&mut self, num_moved_shards: usize, num_surviving_shards: usize) {
        self.num_moved_shards += num_moved_shards;
        self.num_surviving_shards += num_surviving_shards;
        let moved_percent = num_moved_shards as f32 * 100.0 / num_surviving_shards.max(1) as f32;
        self.max_moved_percent = self.max_moved_percent.max(moved_percent);
    }

    fn moved_percent(&self) -> f32 {
        self.num_moved_shards as f32 * 100.0 / self.num_surviving_shards.max(1) as f32
    }
}

/// Churn across successive plans, which is what the cluster actually pays for: every shard that
/// changes indexer is a pipeline restart and a re-read over the network.
///
/// The simulation is a cluster under ingest pressure. Shards scale up by 1.5x per event on a
/// quarter of the sources and drain away again, indexers enter and leave decommissioning, and the
/// plan is rebuilt after every change from the plan before it.
///
/// The same events are replayed with locality awareness disabled, which also means draining
/// indexers are left out of planning entirely, as that path selects only ready ones.
#[test]
fn test_churn_across_successive_plans() {
    let mut rng = StdRng::seed_from_u64(SEED);
    let mut world = ChurnWorld::new(&mut rng);
    let mut previous_plans: [Option<PhysicalIndexingPlan>; 2] = [None, None];
    let mut tallies: [ChurnTally; 2] = [ChurnTally::default(), ChurnTally::default()];
    let mut min_num_shards = usize::MAX;
    let mut max_num_shards = 0;

    for iteration in 0..CHURN_ITERATIONS {
        let growing = (iteration / CHURN_PHASE_LENGTH) % 2 == 0;
        world.scale_shards(growing, &mut rng);
        world.advance_drain_lifecycle(&mut rng);
        world.close_shards_on_draining_indexers(&mut rng);

        let shard_locations = world.shard_locations();
        let num_shards = total_num_shards(&world.sources);
        min_num_shards = min_num_shards.min(num_shards);
        max_num_shards = max_num_shards.max(num_shards);

        for (path_ord, locality_aware) in [true, false].into_iter().enumerate() {
            let indexer_infos = world.indexer_infos(locality_aware);
            let plan = build_physical_indexing_plan(
                &world.sources,
                &indexer_infos,
                locality_aware,
                previous_plans[path_ord].as_ref(),
                &shard_locations,
            );
            if let Some(previous_plan) = &previous_plans[path_ord] {
                let (num_moved_shards, num_surviving_shards) =
                    count_shards_that_moved(previous_plan, &plan);
                tallies[path_ord].record(num_moved_shards, num_surviving_shards);
            }
            previous_plans[path_ord] = Some(plan);
        }
    }

    println!(
        "{CHURN_ITERATIONS} rebuilds, shards {min_num_shards}..{max_num_shards}\n  new    moved \
         {} of {} ({:.2}%), worst rebuild {:.1}%\n  legacy moved {} of {} ({:.2}%), worst rebuild \
         {:.1}%",
        tallies[0].num_moved_shards,
        tallies[0].num_surviving_shards,
        tallies[0].moved_percent(),
        tallies[0].max_moved_percent,
        tallies[1].num_moved_shards,
        tallies[1].num_surviving_shards,
        tallies[1].moved_percent(),
        tallies[1].max_moved_percent,
    );
    assert!(
        tallies[0].moved_percent() <= tallies[1].moved_percent() * 1.2,
        "moved {:.2}% of shards against legacy's {:.2}% over the same events",
        tallies[0].moved_percent(),
        tallies[1].moved_percent()
    );
}
