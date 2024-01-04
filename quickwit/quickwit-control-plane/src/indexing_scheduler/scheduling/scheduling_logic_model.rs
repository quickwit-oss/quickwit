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

use std::collections::btree_map::Entry;
use std::collections::BTreeMap;
use std::num::NonZeroU32;

use quickwit_proto::indexing::CpuCapacity;

pub type SourceOrd = u32;
pub type IndexerOrd = usize;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Source {
    pub source_ord: SourceOrd,
    pub load_per_shard: NonZeroU32,
    pub num_shards: u32,
}

#[derive(Debug)]
pub struct SchedulingProblem {
    sources: Vec<Source>,
    indexer_cpu_capacities: Vec<CpuCapacity>,
}

impl SchedulingProblem {
    pub fn new_solution(&self) -> SchedulingSolution {
        SchedulingSolution::with_num_indexers(self.indexer_cpu_capacities.len())
    }

    pub fn indexer_cpu_capacity(&self, indexer_ord: IndexerOrd) -> CpuCapacity {
        self.indexer_cpu_capacities[indexer_ord]
    }

    pub fn with_indexer_cpu_capacities(
        indexer_cpu_capacities: Vec<CpuCapacity>,
    ) -> SchedulingProblem {
        SchedulingProblem {
            sources: Vec::new(),
            indexer_cpu_capacities,
        }
    }

    pub fn sources(&self) -> impl Iterator<Item = Source> + '_ {
        self.sources.iter().copied()
    }

    pub fn source(&self, source_ord: SourceOrd) -> Source {
        self.sources[source_ord as usize]
    }

    pub fn add_source(&mut self, num_shards: u32, load_per_shard: NonZeroU32) -> SourceOrd {
        let source_ord = self.sources.len() as SourceOrd;
        self.sources.push(Source {
            source_ord,
            num_shards,
            load_per_shard,
        });
        source_ord
    }

    pub fn source_load_per_shard(&self, source_ord: SourceOrd) -> NonZeroU32 {
        self.sources[source_ord as usize].load_per_shard
    }

    pub fn num_sources(&self) -> usize {
        self.sources.len()
    }

    pub fn num_indexers(&self) -> usize {
        self.indexer_cpu_capacities.len()
    }
}

#[derive(Clone, Debug)]
pub struct IndexerAssignment {
    pub indexer_ord: IndexerOrd,
    pub num_shards_per_source: BTreeMap<SourceOrd, u32>,
}

impl IndexerAssignment {
    pub fn new(indexer_ord: IndexerOrd) -> IndexerAssignment {
        IndexerAssignment {
            indexer_ord,
            num_shards_per_source: Default::default(),
        }
    }

    pub fn indexer_available_capacity(&self, problem: &SchedulingProblem) -> CpuCapacity {
        let total_cpu_capacity = self.total_cpu_load(problem);
        let indexer_cpu_capacity = problem.indexer_cpu_capacities[self.indexer_ord];
        if indexer_cpu_capacity <= total_cpu_capacity {
            CpuCapacity::zero()
        } else {
            indexer_cpu_capacity - total_cpu_capacity
        }
    }

    pub fn total_cpu_load(&self, problem: &SchedulingProblem) -> CpuCapacity {
        CpuCapacity::from_cpu_millis(
            self.num_shards_per_source
                .iter()
                .map(|(source_ord, num_shards)| {
                    problem.source_load_per_shard(*source_ord).get() * num_shards
                })
                .sum(),
        )
    }

    pub fn num_shards(&self, source_ord: SourceOrd) -> u32 {
        self.num_shards_per_source
            .get(&source_ord)
            .copied()
            .unwrap_or(0u32)
    }

    pub fn add_shards(&mut self, source_ord: u32, num_shards: u32) {
        *self.num_shards_per_source.entry(source_ord).or_default() += num_shards;
    }

    pub fn remove_shards(&mut self, source_ord: u32, num_shards_removed: u32) {
        let entry = self.num_shards_per_source.entry(source_ord);
        let Entry::Occupied(mut occupied_entry) = entry else {
            assert_eq!(num_shards_removed, 0);
            return;
        };
        let previous_shard_count = *occupied_entry.get();
        assert!(previous_shard_count >= num_shards_removed);
        if previous_shard_count > num_shards_removed {
            *occupied_entry.get_mut() -= num_shards_removed
        } else {
            occupied_entry.remove();
        }
    }
}

#[derive(Clone, Debug)]
pub struct SchedulingSolution {
    pub indexer_assignments: Vec<IndexerAssignment>,
}

impl SchedulingSolution {
    pub fn with_num_indexers(num_indexers: usize) -> SchedulingSolution {
        SchedulingSolution {
            indexer_assignments: (0..num_indexers).map(IndexerAssignment::new).collect(),
        }
    }
    pub fn num_indexers(&self) -> usize {
        self.indexer_assignments.len()
    }
}
