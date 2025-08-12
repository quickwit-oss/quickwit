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

use std::collections::BTreeMap;
use std::collections::btree_map::Entry;
use std::num::NonZeroU32;

use quickwit_proto::indexing::CpuCapacity;

pub type SourceOrd = u32;
pub type IndexerOrd = usize;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Source {
    pub source_ord: SourceOrd,
    pub load_per_shard: NonZeroU32,
    /// Affinities of the source for each indexer.
    /// In the beginning, affinities are initialized to be the count of shards of the source
    /// that are located on the indexer.
    ///
    /// As we compute unassigned sources, we decrease the affinity by the given number of shards,
    /// saturating at 0.
    ///
    /// As a result we only have the invariant
    /// and `affinity(source, indexer) <= num shard of source on indexer`
    pub affinities: BTreeMap<IndexerOrd, u32>,
    pub num_shards: u32,
}

impl Source {
    // Remove a given number of shards, located on the given indexer.
    // Returns `false` if and only if all of the shards have been removed.
    //
    // This function also decrease the affinity of the source for the given indexer
    // by num_shards_to_remove in a saturating way.
    //
    // # Panics
    //
    // If the source does have that many total number of shards to begin with.
    pub fn remove_shards(&mut self, indexer_ord: usize, num_shards_to_remove: u32) -> bool {
        if num_shards_to_remove == 0u32 {
            return self.num_shards > 0u32;
        }
        let entry = self.affinities.entry(indexer_ord);
        self.num_shards = self
            .num_shards
            .checked_sub(num_shards_to_remove)
            .expect("removing more shards than available.");
        if self.num_shards == 0u32 {
            self.affinities.clear();
            return false;
        }
        if let Entry::Occupied(mut affinity_with_indexer_entry) = entry {
            let affinity_with_indexer: &mut u32 = affinity_with_indexer_entry.get_mut();
            let affinity_after_removal = affinity_with_indexer.saturating_sub(num_shards_to_remove);
            if affinity_after_removal == 0u32 {
                affinity_with_indexer_entry.remove();
            } else {
                *affinity_with_indexer = affinity_after_removal;
            }
        }
        true
    }
}

#[derive(Debug, Clone)]
pub struct SchedulingProblem {
    sources: Vec<Source>,
    indexer_cpu_capacities: Vec<CpuCapacity>,
}

impl SchedulingProblem {
    /// Problem constructor.
    ///
    /// Panics if the list of indexers is empty or if one of the
    /// indexer has a null capacity.
    pub fn with_indexer_cpu_capacities(
        indexer_cpu_capacities: Vec<CpuCapacity>,
    ) -> SchedulingProblem {
        assert!(!indexer_cpu_capacities.is_empty());
        assert!(
            indexer_cpu_capacities
                .iter()
                .all(|cpu_capacity| cpu_capacity.cpu_millis() > 0)
        );
        // TODO assert for affinity.
        SchedulingProblem {
            sources: Vec::new(),
            indexer_cpu_capacities,
        }
    }

    pub fn new_solution(&self) -> SchedulingSolution {
        SchedulingSolution::with_num_indexers(self.indexer_cpu_capacities.len())
    }

    pub fn indexer_cpu_capacity(&self, indexer_ord: IndexerOrd) -> CpuCapacity {
        self.indexer_cpu_capacities[indexer_ord]
    }

    /// Scales the cpu capacity by the given scaling factor.
    ///
    /// Resulting cpu capacity are ceiled to the next integer millicpus value.
    pub fn scale_node_capacities(&mut self, scale: f32) {
        for capacity in &mut self.indexer_cpu_capacities {
            let scaled_cpu_millis = (capacity.cpu_millis() as f32 * scale).ceil() as u32;
            *capacity = CpuCapacity::from_cpu_millis(scaled_cpu_millis);
        }
    }

    pub fn total_node_capacities(&self) -> CpuCapacity {
        self.indexer_cpu_capacities
            .iter()
            .copied()
            .fold(CpuCapacity::zero(), |left, right| left + right)
    }

    pub fn total_load(&self) -> u32 {
        self.sources
            .iter()
            .map(|source| source.num_shards * source.load_per_shard.get())
            .sum()
    }

    pub fn sources(&self) -> impl Iterator<Item = Source> + '_ {
        self.sources.iter().cloned()
    }

    pub fn add_source(&mut self, num_shards: u32, load_per_shard: NonZeroU32) -> SourceOrd {
        let source_ord = self.sources.len() as SourceOrd;
        self.sources.push(Source {
            source_ord,
            num_shards,
            load_per_shard,
            affinities: Default::default(),
        });
        source_ord
    }

    /// Increases the affinity source <-> indexer by 1.
    /// This is done to record that the indexer is hosting one shard of the source.
    pub fn inc_affinity(&mut self, source_ord: SourceOrd, indexer_ord: IndexerOrd) {
        let affinity: &mut u32 = self.sources[source_ord as usize]
            .affinities
            .entry(indexer_ord)
            .or_default();
        *affinity += 1;
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

#[derive(Clone, Debug, Eq, PartialEq)]
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

    /// Returns the number of available `mcpu` in the indexer.
    /// If the indexer is over-assigned this method returns a negative value.
    pub fn indexer_available_capacity(&self, problem: &SchedulingProblem) -> i32 {
        let total_cpu_load = self.total_cpu_load(problem);
        let indexer_cpu_capacity = problem.indexer_cpu_capacities[self.indexer_ord];
        indexer_cpu_capacity.cpu_millis() as i32 - total_cpu_load as i32
    }

    pub fn total_cpu_load(&self, problem: &SchedulingProblem) -> u32 {
        self.num_shards_per_source
            .iter()
            .map(|(source_ord, num_shards)| {
                problem.source_load_per_shard(*source_ord).get() * num_shards
            })
            .sum()
    }

    pub fn num_shards(&self, source_ord: SourceOrd) -> u32 {
        self.num_shards_per_source
            .get(&source_ord)
            .copied()
            .unwrap_or(0u32)
    }

    /// Add shards to a source (noop of `num_shards` is 0).
    pub fn add_shards(&mut self, source_ord: u32, num_shards: u32) {
        // No need to fill indexer_assignments with empty assignments.
        if num_shards == 0 {
            return;
        }
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
    // used for tests
    pub capacity_scaling_iterations: usize,
}

impl SchedulingSolution {
    pub fn with_num_indexers(num_indexers: usize) -> SchedulingSolution {
        SchedulingSolution {
            indexer_assignments: (0..num_indexers).map(IndexerAssignment::new).collect(),
            capacity_scaling_iterations: 0,
        }
    }
    pub fn num_indexers(&self) -> usize {
        self.indexer_assignments.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_source() -> Source {
        let mut affinities: BTreeMap<usize, u32> = Default::default();
        affinities.insert(7, 3u32);
        affinities.insert(11, 2u32);
        Source {
            source_ord: 0u32,
            load_per_shard: NonZeroU32::new(1000u32).unwrap(),
            affinities,
            num_shards: 2 + 3,
        }
    }

    #[test]
    fn test_source_remove_simple() {
        let mut source = test_source();
        assert!(source.remove_shards(7, 2));
        assert_eq!(source.num_shards, 5 - 2);
        assert_eq!(source.affinities.get(&7).copied(), Some(1));
        assert_eq!(source.affinities.get(&11).copied(), Some(2));
    }

    #[test]
    fn test_source_remove_all_affinity() {
        let mut source = test_source();
        assert!(source.remove_shards(7, 3));
        assert_eq!(source.num_shards, 5 - 3);
        assert!(!source.affinities.contains_key(&7));
        assert_eq!(source.affinities.get(&11).copied(), Some(2));
    }

    #[test]
    fn test_source_remove_more_than_affinity() {
        let mut source = test_source();
        assert!(source.remove_shards(7, 4));
        assert_eq!(source.num_shards, 5 - 4);
        assert!(!source.affinities.contains_key(&7));
        assert_eq!(source.affinities.get(&11).copied(), Some(2));
    }

    #[test]
    fn test_source_remove_all_shards() {
        let mut source = test_source();
        assert!(!source.remove_shards(7, 5));
        assert_eq!(source.num_shards, 0);
        assert!(source.affinities.is_empty());
    }

    #[test]
    #[should_panic]
    fn test_source_remove_more_than_all_shards() {
        let mut source = test_source();
        assert!(source.remove_shards(7, 6));
    }
}
