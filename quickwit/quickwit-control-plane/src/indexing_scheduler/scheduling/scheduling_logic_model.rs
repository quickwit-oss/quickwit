// Copyright (C) 2023 Quickwit, Inc.
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

pub type SourceOrd = u32;
pub type IndexerOrd = usize;
pub type Load = u32;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Source {
    pub source_id: SourceOrd,
    pub load_per_shard: NonZeroU32,
    pub num_shards: u32,
}

#[derive(Debug)]
pub struct SchedulingProblem {
    sources: Vec<Source>,
    node_max_loads: Vec<Load>,
}

impl SchedulingProblem {
    pub fn new_solution(&self) -> SchedulingSolution {
        SchedulingSolution::with_num_nodes(self.node_max_loads.len())
    }

    pub fn node_max_load(&self, node_id: IndexerOrd) -> Load {
        self.node_max_loads[node_id]
    }

    pub fn with_node_maximum_load(node_max_loads: Vec<Load>) -> SchedulingProblem {
        SchedulingProblem {
            sources: Vec::new(),
            node_max_loads,
        }
    }

    pub fn sources(&self) -> impl Iterator<Item = Source> + '_ {
        self.sources.iter().copied()
    }

    pub fn source(&self, source_id: SourceOrd) -> Source {
        self.sources[source_id as usize]
    }

    pub fn add_source(&mut self, num_shards: u32, load_per_shard: NonZeroU32) -> SourceOrd {
        let source_id = self.sources.len() as SourceOrd;
        self.sources.push(Source {
            source_id,
            num_shards,
            load_per_shard,
        });
        source_id
    }

    pub fn source_load_per_shard(&self, source_id: SourceOrd) -> NonZeroU32 {
        self.sources[source_id as usize].load_per_shard
    }

    pub fn num_sources(&self) -> usize {
        self.sources.len()
    }

    pub fn num_nodes(&self) -> usize {
        self.node_max_loads.len()
    }
}

#[derive(Clone, Debug)]
pub struct NodeAssignment {
    pub node_id: IndexerOrd,
    pub num_shards_per_source: BTreeMap<SourceOrd, u32>,
}

impl NodeAssignment {
    pub fn new(node_id: IndexerOrd) -> NodeAssignment {
        NodeAssignment {
            node_id,
            num_shards_per_source: Default::default(),
        }
    }

    pub fn node_available_capacity(&self, problem: &SchedulingProblem) -> Load {
        problem.node_max_loads[self.node_id].saturating_sub(self.total_load(problem))
    }

    pub fn total_load(&self, problem: &SchedulingProblem) -> Load {
        self.num_shards_per_source
            .iter()
            .map(|(source_id, num_shards)| {
                problem.source_load_per_shard(*source_id).get() * num_shards
            })
            .sum()
    }

    pub fn num_shards(&self, source_id: SourceOrd) -> u32 {
        self.num_shards_per_source
            .get(&source_id)
            .copied()
            .unwrap_or(0u32)
    }

    pub fn add_shard(&mut self, source_id: u32, num_shards: u32) {
        *self.num_shards_per_source.entry(source_id).or_default() += num_shards;
    }

    pub fn remove_shards(&mut self, source_id: u32, num_shards_removed: u32) {
        let entry = self.num_shards_per_source.entry(source_id);
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
    pub node_assignments: Vec<NodeAssignment>,
}

impl SchedulingSolution {
    pub fn with_num_nodes(num_nodes: usize) -> SchedulingSolution {
        SchedulingSolution {
            node_assignments: (0..num_nodes).map(NodeAssignment::new).collect(),
        }
    }

    pub fn num_nodes(&self) -> usize {
        self.node_assignments.len()
    }

    pub fn node_shards(
        &self,
        source_ord: SourceOrd,
    ) -> impl Iterator<Item = (IndexerOrd, NonZeroU32)> + '_ {
        self.node_assignments
            .iter()
            .filter_map(move |node_assignment| {
                let num_shards: NonZeroU32 = node_assignment
                    .num_shards_per_source
                    .get(&source_ord)
                    .copied()
                    .and_then(NonZeroU32::new)?;
                Some((node_assignment.node_id, num_shards))
            })
    }
}
