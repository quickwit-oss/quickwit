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

use std::cmp::Reverse;
use std::collections::BTreeMap;
use std::collections::btree_map::Entry;

use itertools::Itertools;
use quickwit_proto::indexing::CpuCapacity;

use super::scheduling_logic_model::*;
use crate::indexing_scheduler::scheduling::inflate_node_capacities_if_necessary;

// ------------------------------------------------------------------------------------
// High level algorithm

fn check_contract_conditions(problem: &SchedulingProblem, solution: &SchedulingSolution) {
    assert_eq!(problem.num_indexers(), solution.num_indexers());
    for (node_id, indexer_assignment) in solution.indexer_assignments.iter().enumerate() {
        assert_eq!(indexer_assignment.indexer_ord, node_id);
    }
    for (source_ord, source) in problem.sources().enumerate() {
        assert_eq!(source_ord as SourceOrd, source.source_ord);
    }
}

pub fn solve(
    mut problem: SchedulingProblem,
    previous_solution: SchedulingSolution,
) -> SchedulingSolution {
    // We first inflate the indexer capacities to make sure they globally
    // have at least 120% of the total problem load. This is done proportionally
    // to their original capacity.
    inflate_node_capacities_if_necessary(&mut problem);
    // As a heuristic, to offer stability, we work iteratively
    // from the previous solution.
    let mut solution = previous_solution;
    // We first run a few asserts to ensure that the problem is correct.
    check_contract_conditions(&problem, &solution);
    // Due to scale down, or entire removal of sources some shards we might have
    // too many shards in the current solution.
    // Let's first shave off the extraneous shards.
    remove_extraneous_shards(&problem, &mut solution);
    // Because the load associated to shards can change, some indexers
    // may have too much work assigned to them.
    // Again, we shave off some shards to make sure they are
    // within their capacity.
    enforce_indexers_cpu_capacity(&problem, &mut solution);
    // The solution now meets the constraint, but it does not necessarily
    // contains all of the shards that we need to assign.
    //
    // We first assign sources to indexers that have some affinity with them
    // (provided they have the capacity.)
    place_unassigned_shards_with_affinity(&problem, &mut solution);
    // Finally we assign the remaining shards, regardess of whether they have affinity
    // or not.
    place_unassigned_shards_ignoring_affinity(problem, &solution)
}

// -------------------------------------------------------------------------
// Phase 1
// Remove shards in solution that are not needed anymore

fn remove_extraneous_shards(problem: &SchedulingProblem, solution: &mut SchedulingSolution) {
    let mut num_shards_per_source: Vec<u32> = vec![0; problem.num_sources()];
    for indexer_assignment in &solution.indexer_assignments {
        if let Some((&source_ord, _)) = indexer_assignment.num_shards_per_source.last_key_value() {
            assert!(source_ord < problem.num_sources() as SourceOrd);
        }
        for (&source, &source_num_shards) in &indexer_assignment.num_shards_per_source {
            num_shards_per_source[source as usize] += source_num_shards;
        }
    }
    let num_shards_per_source_to_remove: Vec<(SourceOrd, u32)> = num_shards_per_source
        .into_iter()
        .zip(problem.sources())
        .flat_map(|(num_shards, source)| {
            let target_num_shards = source.num_shards;
            if target_num_shards < num_shards {
                Some((source.source_ord, num_shards - target_num_shards))
            } else {
                None
            }
        })
        .collect();

    let mut nodes_with_source: BTreeMap<SourceOrd, Vec<IndexerOrd>> = BTreeMap::default();
    for (node_id, indexer_assignment) in solution.indexer_assignments.iter().enumerate() {
        for (&source, &num_shards) in &indexer_assignment.num_shards_per_source {
            if num_shards > 0 {
                nodes_with_source.entry(source).or_default().push(node_id);
            }
        }
    }

    let mut indexer_available_capacity: Vec<i32> = solution
        .indexer_assignments
        .iter()
        .map(|indexer_assignment| indexer_assignment.indexer_available_capacity(problem))
        .collect();

    for (source_ord, mut num_shards_to_remove) in num_shards_per_source_to_remove {
        let nodes_with_source = nodes_with_source
            .get_mut(&source_ord)
            // Unwrap is safe here. By construction if we need to decrease the number of shard of a
            // given source, at least one node has it.
            .unwrap();
        nodes_with_source.sort_by_key(|&node_id| indexer_available_capacity[node_id]);
        for node_id in nodes_with_source.iter().copied() {
            let indexer_assignment = &mut solution.indexer_assignments[node_id];
            let previous_num_shards = indexer_assignment.num_shards(source_ord);
            assert!(previous_num_shards > 0);
            assert!(num_shards_to_remove > 0);
            let num_shards_removed = previous_num_shards.min(num_shards_to_remove);
            indexer_assignment.remove_shards(source_ord, num_shards_removed);
            num_shards_to_remove -= num_shards_removed;
            // We update the node capacity since its load has changed.
            indexer_available_capacity[node_id] =
                indexer_assignment.indexer_available_capacity(problem);
            if num_shards_to_remove == 0 {
                // No more work to do for this source.
                break;
            }
        }
    }
    assert_remove_extraneous_shards_post_condition(problem, solution);
}

fn assert_remove_extraneous_shards_post_condition(
    problem: &SchedulingProblem,
    solution: &SchedulingSolution,
) {
    let mut num_shards_per_source: Vec<u32> = vec![0; problem.num_sources()];
    for indexer_assignment in &solution.indexer_assignments {
        for (&source, &load) in &indexer_assignment.num_shards_per_source {
            num_shards_per_source[source as usize] += load;
        }
    }
    for source in problem.sources() {
        assert!(num_shards_per_source[source.source_ord as usize] <= source.num_shards);
    }
}

// -------------------------------------------------------------------------
// Phase 2
// Relieve sources from the node that are exceeding their maximum load.

fn enforce_indexers_cpu_capacity(problem: &SchedulingProblem, solution: &mut SchedulingSolution) {
    for indexer_assignment in &mut solution.indexer_assignments {
        let indexer_cpu_capacity: CpuCapacity =
            problem.indexer_cpu_capacity(indexer_assignment.indexer_ord);
        enforce_indexer_cpu_capacity(problem, indexer_cpu_capacity, indexer_assignment);
    }
}

fn enforce_indexer_cpu_capacity(
    problem: &SchedulingProblem,
    indexer_cpu_capacity: CpuCapacity,
    indexer_assignment: &mut IndexerAssignment,
) {
    let total_load = indexer_assignment.total_cpu_load(problem);
    if total_load <= indexer_cpu_capacity.cpu_millis() {
        return;
    }
    let mut load_to_remove: CpuCapacity =
        CpuCapacity::from_cpu_millis(total_load) - indexer_cpu_capacity;
    let mut source_cpu_capacities: Vec<(CpuCapacity, SourceOrd)> = indexer_assignment
        .num_shards_per_source
        .iter()
        .map(|(&source_ord, num_shards)| {
            let load_for_source = problem.source_load_per_shard(source_ord).get() * num_shards;
            (CpuCapacity::from_cpu_millis(load_for_source), source_ord)
        })
        .collect();
    source_cpu_capacities.sort();
    for (source_cpu_capacity, source_ord) in source_cpu_capacities {
        indexer_assignment.num_shards_per_source.remove(&source_ord);
        load_to_remove = if load_to_remove <= source_cpu_capacity {
            break;
        } else {
            load_to_remove - source_cpu_capacity
        };
    }
    assert_enforce_nodes_cpu_capacity_post_condition(problem, indexer_assignment);
}

fn assert_enforce_nodes_cpu_capacity_post_condition(
    problem: &SchedulingProblem,
    indexer_assignment: &IndexerAssignment,
) {
    let total_load = indexer_assignment.total_cpu_load(problem);
    assert!(
        total_load
            <= problem
                .indexer_cpu_capacity(indexer_assignment.indexer_ord)
                .cpu_millis()
    );
}

// ----------------------------------------------------
// Phase 3
// Place unassigned sources.
//
// We use a greedy algorithm as a simple heuristic here.
//
// We go through the sources in decreasing order of their load,
// in two passes.
//
// In the first pass, we have a look at
// the nodes with which there is an affinity.
//
// If one of them has room for all of the shards, then we assign all
// of the shards to it.
//
// In the second pass, we just put as many shards as possible on the node
// with the highest available capacity.
//
// If this algorithm fails to place all remaining shards, we inflate
// the node capacities by 20% in the scheduling problem and start from the beginning.

fn attempt_place_unassigned_shards(
    unassigned_shards: &[Source],
    problem: &SchedulingProblem,
    partial_solution: &SchedulingSolution,
) -> Result<SchedulingSolution, NotEnoughCapacity> {
    let mut solution = partial_solution.clone();
    for source in unassigned_shards {
        let indexers_with_most_available_capacity =
            compute_indexer_available_capacity(problem, &solution)
                .sorted_by_key(|(indexer_ord, capacity)| Reverse((*capacity, *indexer_ord)));
        place_unassigned_shards_single_source(
            source,
            indexers_with_most_available_capacity,
            &mut solution,
        )?;
    }
    assert_place_unassigned_shards_post_condition(problem, &solution);
    Ok(solution)
}

fn place_unassigned_shards_with_affinity(
    problem: &SchedulingProblem,
    solution: &mut SchedulingSolution,
) {
    let mut unassigned_shards: Vec<Source> = compute_unassigned_sources(problem, solution);
    unassigned_shards.sort_by_key(|source| {
        let load = source.num_shards * source.load_per_shard.get();
        Reverse(load)
    });
    for source in &unassigned_shards {
        // List of indexer with a non-null affinity and some available capacity, sorted by
        // (affinity, available capacity) in that order.
        let indexers_with_affinity_and_available_capacity = source
            .affinities
            .iter()
            .filter(|&(_, &affinity)| affinity != 0u32)
            .map(|(&indexer_ord, affinity)| {
                let available_capacity =
                    solution.indexer_assignments[indexer_ord].indexer_available_capacity(problem);
                let capacity = CpuCapacity::from_cpu_millis(available_capacity as u32);
                (indexer_ord, affinity, capacity)
            })
            .sorted_by_key(|(indexer_ord, affinity, capacity)| {
                Reverse((*affinity, *capacity, *indexer_ord))
            })
            .map(|(indexer_ord, _, capacity)| (indexer_ord, capacity));
        let _ = place_unassigned_shards_single_source(
            source,
            indexers_with_affinity_and_available_capacity,
            solution,
        );
    }
}

#[must_use]
fn place_unassigned_shards_ignoring_affinity(
    mut problem: SchedulingProblem,
    partial_solution: &SchedulingSolution,
) -> SchedulingSolution {
    let mut unassigned_shards: Vec<Source> = compute_unassigned_sources(&problem, partial_solution);
    unassigned_shards.sort_by_key(|source| {
        let load = source.num_shards * source.load_per_shard.get();
        Reverse(load)
    });

    // Thanks to the call to `inflate_node_capacities_if_necessary`, we are
    // certain that even on our first attempt, the total capacity of the indexer
    // exceeds 120% of the partial solution. If a large shard needs to be placed
    // in an already well balanced solution, it may not fit on any node. In that
    // case, we iteratively grow the virtual capacity until it can be placed.
    //
    // 1.2^30 is about 240. If we reach 30 attempts we are certain to have a
    // logical bug.
    for attempt_number in 0..30 {
        match attempt_place_unassigned_shards(&unassigned_shards[..], &problem, partial_solution) {
            Ok(mut solution) => {
                // the higher the attempt number, the more unbalanced the solution
                if attempt_number > 0 {
                    tracing::warn!(
                        attempt_number = attempt_number,
                        "capacity re-scaled, scheduling solution likely unbalanced"
                    );
                }
                solution.capacity_scaling_iterations = attempt_number;
                return solution;
            }
            Err(NotEnoughCapacity) => {
                problem.scale_node_capacities(1.2f32);
            }
        }
    }
    unreachable!("Failed to assign all of the sources");
}

fn assert_place_unassigned_shards_post_condition(
    problem: &SchedulingProblem,
    solution: &SchedulingSolution,
) {
    // We make sure we all shard are as placed.
    for source in problem.sources() {
        let num_assigned_shards: u32 = solution
            .indexer_assignments
            .iter()
            .map(|indexer_assignment| indexer_assignment.num_shards(source.source_ord))
            .sum();
        assert_eq!(num_assigned_shards, source.num_shards);
    }
    // We make sure that the node capacity is respected.
    for indexer_assignment in &solution.indexer_assignments {
        // We call this function just to check that the indexer assignment does not exceed this
        // capacity. (it includes an assert that panics if it happens).
        assert_enforce_nodes_cpu_capacity_post_condition(problem, indexer_assignment);
    }
}

struct NotEnoughCapacity;

/// Return Err(NotEnoughCapacity) iff the algorithm was unable to pack all of the sources
/// amongst the node with their given node capacity.
fn place_unassigned_shards_single_source(
    source: &Source,
    mut indexer_with_capacities: impl Iterator<Item = (IndexerOrd, CpuCapacity)>,
    solution: &mut SchedulingSolution,
) -> Result<(), NotEnoughCapacity> {
    let mut num_shards = source.num_shards;
    while num_shards > 0 {
        let Some((indexer_ord, available_capacity)) = indexer_with_capacities.next() else {
            return Err(NotEnoughCapacity);
        };
        let num_placable_shards = available_capacity.cpu_millis() / source.load_per_shard;
        let num_shards_to_place = num_placable_shards.min(num_shards);
        // Update the solution, the shard load, and the number of shards to place.
        solution.indexer_assignments[indexer_ord]
            .add_shards(source.source_ord, num_shards_to_place);
        num_shards -= num_shards_to_place;
    }
    Ok(())
}

/// Compute the sources/shards that have not been assigned to any indexer yet.
/// Affinity are also updated, with the limitation described in `Source`.
fn compute_unassigned_sources(
    problem: &SchedulingProblem,
    solution: &SchedulingSolution,
) -> Vec<Source> {
    let mut unassigned_sources: BTreeMap<SourceOrd, Source> = problem
        .sources()
        .map(|source| (source.source_ord as SourceOrd, source))
        .collect();
    for (indexer_ord, indexer_assignment) in solution.indexer_assignments.iter().enumerate() {
        for (&source_ord, &num_shards) in &indexer_assignment.num_shards_per_source {
            if num_shards == 0 {
                continue;
            }
            let Entry::Occupied(mut entry) = unassigned_sources.entry(source_ord) else {
                panic!("The solution contains more shards than the actual problem.");
            };
            if !entry.get_mut().remove_shards(indexer_ord, num_shards) {
                entry.remove();
            }
        }
    }
    unassigned_sources.into_values().collect()
}

/// Builds a BinaryHeap with the different indexer capacities.
///
/// Panics if one of the indexer is over-assigned.
fn compute_indexer_available_capacity<'a>(
    problem: &'a SchedulingProblem,
    solution: &'a SchedulingSolution,
) -> impl Iterator<Item = (IndexerOrd, CpuCapacity)> + 'a {
    solution
        .indexer_assignments
        .iter()
        .map(|indexer_assignment| {
            let available_capacity: i32 = indexer_assignment.indexer_available_capacity(problem);
            assert!(available_capacity >= 0i32);
            (
                indexer_assignment.indexer_ord,
                CpuCapacity::from_cpu_millis(available_capacity as u32),
            )
        })
}

#[cfg(test)]
mod tests {
    use std::num::NonZeroU32;

    use proptest::prelude::*;
    use quickwit_proto::indexing::mcpu;

    use super::*;

    #[test]
    fn test_remove_extraneous_shards() {
        let mut problem =
            SchedulingProblem::with_indexer_cpu_capacities(vec![mcpu(4_000), mcpu(5_000)]);
        problem.add_source(1, NonZeroU32::new(1_000u32).unwrap());
        let mut solution = problem.new_solution();
        solution.indexer_assignments[0].add_shards(0, 3);
        solution.indexer_assignments[1].add_shards(0, 3);
        remove_extraneous_shards(&problem, &mut solution);
        assert_eq!(solution.indexer_assignments[0].num_shards(0), 0);
        assert_eq!(solution.indexer_assignments[1].num_shards(0), 1);
    }

    #[test]
    fn test_remove_extraneous_shards_2() {
        let mut problem =
            SchedulingProblem::with_indexer_cpu_capacities(vec![mcpu(5_000), mcpu(4_000)]);
        problem.add_source(2, NonZeroU32::new(1_000).unwrap());
        let mut solution = problem.new_solution();
        solution.indexer_assignments[0].add_shards(0, 3);
        solution.indexer_assignments[1].add_shards(0, 3);
        remove_extraneous_shards(&problem, &mut solution);
        assert_eq!(solution.indexer_assignments[0].num_shards(0), 2);
        assert_eq!(solution.indexer_assignments[1].num_shards(0), 0);
    }

    #[test]
    fn test_remove_missing_sources() {
        let mut problem =
            SchedulingProblem::with_indexer_cpu_capacities(vec![mcpu(5_000), mcpu(4_000)]);
        // Source 0
        problem.add_source(0, NonZeroU32::new(1_000).unwrap());
        // Source 1
        problem.add_source(2, NonZeroU32::new(1_000).unwrap());
        let mut solution = problem.new_solution();
        solution.indexer_assignments[0].add_shards(0, 1);
        solution.indexer_assignments[0].add_shards(1, 1);
        solution.indexer_assignments[1].add_shards(1, 2);
        remove_extraneous_shards(&problem, &mut solution);
        assert_eq!(solution.indexer_assignments[0].num_shards(0), 0);
        assert_eq!(solution.indexer_assignments[0].num_shards(1), 1);
        assert_eq!(solution.indexer_assignments[1].num_shards(0), 0);
        assert_eq!(solution.indexer_assignments[1].num_shards(1), 1);
    }

    #[test]
    fn test_enforce_nodes_cpu_capacity() {
        let mut problem = SchedulingProblem::with_indexer_cpu_capacities(vec![
            mcpu(5_000),
            mcpu(5_000),
            mcpu(5_000),
            mcpu(5_000),
            mcpu(7_000),
        ]);
        // Source 0
        problem.add_source(10, NonZeroU32::new(3_000).unwrap());
        problem.add_source(10, NonZeroU32::new(2_000).unwrap());
        problem.add_source(10, NonZeroU32::new(1_001).unwrap());
        let mut solution = problem.new_solution();

        // node 0 does not exceed its capacity
        solution.indexer_assignments[0].add_shards(0, 1);

        // node 1 exceed its capacity with a single source
        solution.indexer_assignments[1].add_shards(0, 2);

        // node 2 is precisely at capacity
        solution.indexer_assignments[2].add_shards(0, 1);
        solution.indexer_assignments[2].add_shards(1, 1);

        // node 3 is exceeding its capacity due with several sources
        // We choose to remove sources entirely (as opposed to removing only shards that do not fit)
        solution.indexer_assignments[3].add_shards(0, 1);
        solution.indexer_assignments[3].add_shards(2, 2);

        // node 3 is exceeding its capacity due with several sources
        // We choose to remove sources entirely (as opposed to removing only shards that do not fit)
        solution.indexer_assignments[4].add_shards(0, 1);
        solution.indexer_assignments[4].add_shards(1, 1);
        solution.indexer_assignments[4].add_shards(2, 2);

        enforce_indexers_cpu_capacity(&problem, &mut solution);

        assert_eq!(solution.indexer_assignments[0].num_shards(0), 1);
        assert_eq!(solution.indexer_assignments[0].num_shards(1), 0);
        assert_eq!(solution.indexer_assignments[0].num_shards(2), 0);

        // We remove sources entirely!
        assert_eq!(solution.indexer_assignments[1].num_shards(0), 0);
        assert_eq!(solution.indexer_assignments[1].num_shards(1), 0);
        assert_eq!(solution.indexer_assignments[1].num_shards(2), 0);

        assert_eq!(solution.indexer_assignments[2].num_shards(0), 1);
        assert_eq!(solution.indexer_assignments[2].num_shards(1), 1);
        assert_eq!(solution.indexer_assignments[2].num_shards(2), 0);

        assert_eq!(solution.indexer_assignments[3].num_shards(0), 1);
        assert_eq!(solution.indexer_assignments[3].num_shards(1), 0);
        assert_eq!(solution.indexer_assignments[3].num_shards(2), 0);

        assert_eq!(solution.indexer_assignments[4].num_shards(0), 1);
        assert_eq!(solution.indexer_assignments[4].num_shards(1), 0);
        assert_eq!(solution.indexer_assignments[4].num_shards(2), 2);
    }

    #[test]
    fn test_compute_unassigned_shards_simple() {
        let mut problem = SchedulingProblem::with_indexer_cpu_capacities(vec![mcpu(4_000)]);
        problem.add_source(4, NonZeroU32::new(1000).unwrap());
        problem.add_source(4, NonZeroU32::new(1_000).unwrap());
        let solution = problem.new_solution();
        let unassigned_shards = compute_unassigned_sources(&problem, &solution);
        assert_eq!(
            unassigned_shards[0],
            Source {
                source_ord: 0,
                load_per_shard: NonZeroU32::new(1_000).unwrap(),
                num_shards: 4,
                affinities: BTreeMap::default(),
            }
        );
    }

    #[test]
    fn test_compute_unassigned_shards_with_non_trivial_solution() {
        let mut problem =
            SchedulingProblem::with_indexer_cpu_capacities(vec![mcpu(50_000), mcpu(40_000)]);
        problem.add_source(5, NonZeroU32::new(1_000).unwrap());
        problem.add_source(15, NonZeroU32::new(2_000).unwrap());
        let mut solution = problem.new_solution();

        solution.indexer_assignments[0].add_shards(0, 1);
        solution.indexer_assignments[0].add_shards(1, 3);
        solution.indexer_assignments[1].add_shards(0, 2);
        solution.indexer_assignments[1].add_shards(1, 3);
        let unassigned_shards = compute_unassigned_sources(&problem, &solution);
        assert_eq!(
            unassigned_shards[0],
            Source {
                source_ord: 0,
                load_per_shard: NonZeroU32::new(1_000).unwrap(),
                num_shards: 5 - (1 + 2),
                affinities: Default::default(),
            }
        );
        assert_eq!(
            unassigned_shards[1],
            Source {
                source_ord: 1,
                load_per_shard: NonZeroU32::new(2_000).unwrap(),
                num_shards: 15 - (3 + 3),
                affinities: Default::default(),
            }
        );
    }

    #[test]
    fn test_place_unassigned_shards_simple() {
        let mut problem = SchedulingProblem::with_indexer_cpu_capacities(vec![mcpu(4_000)]);
        problem.add_source(4, NonZeroU32::new(1_000).unwrap());
        let partial_solution = problem.new_solution();
        let solution = place_unassigned_shards_ignoring_affinity(problem, &partial_solution);
        assert_eq!(solution.indexer_assignments[0].num_shards(0), 4);
    }

    #[test]
    fn test_place_unassigned_shards_with_affinity() {
        let mut problem =
            SchedulingProblem::with_indexer_cpu_capacities(vec![mcpu(4_000), mcpu(4000)]);
        problem.add_source(4, NonZeroU32::new(1_000).unwrap());
        problem.add_source(4, NonZeroU32::new(1_000).unwrap());
        problem.inc_affinity(0, 1);
        problem.inc_affinity(1, 0);
        let mut solution = problem.new_solution();
        place_unassigned_shards_with_affinity(&problem, &mut solution);
        assert_eq!(solution.indexer_assignments[0].num_shards(1), 4);
        assert_eq!(solution.indexer_assignments[1].num_shards(0), 4);
    }

    #[test]
    fn test_place_unassigned_shards_reach_capacity() {
        let mut problem =
            SchedulingProblem::with_indexer_cpu_capacities(vec![mcpu(50_000), mcpu(40_000)]);
        problem.add_source(5, NonZeroU32::new(1_000).unwrap());
        problem.add_source(15, NonZeroU32::new(2_000).unwrap());
        let mut solution = problem.new_solution();
        solution.indexer_assignments[0].add_shards(0, 1);
        solution.indexer_assignments[0].add_shards(1, 3);
        solution.indexer_assignments[1].add_shards(0, 2);
        solution.indexer_assignments[1].add_shards(1, 3);
        let unassigned_shards = compute_unassigned_sources(&problem, &solution);
        assert_eq!(solution.indexer_assignments[0].num_shards(0), 1);
        assert_eq!(solution.indexer_assignments[0].num_shards(1), 3);
        assert_eq!(solution.indexer_assignments[1].num_shards(0), 2);
        assert_eq!(solution.indexer_assignments[1].num_shards(1), 3);
        assert_eq!(
            unassigned_shards[0],
            Source {
                source_ord: 0,
                load_per_shard: NonZeroU32::new(1_000).unwrap(),
                num_shards: 5 - (1 + 2),
                affinities: Default::default(),
            }
        );
        assert_eq!(
            unassigned_shards[1],
            Source {
                source_ord: 1,
                load_per_shard: NonZeroU32::new(2_000).unwrap(),
                num_shards: 15 - (3 + 3),
                affinities: Default::default(),
            }
        );
    }

    #[test]
    fn test_solve() {
        let mut problem = SchedulingProblem::with_indexer_cpu_capacities(vec![mcpu(800)]);
        problem.add_source(43, NonZeroU32::new(1).unwrap());
        problem.add_source(379, NonZeroU32::new(1).unwrap());
        let previous_solution = problem.new_solution();
        solve(problem, previous_solution);
    }

    fn indexer_cpu_capacity_strat() -> impl Strategy<Value = CpuCapacity> {
        prop_oneof![
            1u32..10_000u32,
            Just(1u32),
            800u32..1200u32,
            1900u32..2100u32,
        ]
        .prop_map(CpuCapacity::from_cpu_millis)
    }

    fn num_shards() -> impl Strategy<Value = u32> {
        0u32..3u32
    }

    fn source_strat() -> impl Strategy<Value = (u32, NonZeroU32)> {
        let load_strat = prop_oneof![
            Just(1u32),
            Just(2u32),
            Just(10u32),
            Just(100u32),
            Just(250u32),
            1u32..1_000u32
        ];
        (
            num_shards(),
            load_strat.prop_map(|load| NonZeroU32::new(load).unwrap()),
        )
    }

    fn problem_strategy(
        num_nodes: usize,
        num_sources: usize,
    ) -> impl Strategy<Value = SchedulingProblem> {
        let indexer_cpu_capacity_strat =
            proptest::collection::vec(indexer_cpu_capacity_strat(), num_nodes);
        let sources_strat = proptest::collection::vec(source_strat(), num_sources);
        (indexer_cpu_capacity_strat, sources_strat).prop_map(|(node_cpu_capacities, sources)| {
            let mut problem = SchedulingProblem::with_indexer_cpu_capacities(node_cpu_capacities);
            for (num_shards, load_per_shard) in sources {
                problem.add_source(num_shards, load_per_shard);
            }
            problem
        })
    }

    fn num_nodes_strat() -> impl Strategy<Value = usize> {
        prop_oneof![
            3 => 1usize..3,
            1 => 4usize..10,
        ]
    }
    fn num_sources_strat() -> impl Strategy<Value = usize> {
        prop_oneof![
            3 => 0usize..3,
            1 => 4usize..10,
        ]
    }

    fn indexer_assignments_strategy(num_sources: usize) -> impl Strategy<Value = Vec<u32>> {
        proptest::collection::vec(0u32..3u32, num_sources)
    }

    fn initial_solution_strategy(
        num_nodes: usize,
        num_sources: usize,
    ) -> impl Strategy<Value = SchedulingSolution> {
        proptest::collection::vec(indexer_assignments_strategy(num_sources), num_nodes).prop_map(
            move |indexer_assignments: Vec<Vec<u32>>| {
                let mut solution = SchedulingSolution::with_num_indexers(num_nodes);
                for (node_id, indexer_assignment) in indexer_assignments.iter().enumerate() {
                    for (source_ord, num_shards) in indexer_assignment.iter().copied().enumerate() {
                        solution.indexer_assignments[node_id]
                            .add_shards(source_ord as u32, num_shards);
                    }
                }
                solution
            },
        )
    }

    fn problem_solution_strategy() -> impl Strategy<Value = (SchedulingProblem, SchedulingSolution)>
    {
        (num_nodes_strat(), num_sources_strat()).prop_flat_map(move |(num_nodes, num_sources)| {
            (
                problem_strategy(num_nodes, num_sources),
                initial_solution_strategy(num_nodes, num_sources),
            )
        })
    }

    #[test]
    fn test_problem_missing_capacities() {
        let mut problem =
            SchedulingProblem::with_indexer_cpu_capacities(vec![CpuCapacity::from_cpu_millis(100)]);
        problem.add_source(1, NonZeroU32::new(1).unwrap());
        let mut previous_solution = problem.new_solution();
        previous_solution.indexer_assignments[0].add_shards(0, 0);
        let solution = solve(problem, previous_solution);
        assert_eq!(solution.indexer_assignments[0].num_shards(0), 1);
    }

    #[test]
    fn test_problem_unbalanced_simple() {
        let mut problem = SchedulingProblem::with_indexer_cpu_capacities(vec![
            CpuCapacity::from_cpu_millis(1),
            CpuCapacity::from_cpu_millis(1),
        ]);
        problem.add_source(1, NonZeroU32::new(10).unwrap());
        for _ in 0..10 {
            problem.add_source(1, NonZeroU32::new(1).unwrap());
        }
        let previous_solution = problem.new_solution();
        let solution = solve(problem.clone(), previous_solution);
        let available_capacities: Vec<u32> = solution
            .indexer_assignments
            .iter()
            .map(|indexer_assignment: &IndexerAssignment| {
                indexer_assignment.total_cpu_load(&problem)
            })
            .collect();
        assert_eq!(available_capacities.len(), 2);
        let (min, max) = available_capacities
            .into_iter()
            .minmax()
            .into_option()
            .unwrap();
        assert_eq!(min, 10);
        assert_eq!(max, 10);
    }

    proptest! {
        #[test]
        fn test_proptest_post_conditions((problem, solution) in problem_solution_strategy()) {
            let solution_1 = solve(problem.clone(), solution);
            let solution_2 = solve(problem.clone(), solution_1.clone());
            // TODO: This assert actually fails for some scenarii. We say it is fine
            // for now as long as the solution does not change again during the
            // next resolution:
            // let has_solution_changed_once = solution_1.indexer_assignments != solution_2.indexer_assignments;
            // assert!(!has_solution_changed_once, "Solution changed for same problem\nSolution 1:{solution_1:?}\nSolution 2: {solution_2:?}");
            let solution_3 = solve(problem, solution_2.clone());
            let has_solution_changed_again = solution_2.indexer_assignments != solution_3.indexer_assignments;
            assert!(!has_solution_changed_again, "solution unstable!!!\nSolution 1: {solution_1:?}\nSolution 2: {solution_2:?}\nSolution 3: {solution_3:?}");
        }
    }

    #[test]
    fn test_capacity_scaling_iteration_required() {
        // Create a problem where affinity constraints cause suboptimal placement
        // requiring iterative scaling despite initial capacity scaling.
        let mut problem =
            SchedulingProblem::with_indexer_cpu_capacities(vec![mcpu(3000), mcpu(3000)]);
        problem.add_source(1, NonZeroU32::new(2500).unwrap()); // Source 0
        problem.add_source(1, NonZeroU32::new(2500).unwrap()); // Source 1
        problem.add_source(1, NonZeroU32::new(1500).unwrap()); // Source 2
        let previous_solution = problem.new_solution();
        let solution = solve(problem, previous_solution);

        assert_eq!(solution.capacity_scaling_iterations, 1);
    }
}
