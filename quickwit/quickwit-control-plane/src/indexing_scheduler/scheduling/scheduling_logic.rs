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

use std::cmp::Reverse;
use std::collections::btree_map::Entry;
use std::collections::{BTreeMap, BinaryHeap, HashMap};

use quickwit_proto::indexing::CpuCapacity;

use super::scheduling_logic_model::*;

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
    problem: &SchedulingProblem,
    previous_solution: SchedulingSolution,
) -> (SchedulingSolution, BTreeMap<SourceOrd, u32>) {
    let mut solution = previous_solution;
    check_contract_conditions(problem, &solution);
    remove_extraneous_shards(problem, &mut solution);
    enforce_indexers_cpu_capacity(problem, &mut solution);
    let still_unassigned = place_unassigned_shards(problem, &mut solution);
    // TODO ideally we should have some smarter logic here to bread first search for a better
    // solution.
    (solution, still_unassigned)
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

    let mut nodes_with_source: HashMap<SourceOrd, Vec<IndexerOrd>> = HashMap::default();
    for (node_id, indexer_assignment) in solution.indexer_assignments.iter().enumerate() {
        for (&source, &num_shards) in &indexer_assignment.num_shards_per_source {
            if num_shards > 0 {
                nodes_with_source.entry(source).or_default().push(node_id);
            }
        }
    }

    let mut indexer_available_capacity: Vec<CpuCapacity> = solution
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
// Releave sources from the node that are exceeding their maximum load.

fn enforce_indexers_cpu_capacity(problem: &SchedulingProblem, solution: &mut SchedulingSolution) {
    for indexer_assignment in solution.indexer_assignments.iter_mut() {
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
    if total_load <= indexer_cpu_capacity {
        return;
    }
    let mut load_to_remove: CpuCapacity = total_load - indexer_cpu_capacity;
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
    assert!(total_load <= problem.indexer_cpu_capacity(indexer_assignment.indexer_ord));
}

// ----------------------------------------------------
// Phase 3
// Place unassigned sources.
//
// We use a greedy algorithm as a simple heuristic here.
// We go through the sources in decreasing order of their load.
//
// We then try to place as many shards as possible in the node with the
// highest available capacity.

fn place_unassigned_shards(
    problem: &SchedulingProblem,
    solution: &mut SchedulingSolution,
) -> BTreeMap<SourceOrd, u32> {
    let mut unassigned_shards: Vec<Source> = compute_unassigned_sources(problem, solution);
    unassigned_shards.sort_by_key(|source| {
        let load = source.num_shards * source.load_per_shard.get();
        Reverse(load)
    });
    let mut indexers_with_most_available_capacity: BinaryHeap<(CpuCapacity, IndexerOrd)> =
        compute_indexer_available_capacity(problem, solution);
    let mut unassignable_shards = BTreeMap::new();
    for source in unassigned_shards {
        let num_shards_unassigned = place_unassigned_shards_single_source(
            &source,
            &mut indexers_with_most_available_capacity,
            solution,
        );
        // We haven't been able to place this source entirely.
        if num_shards_unassigned != 0 {
            unassignable_shards.insert(source.source_ord, num_shards_unassigned);
        }
    }
    assert_place_unassigned_shards_post_condition(problem, solution, &unassignable_shards);
    unassignable_shards
}

fn assert_place_unassigned_shards_post_condition(
    problem: &SchedulingProblem,
    solution: &SchedulingSolution,
    unassigned_shards: &BTreeMap<SourceOrd, u32>,
) {
    // We make sure we all shard are cound as place or unassigned.
    for source in problem.sources() {
        let num_assigned_shards: u32 = solution
            .indexer_assignments
            .iter()
            .map(|indexer_assignment| indexer_assignment.num_shards(source.source_ord))
            .sum();
        assert_eq!(
            num_assigned_shards
                + unassigned_shards
                    .get(&source.source_ord)
                    .copied()
                    .unwrap_or(0),
            source.num_shards
        );
    }
    // We make sure that all unassigned shard cannot be placed.
    for indexer_assignment in &solution.indexer_assignments {
        let available_capacity: CpuCapacity =
            indexer_assignment.indexer_available_capacity(problem);
        for (&source_ord, &num_shards) in unassigned_shards {
            assert!(num_shards > 0);
            let source = problem.source(source_ord);
            assert!(source.load_per_shard.get() > available_capacity.cpu_millis());
        }
    }
}

fn place_unassigned_shards_single_source(
    source: &Source,
    indexer_available_capacities: &mut BinaryHeap<(CpuCapacity, IndexerOrd)>,
    solution: &mut SchedulingSolution,
) -> u32 {
    let mut num_shards = source.num_shards;
    while num_shards > 0 {
        let Some(mut node_with_most_capacity) = indexer_available_capacities.peek_mut() else {
            break;
        };
        let node_id = node_with_most_capacity.1;
        let available_capacity = &mut node_with_most_capacity.0;
        let num_placable_shards = available_capacity.cpu_millis() / source.load_per_shard;
        let num_shards_to_place = num_placable_shards.min(num_shards);
        // We cannot place more shards with this load.
        if num_shards_to_place == 0 {
            break;
        }
        // TODO take in account colocation.
        // Update the solution, the shard load, and the number of shards to place.
        solution.indexer_assignments[node_id].add_shards(source.source_ord, num_shards_to_place);
        *available_capacity = *available_capacity
            - CpuCapacity::from_cpu_millis(num_shards_to_place * source.load_per_shard.get());
        num_shards -= num_shards_to_place;
    }
    num_shards
}

fn compute_unassigned_sources(
    problem: &SchedulingProblem,
    solution: &SchedulingSolution,
) -> Vec<Source> {
    let mut unassigned_sources: BTreeMap<SourceOrd, Source> = problem
        .sources()
        .map(|source| (source.source_ord as SourceOrd, source))
        .collect();
    for indexer_assignment in &solution.indexer_assignments {
        for (&source_ord, &num_shards) in &indexer_assignment.num_shards_per_source {
            if num_shards == 0 {
                continue;
            }
            let Entry::Occupied(mut entry) = unassigned_sources.entry(source_ord) else {
                panic!("The solution contains more shards than the actual problem.");
            };
            entry.get_mut().num_shards -= num_shards;
            if entry.get().num_shards == 0 {
                entry.remove();
            }
        }
    }
    unassigned_sources.into_values().collect()
}

fn compute_indexer_available_capacity(
    problem: &SchedulingProblem,
    solution: &SchedulingSolution,
) -> BinaryHeap<(CpuCapacity, IndexerOrd)> {
    let mut indexer_available_capacity: BinaryHeap<(CpuCapacity, IndexerOrd)> =
        BinaryHeap::with_capacity(problem.num_indexers());
    for indexer_assignment in &solution.indexer_assignments {
        let available_capacity = indexer_assignment.indexer_available_capacity(problem);
        indexer_available_capacity.push((available_capacity, indexer_assignment.indexer_ord));
    }
    indexer_available_capacity
}
#[cfg(test)]
mod tests {
    use std::num::NonZeroU32;

    use proptest::prelude::*;
    use quickwit_proto::indexing::mcpu;

    use super::*;

    #[test]
    fn test_remove_extranous_shards() {
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
    fn test_remove_extranous_shards_2() {
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
        let mut problem =
            SchedulingProblem::with_indexer_cpu_capacities(vec![mcpu(0), mcpu(4_000)]);
        problem.add_source(4, NonZeroU32::new(1000).unwrap());
        problem.add_source(4, NonZeroU32::new(1_000).unwrap());
        let solution = problem.new_solution();
        let unassigned_shards = compute_unassigned_sources(&problem, &solution);
        assert_eq!(
            unassigned_shards[0],
            Source {
                source_ord: 0,
                load_per_shard: NonZeroU32::new(1_000).unwrap(),
                num_shards: 4
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
                num_shards: 5 - (1 + 2)
            }
        );
        assert_eq!(
            unassigned_shards[1],
            Source {
                source_ord: 1,
                load_per_shard: NonZeroU32::new(2_000).unwrap(),
                num_shards: 15 - (3 + 3)
            }
        );
    }

    #[test]
    fn test_place_unassigned_shards_simple() {
        let mut problem = SchedulingProblem::with_indexer_cpu_capacities(vec![mcpu(4_000)]);
        problem.add_source(4, NonZeroU32::new(1_000).unwrap());
        let mut solution = problem.new_solution();
        let unassigned = place_unassigned_shards(&problem, &mut solution);
        assert_eq!(solution.indexer_assignments[0].num_shards(0), 4);
        assert!(unassigned.is_empty());
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
                num_shards: 5 - (1 + 2)
            }
        );
        assert_eq!(
            unassigned_shards[1],
            Source {
                source_ord: 1,
                load_per_shard: NonZeroU32::new(2_000).unwrap(),
                num_shards: 15 - (3 + 3)
            }
        );
    }

    #[test]
    fn test_solve() {
        let mut problem = SchedulingProblem::with_indexer_cpu_capacities(vec![mcpu(800)]);
        problem.add_source(43, NonZeroU32::new(1).unwrap());
        problem.add_source(379, NonZeroU32::new(1).unwrap());
        let previous_solution = problem.new_solution();
        solve(&problem, previous_solution);
    }

    fn indexer_cpu_capacity_strat() -> impl Strategy<Value = CpuCapacity> {
        prop_oneof![
            0u32..10_000u32,
            Just(0u32),
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
            3 => 0usize..3,
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
    fn test_problem_leading_to_zero_shard() {
        let mut problem = SchedulingProblem::with_indexer_cpu_capacities(vec![
            CpuCapacity::from_cpu_millis(0),
            CpuCapacity::from_cpu_millis(0),
        ]);
        problem.add_source(0, NonZeroU32::new(1).unwrap());
        let mut previous_solution = problem.new_solution();
        previous_solution.indexer_assignments[0].add_shards(0, 0);
        previous_solution.indexer_assignments[1].add_shards(0, 0);
        let (solution, still_unassigned) = solve(&problem, previous_solution);
        assert_eq!(solution.indexer_assignments[0].num_shards(0), 0);
        assert_eq!(solution.indexer_assignments[1].num_shards(0), 0);
        assert!(still_unassigned.is_empty());
    }

    proptest! {
        #[test]
        fn test_proptest_post_conditions((problem, solution) in problem_solution_strategy()) {
            solve(&problem, solution);
        }
    }
}
