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

use super::scheduling_logic_model::*;

// ------------------------------------------------------------------------------------
// High level algorithm

fn check_contract_conditions(problem: &SchedulingProblem, solution: &SchedulingSolution) {
    assert_eq!(problem.num_nodes(), solution.num_nodes());
    for (node_id, node_assignment) in solution.node_assignments.iter().enumerate() {
        assert_eq!(node_assignment.node_id, node_id);
    }
    for (source_id, source) in problem.sources().enumerate() {
        assert_eq!(source_id as SourceOrd, source.source_id);
    }
}

pub fn solve(
    problem: &SchedulingProblem,
    previous_solution: SchedulingSolution,
) -> (SchedulingSolution, BTreeMap<SourceOrd, u32>) {
    let mut solution = previous_solution;
    check_contract_conditions(problem, &solution);
    remove_extraneous_shards(problem, &mut solution);
    enforce_nodes_max_load(problem, &mut solution);
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
    for node_assignment in &mut solution.node_assignments {
        node_assignment.truncate_num_sources(problem.num_sources());
        for (&source, &load) in &node_assignment.num_shards_per_source {
            num_shards_per_source[source as usize] += load;
        }
    }
    let num_shards_per_source_to_remove: Vec<(SourceOrd, u32)> = num_shards_per_source
        .into_iter()
        .zip(problem.sources())
        .flat_map(|(num_shards, source)| {
            let target_num_shards = source.num_shards;
            if target_num_shards < num_shards {
                Some((source.source_id, num_shards - target_num_shards))
            } else {
                None
            }
        })
        .collect();

    let mut nodes_with_source: HashMap<SourceOrd, Vec<NodeOrd>> = HashMap::default();
    for (node_id, node_assignment) in solution.node_assignments.iter().enumerate() {
        for &source in node_assignment.num_shards_per_source.keys() {
            nodes_with_source.entry(source).or_default().push(node_id);
        }
    }

    let mut node_available_capacity: Vec<Load> = solution
        .node_assignments
        .iter()
        .map(|node_assignment| node_assignment.node_available_capacity(problem))
        .collect();

    for (source_id, mut num_shards_to_remove) in num_shards_per_source_to_remove {
        let nodes_with_source = nodes_with_source
            .get_mut(&source_id)
            // Unwrap is safe here. By construction if we need to decrease the number of shard of a
            // given source, at least one node has it.
            .unwrap();
        nodes_with_source.sort_by_key(|&node_id| node_available_capacity[node_id]);
        for node_id in nodes_with_source.iter().copied() {
            let node_assignment = &mut solution.node_assignments[node_id];
            let previous_num_shards = node_assignment.num_shards(source_id);
            assert!(previous_num_shards > 0);
            assert!(num_shards_to_remove > 0);
            let num_shards_removed = previous_num_shards.min(num_shards_to_remove);
            node_assignment.remove_shards(source_id, num_shards_removed);
            num_shards_to_remove -= num_shards_removed;
            // We update the node capacity since its load has changed.
            node_available_capacity[node_id] = node_assignment.node_available_capacity(problem);
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
    for node_assignment in &solution.node_assignments {
        for (&source, &load) in &node_assignment.num_shards_per_source {
            num_shards_per_source[source as usize] += load;
        }
    }
    for source in problem.sources() {
        assert!(num_shards_per_source[source.source_id as usize] <= source.num_shards);
    }
}

// -------------------------------------------------------------------------
// Phase 2
// Releave sources from the node that are exceeding their maximum load.

fn enforce_nodes_max_load(problem: &SchedulingProblem, solution: &mut SchedulingSolution) {
    for node_assignment in solution.node_assignments.iter_mut() {
        let node_max_load: Load = problem.node_max_load(node_assignment.node_id);
        enforce_node_max_load(problem, node_max_load, node_assignment);
    }
}

fn enforce_node_max_load(
    problem: &SchedulingProblem,
    node_max_load: Load,
    node_assignment: &mut NodeAssignment,
) {
    let total_load = node_assignment.total_load(problem);
    if total_load <= node_max_load {
        return;
    }
    let mut load_to_remove = total_load - node_max_load;
    let mut load_sources: Vec<(Load, SourceOrd)> = node_assignment
        .num_shards_per_source
        .iter()
        .map(|(source_id, num_shards)| {
            let load_for_source = problem.source_load_per_shard(*source_id) * num_shards;
            (load_for_source, *source_id)
        })
        .collect();
    load_sources.sort();
    for (load, source_id) in load_sources {
        node_assignment.num_shards_per_source.remove(&source_id);
        load_to_remove = load_to_remove.saturating_sub(load);
        if load_to_remove == 0 {
            break;
        }
    }
    assert_enforce_nodes_max_load_post_condition(problem, node_assignment);
}

fn assert_enforce_nodes_max_load_post_condition(
    problem: &SchedulingProblem,
    node_assignment: &NodeAssignment,
) {
    let total_load = node_assignment.total_load(problem);
    assert!(total_load <= problem.node_max_load(node_assignment.node_id));
}

// ----------------------------------------------------
// Phase 3
// Place unassigned sources.
//
// We use a greedy algorithm as a simple heuristic here.
// We go through the sources in decreasing order of their load.
//
// We then try to place as many shards as possible in the node with the
// highest load.

fn place_unassigned_shards(
    problem: &SchedulingProblem,
    solution: &mut SchedulingSolution,
) -> BTreeMap<SourceOrd, u32> {
    let mut unassigned_shards: Vec<Source> = compute_unassigned_sources(problem, solution);
    unassigned_shards.sort_by_key(|source| {
        let load = source.num_shards * source.load_per_shard;
        Reverse(load)
    });
    let mut node_with_least_loads: BinaryHeap<(Load, NodeOrd)> =
        compute_node_available_capacity(problem, solution);
    let mut unassignable_shards = BTreeMap::new();
    for source in unassigned_shards {
        let num_shards_unassigned =
            place_unassigned_shards_single_source(&source, &mut node_with_least_loads, solution);
        // We haven't been able to place this source entirely.
        if num_shards_unassigned != 0 {
            unassignable_shards.insert(source.source_id, num_shards_unassigned);
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
            .node_assignments
            .iter()
            .map(|node_assignment| node_assignment.num_shards(source.source_id))
            .sum();
        assert_eq!(
            num_assigned_shards
                + unassigned_shards
                    .get(&source.source_id)
                    .copied()
                    .unwrap_or(0),
            source.num_shards
        );
    }
    // We make sure that all unassigned shard cannot be placed.
    for node_assignment in &solution.node_assignments {
        let available_capacity: Load = node_assignment.node_available_capacity(problem);
        for (&source_id, &num_shards) in unassigned_shards {
            assert!(num_shards > 0);
            let source = problem.source(source_id);
            assert!(source.load_per_shard > available_capacity);
        }
    }
}

fn place_unassigned_shards_single_source(
    source: &Source,
    node_with_least_loads: &mut BinaryHeap<(Load, NodeOrd)>,
    solution: &mut SchedulingSolution,
) -> u32 {
    let mut num_shards = source.num_shards;
    while num_shards > 0 {
        let Some(mut node_with_least_load) = node_with_least_loads.peek_mut() else {
            break;
        };
        let node_id = node_with_least_load.1;
        let available_capacity = &mut node_with_least_load.0;
        let num_placable_shards = *available_capacity / source.load_per_shard;
        let num_shards_to_place = num_placable_shards.min(num_shards);
        // We cannot place more shards with this load.
        if num_shards_to_place == 0 {
            break;
        }
        // TODO take in account colocation.
        // Update the solution, the shard load, and the number of shards to place.
        solution.node_assignments[node_id].add_shard(source.source_id, num_shards_to_place);
        *available_capacity -= num_shards_to_place * source.load_per_shard;
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
        .map(|source| (source.source_id as SourceOrd, source))
        .collect();
    for node_assignment in &solution.node_assignments {
        for (&source_id, &num_shards) in &node_assignment.num_shards_per_source {
            let Entry::Occupied(mut entry) = unassigned_sources.entry(source_id) else {
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

fn compute_node_available_capacity(
    problem: &SchedulingProblem,
    solution: &SchedulingSolution,
) -> BinaryHeap<(Load, NodeOrd)> {
    let mut node_available_capacity: BinaryHeap<(Load, NodeOrd)> =
        BinaryHeap::with_capacity(problem.num_nodes());
    for node_assignment in &solution.node_assignments {
        let available_capacity = node_assignment.node_available_capacity(problem);
        node_available_capacity.push((available_capacity, node_assignment.node_id));
    }
    node_available_capacity
}
#[cfg(test)]
mod tests {
    use proptest::prelude::*;

    use super::*;

    #[test]
    fn test_remove_extranous_shards() {
        let mut problem = SchedulingProblem::with_node_maximum_load(vec![4_000, 5_000]);
        problem.add_source(1, 1_000);
        let mut solution = problem.new_solution();
        solution.node_assignments[0].add_shard(0, 3);
        solution.node_assignments[1].add_shard(0, 3);
        remove_extraneous_shards(&problem, &mut solution);
        assert_eq!(solution.node_assignments[0].num_shards(0), 0);
        assert_eq!(solution.node_assignments[1].num_shards(0), 1);
    }

    #[test]
    fn test_remove_extranous_shards_2() {
        let mut problem = SchedulingProblem::with_node_maximum_load(vec![5_000, 4_000]);
        problem.add_source(2, 1_000);
        let mut solution = problem.new_solution();
        solution.node_assignments[0].add_shard(0, 3);
        solution.node_assignments[1].add_shard(0, 3);
        remove_extraneous_shards(&problem, &mut solution);
        assert_eq!(solution.node_assignments[0].num_shards(0), 2);
        assert_eq!(solution.node_assignments[1].num_shards(0), 0);
    }

    #[test]
    fn test_remove_missing_sources() {
        let mut problem = SchedulingProblem::with_node_maximum_load(vec![5_000, 4_000]);
        // Source 0
        problem.add_source(0, 1_000);
        // Source 1
        problem.add_source(2, 1_000);
        let mut solution = problem.new_solution();
        solution.node_assignments[0].add_shard(0, 1);
        solution.node_assignments[0].add_shard(1, 1);
        solution.node_assignments[1].add_shard(1, 2);
        remove_extraneous_shards(&problem, &mut solution);
        assert_eq!(solution.node_assignments[0].num_shards(0), 0);
        assert_eq!(solution.node_assignments[0].num_shards(1), 1);
        assert_eq!(solution.node_assignments[1].num_shards(0), 0);
        assert_eq!(solution.node_assignments[1].num_shards(1), 1);
    }

    #[test]
    fn test_truncate_sources() {
        let mut problem = SchedulingProblem::with_node_maximum_load(vec![5_000, 4_000]);
        // Source 0
        problem.add_source(1, 1_000);
        let mut solution = problem.new_solution();
        solution.node_assignments[0].add_shard(0, 1);
        solution.node_assignments[0].add_shard(1, 1);
        remove_extraneous_shards(&problem, &mut solution);
        assert_eq!(solution.node_assignments[0].num_shards(0), 1);
        assert_eq!(solution.node_assignments[0].num_shards(1), 0);
    }

    #[test]
    fn test_enforce_nodes_max_load() {
        let mut problem =
            SchedulingProblem::with_node_maximum_load(vec![5_000, 5_000, 5_000, 5_000, 7_000]);
        // Source 0
        problem.add_source(10, 3_000);
        problem.add_source(10, 2_000);
        problem.add_source(10, 1_001);
        let mut solution = problem.new_solution();

        // node 0 does not exceed its capacity
        solution.node_assignments[0].add_shard(0, 1);

        // node 1 exceed its capacity with a single source
        solution.node_assignments[1].add_shard(0, 2);

        // node 2 is precisely at capacity
        solution.node_assignments[2].add_shard(0, 1);
        solution.node_assignments[2].add_shard(1, 1);

        // node 3 is exceeding its capacity due with several sources
        // We choose to remove sources entirely (as opposed to removing only shards that do not fit)
        solution.node_assignments[3].add_shard(0, 1);
        solution.node_assignments[3].add_shard(2, 2);

        // node 3 is exceeding its capacity due with several sources
        // We choose to remove sources entirely (as opposed to removing only shards that do not fit)
        solution.node_assignments[4].add_shard(0, 1);
        solution.node_assignments[4].add_shard(1, 1);
        solution.node_assignments[4].add_shard(2, 2);

        enforce_nodes_max_load(&problem, &mut solution);

        assert_eq!(solution.node_assignments[0].num_shards(0), 1);
        assert_eq!(solution.node_assignments[0].num_shards(1), 0);
        assert_eq!(solution.node_assignments[0].num_shards(2), 0);

        // We remove sources entirely!
        assert_eq!(solution.node_assignments[1].num_shards(0), 0);
        assert_eq!(solution.node_assignments[1].num_shards(1), 0);
        assert_eq!(solution.node_assignments[1].num_shards(2), 0);

        assert_eq!(solution.node_assignments[2].num_shards(0), 1);
        assert_eq!(solution.node_assignments[2].num_shards(1), 1);
        assert_eq!(solution.node_assignments[2].num_shards(2), 0);

        assert_eq!(solution.node_assignments[3].num_shards(0), 1);
        assert_eq!(solution.node_assignments[3].num_shards(1), 0);
        assert_eq!(solution.node_assignments[3].num_shards(2), 0);

        assert_eq!(solution.node_assignments[4].num_shards(0), 1);
        assert_eq!(solution.node_assignments[4].num_shards(1), 0);
        assert_eq!(solution.node_assignments[4].num_shards(2), 2);
    }

    #[test]
    fn test_compute_unassigned_shards_simple() {
        let mut problem = SchedulingProblem::with_node_maximum_load(vec![0, 4_000]);
        problem.add_source(4, 1000);
        problem.add_source(4, 1_000);
        let solution = problem.new_solution();
        let unassigned_shards = compute_unassigned_sources(&problem, &solution);
        assert_eq!(
            unassigned_shards[0],
            Source {
                source_id: 0,
                load_per_shard: 1_000,
                num_shards: 4
            }
        );
    }

    #[test]
    fn test_compute_unassigned_shards_with_non_trivial_solution() {
        let mut problem = SchedulingProblem::with_node_maximum_load(vec![50_000, 40_000]);
        problem.add_source(5, 1_000);
        problem.add_source(15, 2_000);
        let mut solution = problem.new_solution();

        solution.node_assignments[0].add_shard(0, 1);
        solution.node_assignments[0].add_shard(1, 3);
        solution.node_assignments[1].add_shard(0, 2);
        solution.node_assignments[1].add_shard(1, 3);
        let unassigned_shards = compute_unassigned_sources(&problem, &solution);
        assert_eq!(
            unassigned_shards[0],
            Source {
                source_id: 0,
                load_per_shard: 1_000,
                num_shards: 5 - (1 + 2)
            }
        );
        assert_eq!(
            unassigned_shards[1],
            Source {
                source_id: 1,
                load_per_shard: 2_000,
                num_shards: 15 - (3 + 3)
            }
        );
    }

    #[test]
    fn test_place_unassigned_shards_simple() {
        let mut problem = SchedulingProblem::with_node_maximum_load(vec![4_000]);
        problem.add_source(4, 1_000);
        let mut solution = problem.new_solution();
        let unassigned = place_unassigned_shards(&problem, &mut solution);
        assert_eq!(solution.node_assignments[0].num_shards(0), 4);
        assert!(unassigned.is_empty());
    }

    #[test]
    fn test_place_unassigned_shards_reach_capacity() {
        let mut problem = SchedulingProblem::with_node_maximum_load(vec![50_000, 40_000]);
        problem.add_source(5, 1_000);
        problem.add_source(15, 2_000);
        let mut solution = problem.new_solution();
        solution.node_assignments[0].add_shard(0, 1);
        solution.node_assignments[0].add_shard(1, 3);
        solution.node_assignments[1].add_shard(0, 2);
        solution.node_assignments[1].add_shard(1, 3);
        let unassigned_shards = compute_unassigned_sources(&problem, &solution);
        assert_eq!(solution.node_assignments[0].num_shards(0), 1);
        assert_eq!(solution.node_assignments[0].num_shards(1), 3);
        assert_eq!(solution.node_assignments[1].num_shards(0), 2);
        assert_eq!(solution.node_assignments[1].num_shards(1), 3);
        assert_eq!(
            unassigned_shards[0],
            Source {
                source_id: 0,
                load_per_shard: 1_000,
                num_shards: 5 - (1 + 2)
            }
        );
        assert_eq!(
            unassigned_shards[1],
            Source {
                source_id: 1,
                load_per_shard: 2_000,
                num_shards: 15 - (3 + 3)
            }
        );
    }

    #[test]
    fn test_solve() {
        let mut problem = SchedulingProblem::with_node_maximum_load(vec![800]);
        problem.add_source(43, 1);
        problem.add_source(379, 1);
        let previous_solution = problem.new_solution();
        solve(&problem, previous_solution);
    }

    fn node_max_load_strat() -> impl Strategy<Value = Load> {
        prop_oneof![
            0u32..10_000u32,
            Just(0u32),
            800u32..1200u32,
            1900u32..2100u32,
        ]
    }

    fn num_shards() -> impl Strategy<Value = u32> {
        0u32..3u32
    }

    fn source_strat() -> impl Strategy<Value = (u32, Load)> {
        (num_shards(), 0u32..1_000u32)
    }

    fn problem_strategy(
        num_nodes: usize,
        num_sources: usize,
    ) -> impl Strategy<Value = SchedulingProblem> {
        let node_max_loads_strat = proptest::collection::vec(node_max_load_strat(), num_nodes);
        let sources_strat = proptest::collection::vec(source_strat(), num_sources);
        (node_max_loads_strat, sources_strat).prop_map(|(node_max_loads, sources)| {
            let mut problem = SchedulingProblem::with_node_maximum_load(node_max_loads);
            for (num_shards, load_per_shard) in sources {
                problem.add_source(num_shards, load_per_shard);
            }
            problem
        })
    }

    fn node_assignments_strategy(num_sources: usize) -> impl Strategy<Value = Vec<u32>> {
        proptest::collection::vec(0u32..3u32, num_sources)
    }

    fn initial_solution_strategy(
        num_nodes: usize,
        num_sources: usize,
    ) -> impl Strategy<Value = SchedulingSolution> {
        proptest::collection::vec(node_assignments_strategy(num_sources), num_nodes).prop_map(
            move |node_assignments: Vec<Vec<u32>>| {
                let mut solution = SchedulingSolution::with_num_nodes(num_nodes);
                for (node_id, node_assignment) in node_assignments.iter().enumerate() {
                    for (source_id, num_shards) in node_assignment.iter().copied().enumerate() {
                        if num_shards > 0 {
                            solution.node_assignments[node_id]
                                .add_shard(source_id as u32, num_shards);
                        }
                    }
                }
                solution
            },
        )
    }

    fn problem_solution_strategy() -> impl Strategy<Value = (SchedulingProblem, SchedulingSolution)>
    {
        ((0usize..3), (0usize..3)).prop_flat_map(move |(num_nodes, num_sources)| {
            (
                problem_strategy(num_nodes, num_sources),
                initial_solution_strategy(num_nodes, num_sources),
            )
        })
    }

    proptest! {
        #[test]
        fn test_proptest_post_conditions((problem, solution) in problem_solution_strategy()) {
            solve(&problem, solution);
        }
    }
}
