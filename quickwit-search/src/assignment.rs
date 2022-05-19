// Copyright (C) 2021 Quickwit, Inc.
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

use std::collections::hash_map::DefaultHasher;
use std::collections::{HashMap, HashSet};
use std::hash::{Hash, Hasher};

use crate::search_client_pool::Job;

struct Node<N: NodeId> {
    pub node_id: N,
    pub load: u64,
}

impl<N: NodeId> Hash for Node<N> {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.node_id.hash(state);
    }
}

impl<N: NodeId> Node<N> {
    fn new(node_id: N) -> Node<N> {
        Node {
            node_id,
            load: 0u64,
        }
    }
}

/// Sorts the list of node base on rendez-vous-hashing.
/// Nodes are ordered by decreasing order of computed `hash_key`
fn sort_by_rendez_vous_hash<H: Hash>(nodes: &mut [H], split_id: &str) {
    let mut salted_hash = DefaultHasher::new();
    split_id.hash(&mut salted_hash);
    nodes.sort_by_cached_key(|node| {
        let mut state = salted_hash.clone();
        node.hash(&mut state);
        state.finish()
    });
}

fn pick_node_for_job<'a, N: NodeId, J: Job>(
    job: &J,
    nodes: &'a mut [Node<N>],
    max_load_per_node: u64,
) -> Option<(usize, &'a mut Node<N>)> {
    sort_by_rendez_vous_hash(nodes, job.split_id());
    let job_cost = job.cost() as u64;
    nodes
        .iter_mut()
        .filter(|node: &&mut Node<N>| {
            dbg!(node.load);
            dbg!(job_cost);
            node.load + job_cost <= max_load_per_node
        })
        .enumerate()
        .next()
}

pub trait NodeId: Hash + Eq + Copy {}

impl<'a> NodeId for &'a str {}
impl<'a> NodeId for u64 {}

fn assign_job_to_node<N: NodeId, J: Job>(
    job: &J,
    node: &mut Node<N>,
    assignment_map: &mut HashMap<N, Vec<J>>,
) {
    node.load += job.cost() as u64;
    assignment_map
        .entry(node.node_id)
        .or_default()
        .push(job.clone());
}

fn ceildiv(numerator: u64, denominator: u64) -> u64 {
    (numerator + (denominator - 1)) / denominator
}

pub struct Assignment<N: NodeId, J: Job> {
    pub node_to_jobs: HashMap<N, Vec<J>>,
    pub statistics: AssignmentStatistics,
}

impl<N: NodeId, J: Job> Default for Assignment<N, J> {
    fn default() -> Self {
        Assignment {
            node_to_jobs: Default::default(),
            statistics: AssignmentStatistics::default(),
        }
    }
}

#[derive(Debug, Default)]
pub struct AssignmentStatistics {
    affinity_counts: [usize; 3],
    max_cost: u64,
}

impl AssignmentStatistics {
    fn max_update(&mut self, other: Self) {
        self.max_cost = self.max_cost.max(other.max_cost);
        for i in 0..3 {
            self.affinity_counts[i] = self.affinity_counts[i].max(other.affinity_counts[i]);
        }
    }

    fn score(&self) -> u64 {
        self.max_cost + self.affinity_counts[1] as u64 / 2 + self.affinity_counts[2] as u64
    }
}

fn assign_jobs_to_nodes_given_limit<N: NodeId, J: Job>(
    jobs: &[J],
    nodes: &mut [Node<N>],
    max_load_per_node: u64,
) -> Option<Assignment<N, J>> {
    let mut affinity_counts = [0; 3];
    let mut node_to_jobs: HashMap<N, Vec<J>> = HashMap::with_capacity(nodes.len());
    for job in jobs {
        let (affinity, chosen_node) = pick_node_for_job(job, nodes, max_load_per_node)?;
        assign_job_to_node(job, chosen_node, &mut node_to_jobs);
        affinity_counts[affinity] += 1;
    }
    let max_cost = node_to_jobs
        .iter()
        .map(|(_, jobs)| jobs.iter().map(|job| job.cost() as u64).sum())
        .max()
        .unwrap_or(0);
    Some(Assignment {
        node_to_jobs,
        statistics: AssignmentStatistics {
            affinity_counts,
            max_cost,
        },
    })
}

pub fn assign_jobs_to_nodes<N: NodeId, J: Job>(
    node_ids: &[N],
    mut jobs: Vec<J>,
) -> Assignment<N, J> {
    if jobs.is_empty() {
        return Assignment::default();
    }
    if node_ids.is_empty() {
        panic!("No node available.");
    }
    jobs.sort_by_key(|job| std::cmp::Reverse(job.cost()));
    let mut nodes: Vec<Node<N>> = node_ids.iter().cloned().map(Node::new).collect();

    let total_cost: u64 = jobs.iter().map(|job| job.cost() as u64).sum();
    let margin_cost = jobs
        .iter()
        .take(jobs.len())
        .last()
        .map(|job| job.cost())
        .unwrap() as u64;

    let mut solutions = Vec::new();

    let mut max_load_per_nodes = HashSet::new();
    let max_load_per_node_low = ceildiv(total_cost, node_ids.len() as u64);
    let max_job_cost = jobs[0].cost() as u64;
    if max_load_per_node_low >= max_job_cost {
        max_load_per_nodes.insert(max_load_per_node_low);
    } else {
        max_load_per_nodes.insert(max_job_cost);
    }
    let max_load_per_node_high = max_load_per_node_low + margin_cost;
    if max_load_per_node_high >= max_job_cost {
        max_load_per_nodes.insert(max_load_per_node_high);
    }
    dbg!(&max_load_per_nodes);

    for max_load_per_node in max_load_per_nodes {
        solutions.extend(assign_jobs_to_nodes_given_limit(
            &jobs[..],
            &mut nodes,
            max_load_per_node,
        ));
    }

    println!("-----");
    for solution in &solutions {
        println!("{:?}", &solution.statistics);
    }

    let v = solutions
        .into_iter()
        .min_by_key(|solution| solution.statistics.score())
        .unwrap();
    println!("pciked {:?}", &v.statistics);
    v
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use ::rand::thread_rng;
    use rand::distributions::Uniform;
    use rand::prelude::SliceRandom;
    use rand::Rng;

    use crate::assignment::{assign_jobs_to_nodes, AssignmentStatistics};
    use crate::search_client_pool::Job;

    use super::sort_by_rendez_vous_hash;

    #[test]
    fn test_sort_by_rendez_vous_deterministic() {
        let mut nodes: Vec<usize> = vec![1, 2, 3, 4, 5, 6, 7, 8, 10];
        sort_by_rendez_vous_hash(&mut nodes[..], "hello");
        assert_eq!(&nodes[..], &[3, 6, 8, 2, 10, 1, 5, 4, 7]);
        nodes.shuffle(&mut thread_rng());
        sort_by_rendez_vous_hash(&mut nodes[..], "hello");
        assert_eq!(&nodes[..], &[3, 6, 8, 2, 10, 1, 5, 4, 7]);
    }

    #[test]
    fn test_sort_by_rendez_vous_remove_one() {
        let mut nodes: Vec<usize> = vec![1, 3, 4, 5, 6, 7, 8, 10];
        sort_by_rendez_vous_hash(&mut nodes[..], "hello");
        assert_eq!(&nodes[..], &[3, 6, 8, 10, 1, 5, 4, 7]);
    }

    impl<'a> Job for (&'a str, u32) {
        fn split_id(&self) -> &str {
            self.0
        }

        fn cost(&self) -> u32 {
            self.1
        }
    }

    fn build_hashmap(
        assignments: &[(&'static str, Vec<(&'static str, u32)>)],
    ) -> HashMap<&'static str, Vec<(&'static str, u32)>> {
        assignments.iter().cloned().collect()
    }

    #[test]
    fn test_assign_no_jobs_some_nodes() {
        let no_jobs: Vec<(&'static str, u32)> = Vec::new();
        let assignment = assign_jobs_to_nodes(&["node1", "node2"], no_jobs);
        assert!(assignment.node_to_jobs.is_empty());
    }

    #[test]
    fn test_assign_no_jobs_no_nodes() {
        let no_jobs: Vec<(&'static str, u32)> = Vec::new();
        let no_nodes: &[&str] = &[];
        let assignment = assign_jobs_to_nodes(no_nodes, no_jobs);
        assert!(assignment.node_to_jobs.is_empty());
    }

    #[test]
    #[should_panic(expected = "No node available.")]
    fn test_assign_some_jobs_no_nodes() {
        let no_nodes: &[&str] = &[];
        assign_jobs_to_nodes(no_nodes, vec![("job", 10u32)]);
    }

    #[test]
    fn test_assign_jobs_to_nodes() {
        let assignment = assign_jobs_to_nodes(&["node1", "node2"], vec![("split1", 100u32)]);
        let expected = build_hashmap(&[("node2", vec![("split1", 100u32)])]);
        assert_eq!(assignment.node_to_jobs, expected);
    }

    const NUM_SIMULATIONS: usize = 100;

    fn simulate(costs: &[u32], num_nodes: usize) -> AssignmentStatistics {
        assert!(costs.len() < 10_000);
        let split_ids: Vec<String> = (0..costs.len()).map(|i| format!("{}", i)).collect();
        let node_ids: Vec<u64> = thread_rng()
            .sample_iter(Uniform::new(0, 10_000_000))
            .take(num_nodes)
            .collect();
        let mut max_statistics = AssignmentStatistics::default();
        for _ in 0..NUM_SIMULATIONS {
            let jobs: Vec<(&str, u32)> = costs
                .iter()
                .cloned()
                .enumerate()
                .map(|(i, cost)| (split_ids[i].as_str(), cost))
                .collect();
            let assignment = assign_jobs_to_nodes(&node_ids[..], jobs);
            let statistics = assignment.statistics;
            max_statistics.max_update(statistics);
        }
        max_statistics
    }

    #[test]
    fn test_simulate_assignment_nodes_splits() {
        let costs = vec![3u32; 4];
        let outcome = simulate(&costs[..], 4);
        dbg!(&outcome);
    }

    #[test]
    fn test_simulate_assignment_lots_of_splits() {
        let costs = vec![3u32; 100];
        let statistics = simulate(&costs[..], 4);
        assert!(statistics.max_cost <= 26);
        assert!(statistics.affinity_counts[2] <= 3);
    }

    #[test]
    fn test_simulate_assignment_big_large_mix() {
        let mut costs = vec![3u32; 100];
        costs.extend(vec![1u32; 100]);
        let statistics = simulate(&costs[..], 4);
        assert!(statistics.max_cost <= 26);
        assert!(statistics.affinity_counts[2] <= 3);
    }
}
