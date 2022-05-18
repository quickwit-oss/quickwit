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
use std::collections::HashMap;
use std::hash::{Hash, Hasher};

use crate::search_client_pool::Job;

#[derive(Debug, Clone)]
struct Node<N: NodeId> {
    pub node_id: N,
    pub load: u64,
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
fn sort_by_rendez_vous_hash<N: NodeId>(nodes: &mut [Node<N>], split_id: &str) {
    let mut salted_hash = DefaultHasher::new();
    split_id.hash(&mut salted_hash);
    nodes.sort_by_cached_key(|node| {
        let mut state = salted_hash.clone();
        node.node_id.hash(&mut state);
        state.finish()
    });
}

fn pick_node_for_job<'a, N: NodeId, J: Job>(job: &J, nodes: &'a mut [Node<N>]) -> &'a mut Node<N> {
    sort_by_rendez_vous_hash(nodes, job.split_id());

    // choose one of the the first two nodes based on least loaded
    let chosen_node_index: usize = if nodes.len() >= 2 {
        if nodes[0].load > nodes[1].load {
            1
        } else {
            0
        }
    } else {
        0
    };
    &mut nodes[chosen_node_index]
}

pub trait NodeId: Hash + Eq + Copy {}

fn assign_job_to_node<N: NodeId, J: Job>(
    job: J,
    node: &mut Node<N>,
    assignment_map: &mut HashMap<N, Vec<J>>,
) {
    node.load += job.cost() as u64;
    assignment_map.entry(node.node_id).or_default().push(job);
}

pub fn assign_jobs_to_nodes<N: NodeId, J: Job>(
    node_ids: &mut [N],
    jobs: Vec<J>,
) -> HashMap<N, Vec<J>> {
    let mut assignment_map: HashMap<N, Vec<J>> = HashMap::with_capacity(node_ids.len());
    let mut nodes: Vec<Node<N>> = node_ids.iter().cloned().map(Node::new).collect();
    for job in jobs {
        let chosen_node: &mut Node<N> = pick_node_for_job(&job, &mut nodes[..]);
        assign_job_to_node(job, chosen_node, &mut assignment_map);
    }
    assignment_map
}
