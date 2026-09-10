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

use std::cmp::Ordering;
use std::collections::{HashMap, HashSet};

use fnv::FnvHashSet;
use quickwit_ingest::IngesterPool;
use quickwit_proto::ingest::Shard;
use quickwit_proto::ingest::ingester::IngesterStatus;
use quickwit_proto::types::{NodeId, SourceUid};
use rand::prelude::IndexedRandom;
use rand::seq::SliceRandom;
use rand::{Rng, RngExt, rng};

use crate::model::ControlPlaneModel;

pub(super) type AvailabilityZoneGroup = Option<String>;

#[derive(Clone, Debug)]
pub(super) struct PlannedOpen {
    pub source_uid: SourceUid,
    pub target_node_id: NodeId,
    pub predecessor: Option<Shard>,
}

#[derive(Clone, Debug)]
pub(super) struct PendingReplacement {
    pub predecessor: Shard,
    pub origin_group: AvailabilityZoneGroup,
}

#[derive(Clone, Debug)]
pub(super) struct PlacementNode {
    pub node_id: NodeId,
    pub az_group: AvailabilityZoneGroup,
    pub projected_num_open_shards: usize,
    /// Only input-snapshot shards are movable. A planned replacement cannot become a donor in the
    /// same plan.
    pub movable_open_shards: Vec<Shard>,
}

/// One ephemeral snapshot drives planning. In particular, the destination selected here is kept
/// in `PlannedOpen` and is never reconstructed by the executor.
#[derive(Clone, Debug)]
pub(super) struct PlacementState {
    pub nodes: Vec<PlacementNode>,
    pub source_counts_by_group: HashMap<(SourceUid, AvailabilityZoneGroup), usize>,
    pub ready_nodes_by_group: HashMap<AvailabilityZoneGroup, usize>,
}

pub(super) fn loads_are_balanced(loads: impl Iterator<Item = usize>) -> bool {
    let mut minimum = usize::MAX;
    let mut maximum = 0;
    let mut num_loads = 0;
    for load in loads {
        minimum = minimum.min(load);
        maximum = maximum.max(load);
        num_loads += 1;
    }
    if num_loads <= 1 {
        return true;
    }
    maximum < minimum + minimum.div_ceil(10).max(2)
}

impl PlacementState {
    pub fn from_model(
        ingester_pool: &IngesterPool,
        unavailable_ingesters: &FnvHashSet<NodeId>,
        model: &ControlPlaneModel,
    ) -> (Self, Vec<PendingReplacement>) {
        let mut nodes = Vec::new();
        let mut node_index_by_id = HashMap::new();
        let mut active_group_by_id = HashMap::new();
        let mut retiring_ingesters = HashSet::new();
        let mut ready_nodes_by_group: HashMap<AvailabilityZoneGroup, usize> = HashMap::new();

        for (node_id, ingester) in ingester_pool.keys_values() {
            if ingester.status.is_ready() || ingester.status == IngesterStatus::Retiring {
                active_group_by_id.insert(node_id.clone(), ingester.availability_zone.clone());
            }
            if ingester.status == IngesterStatus::Retiring {
                retiring_ingesters.insert(node_id.clone());
                continue;
            }
            if !ingester.status.is_ready() || unavailable_ingesters.contains(&node_id) {
                continue;
            }
            let az_group = ingester.availability_zone;
            *ready_nodes_by_group.entry(az_group.clone()).or_default() += 1;
            node_index_by_id.insert(node_id.clone(), nodes.len());
            nodes.push(PlacementNode {
                node_id,
                az_group,
                projected_num_open_shards: 0,
                movable_open_shards: Vec::new(),
            });
        }

        let mut source_counts_by_group = HashMap::new();
        let mut pending_replacements = Vec::new();
        for shard_entry in model.all_shards() {
            if !shard_entry.is_open() {
                continue;
            }
            let ingester_id = NodeId::from_str(&shard_entry.ingester_id);
            let Some(az_group) = active_group_by_id.get(&ingester_id).cloned() else {
                // Unavailable/decommissioned hosts cannot perform the predecessor close.
                continue;
            };
            let source_uid = shard_entry.shard.source_uid();
            *source_counts_by_group
                .entry((source_uid, az_group.clone()))
                .or_default() += 1;
            if let Some(&node_index) = node_index_by_id.get(&ingester_id) {
                nodes[node_index].projected_num_open_shards += 1;
                nodes[node_index]
                    .movable_open_shards
                    .push(shard_entry.shard.clone());
            } else if retiring_ingesters.contains(&ingester_id) {
                pending_replacements.push(PendingReplacement {
                    predecessor: shard_entry.shard.clone(),
                    origin_group: az_group,
                });
            }
        }
        (
            Self {
                nodes,
                source_counts_by_group,
                ready_nodes_by_group,
            },
            pending_replacements,
        )
    }

    pub fn is_balanced(&self) -> bool {
        loads_are_balanced(self.nodes.iter().map(|node| node.projected_num_open_shards))
    }

    pub(super) fn is_balanced_after_increment(&self, target: usize) -> bool {
        loads_are_balanced(
            self.nodes
                .iter()
                .enumerate()
                .map(|(index, node)| node.projected_num_open_shards + usize::from(index == target)),
        )
    }

    pub(super) fn minimum(&self) -> usize {
        self.nodes
            .iter()
            .map(|node| node.projected_num_open_shards)
            .min()
            .unwrap()
    }

    fn maximum(&self) -> usize {
        self.nodes
            .iter()
            .map(|node| node.projected_num_open_shards)
            .max()
            .unwrap()
    }

    pub fn source_count(&self, source: &SourceUid, group: &AvailabilityZoneGroup) -> usize {
        self.source_counts_by_group
            .get(&(source.clone(), group.clone()))
            .copied()
            .unwrap_or(0)
    }

    pub(super) fn group_size(&self, group: &AvailabilityZoneGroup) -> usize {
        // A fully retiring group can have no Ready denominator. One keeps the score defined while
        // actual placement feasibility still comes only from `nodes`.
        self.ready_nodes_by_group
            .get(group)
            .copied()
            .unwrap_or(1)
            .max(1)
    }

    fn source_surplus(
        &self,
        source: &SourceUid,
        donor: &AvailabilityZoneGroup,
        target: &AvailabilityZoneGroup,
    ) -> (i128, i128) {
        let donor_count = self.source_count(source, donor) as i128;
        let target_count = self.source_count(source, target) as i128;
        let donor_nodes = self.group_size(donor) as i128;
        let target_nodes = self.group_size(target) as i128;
        (
            donor_count * target_nodes - target_count * donor_nodes,
            donor_nodes * target_nodes,
        )
    }

    fn compare_surplus(
        &self,
        left: (&SourceUid, &AvailabilityZoneGroup, &AvailabilityZoneGroup),
        right: (&SourceUid, &AvailabilityZoneGroup, &AvailabilityZoneGroup),
    ) -> Ordering {
        let left = self.source_surplus(left.0, left.1, left.2);
        let right = self.source_surplus(right.0, right.1, right.2);
        (left.0 * right.1).cmp(&(right.0 * left.1))
    }

    fn apply_source_move(
        &mut self,
        source: &SourceUid,
        donor: &AvailabilityZoneGroup,
        target: &AvailabilityZoneGroup,
    ) {
        if donor == target {
            return;
        }
        let donor_count = self
            .source_counts_by_group
            .get_mut(&(source.clone(), donor.clone()))
            .expect("planned predecessor must be present in donor source count");
        *donor_count = donor_count.checked_sub(1).expect("positive donor count");
        *self
            .source_counts_by_group
            .entry((source.clone(), target.clone()))
            .or_default() += 1;
    }

    fn choose_mandatory_pair<R: Rng + ?Sized>(
        &self,
        pending: &[PendingReplacement],
        rng: &mut R,
    ) -> (usize, usize) {
        if self.is_balanced() {
            let mut local_pairs = Vec::new();
            for (pending_index, replacement) in pending.iter().enumerate() {
                let Some(group_minimum) = self
                    .nodes
                    .iter()
                    .filter(|node| node.az_group == replacement.origin_group)
                    .map(|node| node.projected_num_open_shards)
                    .min()
                else {
                    continue;
                };
                for (node_index, node) in self.nodes.iter().enumerate() {
                    if node.az_group == replacement.origin_group
                        && node.projected_num_open_shards == group_minimum
                        && self.is_balanced_after_increment(node_index)
                    {
                        local_pairs.push((pending_index, node_index));
                    }
                }
            }
            if !local_pairs.is_empty() {
                let lowest_load = local_pairs
                    .iter()
                    .map(|pair| self.nodes[pair.1].projected_num_open_shards)
                    .min()
                    .unwrap();
                local_pairs
                    .retain(|pair| self.nodes[pair.1].projected_num_open_shards == lowest_load);
                return *local_pairs.choose(rng).unwrap();
            }
        }

        let minimum = self.minimum();
        let mut pairs = Vec::new();
        for pending_index in 0..pending.len() {
            for (node_index, node) in self.nodes.iter().enumerate() {
                if node.projected_num_open_shards == minimum {
                    pairs.push((pending_index, node_index));
                }
            }
        }
        let same_group: Vec<(usize, usize)> = pairs
            .iter()
            .copied()
            .filter(|pair| pending[pair.0].origin_group == self.nodes[pair.1].az_group)
            .collect();
        if let Some(pair) = same_group.choose(rng) {
            return *pair;
        }

        let mut best = Vec::new();
        for pair in pairs {
            let candidate = &pending[pair.0];
            let source = candidate.predecessor.source_uid();
            let comparison = best.first().map(|best_pair: &(usize, usize)| {
                let incumbent = &pending[best_pair.0];
                let incumbent_source = incumbent.predecessor.source_uid();
                self.compare_surplus(
                    (
                        &source,
                        &candidate.origin_group,
                        &self.nodes[pair.1].az_group,
                    ),
                    (
                        &incumbent_source,
                        &incumbent.origin_group,
                        &self.nodes[best_pair.1].az_group,
                    ),
                )
            });
            match comparison {
                None | Some(Ordering::Greater) => best = vec![pair],
                Some(Ordering::Equal) => best.push(pair),
                Some(Ordering::Less) => {}
            }
        }
        *best.choose(rng).unwrap()
    }

    pub fn plan_rebalance(&mut self, pending: Vec<PendingReplacement>) -> Vec<PlannedOpen> {
        self.plan_rebalance_with_rng(pending, &mut rng())
    }

    pub(super) fn plan_rebalance_with_rng<R: Rng + ?Sized>(
        &mut self,
        mut pending: Vec<PendingReplacement>,
        rng: &mut R,
    ) -> Vec<PlannedOpen> {
        if self.nodes.is_empty() {
            return Vec::new();
        }
        pending.shuffle(rng);
        let mut plan = Vec::new();
        while !pending.is_empty() {
            let (pending_index, target_index) = self.choose_mandatory_pair(&pending, rng);
            let replacement = pending.swap_remove(pending_index);
            let source = replacement.predecessor.source_uid();
            let target_group = self.nodes[target_index].az_group.clone();
            self.nodes[target_index].projected_num_open_shards += 1;
            self.apply_source_move(&source, &replacement.origin_group, &target_group);
            plan.push(PlannedOpen {
                source_uid: source,
                target_node_id: self.nodes[target_index].node_id.clone(),
                predecessor: Some(replacement.predecessor),
            });
        }

        while !self.is_balanced() {
            let minimum = self.minimum();
            let maximum = self.maximum();
            assert!(maximum >= minimum + 2);
            let minimum_nodes: Vec<usize> = self
                .nodes
                .iter()
                .enumerate()
                .filter_map(|(index, node)| {
                    (node.projected_num_open_shards == minimum).then_some(index)
                })
                .collect();
            let maximum_nodes: Vec<usize> = self
                .nodes
                .iter()
                .enumerate()
                .filter_map(|(index, node)| {
                    (node.projected_num_open_shards == maximum
                        && !node.movable_open_shards.is_empty())
                    .then_some(index)
                })
                .collect();
            assert!(
                !maximum_nodes.is_empty(),
                "projected maximum needs an actual donor"
            );

            let mut local_pairs = Vec::new();
            for &donor in &maximum_nodes {
                for &target in &minimum_nodes {
                    if self.nodes[donor].az_group == self.nodes[target].az_group {
                        local_pairs.push((donor, target));
                    }
                }
            }
            let (donor, target, shard_index) = if let Some(&(donor, target)) =
                local_pairs.choose(rng)
            {
                let shard_index = rng.random_range(0..self.nodes[donor].movable_open_shards.len());
                (donor, target, shard_index)
            } else {
                let mut best = Vec::new();
                for &donor in &maximum_nodes {
                    for &target in &minimum_nodes {
                        for (shard_index, shard) in
                            self.nodes[donor].movable_open_shards.iter().enumerate()
                        {
                            let source = shard.source_uid();
                            let comparison = best.first().map(
                                |&(best_donor, best_target, best_shard): &(usize, usize, usize)| {
                                    let best_source = self.nodes[best_donor].movable_open_shards
                                        [best_shard]
                                        .source_uid();
                                    self.compare_surplus(
                                        (
                                            &source,
                                            &self.nodes[donor].az_group,
                                            &self.nodes[target].az_group,
                                        ),
                                        (
                                            &best_source,
                                            &self.nodes[best_donor].az_group,
                                            &self.nodes[best_target].az_group,
                                        ),
                                    )
                                },
                            );
                            let candidate = (donor, target, shard_index);
                            match comparison {
                                None | Some(Ordering::Greater) => best = vec![candidate],
                                Some(Ordering::Equal) => best.push(candidate),
                                Some(Ordering::Less) => {}
                            }
                        }
                    }
                }
                *best.choose(rng).unwrap()
            };

            let donor_group = self.nodes[donor].az_group.clone();
            let target_group = self.nodes[target].az_group.clone();
            let predecessor = self.nodes[donor]
                .movable_open_shards
                .swap_remove(shard_index);
            let source = predecessor.source_uid();
            self.nodes[donor].projected_num_open_shards -= 1;
            self.nodes[target].projected_num_open_shards += 1;
            self.apply_source_move(&source, &donor_group, &target_group);
            plan.push(PlannedOpen {
                source_uid: source,
                target_node_id: self.nodes[target].node_id.clone(),
                predecessor: Some(predecessor),
            });
        }
        plan
    }
}
