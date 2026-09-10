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
use std::collections::HashMap;

use quickwit_proto::types::SourceUid;
use rand::prelude::IndexedRandom;
use rand::rng;
use rand::seq::SliceRandom;

use super::az_aware_placement::{AvailabilityZoneGroup, PlacementState, PlannedOpen};

impl PlacementState {
    fn compare_density(
        &self,
        source: &SourceUid,
        left: &AvailabilityZoneGroup,
        right: &AvailabilityZoneGroup,
    ) -> Ordering {
        let left_count = self.source_count(source, left) as u128;
        let right_count = self.source_count(source, right) as u128;
        let left_nodes = self.group_size(left) as u128;
        let right_nodes = self.group_size(right) as u128;
        (left_count * right_nodes).cmp(&(right_count * left_nodes))
    }

    pub fn plan_fresh_openings(
        &mut self,
        num_shards_by_source: HashMap<SourceUid, usize>,
    ) -> Vec<PlannedOpen> {
        if self.nodes.is_empty() {
            return Vec::new();
        }
        let mut requests: Vec<SourceUid> = num_shards_by_source
            .into_iter()
            .flat_map(|(source, count)| std::iter::repeat_n(source, count))
            .collect();
        let mut rng = rng();
        requests.shuffle(&mut rng);
        let mut plan = Vec::with_capacity(requests.len());
        for source in requests {
            let mut candidates: Vec<usize> = if self.is_balanced() {
                (0..self.nodes.len())
                    .filter(|&index| self.is_balanced_after_increment(index))
                    .collect()
            } else {
                let minimum = self.minimum();
                self.nodes
                    .iter()
                    .enumerate()
                    .filter_map(|(index, node)| {
                        (node.projected_num_open_shards == minimum).then_some(index)
                    })
                    .collect()
            };
            assert!(!candidates.is_empty());
            let mut best_groups = Vec::new();
            for &index in &candidates {
                let group = &self.nodes[index].az_group;
                if best_groups.contains(group) {
                    continue;
                }
                let comparison = best_groups
                    .first()
                    .map(|best| self.compare_density(&source, group, best));
                match comparison {
                    None | Some(Ordering::Less) => best_groups = vec![group.clone()],
                    Some(Ordering::Equal) => best_groups.push(group.clone()),
                    Some(Ordering::Greater) => {}
                }
            }
            let target_group = best_groups.choose(&mut rng).unwrap().clone();
            candidates.retain(|&index| self.nodes[index].az_group == target_group);
            let group_minimum = candidates
                .iter()
                .map(|&index| self.nodes[index].projected_num_open_shards)
                .min()
                .unwrap();
            candidates
                .retain(|&index| self.nodes[index].projected_num_open_shards == group_minimum);
            let target = *candidates.choose(&mut rng).unwrap();
            self.nodes[target].projected_num_open_shards += 1;
            *self
                .source_counts_by_group
                .entry((source.clone(), target_group))
                .or_default() += 1;
            plan.push(PlannedOpen {
                source_uid: source,
                target_node_id: self.nodes[target].node_id.clone(),
                predecessor: None,
            });
        }
        plan
    }
}
