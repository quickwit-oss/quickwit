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

use std::collections::HashMap;

use quickwit_proto::ingest::{Shard, ShardState};
use quickwit_proto::types::{IndexUid, NodeId, ShardId, SourceUid};

use super::az_aware_placement::{
    AvailabilityZoneGroup, PendingReplacement, PlacementNode, PlacementState, PlannedOpen,
    loads_are_balanced,
};

pub(super) type NodeSpec<'a> = (&'a str, Option<&'a str>, Vec<(&'a str, usize)>);
type RetiringSpec<'a> = (&'a str, Option<&'a str>, &'a str, usize);

pub(super) fn source_uid(source_id: &str) -> SourceUid {
    SourceUid {
        index_uid: IndexUid::for_test("az-placement", 0),
        source_id: source_id.to_string(),
    }
}

fn shard(shard_id: u64, source_id: &str, ingester_id: &str) -> Shard {
    Shard {
        index_uid: Some(IndexUid::for_test("az-placement", 0)),
        source_id: source_id.to_string(),
        shard_id: Some(ShardId::from(shard_id)),
        ingester_id: ingester_id.to_string(),
        shard_state: ShardState::Open as i32,
        ..Default::default()
    }
}

pub(super) fn scenario(
    ready: Vec<NodeSpec<'_>>,
    retiring: Vec<RetiringSpec<'_>>,
) -> (PlacementState, Vec<PendingReplacement>) {
    let mut next_shard_id = 0;
    let mut nodes = Vec::new();
    let mut source_counts_by_group = HashMap::new();
    let mut ready_nodes_by_group = HashMap::new();
    for (node_id, az, source_counts) in ready {
        let group = az.map(str::to_string);
        *ready_nodes_by_group.entry(group.clone()).or_default() += 1;
        let mut movable_open_shards = Vec::new();
        for (source_id, count) in source_counts {
            *source_counts_by_group
                .entry((source_uid(source_id), group.clone()))
                .or_default() += count;
            for _ in 0..count {
                movable_open_shards.push(shard(next_shard_id, source_id, node_id));
                next_shard_id += 1;
            }
        }
        nodes.push(PlacementNode {
            node_id: NodeId::from_str(node_id),
            az_group: group,
            projected_num_open_shards: movable_open_shards.len(),
            movable_open_shards,
        });
    }
    let mut pending = Vec::new();
    for (node_id, az, source_id, count) in retiring {
        let group = az.map(str::to_string);
        *source_counts_by_group
            .entry((source_uid(source_id), group.clone()))
            .or_default() += count;
        for _ in 0..count {
            pending.push(PendingReplacement {
                predecessor: shard(next_shard_id, source_id, node_id),
                origin_group: group.clone(),
            });
            next_shard_id += 1;
        }
    }
    (
        PlacementState {
            nodes,
            source_counts_by_group,
            ready_nodes_by_group,
        },
        pending,
    )
}

fn node_group(state: &PlacementState, node_id: &str) -> AvailabilityZoneGroup {
    state
        .nodes
        .iter()
        .find(|node| node.node_id == node_id)
        .unwrap()
        .az_group
        .clone()
}

pub(super) fn loads(state: &PlacementState) -> Vec<usize> {
    state
        .nodes
        .iter()
        .map(|node| node.projected_num_open_shards)
        .collect()
}

pub(super) fn assert_real_max_to_min_moves(initial: &PlacementState, plan: &[PlannedOpen]) {
    let mut current: HashMap<String, usize> = initial
        .nodes
        .iter()
        .map(|node| (node.node_id.to_string(), node.projected_num_open_shards))
        .collect();
    for item in plan {
        let predecessor = item.predecessor.as_ref().unwrap();
        if let Some(donor_load) = current.get(&predecessor.ingester_id).copied() {
            let minimum = *current.values().min().unwrap();
            let maximum = *current.values().max().unwrap();
            let target_load = current[item.target_node_id.as_str()];
            let potential_before: usize = current.values().map(|load| load * load).sum();
            assert_eq!(donor_load, maximum);
            assert_eq!(target_load, minimum);
            *current.get_mut(&predecessor.ingester_id).unwrap() -= 1;
            *current.get_mut(item.target_node_id.as_str()).unwrap() += 1;
            let potential_after: usize = current.values().map(|load| load * load).sum();
            assert!(potential_after < potential_before);
        } else {
            *current.get_mut(item.target_node_id.as_str()).unwrap() += 1;
        }
    }
    assert!(loads_are_balanced(current.values().copied()));
}

#[test]
fn two_node_counterexample_moves_to_the_actual_minimum_and_stops() {
    let (mut state, pending) = scenario(
        vec![("a", None, vec![("source", 4)]), ("b", None, vec![])],
        vec![],
    );
    let initial = state.clone();
    let plan = state.plan_rebalance(pending);
    assert_eq!(plan.len(), 2);
    assert!(plan.iter().all(|item| item.target_node_id == "b"));
    assert_real_max_to_min_moves(&initial, &plan);
    assert_eq!(loads(&state), vec![2, 2]);
    assert!(state.plan_rebalance(Vec::new()).is_empty());
    eprintln!(
        "scenario=4/0 moves=2 final={:?} fixed_point=true",
        loads(&state)
    );
}

#[test]
fn constrained_nine_node_retirement_keeps_only_feasible_locality() {
    let mut ready = Vec::new();
    for az in ["a", "b", "c"] {
        let count = if az == "a" { 2 } else { 3 };
        for ordinal in 0..count {
            ready.push((
                Box::leak(format!("{az}-{ordinal}").into_boxed_str()) as &str,
                Some(az),
                vec![("source", 9)],
            ));
        }
    }
    let (mut state, pending) = scenario(ready, vec![("a-retiring", Some("a"), "source", 9)]);
    let original = state.clone();
    let plan = state.plan_rebalance(pending);
    let local = plan
        .iter()
        .filter(|item| node_group(&state, item.target_node_id.as_str()).as_deref() == Some("a"))
        .count();
    assert_eq!(local, 3);
    assert!(state.is_balanced());
    let mut final_loads = loads(&state);
    final_loads.sort_unstable();
    assert_eq!(final_loads, vec![10, 10, 10, 10, 10, 10, 10, 11]);
    assert_real_max_to_min_moves(&original, &plan);

    state.nodes.push(PlacementNode {
        node_id: NodeId::from_str("a-returned"),
        az_group: Some("a".to_string()),
        projected_num_open_shards: 0,
        movable_open_shards: Vec::new(),
    });
    *state
        .ready_nodes_by_group
        .get_mut(&Some("a".to_string()))
        .unwrap() += 1;
    let return_initial = state.clone();
    let return_plan = state.plan_rebalance(Vec::new());
    assert_eq!(return_plan.len(), 9);
    assert!(return_plan[..3].iter().all(|item| {
        item.predecessor
            .as_ref()
            .unwrap()
            .ingester_id
            .starts_with("a-")
            && item.target_node_id == "a-returned"
    }));
    assert!(
        return_plan[3..]
            .iter()
            .all(|item| item.target_node_id == "a-returned")
    );
    assert_real_max_to_min_moves(&return_initial, &return_plan);
    assert_eq!(loads(&state), vec![9; 9]);
    assert!(state.plan_rebalance(Vec::new()).is_empty());
    eprintln!(
        "scenario=9-node-remove-return retire_same_az={local}/9 return_same_az=3/9 final=9x9 \
         fixed_point=true"
    );
}

#[test]
fn healthy_thirty_nine_node_rollout_retains_all_retirement_locality() {
    let mut ready = Vec::new();
    for (az, count) in [("a", 12), ("b", 13), ("c", 13)] {
        for ordinal in 0..count {
            ready.push((
                Box::leak(format!("{az}-{ordinal}").into_boxed_str()) as &str,
                Some(az),
                vec![("source", 22)],
            ));
        }
    }
    let (mut state, pending) = scenario(ready, vec![("a-retiring", Some("a"), "source", 22)]);
    let plan = state.plan_rebalance(pending);
    let local = plan
        .iter()
        .filter(|item| node_group(&state, item.target_node_id.as_str()).as_deref() == Some("a"))
        .count();
    assert_eq!(local, 22);
    assert!(state.is_balanced());
    assert_eq!(loads(&state).into_iter().max(), Some(24));
    eprintln!("scenario=39-node-rollout same_az={local}/22 final_range=22..24");
}

#[test]
fn returned_ordinal_uses_local_moves_then_restores_forced_sources() {
    let mut ready = vec![
        ("a-0", Some("a"), vec![("base", 11)]),
        ("a-1", Some("a"), vec![("base", 10)]),
        ("a-2", Some("a"), vec![]),
    ];
    for az in ["b", "c"] {
        for ordinal in 0..3 {
            ready.push((
                Box::leak(format!("{az}-{ordinal}").into_boxed_str()) as &str,
                Some(az),
                vec![("base", 7), ("forced", 3)],
            ));
        }
    }
    let (mut state, pending) = scenario(ready, vec![]);
    let plan = state.plan_rebalance(pending);
    assert_eq!(plan.len(), 9);
    for item in &plan[..3] {
        assert_eq!(
            item.predecessor.as_ref().unwrap().ingester_id.as_bytes()[0],
            b'a'
        );
        assert_eq!(item.target_node_id, "a-2");
    }
    assert!(
        plan[3..]
            .iter()
            .all(|item| item.source_uid.source_id == "forced" && item.target_node_id == "a-2")
    );
    for az in ["a", "b", "c"] {
        assert_eq!(
            state.source_count(&source_uid("forced"), &Some(az.to_string())),
            6
        );
    }
    assert_eq!(loads(&state), vec![9; 9]);
    eprintln!("scenario=returned-ordinal local_moves=3 source_aware_returns=6 final=9x9");
}

#[test]
fn unequal_az_sizes_do_not_overload_the_small_group() {
    let mut ready = vec![("a-0", Some("a"), vec![("source", 9)])];
    for ordinal in 0..4 {
        ready.push((
            Box::leak(format!("b-{ordinal}").into_boxed_str()) as &str,
            Some("b"),
            vec![("source", 9)],
        ));
    }
    let (mut state, pending) = scenario(ready, vec![("a-retiring", Some("a"), "source", 9)]);
    let plan = state.plan_rebalance(pending);
    let local = plan
        .iter()
        .filter(|item| item.target_node_id == "a-0")
        .count();
    assert!(local < 9);
    assert!(state.is_balanced());
    eprintln!(
        "scenario=unequal-az same_az={local}/9 final={:?}",
        loads(&state)
    );
}

#[test]
fn fresh_openings_repair_source_skew_when_globally_safe() {
    let ready = vec![
        ("a-0", Some("a"), vec![("skew", 3), ("fill", 2)]),
        ("a-1", Some("a"), vec![("skew", 2), ("fill", 3)]),
        ("b-0", Some("b"), vec![("fill", 5)]),
        ("b-1", Some("b"), vec![("fill", 5)]),
    ];
    let (mut state, _) = scenario(ready, vec![]);
    let plan = state.plan_fresh_openings(HashMap::from([(source_uid("skew"), 2)]));
    assert!(
        plan.iter()
            .all(|item| item.target_node_id.as_str().starts_with("b-"))
    );
    assert_eq!(
        state.source_count(&source_uid("skew"), &Some("b".into())),
        2
    );
    assert!(state.is_balanced());
    eprintln!("scenario=fresh-skew targets=b,b source_counts=a:5,b:2");
}

#[test]
fn production_loop_shape_converges_in_one_full_plan() {
    let mut ready = vec![("node-0", Some("a"), vec![("source", 129)])];
    for ordinal in 1..=9 {
        ready.push((
            Box::leak(format!("node-{ordinal}").into_boxed_str()) as &str,
            Some(if ordinal <= 6 { "a" } else { "b" }),
            vec![("source", 15)],
        ));
    }
    for ordinal in 10..=18 {
        ready.push((
            Box::leak(format!("node-{ordinal}").into_boxed_str()) as &str,
            Some(if ordinal <= 12 { "b" } else { "c" }),
            vec![("source", 16)],
        ));
    }
    let (mut state, pending) = scenario(ready, vec![]);
    let initial = state.clone();
    assert_eq!(loads(&state).into_iter().sum::<usize>(), 408);
    let plan = state.plan_rebalance(pending);
    assert_eq!(plan.len(), 106);
    assert_real_max_to_min_moves(&initial, &plan);
    assert!(state.plan_rebalance(Vec::new()).is_empty());
    let final_loads = loads(&state);
    eprintln!(
        "scenario=live-loop ready_shards=408 ready_ingesters=19 first_plan=106 second_plan=0 \
         final_min={} final_max={}",
        final_loads.iter().min().unwrap(),
        final_loads.iter().max().unwrap()
    );
}
