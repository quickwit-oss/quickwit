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

use rand::SeedableRng;
use rand::rngs::StdRng;

use super::az_aware_placement::PlacementState;
use super::az_aware_placement_tests::{
    NodeSpec, assert_real_max_to_min_moves, loads, scenario, source_uid,
};

type CompactNode = (&'static str, &'static str, &'static [(&'static str, usize)]);

const SOURCES: [&str; 7] = [
    "draft", "errors", "fantasy", "halo", "sports", "stat", "main",
];

/// Exact open-shard membership from the 20-node rollout that exposed the replacement loop. Source
/// names are shortened, while every node load and source/AZ count is preserved.
const LIVE_NODES: [CompactNode; 20] = [
    (
        "0",
        "c",
        &[
            ("draft", 2),
            ("fantasy", 3),
            ("sports", 3),
            ("stat", 3),
            ("main", 10),
        ],
    ),
    (
        "1",
        "a",
        &[
            ("draft", 3),
            ("errors", 3),
            ("fantasy", 3),
            ("sports", 4),
            ("stat", 5),
            ("main", 11),
        ],
    ),
    (
        "2",
        "b",
        &[
            ("draft", 2),
            ("errors", 2),
            ("fantasy", 2),
            ("stat", 3),
            ("main", 10),
        ],
    ),
    ("3", "b", &[("sports", 2), ("stat", 1), ("main", 1)]),
    (
        "4",
        "c",
        &[
            ("draft", 3),
            ("errors", 1),
            ("fantasy", 3),
            ("sports", 5),
            ("stat", 2),
            ("main", 7),
        ],
    ),
    (
        "5",
        "c",
        &[
            ("draft", 4),
            ("fantasy", 1),
            ("sports", 2),
            ("stat", 3),
            ("main", 11),
        ],
    ),
    (
        "6",
        "a",
        &[
            ("draft", 2),
            ("fantasy", 4),
            ("halo", 1),
            ("sports", 4),
            ("stat", 7),
            ("main", 14),
        ],
    ),
    (
        "7",
        "b",
        &[("draft", 2), ("fantasy", 1), ("sports", 1), ("stat", 1)],
    ),
    (
        "8",
        "a",
        &[
            ("draft", 2),
            ("errors", 2),
            ("fantasy", 5),
            ("sports", 4),
            ("stat", 7),
            ("main", 7),
        ],
    ),
    (
        "9",
        "a",
        &[
            ("draft", 3),
            ("errors", 1),
            ("fantasy", 5),
            ("sports", 4),
            ("stat", 9),
            ("main", 7),
        ],
    ),
    (
        "10",
        "c",
        &[
            ("errors", 1),
            ("fantasy", 2),
            ("sports", 3),
            ("stat", 6),
            ("main", 10),
        ],
    ),
    ("11", "b", &[("draft", 1), ("stat", 3), ("main", 1)]),
    ("12", "b", &[("draft", 1), ("stat", 1), ("main", 2)]),
    (
        "13",
        "c",
        &[
            ("draft", 2),
            ("errors", 2),
            ("fantasy", 1),
            ("sports", 1),
            ("stat", 3),
            ("main", 12),
        ],
    ),
    (
        "14",
        "a",
        &[
            ("draft", 2),
            ("errors", 2),
            ("fantasy", 4),
            ("sports", 3),
            ("stat", 7),
            ("main", 33),
        ],
    ),
    (
        "15",
        "c",
        &[
            ("errors", 1),
            ("fantasy", 4),
            ("sports", 2),
            ("stat", 3),
            ("main", 12),
        ],
    ),
    ("16", "b", &[("fantasy", 1), ("sports", 1), ("main", 2)]),
    (
        "17",
        "c",
        &[("fantasy", 2), ("sports", 4), ("stat", 3), ("main", 12)],
    ),
    ("18", "b", &[("draft", 1), ("stat", 1), ("main", 2)]),
    (
        "19",
        "a",
        &[
            ("draft", 3),
            ("errors", 1),
            ("fantasy", 2),
            ("sports", 2),
            ("stat", 8),
            ("main", 36),
        ],
    ),
];

fn live_rollout_fixture() -> Vec<NodeSpec<'static>> {
    LIVE_NODES
        .iter()
        .map(|(node, az, sources)| (*node, Some(*az), sources.to_vec()))
        .collect()
}

fn source_geography_metrics(state: &PlacementState) -> (f64, f64) {
    let ready_total: usize = state.ready_nodes_by_group.values().sum();
    let mut normalized_l1 = 0.0;
    let mut weighted_variance = 0.0;
    for source_id in SOURCES {
        let source = source_uid(source_id);
        let total: usize = ["a", "b", "c"]
            .iter()
            .map(|az| state.source_count(&source, &Some((*az).to_string())))
            .sum();
        for az in ["a", "b", "c"] {
            let group = Some(az.to_string());
            let group_size = state.ready_nodes_by_group[&group];
            let actual = state.source_count(&source, &group) as f64;
            let expected = total as f64 * group_size as f64 / ready_total as f64;
            let deviation = actual - expected;
            normalized_l1 += deviation.abs();
            weighted_variance += deviation * deviation / group_size as f64;
        }
    }
    (normalized_l1, weighted_variance)
}

fn az_loads(state: &PlacementState) -> HashMap<&str, usize> {
    let mut az_loads = HashMap::new();
    for node in &state.nodes {
        *az_loads
            .entry(node.az_group.as_deref().unwrap())
            .or_default() += node.projected_num_open_shards;
    }
    az_loads
}

#[test]
fn exact_live_rollout_converges_and_repairs_source_geography() {
    let (mut state, pending) = scenario(live_rollout_fixture(), vec![]);
    let initial = state.clone();
    let (initial_l1, initial_variance) = source_geography_metrics(&state);
    assert!((initial_l1 - 210.0).abs() < 0.01);
    assert!((initial_variance - 960.34).abs() < 0.01);
    assert_eq!(
        az_loads(&state),
        HashMap::from([("a", 220), ("b", 45), ("c", 149)])
    );

    // Seeding affects only randomized tie-breaking; production uses the same unit-greedy planner
    // with the process RNG.
    let plan = state.plan_rebalance_with_rng(pending, &mut StdRng::seed_from_u64(0));
    assert_eq!(plan.len(), 96);
    assert_real_max_to_min_moves(&initial, &plan);
    assert_eq!(
        az_loads(&state),
        HashMap::from([("a", 126), ("b", 141), ("c", 147)])
    );
    assert_eq!(loads(&state).into_iter().min(), Some(20));
    assert_eq!(loads(&state).into_iter().max(), Some(21));
    let (final_l1, final_variance) = source_geography_metrics(&state);
    let final_counts: Vec<_> = SOURCES
        .iter()
        .map(|source_id| {
            let source = source_uid(source_id);
            (
                source_id,
                state.source_count(&source, &Some("a".to_string())),
                state.source_count(&source, &Some("b".to_string())),
                state.source_count(&source, &Some("c".to_string())),
            )
        })
        .collect();
    eprintln!(
        "scenario=exact-live-rollout moves=96 az=126/141/147 \
         normalized_l1={initial_l1:.1}->{final_l1:.1} \
         weighted_variance={initial_variance:.2}->{final_variance:.2} \
         final_counts={final_counts:?} fixed_point=true"
    );
    assert!(final_l1 <= 26.61, "final normalized L1 was {final_l1}");
    assert!(
        final_variance <= 8.07,
        "final weighted variance was {final_variance}"
    );
    assert!(state.plan_rebalance(Vec::new()).is_empty());
}
