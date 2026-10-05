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
use std::hash::{Hash, Hasher};

use siphasher::sip::SipHasher;

/// Computes the affinity of a node for a given `key`.
/// A higher value means a higher affinity.
/// This is the `rendezvous hash`.
pub fn node_affinity<T: Hash, U: Hash>(node: T, key: &U) -> u64 {
    let mut state = SipHasher::new();
    key.hash(&mut state);
    node.hash(&mut state);
    state.finish()
}

/// Sorts the list of node ordered by decreasing affinity values.
/// This is called rendezvous hashing.
pub fn sort_by_rendez_vous_hash<T: Hash, U: Hash>(nodes: &mut [T], key: U) {
    nodes.sort_by_cached_key(|node| Reverse(node_affinity(node, &key)));
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_utils_sort_by_rendez_vous_hash() {
        let mut nodes_four = vec!["node-4", "node-3", "node-1", "node-2"];
        sort_by_rendez_vous_hash(&mut nodes_four, "key");

        let mut nodes_three = vec!["node-1", "node-2", "node-4"];
        sort_by_rendez_vous_hash(&mut nodes_three, "key");
        let expected_three: Vec<&str> = nodes_four
            .iter()
            .copied()
            .filter(|node| *node != "node-3")
            .collect();
        assert_eq!(nodes_three, expected_three);

        let mut nodes_two = vec!["node-1", "node-4"];
        sort_by_rendez_vous_hash(&mut nodes_two, "key");
        let expected_two: Vec<&str> = nodes_four
            .iter()
            .copied()
            .filter(|node| *node == "node-1" || *node == "node-4")
            .collect();
        assert_eq!(nodes_two, expected_two);
    }
}
