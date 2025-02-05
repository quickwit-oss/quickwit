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
    use std::net::SocketAddr;

    use super::*;
    use crate::SocketAddrLegacyHash;

    fn test_socket_addr(last_byte: u8) -> SocketAddr {
        ([127, 0, 0, last_byte], 10_000u16).into()
    }

    #[test]
    fn test_utils_sort_by_rendez_vous_hash() {
        let socket1 = test_socket_addr(1);
        let socket2 = test_socket_addr(2);
        let socket3 = test_socket_addr(3);
        let socket4 = test_socket_addr(4);

        let legacy_socket1 = SocketAddrLegacyHash(&socket1);
        let legacy_socket2 = SocketAddrLegacyHash(&socket2);
        let legacy_socket3 = SocketAddrLegacyHash(&socket3);
        let legacy_socket4 = SocketAddrLegacyHash(&socket4);

        let mut socket_set1 = vec![
            legacy_socket4,
            legacy_socket3,
            legacy_socket1,
            legacy_socket2,
        ];
        sort_by_rendez_vous_hash(&mut socket_set1, "key");

        let mut socket_set2 = vec![legacy_socket1, legacy_socket2, legacy_socket4];
        sort_by_rendez_vous_hash(&mut socket_set2, "key");

        let mut socket_set3 = vec![legacy_socket1, legacy_socket4];
        sort_by_rendez_vous_hash(&mut socket_set3, "key");

        assert_eq!(
            socket_set1,
            &[
                legacy_socket1,
                legacy_socket2,
                legacy_socket3,
                legacy_socket4
            ]
        );
        assert_eq!(
            socket_set2,
            &[legacy_socket1, legacy_socket2, legacy_socket4]
        );
        assert_eq!(socket_set3, &[legacy_socket1, legacy_socket4]);
    }
}
