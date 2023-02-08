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
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};

/// Computes the affinity of a node for a given `key`.
/// A higher value means a higher affinity.
/// This is the `rendezvous hash`.
fn node_affinity<T: Hash, U: Hash>(node: T, key: &U) -> u64 {
    let mut state = DefaultHasher::new();
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

    fn test_socket_addr(last_byte: u8) -> SocketAddr {
        ([127, 0, 0, last_byte], 10_000u16).into()
    }

    #[test]
    fn test_utils_sort_by_rendez_vous_hash() {
        let socket1 = test_socket_addr(1);
        let socket2 = test_socket_addr(2);
        let socket3 = test_socket_addr(3);
        let socket4 = test_socket_addr(4);

        let mut socket_set1 = vec![socket1, socket2, socket3, socket4];
        sort_by_rendez_vous_hash(&mut socket_set1, "key");

        let mut socket_set2 = vec![socket1, socket2, socket4];
        sort_by_rendez_vous_hash(&mut socket_set2, "key");

        let mut socket_set3 = vec![socket1, socket4];
        sort_by_rendez_vous_hash(&mut socket_set3, "key");

        assert_eq!(socket_set1, &[socket1, socket3, socket2, socket4]);
        assert_eq!(socket_set2, &[socket1, socket2, socket4]);
        assert_eq!(socket_set3, &[socket1, socket4]);
    }
}
