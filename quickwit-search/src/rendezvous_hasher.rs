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

use std::cmp::Reverse;
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};

/// Computes the affinity of a node with a given `split_id`.
///
/// We rely on rendez-vous hashing here.
fn node_affinity<T: Hash>(node: T, split_id: &str) -> u64 {
    let mut state = DefaultHasher::new();
    split_id.hash(&mut state);
    node.hash(&mut state);
    state.finish()
}

/// Sorts the list of node base on rendez-vous-hashing.
/// Nodes are ordered by decreasing order of computed `hash_key`
pub(crate) fn sort_by_rendez_vous_hash<T: Hash>(nodes: &mut [T], split_id: &str) {
    nodes.sort_by_cached_key(|node| Reverse(node_affinity(node, split_id)));
}

#[cfg(test)]
mod tests {
    use std::net::{IpAddr, Ipv4Addr, SocketAddr};

    use super::*;

    fn test_socket_addr(last_byte: u8) -> SocketAddr {
        let ip_addr = Ipv4Addr::new(127, 0, 0, last_byte);
        SocketAddr::new(IpAddr::V4(ip_addr), 10000)
    }

    #[test]
    fn test_utils_sort_by_rendez_vous_hash() {
        let socket1 = test_socket_addr(1);
        let socket2 = test_socket_addr(2);
        let socket3 = test_socket_addr(3);
        let socket4 = test_socket_addr(4);
        let mut socket_set1 = vec![socket1, socket2, socket3, socket4];
        sort_by_rendez_vous_hash(&mut socket_set1, "key");
        assert_eq!(socket_set1, &[socket2, socket3, socket1, socket4]);

        let mut socket_set2 = vec![socket1, socket2, socket4];
        sort_by_rendez_vous_hash(&mut socket_set2, "key");
        assert_eq!(socket_set2, &[socket2, socket1, socket4]);

        let mut socket_set3 = vec![socket1, socket4];
        sort_by_rendez_vous_hash(&mut socket_set3, "key");
        assert_eq!(socket_set3, &[socket1, socket4]);
    }
}
