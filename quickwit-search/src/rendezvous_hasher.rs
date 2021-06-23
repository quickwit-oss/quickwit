//  Quickwit
//  Copyright (C) 2021 Quickwit Inc.
//
//  Quickwit is offered under the AGPL v3.0 and as commercial software.
//  For commercial licensing, contact us at hello@quickwit.io.
//
//  AGPL:
//  This program is free software: you can redistribute it and/or modify
//  it under the terms of the GNU Affero General Public License as
//  published by the Free Software Foundation, either version 3 of the
//  License, or (at your option) any later version.
//
//  This program is distributed in the hope that it will be useful,
//  but WITHOUT ANY WARRANTY; without even the implied warranty of
//  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
//  GNU Affero General Public License for more details.
//
//  You should have received a copy of the GNU Affero General Public License
//  along with this program.  If not, see <http://www.gnu.org/licenses/>.

use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::net::SocketAddr;

/// Node is a utility struct used to represent a rendez-vous hashing node.
/// It's used to track the load and the computed hash for a given key
#[derive(Debug, Clone)]
pub struct Node {
    pub peer_grpc_addr: SocketAddr,
    // The load of this node
    pub load: u64,
    // The combined hash value of a key and the node's id
    pub hash_key: u64,
}

impl Node {
    /// Create a new instance of [`Node`]
    pub fn new(peer_grpc_addr: SocketAddr, load: u64) -> Self {
        Self {
            peer_grpc_addr,
            load,
            hash_key: 0,
        }
    }

    /// Computes the hash of this node with a key
    pub fn compute_hash_with_key(&mut self, key: &str) {
        let mut state = DefaultHasher::new();
        key.hash(&mut state);
        self.peer_grpc_addr.hash(&mut state);
        self.hash_key = state.finish();
    }
}

/// Sorts the list of node base on rendez-vous-hashing.
/// Nodes are ordered by decreasing order of computed `hash_key`
pub fn sort_by_rendez_vous_hash(nodes: &mut [Node], key: &str) {
    for node in nodes.iter_mut() {
        node.compute_hash_with_key(key);
    }
    nodes.sort_unstable_by(|left, right| (right.hash_key).cmp(&(left.hash_key)));
}

#[cfg(test)]
mod tests {
    use std::net::IpAddr;
    use std::net::Ipv4Addr;

    use super::*;

    fn check_nodes_order(nodes: Vec<Node>, expected_nodes: Vec<SocketAddr>) {
        assert_eq!(
            nodes
                .iter()
                .map(|node| node.peer_grpc_addr)
                .collect::<Vec<_>>(),
            expected_nodes
        );
    }

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
        let mut node_set1 = vec![
            Node::new(socket1, 0),
            Node::new(socket2, 0),
            Node::new(socket3, 0),
            Node::new(socket4, 0),
        ];
        sort_by_rendez_vous_hash(&mut node_set1, "key");
        check_nodes_order(node_set1, vec![socket2, socket3, socket1, socket4]);

        let mut node_set2 = vec![
            Node::new(socket1, 0),
            Node::new(socket2, 0),
            Node::new(socket4, 0),
        ];
        sort_by_rendez_vous_hash(&mut node_set2, "key");
        check_nodes_order(node_set2, vec![socket2, socket1, socket4]);

        let mut node_set3 = vec![Node::new(socket1, 0), Node::new(socket4, 0)];
        sort_by_rendez_vous_hash(&mut node_set3, "key");
        check_nodes_order(node_set3, vec![socket1, socket4]);
    }
}
