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

use std::hash::Hasher;
use std::net::SocketAddr;

/// Computes the hash of socket addr, the way it was done before Rust 1.81
///
/// In <https://github.com/rust-lang/rust/commit/ba620344301aaa3b2733575a0696cdfd877edbdf>
/// rustc change the implementation of Hash for IpAddr v4 and v6.
///
/// The idea was to not hash an array of bytes but instead interpret it as a register
/// and hash this.
///
/// This was done for performance reason, but this change the result of the hash function
/// used to compute affinity in quickwit. As a result, the switch would invalidate all
/// existing cache.
///
/// In order to avoid this, we introduce the following function that reproduces the old
/// behavior.
#[repr(transparent)]
#[derive(Debug, Eq, PartialEq, Copy, Clone)]
pub struct SocketAddrLegacyHash<'a>(pub &'a SocketAddr);

impl std::hash::Hash for SocketAddrLegacyHash<'_> {
    fn hash<H: Hasher>(&self, state: &mut H) {
        std::mem::discriminant(self.0).hash(state);
        match self.0 {
            SocketAddr::V4(socket_addr_v4) => {
                socket_addr_v4.ip().octets().hash(state);
                socket_addr_v4.port().hash(state);
            }
            SocketAddr::V6(socket_addr_v6) => {
                socket_addr_v6.ip().octets().hash(state);
                socket_addr_v6.port().hash(state);
                socket_addr_v6.flowinfo().hash(state);
                socket_addr_v6.scope_id().hash(state);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::net::SocketAddrV6;

    use super::*;

    fn sample_socket_addr_v4() -> SocketAddr {
        "17.12.15.3:1834".parse().unwrap()
    }

    fn sample_socket_addr_v6() -> SocketAddr {
        let mut socket_addr_v6: SocketAddrV6 = "[fe80::240:63ff:fede:3c19]:8080".parse().unwrap();
        socket_addr_v6.set_scope_id(4047u32);
        socket_addr_v6.set_flowinfo(303u32);
        socket_addr_v6.into()
    }

    fn compute_hash(hashable: impl std::hash::Hash) -> u64 {
        // I wish I could have used the sip hasher but we don't have the deps here and I did
        // not want to move that code to quickwit-common.
        //
        // If test break because rust changed its default hasher, we can just update the tests in
        // this file with the new values.
        let mut hasher = siphasher::sip::SipHasher::default();
        hashable.hash(&mut hasher);
        hasher.finish()
    }

    #[test]
    fn test_legacy_hash_socket_addr_v4() {
        let h = compute_hash(SocketAddrLegacyHash(&sample_socket_addr_v4()));
        // This value is coming from using rust 1.80 to hash socket addr
        assert_eq!(h, 8725442259486497862);
    }

    #[test]
    fn test_legacy_hash_socket_addr_v6() {
        let h = compute_hash(SocketAddrLegacyHash(&sample_socket_addr_v6()));
        // This value is coming from using rust 1.80 to hash socket addr
        assert_eq!(h, 14277248675058176752);
    }
}
