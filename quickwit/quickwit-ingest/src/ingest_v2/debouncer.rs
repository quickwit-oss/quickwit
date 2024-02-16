// Copyright (C) 2024 Quickwit, Inc.
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

use std::collections::HashMap;
use std::sync::Arc;

use quickwit_proto::control_plane::{
    GetOrCreateOpenShardsRequest, GetOrCreateOpenShardsSubrequest,
};
use quickwit_proto::ingest::ShardIds;
use quickwit_proto::types::{IndexId, SourceId};
use tokio::sync::{OwnedRwLockWriteGuard, RwLock};

/// Debounces [`GetOrCreateOpenShardsRequest`](quickwit_proto::control_plane::GetOrCreateOpenShardsRequest) requests by index and source IDs. It gives away a permit to the first request and a barrier to subsequent requests.
#[derive(Default)]
pub(super) struct Debouncer {
    locks: HashMap<(IndexId, SourceId), Arc<RwLock<()>>>,
}

impl Debouncer {
    pub async fn acquire(
        &mut self,
        index_id: &str,
        source_id: &str,
    ) -> Result<PermitGuard, BarrierGuard> {
        let key = (index_id.to_string(), source_id.to_string());
        let lock = self.locks.entry(key).or_default();

        if let Ok(permit) = lock.clone().try_write_owned() {
            Ok(PermitGuard(permit))
        } else {
            let barrier = lock.clone();
            Err(BarrierGuard(barrier))
        }
    }
}

pub(super) struct PermitGuard(OwnedRwLockWriteGuard<()>);

pub(super) struct BarrierGuard(Arc<RwLock<()>>);

impl BarrierGuard {
    pub async fn wait(self) {
        let _ = self.0.read().await;
    }
}

#[derive(Default)]
pub(super) struct Rendezvous {
    permits: Vec<PermitGuard>,
    barriers: Vec<BarrierGuard>,
}

impl Rendezvous {
    /// Releases the permits and waits for the barriers to be released.
    pub async fn wait(mut self) {
        self.permits.clear();

        for barrier in self.barriers {
            barrier.wait().await;
        }
    }
}

#[derive(Default)]
pub(super) struct DebouncedGetOrCreateOpenShardsRequest {
    subrequests: Vec<GetOrCreateOpenShardsSubrequest>,
    pub closed_shards: Vec<ShardIds>,
    pub unavailable_leaders: Vec<String>,
    rendezvous: Rendezvous,
}

impl DebouncedGetOrCreateOpenShardsRequest {
    pub fn is_empty(&self) -> bool {
        self.subrequests.is_empty()
    }

    pub fn take(self) -> (Option<GetOrCreateOpenShardsRequest>, Rendezvous) {
        if self.is_empty() {
            return (None, self.rendezvous);
        }
        let request = GetOrCreateOpenShardsRequest {
            subrequests: self.subrequests,
            closed_shards: self.closed_shards,
            unavailable_leaders: self.unavailable_leaders,
        };
        (Some(request), self.rendezvous)
    }

    pub fn push_subrequest(
        &mut self,
        subrequest: GetOrCreateOpenShardsSubrequest,
        permit: PermitGuard,
    ) {
        self.subrequests.push(subrequest);
        self.rendezvous.permits.push(permit);
    }

    pub fn push_barrier(&mut self, barrier: BarrierGuard) {
        self.rendezvous.barriers.push(barrier);
    }
}

#[cfg(test)]
mod tests {
    use super::Rendezvous;

    impl Rendezvous {
        pub fn is_empty(&self) -> bool {
            self.permits.is_empty() && self.barriers.is_empty()
        }

        pub fn num_permits(&self) -> usize {
            self.permits.len()
        }

        pub fn num_barriers(&self) -> usize {
            self.barriers.len()
        }
    }
}
