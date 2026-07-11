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

use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

use quickwit_proto::control_plane::{
    GetOrCreateOpenShardsRequest, GetOrCreateOpenShardsSubrequest,
};
use quickwit_proto::ingest::ShardIds;
use quickwit_proto::types::{IndexId, SourceId};
use tokio::sync::{OwnedRwLockWriteGuard, RwLock};

#[derive(Default)]
struct Debouncer(Arc<RwLock<()>>);

impl Debouncer {
    fn acquire(&self) -> Result<PermitGuard, BarrierGuard> {
        if let Ok(permit) = self.0.clone().try_write_owned() {
            Ok(PermitGuard(permit))
        } else {
            let barrier = self.0.clone();
            Err(BarrierGuard(barrier))
        }
    }
}

#[derive(Debug)]
pub(super) struct PermitGuard(#[allow(dead_code)] OwnedRwLockWriteGuard<()>);

#[derive(Debug)]
pub(super) struct BarrierGuard(Arc<RwLock<()>>);

impl BarrierGuard {
    pub async fn wait(self) {
        let _ = self.0.read().await;
    }
}

/// Debounces [`GetOrCreateOpenShardsRequest`] requests by index and source IDs. It gives away a
/// permit to the first request and a barrier to subsequent requests.
#[derive(Default)]
pub(super) struct GetOrCreateOpenShardsRequestDebouncer {
    debouncers: HashMap<(IndexId, SourceId), Debouncer>,
}

impl GetOrCreateOpenShardsRequestDebouncer {
    pub fn acquire(
        &mut self,
        index_id: &str,
        source_id: &str,
    ) -> Result<PermitGuard, BarrierGuard> {
        let key = (index_id.to_string(), source_id.to_string());
        self.debouncers.entry(key).or_default().acquire()
    }
}

#[derive(Default)]
pub(super) struct DebouncedGetOrCreateOpenShardsRequest {
    subrequests_by_unavailable_leaders: BTreeMap<Vec<String>, Vec<GetOrCreateOpenShardsSubrequest>>,
    pub closed_shards: Vec<ShardIds>,
    rendezvous: Rendezvous,
}

impl DebouncedGetOrCreateOpenShardsRequest {
    pub fn is_empty(&self) -> bool {
        self.subrequests_by_unavailable_leaders.is_empty()
    }

    pub fn take(self) -> (Vec<GetOrCreateOpenShardsRequest>, Rendezvous) {
        let mut closed_shards_opt = Some(self.closed_shards);
        let requests = self
            .subrequests_by_unavailable_leaders
            .into_iter()
            .map(
                |(unavailable_leaders, subrequests)| GetOrCreateOpenShardsRequest {
                    subrequests,
                    // Closed-shard feedback is request-wide. Send it once even when source-specific
                    // exclusions require splitting the control-plane request.
                    closed_shards: closed_shards_opt.take().unwrap_or_default(),
                    unavailable_leaders,
                },
            )
            .collect();
        (requests, self.rendezvous)
    }

    pub fn push_subrequest(
        &mut self,
        subrequest: GetOrCreateOpenShardsSubrequest,
        permit: PermitGuard,
        mut unavailable_leaders: Vec<String>,
    ) {
        unavailable_leaders.sort_unstable();
        unavailable_leaders.dedup();
        self.subrequests_by_unavailable_leaders
            .entry(unavailable_leaders)
            .or_default()
            .push(subrequest);
        self.rendezvous.permits.push(permit);
    }

    pub fn push_barrier(&mut self, barrier: BarrierGuard) {
        self.rendezvous.barriers.push(barrier);
    }
}

#[derive(Default)]
pub(super) struct Rendezvous {
    permits: Vec<PermitGuard>,
    barriers: Vec<BarrierGuard>,
}

impl Rendezvous {
    /// Releases the permits and waits for the barriers to be lifted.
    pub async fn wait(mut self) {
        // Releasing the permits before waiting for the barriers is necessary to avoid
        // dead locks.
        self.permits.clear();

        for barrier in self.barriers {
            barrier.wait().await;
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::Duration;

    use super::*;

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

    #[tokio::test]
    async fn test_debouncer() {
        let debouncer = Debouncer::default();

        let permit = debouncer.acquire().unwrap();
        let barrier = debouncer.acquire().unwrap_err();
        drop(permit);
        barrier.wait().await;

        let permit = debouncer.acquire().unwrap();
        let barrier = debouncer.acquire().unwrap_err();
        let flag = Arc::new(AtomicUsize::new(0));

        let flag_clone = flag.clone();
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(100)).await;
            flag_clone.store(1, Ordering::Release);
            drop(permit);
        });
        let flag_clone = flag.clone();
        tokio::spawn(async move {
            let _ = barrier.wait().await;
            flag_clone.store(2, Ordering::Release);
        });
        tokio::time::sleep(Duration::from_millis(200)).await;
        assert_eq!(flag.load(Ordering::Acquire), 2);
    }

    #[test]
    fn test_get_or_create_open_shards_request_debouncer() {
        let mut debouncer = GetOrCreateOpenShardsRequestDebouncer::default();

        let _permit_foo: PermitGuard = debouncer.acquire("test-index", "test-source-foo").unwrap();

        let _barrier = debouncer
            .acquire("test-index", "test-source-foo")
            .unwrap_err();

        let _permit_bar: PermitGuard = debouncer.acquire("test-index", "test-source-bar").unwrap();
    }

    #[tokio::test]
    async fn test_debounced_get_or_create_open_shards_request() {
        let debounced_request = DebouncedGetOrCreateOpenShardsRequest::default();
        assert!(debounced_request.is_empty());

        let (requests, rendezvous) = debounced_request.take();
        assert!(requests.is_empty());
        assert!(rendezvous.is_empty());

        let mut debouncer = GetOrCreateOpenShardsRequestDebouncer::default();
        let mut debounced_request = DebouncedGetOrCreateOpenShardsRequest::default();

        let permit = debouncer.acquire("test-index", "test-source-foo").unwrap();
        debounced_request.push_subrequest(
            GetOrCreateOpenShardsSubrequest {
                index_id: "test-index".to_string(),
                source_id: "test-source-foo".to_string(),
                ..Default::default()
            },
            permit,
            Vec::new(),
        );

        let permit = debouncer.acquire("test-index", "test-source-bar").unwrap();
        debounced_request.push_subrequest(
            GetOrCreateOpenShardsSubrequest {
                index_id: "test-index".to_string(),
                source_id: "test-source-bar".to_string(),
                ..Default::default()
            },
            permit,
            vec!["test-node".to_string()],
        );
        let permit = debouncer.acquire("test-index", "test-source-baz").unwrap();
        debounced_request.push_subrequest(
            GetOrCreateOpenShardsSubrequest {
                index_id: "test-index".to_string(),
                source_id: "test-source-baz".to_string(),
                ..Default::default()
            },
            permit,
            vec!["test-node".to_string(), "test-node".to_string()],
        );

        let barrier = debouncer
            .acquire("test-index", "test-source-foo")
            .unwrap_err();
        debounced_request.push_barrier(barrier);

        let (requests, rendezvous) = debounced_request.take();

        assert_eq!(requests.len(), 2);
        assert_eq!(requests[0].subrequests.len(), 1);
        assert!(requests[0].unavailable_leaders.is_empty());
        assert_eq!(requests[1].subrequests.len(), 2);
        assert_eq!(requests[1].unavailable_leaders, ["test-node"]);
        assert_eq!(rendezvous.num_permits(), 3);
        assert_eq!(rendezvous.num_barriers(), 1);
    }
}
