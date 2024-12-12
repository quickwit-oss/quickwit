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

use std::collections::VecDeque;
use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};

use bytesize::ByteSize;
use quickwit_common::metrics::GaugeGuard;
use tokio::sync::oneshot;
use tracing::warn;

/// `SearchPermitProvider` is a distributor of permits to perform single split
/// search operation.
///
/// - Two types of resources are managed: memory allocations and download slots.
/// - Requests are served in order.
#[derive(Clone)]
pub struct SearchPermitProvider {
    inner_arc: Arc<Mutex<InnerSearchPermitProvider>>,
}

impl SearchPermitProvider {
    pub fn new(
        num_download_slots: usize,
        memory_budget: ByteSize,
        initial_allocation: ByteSize,
    ) -> SearchPermitProvider {
        SearchPermitProvider {
            inner_arc: Arc::new(Mutex::new(InnerSearchPermitProvider {
                num_download_slots_available: num_download_slots,
                memory_budget: memory_budget.as_u64(),
                permits_requests: VecDeque::new(),
                memory_allocated: 0u64,
                initial_allocation: initial_allocation.as_u64(),
            })),
        }
    }

    /// Returns a list of future permits in the form of awaitable futures.
    ///
    /// The permits returned are guaranteed to be resolved in order.
    /// In addition, the permits are guaranteed to be resolved before permits returned by
    /// subsequent calls to this function (or `get_permit`).
    #[must_use]
    pub fn get_permits(&self, num_permits: usize) -> Vec<SearchPermitFuture> {
        let mut permits_lock = self.inner_arc.lock().unwrap();
        permits_lock.get_permits(num_permits, &self.inner_arc)
    }
}

struct InnerSearchPermitProvider {
    num_download_slots_available: usize,

    // Note it is possible for memory_allocated to exceed memory_budget temporarily,
    // if and only if a split leaf search task ended up using more than `initial_allocation`.
    //
    // When it happens, new permits will not be assigned until the memory is freed.
    memory_budget: u64,
    memory_allocated: u64,
    initial_allocation: u64,
    permits_requests: VecDeque<oneshot::Sender<SearchPermit>>,
}

impl InnerSearchPermitProvider {
    fn get_permits(
        &mut self,
        num_permits: usize,
        inner_arc: &Arc<Mutex<InnerSearchPermitProvider>>,
    ) -> Vec<SearchPermitFuture> {
        let mut permits = Vec::with_capacity(num_permits);
        for _ in 0..num_permits {
            let (tx, rx) = oneshot::channel();
            self.permits_requests.push_back(tx);
            permits.push(SearchPermitFuture(rx));
        }
        self.assign_available_permits(inner_arc);
        permits
    }

    /// Called each time a permit is requested or released
    ///
    /// Calling lock on `inner_arc` inside this method will cause a deadlock as
    /// `&mut self` and `inner_arc` reference the same instance.
    fn assign_available_permits(&mut self, inner_arc: &Arc<Mutex<InnerSearchPermitProvider>>) {
        while self.num_download_slots_available > 0
            && self.memory_allocated + self.initial_allocation <= self.memory_budget
        {
            let Some(permit_requester_tx) = self.permits_requests.pop_front() else {
                break;
            };
            let mut ongoing_gauge_guard = GaugeGuard::from_gauge(
                &crate::SEARCH_METRICS.leaf_search_single_split_tasks_ongoing,
            );
            ongoing_gauge_guard.add(1);
            let send_res = permit_requester_tx.send(SearchPermit {
                _ongoing_gauge_guard: ongoing_gauge_guard,
                inner_arc: inner_arc.clone(),
                warmup_permit_held: true,
                memory_allocation: self.initial_allocation,
            });
            match send_res {
                Ok(()) => {
                    self.num_download_slots_available -= 1;
                    self.memory_allocated += self.initial_allocation;
                }
                Err(search_permit) => {
                    // We cannot just decrease the num_permits_available in all case and rely on
                    // the drop logic here: it would cause a dead lock on the inner_arc Mutex.
                    search_permit.drop_without_recycling_permit();
                }
            }
        }
        crate::SEARCH_METRICS
            .leaf_search_single_split_tasks_pending
            .set(self.permits_requests.len() as i64);
    }
}

pub struct SearchPermit {
    _ongoing_gauge_guard: GaugeGuard<'static>,
    inner_arc: Arc<Mutex<InnerSearchPermitProvider>>,
    warmup_permit_held: bool,
    memory_allocation: u64,
}

impl SearchPermit {
    /// After warm up, we have a proper estimate of the memory usage of a single split leaf search.
    ///
    /// We can then set the actual memory usage.
    pub fn set_actual_memory_usage_and_release_permit_after(&mut self, new_memory_usage: u64) {
        if new_memory_usage > self.memory_allocation {
            warn!(
                memory_usage = new_memory_usage,
                memory_allocation = self.memory_allocation,
                "current leaf search is consuming more memory than the initial allocation"
            );
        }
        let mut inner_guard = self.inner_arc.lock().unwrap();
        let delta = new_memory_usage as i64 - inner_guard.initial_allocation as i64;
        inner_guard.memory_allocated += delta as u64;
        inner_guard.num_download_slots_available += 1;
        if inner_guard.memory_allocated > inner_guard.memory_budget {
            warn!(
                memory_allocated = inner_guard.memory_allocated,
                memory_budget = inner_guard.memory_budget,
                "memory allocated exceeds memory budget"
            );
        }
        self.memory_allocation = new_memory_usage;
        inner_guard.assign_available_permits(&self.inner_arc);
    }

    fn drop_without_recycling_permit(mut self) {
        self.warmup_permit_held = false;
        self.memory_allocation = 0u64;
        drop(self);
    }
}

impl Drop for SearchPermit {
    fn drop(&mut self) {
        // This is not just an optimization. This is necessary to avoid a dead lock when the
        // permit requester dropped its receiver channel.
        if !self.warmup_permit_held && self.memory_allocation == 0 {
            return;
        }
        let mut inner_guard = self.inner_arc.lock().unwrap();
        if self.warmup_permit_held {
            inner_guard.num_download_slots_available += 1;
        }
        inner_guard.memory_allocated -= self.memory_allocation;
        inner_guard.assign_available_permits(&self.inner_arc);
    }
}

pub struct SearchPermitFuture(oneshot::Receiver<SearchPermit>);

impl Future for SearchPermitFuture {
    type Output = Option<SearchPermit>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let receiver = Pin::new(&mut self.get_mut().0);
        match receiver.poll(cx) {
            Poll::Ready(Ok(search_permit)) => Poll::Ready(Some(search_permit)),
            Poll::Ready(Err(_)) => panic!("Failed to acquire permit. This should never happen! Please, report on https://github.com/quickwit-oss/quickwit/issues."),
            Poll::Pending => Poll::Pending,
        }
    }
}

// #[cfg(test)]
// mod tests {
//     use tokio::task::JoinSet;

//     use super::*;

//     #[tokio::test]
//     async fn test_search_permits_get_permits_future() {
//         // We test here that `get_permits_futures` does not interleave
//         let search_permits = SearchPermitProvider::new(1);
//         let mut all_futures = Vec::new();
//         let first_batch_of_permits = search_permits.get_permits(10);
//         assert_eq!(first_batch_of_permits.len(), 10);
//         all_futures.extend(
//             first_batch_of_permits
//                 .into_iter()
//                 .enumerate()
//                 .map(move |(i, fut)| ((1, i), fut)),
//         );

//         let second_batch_of_permits = search_permits.get_permits(10);
//         assert_eq!(second_batch_of_permits.len(), 10);
//         all_futures.extend(
//             second_batch_of_permits
//                 .into_iter()
//                 .enumerate()
//                 .map(move |(i, fut)| ((2, i), fut)),
//         );

//         use rand::seq::SliceRandom;
//         // not super useful, considering what join set does, but still a tiny bit more sound.
//         all_futures.shuffle(&mut rand::thread_rng());

//         let mut join_set = JoinSet::new();
//         for (res, fut) in all_futures {
//             join_set.spawn(async move {
//                 let permit = fut.await;
//                 (res, permit)
//             });
//         }
//         let mut ordered_result: Vec<(usize, usize)> = Vec::with_capacity(20);
//         while let Some(Ok(((batch_id, order), _permit))) = join_set.join_next().await {
//             ordered_result.push((batch_id, order));
//         }

//         assert_eq!(ordered_result.len(), 20);
//         for (i, res) in ordered_result[0..10].iter().enumerate() {
//             assert_eq!(res, &(1, i));
//         }
//         for (i, res) in ordered_result[10..20].iter().enumerate() {
//             assert_eq!(res, &(2, i));
//         }
//     }

//     #[tokio::test]
//     async fn test_search_permits_receiver_race_condition() {
//         // Here we test that we don't have a problem if the Receiver is dropped.
//         // In particular, we want to check that there is not a race condition where drop attempts
// to         // lock the mutex.
//         let search_permits = SearchPermitProvider::new(1);
//         let permit_rx = search_permits.get_permit();
//         let permit_rx2 = search_permits.get_permit();
//         drop(permit_rx2);
//         drop(permit_rx);
//         let _permit_rx = search_permits.get_permit();
//     }
// }
