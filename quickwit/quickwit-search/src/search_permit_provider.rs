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
use std::task::{Context, Poll};

use bytesize::ByteSize;
use quickwit_common::metrics::GaugeGuard;
use tokio::sync::{mpsc, oneshot};
use tracing::warn;

/// Distributor of permits to perform split search operation.
///
/// Requests are served in order. Each permit initially reserves a slot for the
/// warmup (limit concurrent downloads) and a pessimistic amount of memory. Once
/// the warmup is completed, the actual memory usage is set and the warmup slot
/// is released. Once the search is completed and the permit is dropped, the
/// remaining memory is also released.
#[derive(Clone)]
pub struct SearchPermitProvider {
    sender: mpsc::UnboundedSender<SearchPermitMessage>,
}

pub enum SearchPermitMessage {
    Request {
        permit_sender: oneshot::Sender<Vec<SearchPermitFuture>>,
        num_permits: usize,
    },
    WarmupCompleted {
        memory_delta: i64,
    },
    Drop {
        memory_size: u64,
        warmup_permit_held: bool,
    },
}

impl SearchPermitProvider {
    pub fn new(
        num_download_slots: usize,
        memory_budget: ByteSize,
        initial_allocation: ByteSize,
    ) -> Self {
        let (sender, receiver) = mpsc::unbounded_channel();
        let mut actor = SearchPermitActor {
            msg_receiver: receiver,
            msg_sender: sender.downgrade(),
            num_warmup_slots_available: num_download_slots,
            total_memory_budget: memory_budget.as_u64(),
            permits_requests: VecDeque::new(),
            total_memory_allocated: 0u64,
            per_permit_initial_memory_allocation: initial_allocation.as_u64(),
        };
        tokio::spawn(async move { actor.run().await });
        Self { sender }
    }

    /// Returns `num_permits` futures that complete once enough resources are
    /// available.
    ///
    /// The permits returned are guaranteed to be resolved in order. In
    /// addition, the permits are guaranteed to be resolved before permits
    /// returned by subsequent calls to this function.
    pub async fn get_permits(&self, num_permits: usize) -> Vec<SearchPermitFuture> {
        let (permit_sender, permit_receiver) = oneshot::channel();
        self.sender
            .send(SearchPermitMessage::Request {
                permit_sender,
                num_permits,
            })
            .expect("Receiver lives longer than sender");
        permit_receiver
            .await
            .expect("Receiver lives longer than sender")
    }
}

struct SearchPermitActor {
    msg_receiver: mpsc::UnboundedReceiver<SearchPermitMessage>,
    msg_sender: mpsc::WeakUnboundedSender<SearchPermitMessage>,
    num_warmup_slots_available: usize,
    /// Note it is possible for memory_allocated to exceed memory_budget temporarily,
    /// if and only if a split leaf search task ended up using more than `initial_allocation`.
    /// When it happens, new permits will not be assigned until the memory is freed.
    total_memory_budget: u64,
    total_memory_allocated: u64,
    per_permit_initial_memory_allocation: u64,
    permits_requests: VecDeque<oneshot::Sender<SearchPermit>>,
}

impl SearchPermitActor {
    async fn run(&mut self) {
        // Stops when the last clone of SearchPermitProvider is dropped.
        while let Some(msg) = self.msg_receiver.recv().await {
            self.handle_message(msg);
        }
    }

    fn handle_message(&mut self, msg: SearchPermitMessage) {
        match msg {
            SearchPermitMessage::Request {
                num_permits,
                permit_sender,
            } => {
                let mut permits = Vec::with_capacity(num_permits);
                for _ in 0..num_permits {
                    let (tx, rx) = oneshot::channel();
                    self.permits_requests.push_back(tx);
                    permits.push(SearchPermitFuture(rx));
                }
                self.assign_available_permits();
                permit_sender
                    .send(permits)
                    .ok()
                    // This is a request response pattern, so we can safely ignore the error.
                    .expect("Receiver lives longer than sender");
            }
            SearchPermitMessage::WarmupCompleted { memory_delta } => {
                self.num_warmup_slots_available += 1;
                if self.total_memory_allocated as i64 + memory_delta < 0 {
                    panic!("More memory released than allocated, should never happen.")
                }
                self.total_memory_allocated =
                    (self.total_memory_allocated as i64 + memory_delta) as u64;
                self.assign_available_permits();
            }
            SearchPermitMessage::Drop {
                memory_size,
                warmup_permit_held,
            } => {
                if warmup_permit_held {
                    self.num_warmup_slots_available += 1;
                }
                self.total_memory_allocated = self
                    .total_memory_allocated
                    .checked_sub(memory_size)
                    .expect("More memory released than allocated, should never happen.");
                self.assign_available_permits();
            }
        }
    }

    fn assign_available_permits(&mut self) {
        while self.num_warmup_slots_available > 0
            && self.total_memory_allocated + self.per_permit_initial_memory_allocation
                <= self.total_memory_budget
        {
            let Some(permit_requester_tx) = self.permits_requests.pop_front() else {
                break;
            };
            let mut ongoing_gauge_guard = GaugeGuard::from_gauge(
                &crate::SEARCH_METRICS.leaf_search_single_split_tasks_ongoing,
            );
            ongoing_gauge_guard.add(1);
            self.total_memory_allocated += self.per_permit_initial_memory_allocation;
            permit_requester_tx
                .send(SearchPermit {
                    _ongoing_gauge_guard: ongoing_gauge_guard,
                    msg_sender: self.msg_sender.clone(),
                    memory_allocation: self.per_permit_initial_memory_allocation,
                    warmup_permit_held: true,
                })
                // if the requester dropped its receiver, we drop the newly
                // created SearchPermit which releases the resources
                .ok();
        }
        crate::SEARCH_METRICS
            .leaf_search_single_split_tasks_pending
            .set(self.permits_requests.len() as i64);
    }
}

pub struct SearchPermit {
    _ongoing_gauge_guard: GaugeGuard<'static>,
    msg_sender: mpsc::WeakUnboundedSender<SearchPermitMessage>,
    memory_allocation: u64,
    warmup_permit_held: bool,
}

impl SearchPermit {
    /// After warm up, we have a proper estimate of the memory usage of a single
    /// split leaf search. We can thus set the actual memory usage and release
    /// the warmup slot.
    pub fn warmup_completed(&mut self, new_memory_usage: u64) {
        if new_memory_usage > self.memory_allocation {
            warn!(
                memory_usage = new_memory_usage,
                memory_allocation = self.memory_allocation,
                "current leaf search is consuming more memory than the initial allocation"
            );
        }
        let memory_delta = new_memory_usage as i64 - self.memory_allocation as i64;
        self.warmup_permit_held = false;
        self.send_if_still_running(SearchPermitMessage::WarmupCompleted { memory_delta });
    }

    fn send_if_still_running(&self, msg: SearchPermitMessage) {
        if let Some(sender) = self.msg_sender.upgrade() {
            sender
                .send(msg)
                // Receiver instance in the event loop is never dropped or
                // closed as long as there is a strong sender reference.
                .expect("Receiver should live longer than sender");
        }
    }
}

impl Drop for SearchPermit {
    fn drop(&mut self) {
        self.send_if_still_running(SearchPermitMessage::Drop {
            memory_size: self.memory_allocation,
            warmup_permit_held: self.warmup_permit_held,
        });
    }
}

pub struct SearchPermitFuture(oneshot::Receiver<SearchPermit>);

impl Future for SearchPermitFuture {
    type Output = SearchPermit;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let receiver = Pin::new(&mut self.get_mut().0);
        match receiver.poll(cx) {
            Poll::Ready(Ok(search_permit)) => Poll::Ready(search_permit),
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
