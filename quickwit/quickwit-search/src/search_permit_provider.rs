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

use std::collections::BinaryHeap;
use std::collections::binary_heap::PeekMut;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::task::{Context, Poll};
use std::time::{Duration, Instant};

use bytesize::ByteSize;
use quickwit_directories::OutOfMemoryError;
use quickwit_metrics::GaugeGuard;
use tokio::sync::{mpsc, oneshot};
use tracing::warn;

use crate::metrics::{
    LEAF_SEARCH_SINGLE_SPLIT_TASKS_ONGOING, LEAF_SEARCH_SINGLE_SPLIT_TASKS_PENDING,
    SEARCHER_NODE_LOAD,
};
use crate::split_warmup_memory::SplitWarmupMemory;
use crate::warmup_memory::GlobalWarmupMemory;

/// Distributor of permits to perform split search operation.
///
/// Requests are served by priority, then by lowest estimated cost. A permit is granted when a
/// warmup slot is available (to limit concurrent downloads). It comes with an empty warmup memory
/// reservation, younger than the ones of all the permits granted before.
///
/// The split search then reserves a pessimistic amount of memory with
/// [`SearchPermit::warmup_memory`], and grows its reservation as data is downloaded. It waits
/// if the warmup memory is full, and fails if the per-split limit would be exceeded (see
/// [`GlobalWarmupMemory`] for why waiting cannot deadlock). Once the warmup is completed, the
/// unused memory and the warmup slot are released. Once the search is completed and the permit is
/// dropped, the remaining memory is also released.
#[derive(Clone)]
pub struct SearchPermitProvider {
    message_sender: mpsc::UnboundedSender<SearchPermitMessage>,
    /// Lock-free view of [`SearchPermitActor::total_job_cost`].
    total_job_cost: Arc<AtomicUsize>,
    #[allow(dead_code)]
    actor_join_handle: Arc<tokio::task::JoinHandle<SearchPermitActor>>,
}

/// Metadata for a leaf search task.
pub(crate) struct LeafSearchTaskMetadata {
    /// Lower values have higher priority.
    pub priority: i32,
    /// Estimated query aware cost of each split search, in the same arbitrary unit as
    /// [`Job::cost()`]. Used to report the current load of this node to the job placer and to
    /// prioritize leaf requests.
    pub split_job_costs: Vec<usize>,
}

pub enum SearchPermitMessage {
    RequestWithOffload {
        task_metadata: LeafSearchTaskMetadata,
        /// Maximum number of pending requests. If granting all
        /// requested permits would cause the number of pending requests to exceed this threshold,
        /// some permits will be offloaded to Lambda.
        offload_threshold: usize,
        /// Channel to return the result message from the actor.
        /// When offloading permits, the number of futures can be < to the number of requested
        /// permits.
        permit_resp_tx: oneshot::Sender<Vec<SearchPermitFuture>>,
    },
    FreeWarmupSlot,
    /// Sent when a permit is dropped.
    ///
    /// The permit memory is freed separately, once the split's `CachingDirectory` is dropped too.
    Drop {
        warmup_slot_freed: bool,
    },
}

impl SearchPermitProvider {
    /// Creates a provider with a per-split limit of a quarter of `memory_budget`, like the default
    /// searcher config.
    #[cfg(test)]
    pub fn new(num_download_slots: usize, memory_budget: ByteSize) -> Self {
        let split_max_allocation = ByteSize(memory_budget.as_u64() / 4);
        Self::with_split_max_allocation(num_download_slots, memory_budget, split_max_allocation)
    }

    /// Creates a provider.
    ///
    /// - `memory_budget`: total warmup memory of the split searches of this node.
    /// - `split_max_allocation`: warmup memory a single split search can reserve at most. It is
    ///   also the part of `memory_budget` reserved for the oldest split search, so the other split
    ///   searches share `memory_budget - split_max_allocation`.
    ///
    /// # Panics
    ///
    /// Panics if `split_max_allocation` is larger than `memory_budget`.
    pub fn with_split_max_allocation(
        num_download_slots: usize,
        memory_budget: ByteSize,
        split_max_allocation: ByteSize,
    ) -> Self {
        let (message_sender, message_receiver) = mpsc::unbounded_channel();
        let total_job_cost = Arc::new(AtomicUsize::new(0));
        let warmup_memory = GlobalWarmupMemory::new(memory_budget, split_max_allocation);
        let actor = SearchPermitActor {
            msg_receiver: message_receiver,
            msg_sender: message_sender.downgrade(),
            num_warmup_slots_available: num_download_slots,
            warmup_memory,
            permits_requests: BinaryHeap::new(),
            total_job_cost: total_job_cost.clone(),
        };
        let actor_join_handle = Arc::new(tokio::spawn(actor.run()));
        Self {
            message_sender,
            total_job_cost,
            actor_join_handle,
        }
    }

    #[cfg(test)]
    async fn stop_and_unwrap(self) -> SearchPermitActor {
        let SearchPermitProvider {
            message_sender,
            actor_join_handle,
            ..
        } = self;
        drop(message_sender);
        Arc::into_inner(actor_join_handle).unwrap().await.unwrap()
    }

    /// Returns the current load of this node, expressed as the sum of
    /// [`LeafSearchTaskMetadata::split_job_costs`] across all queued and active tasks.
    ///
    /// This value uses the same arbitrary unit as [`Job::cost()`] in the search job placer, so it
    /// can be used directly to seed the job placement algorithm.
    ///
    /// Reads a lock-free atomic snapshot updated by the actor, so callers do not contend on the
    /// actor mailbox.
    pub fn get_load(&self) -> usize {
        self.total_job_cost.load(Ordering::Relaxed)
    }

    /// Returns permits for local splits
    ///
    /// The returned futures are guaranteed to resolve in order.
    pub(crate) async fn get_permits(
        &self,
        task_metadata: LeafSearchTaskMetadata,
    ) -> Vec<SearchPermitFuture> {
        self.get_permits_with_offload(task_metadata, usize::MAX)
            .await
    }

    /// Returns permits for local splits and a list of split indices to offload.
    ///
    /// The actor checks the current pending queue depth. If adding all splits
    /// would exceed `offload_threshold` pending requests, only enough splits
    /// to fill up to the threshold are processed locally; the rest are offloaded.
    ///
    /// The returned futures are guaranteed to resolve in order.
    ///
    /// If `offload_threshold` is 0, all splits are offloaded.
    /// If `offload_threshold` is usize::MAX, all splits are processed locally.
    pub(crate) async fn get_permits_with_offload(
        &self,
        task_metadata: LeafSearchTaskMetadata,
        offload_threshold: usize,
    ) -> Vec<SearchPermitFuture> {
        if task_metadata.split_job_costs.is_empty() {
            return Vec::new();
        }
        let (permit_sender, permit_receiver) = oneshot::channel();
        self.message_sender
            .send(SearchPermitMessage::RequestWithOffload {
                permit_resp_tx: permit_sender,
                task_metadata,
                offload_threshold,
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
    /// Warmup memory of all the split searches. The actor only creates an empty guard for each
    /// permit it grants, in grant order: split searches reserve their memory themselves.
    warmup_memory: GlobalWarmupMemory,
    /// Sum of [`LeafSearchTaskMetadata::split_job_costs`] for all queued and active tasks.
    ///
    /// Incremented when a task enters [`Self::permits_requests`], decremented when
    /// its [`SearchPermit`] is dropped (which happens both on normal completion and
    /// when the future is dropped before being resolved).
    ///
    /// Shared with [`SearchPermitProvider`] so callers can read the load lock-free.
    /// Only the actor mutates it, so [`Ordering::Relaxed`] is sufficient.
    total_job_cost: Arc<AtomicUsize>,
    permits_requests: BinaryHeap<LeafPermitRequest>,
}

struct SingleSplitPermitRequest {
    permit_sender: oneshot::Sender<SearchPermit>,
    job_cost: usize,
    requested_at: Instant,
}

struct LeafPermitRequest {
    /// Lower values have higher priority.
    priority: i32,
    /// Single split permit requests for this leaf search.
    single_split_permit_requests: std::vec::IntoIter<SingleSplitPermitRequest>,
    // Sum of job costs of all unconsumed requests in `single_split_permit_requests`.
    remaining_job_cost: usize,
}

impl Ord for LeafPermitRequest {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        // we compare other with self and not the other way around because we want a min-heap and
        // Rust's is a max-heap
        other
            .priority
            .cmp(&self.priority)
            .then_with(|| other.remaining_job_cost.cmp(&self.remaining_job_cost))
    }
}

impl PartialOrd for LeafPermitRequest {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl PartialEq for LeafPermitRequest {
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other).is_eq()
    }
}

impl Eq for LeafPermitRequest {}

impl LeafPermitRequest {
    // `task_metadata` must not be empty.
    fn from_task_metadata(
        task_metadata: LeafSearchTaskMetadata,
    ) -> (Self, Vec<SearchPermitFuture>) {
        let LeafSearchTaskMetadata {
            priority,
            split_job_costs,
        } = task_metadata;
        assert!(
            !split_job_costs.is_empty(),
            "task_metadata must not be empty"
        );
        // Stamped on every `SingleSplitPermitRequest` we're about to enqueue.
        // The actor will compute `requested_at.elapsed()` at grant time to
        // report the permit's acquisition latency.
        let requested_at = Instant::now();
        let mut remaining_job_cost = 0;
        let mut permits = Vec::with_capacity(split_job_costs.len());
        let mut single_split_permit_requests = Vec::with_capacity(split_job_costs.len());
        for job_cost in split_job_costs {
            let (tx, rx) = oneshot::channel();
            // we keep our internal list of permits and the returned wait handles in the
            // same order to make sure we emit each permit in the right order. Doing otherwise
            // may cause deadlocks
            remaining_job_cost += job_cost;
            single_split_permit_requests.push(SingleSplitPermitRequest {
                permit_sender: tx,
                job_cost,
                requested_at,
            });
            permits.push(SearchPermitFuture(rx));
        }
        (
            LeafPermitRequest {
                priority,
                single_split_permit_requests: single_split_permit_requests.into_iter(),
                remaining_job_cost,
            },
            permits,
        )
    }

    fn pop_next(&mut self) -> Option<SingleSplitPermitRequest> {
        let permit_request = self.single_split_permit_requests.next()?;

        if permit_request.job_cost > self.remaining_job_cost {
            warn!(
                job_cost = permit_request.job_cost,
                remaining_job_cost = self.remaining_job_cost,
                "remaining job cost underflow"
            );
        }
        self.remaining_job_cost = self
            .remaining_job_cost
            .saturating_sub(permit_request.job_cost);
        Some(permit_request)
    }

    fn is_empty(&self) -> bool {
        self.single_split_permit_requests.as_slice().is_empty()
    }
}

impl SearchPermitActor {
    async fn run(mut self) -> Self {
        // Stops when the last clone of SearchPermitProvider is dropped.
        while let Some(msg) = self.msg_receiver.recv().await {
            self.handle_message(msg);
        }
        self
    }

    fn handle_message(&mut self, msg: SearchPermitMessage) {
        match msg {
            SearchPermitMessage::RequestWithOffload {
                mut task_metadata,
                permit_resp_tx: permit_sender,
                offload_threshold,
            } => {
                let current_pending = self
                    .permits_requests
                    .iter()
                    .map(|req| req.single_split_permit_requests.as_slice().len())
                    .sum();
                // How many new splits can we accept locally before hitting the threshold.
                let local_capacity = offload_threshold.saturating_sub(current_pending);

                // If this indeed truncates the task_metadata vector, other splits will be
                // offloaded to lambdas.
                task_metadata.split_job_costs.truncate(local_capacity);

                // We special case here in order to avoid pushing empty request in the queue.
                // (they would never be removed)
                if task_metadata.split_job_costs.is_empty() {
                    let _ = permit_sender.send(Vec::new());
                    return;
                }

                // Increment total_job_cost for all tasks entering the queue (both pending and
                // those that will be granted immediately by assign_available_permits below).
                let added: usize = task_metadata.split_job_costs.iter().sum();
                // fetch_add returns the previous value, so add `added` to get the new total.
                let new_load = self.total_job_cost.fetch_add(added, Ordering::Relaxed) + added;
                SEARCHER_NODE_LOAD.set(new_load as f64);

                let (leaf_permit_request, permit_futures) =
                    LeafPermitRequest::from_task_metadata(task_metadata);
                self.permits_requests.push(leaf_permit_request);
                self.assign_available_permits();
                let _ = permit_sender.send(permit_futures);
            }
            SearchPermitMessage::FreeWarmupSlot => {
                self.num_warmup_slots_available += 1;
                self.assign_available_permits();
            }
            SearchPermitMessage::Drop { warmup_slot_freed } => {
                // total_job_cost is decremented synchronously in `SearchPermit::Drop`. This eases
                // testing (no need to wait for the queue to drain to observe the cost was updated)
                if !warmup_slot_freed {
                    self.num_warmup_slots_available += 1;
                }
                self.assign_available_permits();
            }
        }
    }

    /// Pops the next request if a warmup slot is available.
    fn pop_next_request_if_serviceable(&mut self) -> Option<SingleSplitPermitRequest> {
        if self.num_warmup_slots_available == 0 {
            return None;
        }
        let mut peeked = self.permits_requests.peek_mut()?;
        assert!(
            !peeked.is_empty(),
            "unexpected empty permits_requests present in the search permit provider queue"
        );
        let permit_request = peeked.pop_next()?;
        if peeked.is_empty() {
            PeekMut::pop(peeked);
        }
        Some(permit_request)
    }

    fn assign_available_permits(&mut self) {
        while let Some(permit_request) = self.pop_next_request_if_serviceable() {
            let ongoing_gauge_guard = GaugeGuard::new(&LEAF_SEARCH_SINGLE_SPLIT_TASKS_ONGOING, 1.0);
            self.num_warmup_slots_available -= 1;
            // Created at grant time: the warmup memory requests of split searches are served in
            // grant order.
            let memory_guard = self.warmup_memory.new_guard();
            permit_request
                .permit_sender
                .send(SearchPermit {
                    _ongoing_gauge_guard: ongoing_gauge_guard,
                    msg_sender: self.msg_sender.clone(),
                    warmup_memory: Arc::new(SplitWarmupMemory::new(memory_guard)),
                    warmup_slot_freed: false,
                    job_cost: permit_request.job_cost,
                    total_job_cost: self.total_job_cost.clone(),
                    wait_for_acquisition: permit_request.requested_at.elapsed(),
                })
                // if the requester dropped its receiver, we drop the newly
                // created SearchPermit which releases the resources
                .ok();
        }
        LEAF_SEARCH_SINGLE_SPLIT_TASKS_PENDING.set(self.permits_requests.len() as f64);
    }
}

/// Permit to search a single split.
pub struct SearchPermit {
    _ongoing_gauge_guard: GaugeGuard,
    msg_sender: mpsc::WeakUnboundedSender<SearchPermitMessage>,
    /// Shared with the split's `CachingDirectory`, see [`SearchPermit::warmup_memory`].
    warmup_memory: Arc<SplitWarmupMemory>,
    warmup_slot_freed: bool,
    job_cost: usize,
    /// Shared with the actor; decremented in `Drop`.
    total_job_cost: Arc<AtomicUsize>,
    /// Time the request spent in the permit queue.
    wait_for_acquisition: Duration,
}

impl SearchPermit {
    /// Time spent acquiring this permit (request → grant).
    pub fn wait_for_acquisition(&self) -> Duration {
        self.wait_for_acquisition
    }

    /// Reserves `initial_memory_allocation` for the warmup of this split search, waiting until it
    /// is available, then returns the warmup memory to give to the split's `CachingDirectory`.
    ///
    /// Contracts:
    /// - called once, before the warmup. Calling it again after the reservation was shrunk grows it
    ///   back;
    /// - `initial_memory_allocation` is lower or equal to the per-split limit. Otherwise, this
    ///   fails with `OutOfMemoryError`.
    ///
    /// The memory is freed when both the permit and all the clones returned here are dropped.
    pub async fn warmup_memory(
        &self,
        initial_memory_allocation: ByteSize,
    ) -> Result<Arc<SplitWarmupMemory>, OutOfMemoryError> {
        self.warmup_memory
            .reserve_capacity(initial_memory_allocation.as_u64())
            .await?;
        Ok(self.warmup_memory.clone())
    }

    /// Shrinks the memory reservation to `max_memory_allocation`, but never below the memory in
    /// use. Does nothing if the reservation is already smaller.
    pub fn shrink_memory_allocation(&self, max_memory_allocation: ByteSize) {
        self.warmup_memory
            .shrink_reservation(max_memory_allocation.as_u64());
    }

    /// Shrinks the memory reservation to the memory in use.
    pub fn release_unused_memory(&self) {
        self.warmup_memory.release_unused_memory();
    }

    /// Drop the warmup permit, allowing more downloads to be started. Only one
    /// slot is attached to each permit so calling this again has no effect.
    pub fn free_warmup_slot(&mut self) {
        if self.warmup_slot_freed {
            return;
        }
        self.warmup_slot_freed = true;
        send_if_still_running(&self.msg_sender, SearchPermitMessage::FreeWarmupSlot);
    }

    /// Memory currently reserved by this permit.
    #[cfg(test)]
    pub fn memory_allocation(&self) -> ByteSize {
        ByteSize(self.warmup_memory.reserved_num_bytes())
    }

    pub fn job_cost(&self) -> usize {
        self.job_cost
    }
}

impl Drop for SearchPermit {
    fn drop(&mut self) {
        let prev = self
            .total_job_cost
            .try_update(Ordering::Relaxed, Ordering::Relaxed, |v| {
                Some(v.saturating_sub(self.job_cost))
            })
            .expect("closure always returns Some");
        if self.job_cost > prev {
            warn!(
                job_cost = self.job_cost,
                total_job_cost = prev,
                "job cost underflow: more job cost released than allocated"
            );
        }
        // fetch_update returns the previous value, so subtract job_cost to get the new total.
        SEARCHER_NODE_LOAD.set(prev.saturating_sub(self.job_cost) as f64);
        send_if_still_running(
            &self.msg_sender,
            SearchPermitMessage::Drop {
                warmup_slot_freed: self.warmup_slot_freed,
            },
        );
        // If this permit holds the last reference to the warmup memory, the memory is freed when
        // the `warmup_memory` field is dropped, right after this function. The waiting split
        // searches are then served.
    }
}

/// Sends `msg` to the actor, unless the actor is stopped.
fn send_if_still_running(
    msg_sender: &mpsc::WeakUnboundedSender<SearchPermitMessage>,
    msg: SearchPermitMessage,
) {
    if let Some(sender) = msg_sender.upgrade() {
        sender
            .send(msg)
            // Receiver instance in the event loop is never dropped or
            // closed as long as there is a strong sender reference.
            .expect("Receiver should live longer than sender");
    }
}

pub struct SearchPermitFuture(oneshot::Receiver<SearchPermit>);

impl Future for SearchPermitFuture {
    type Output = SearchPermit;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let receiver = Pin::new(&mut self.get_mut().0);
        match receiver.poll(cx) {
            Poll::Ready(Ok(search_permit)) => Poll::Ready(search_permit),
            Poll::Ready(Err(_)) => panic!(
                "Failed to acquire permit. This should never happen! Please, report on https://github.com/quickwit-oss/quickwit/issues."
            ),
            Poll::Pending => Poll::Pending,
        }
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use futures::StreamExt;
    use quickwit_directories::MemoryReserver;
    use rand::seq::SliceRandom;
    use tokio::task::JoinSet;

    use super::*;

    fn make_splits(count: usize, job_cost: usize, priority: i32) -> LeafSearchTaskMetadata {
        LeafSearchTaskMetadata {
            priority,
            split_job_costs: vec![job_cost; count],
        }
    }

    #[tokio::test]
    async fn test_search_permit_priority_precedes_cost() {
        let permit_provider = SearchPermitProvider::new(1, ByteSize::mb(100));
        let blocker = permit_provider
            .get_permits(make_splits(1, 5, 0))
            .await
            .pop()
            .unwrap()
            .await;

        let negative_priority = permit_provider.get_permits(make_splits(1, 1000, -10)).await;
        let default_priority = permit_provider.get_permits(make_splits(1, 100, 0)).await;
        let positive_priority = permit_provider.get_permits(make_splits(1, 10, 10)).await;

        let mut join_set = JoinSet::new();
        for (request, permit_futures) in [
            ("negative", negative_priority),
            ("default", default_priority),
            ("positive", positive_priority),
        ] {
            for (split_idx, permit_future) in permit_futures.into_iter().enumerate() {
                join_set.spawn(async move {
                    let permit = permit_future.await;
                    (request, split_idx, permit)
                });
            }
        }

        drop(blocker);

        let mut execution_order = Vec::new();
        while let Some(result) = join_set.join_next().await {
            let (request, split_idx, _permit) = result.unwrap();
            execution_order.push((request, split_idx));
        }
        assert_eq!(
            execution_order,
            vec![("negative", 0), ("default", 0), ("positive", 0),]
        );
    }

    #[tokio::test]
    async fn test_search_permit_order_uses_remaining_job_cost() {
        let permit_provider = SearchPermitProvider::new(1, ByteSize::mb(100));
        let blocker = permit_provider
            .get_permits(make_splits(1, 5, 0))
            .await
            .pop()
            .unwrap()
            .await;

        let high_cost = permit_provider.get_permits(make_splits(1, 1000, 0)).await;
        let low_cost = permit_provider.get_permits(make_splits(3, 100, 0)).await;

        let mut join_set = JoinSet::new();
        for (request, permit_futures) in [("high", high_cost), ("low", low_cost)] {
            for (split_idx, permit_future) in permit_futures.into_iter().enumerate() {
                join_set.spawn(async move {
                    let permit = permit_future.await;
                    (request, split_idx, permit)
                });
            }
        }

        drop(blocker);

        let (request, split_idx, first_permit) = join_set.join_next().await.unwrap().unwrap();
        assert_eq!((request, split_idx), ("low", 0));
        let mut execution_order = vec![(request, split_idx)];

        // Hold the first permit while enqueueing work costing more than low's remaining
        // cost (200), but less than its original cost (300).
        let medium_cost = permit_provider.get_permits(make_splits(1, 250, 0)).await;
        for (split_idx, permit_future) in medium_cost.into_iter().enumerate() {
            join_set.spawn(async move {
                let permit = permit_future.await;
                ("medium", split_idx, permit)
            });
        }
        drop(first_permit);
        while let Some(result) = join_set.join_next().await {
            let (request, split_idx, _permit) = result.unwrap();
            execution_order.push((request, split_idx));
        }
        assert_eq!(
            execution_order,
            vec![
                ("low", 0),
                ("low", 1),
                ("low", 2),
                ("medium", 0),
                ("high", 0)
            ]
        );
    }

    #[tokio::test]
    async fn test_search_permit_order() {
        let permit_provider = SearchPermitProvider::new(1, ByteSize::mb(100));
        let mut all_futures = Vec::new();
        let first_batch_of_permits = permit_provider.get_permits(make_splits(10, 5, 0)).await;
        assert_eq!(first_batch_of_permits.len(), 10);
        all_futures.extend(
            first_batch_of_permits
                .into_iter()
                .enumerate()
                .map(move |(i, fut)| ((1, i), fut)),
        );

        let second_batch_of_permits = permit_provider.get_permits(make_splits(10, 5, 0)).await;
        assert_eq!(second_batch_of_permits.len(), 10);
        all_futures.extend(
            second_batch_of_permits
                .into_iter()
                .enumerate()
                .map(move |(i, fut)| ((2, i), fut)),
        );

        // not super useful, considering what join set does, but still a tiny bit more sound.
        all_futures.shuffle(&mut rand::rng());

        let mut join_set = JoinSet::new();
        for (res, fut) in all_futures {
            join_set.spawn(async move {
                let permit = fut.await;
                (res, permit)
            });
        }
        let mut ordered_result: Vec<(usize, usize)> = Vec::with_capacity(20);
        while let Some(Ok(((batch_id, order), _permit))) = join_set.join_next().await {
            ordered_result.push((batch_id, order));
        }

        assert_eq!(ordered_result.len(), 20);
        for (i, res) in ordered_result[0..10].iter().enumerate() {
            assert_eq!(res, &(1, i));
        }
        for (i, res) in ordered_result[10..20].iter().enumerate() {
            assert_eq!(res, &(2, i));
        }
    }

    #[tokio::test]
    async fn test_search_permit_order_with_concurrent_search() {
        let permit_provider = SearchPermitProvider::new(4, ByteSize::mb(100));
        let mut all_futures = Vec::new();
        let first_batch_of_permits = permit_provider.get_permits(make_splits(8, 5, 0)).await;
        assert_eq!(first_batch_of_permits.len(), 8);
        all_futures.extend(
            first_batch_of_permits
                .into_iter()
                .enumerate()
                .map(move |(i, fut)| ((1, i), fut)),
        );

        let second_batch_of_permits = permit_provider.get_permits(make_splits(2, 5, 0)).await;
        all_futures.extend(
            second_batch_of_permits
                .into_iter()
                .enumerate()
                .map(move |(i, fut)| ((2, i), fut)),
        );

        let third_batch_of_permits = permit_provider.get_permits(make_splits(6, 5, 0)).await;
        all_futures.extend(
            third_batch_of_permits
                .into_iter()
                .enumerate()
                .map(move |(i, fut)| ((3, i), fut)),
        );

        // not super useful, considering what join set does, but still a tiny bit more sound.
        all_futures.shuffle(&mut rand::rng());

        let mut join_set = JoinSet::new();
        for (res, fut) in all_futures {
            join_set.spawn(async move {
                let permit = fut.await;
                (res, permit)
            });
        }
        let mut ordered_result: Vec<(usize, usize)> = Vec::with_capacity(20);
        while let Some(Ok(((batch_id, order), _permit))) = join_set.join_next().await {
            ordered_result.push((batch_id, order));
        }

        let mut counters = [0; 4];
        let expected_result: Vec<(usize, usize)> = [
            1, 1, 1, 1, // initial 4 permits
            2, 2, 1, 1, 1, 1, 3, 3, 3, 3, 3, 3,
        ]
        .into_iter()
        .map(|batch_id| {
            let order = counters[batch_id];
            counters[batch_id] += 1;
            (batch_id, order)
        })
        .collect();

        // for the first 4 permits, the order is not well defined as they are all granted at once,
        // and we poll futures in a random order. We sort them to fix that artifact
        ordered_result[..4].sort();
        assert_eq!(ordered_result, expected_result);
    }

    #[tokio::test]
    async fn test_search_permit_early_drops() {
        let permit_provider = SearchPermitProvider::new(1, ByteSize::mb(100));
        let permit_fut1 = permit_provider
            .get_permits(make_splits(1, 5, 0))
            .await
            .into_iter()
            .next()
            .unwrap();
        let permit_fut2 = permit_provider
            .get_permits(make_splits(1, 5, 0))
            .await
            .into_iter()
            .next()
            .unwrap();
        drop(permit_fut1);
        let permit = permit_fut2.await;
        // The permit comes without memory: the split search reserves it.
        assert_eq!(permit.memory_allocation(), ByteSize(0));
        permit.warmup_memory(ByteSize::mb(10)).await.unwrap();
        assert_eq!(permit.memory_allocation(), ByteSize::mb(10));
        assert!(!permit_provider.actor_join_handle.is_finished());

        let _permit_fut3 = permit_provider
            .get_permits(make_splits(1, 5, 0))
            .await
            .into_iter()
            .next()
            .unwrap();
        let SearchPermitProvider {
            message_sender,
            actor_join_handle,
            ..
        } = permit_provider;
        drop(message_sender);
        Arc::into_inner(actor_join_handle).unwrap().await.unwrap();
    }

    /// Tries to wait for a permit
    async fn try_get(permit_fut: SearchPermitFuture) -> anyhow::Result<SearchPermit> {
        // using a short timeout is a bit flaky, but it should be enough for these tests
        let permit = tokio::time::timeout(Duration::from_millis(20), permit_fut).await?;
        Ok(permit)
    }

    /// Tries to reserve an initial warmup memory of 10MB for a permit.
    async fn try_reserve_initial_memory(
        permit: &SearchPermit,
    ) -> anyhow::Result<Arc<SplitWarmupMemory>> {
        // using a short timeout is a bit flaky, but it should be enough for these tests
        let warmup_memory_fut = permit.warmup_memory(ByteSize::mb(10));
        let warmup_memory =
            tokio::time::timeout(Duration::from_millis(20), warmup_memory_fut).await??;
        Ok(warmup_memory)
    }

    #[tokio::test]
    async fn test_memory_budget() {
        // 110MB - 10MB = 100MB are shared by the split searches other than the oldest one.
        let permit_provider = SearchPermitProvider::with_split_max_allocation(
            100,
            ByteSize::mb(110),
            ByteSize::mb(10),
        );
        let permit_futs = permit_provider.get_permits(make_splits(14, 5, 0)).await;
        // The permits only wait for warmup slots: they are all granted.
        let mut permits: Vec<SearchPermit> = futures::stream::iter(permit_futs.into_iter())
            .buffered(1)
            .collect()
            .await;
        let mut blocked_permits = permits.split_off(10).into_iter();
        assert_eq!(blocked_permits.len(), 4);
        // we should be able to reserve the memory of 10 permits right away (100MB / 10MB)
        for permit in &permits {
            try_reserve_initial_memory(permit).await.unwrap();
        }
        // the next permit is blocked by the memory budget
        let next_permit = blocked_permits.next().unwrap();
        try_reserve_initial_memory(&next_permit)
            .await
            .err()
            .unwrap();
        // if we drop one of the permits, the next one can reserve its memory
        permits.drain(0..1);
        try_reserve_initial_memory(&next_permit).await.unwrap();
        permits.push(next_permit);
        // the next permit is blocked again by the memory budget
        let next_permit = blocked_permits.next().unwrap();
        try_reserve_initial_memory(&next_permit)
            .await
            .err()
            .unwrap();
        // by setting a more accurate memory usage after a completed warmup, more permits can
        // reserve their memory
        permits[0].shrink_memory_allocation(ByteSize::mb(4));
        permits[1].shrink_memory_allocation(ByteSize::mb(6));
        try_reserve_initial_memory(&next_permit).await.unwrap();
    }

    #[tokio::test]
    async fn test_all_memory_freed_after_permits_dropped() {
        let permit_provider = SearchPermitProvider::new(2, ByteSize::mb(100));
        let permit_futs = permit_provider.get_permits(make_splits(6, 5, 0)).await;
        // Drop a pending permit future before it is granted.
        let mut permit_futs = permit_futs.into_iter();
        drop(permit_futs.next_back());
        for permit_fut in permit_futs {
            let mut permit = permit_fut.await;
            permit.shrink_memory_allocation(ByteSize::mb(3));
            permit.free_warmup_slot();
        }
        let permit_actor = permit_provider.stop_and_unwrap().await;
        assert_eq!(permit_actor.warmup_memory.allocated(), 0);
        assert!(permit_actor.permits_requests.is_empty());
    }

    #[tokio::test]
    async fn test_get_permits_with_offload_threshold_max_returns_all() {
        let permit_provider = SearchPermitProvider::new(100, ByteSize::mb(100));
        let permits = permit_provider
            .get_permits_with_offload(make_splits(8, 5, 0), usize::MAX)
            .await;
        assert_eq!(permits.len(), 8);
    }

    #[tokio::test]
    async fn test_get_permits_with_offload_threshold_zero_returns_none() {
        let permit_provider = SearchPermitProvider::new(100, ByteSize::mb(100));
        let permits = permit_provider
            .get_permits_with_offload(make_splits(5, 5, 0), 0)
            .await;
        assert!(permits.is_empty());
        let permit_actor = permit_provider.stop_and_unwrap().await;
        assert!(permit_actor.permits_requests.is_empty());
    }

    #[tokio::test]
    async fn test_get_permits_with_offload_truncates_to_threshold() {
        let permit_provider = SearchPermitProvider::new(100, ByteSize::mb(100));
        let permits = permit_provider
            .get_permits_with_offload(make_splits(10, 5, 0), 4)
            .await;
        assert_eq!(permits.len(), 4);
    }

    #[tokio::test]
    async fn test_get_permits_with_offload_futures_resolve_in_order() {
        // We use a search permit provider with a capacity of 1 to make sure that the permits are
        // resolved in order.
        let permit_provider = SearchPermitProvider::new(1, ByteSize::mb(100));
        let permits = permit_provider
            .get_permits_with_offload(make_splits(4, 5, 0), 10)
            .await;
        assert_eq!(permits.len(), 4);
        let mut futs: Vec<_> = permits
            .into_iter()
            .enumerate()
            .map(|(i, permit_fut)| async move {
                permit_fut.await;
                i
            })
            .collect();
        futs.shuffle(&mut rand::rng());
        let mut join_set = JoinSet::new();
        for fut in futs {
            join_set.spawn(fut);
        }
        let mut results = Vec::new();
        while let Some(result) = join_set.join_next().await {
            results.push(result.unwrap());
        }
        assert_eq!(results, vec![0, 1, 2, 3]);
    }

    #[tokio::test]
    async fn test_get_permits_with_offload_pending_consumed_frees_capacity() {
        let permit_provider = SearchPermitProvider::new(100, ByteSize::mb(100));
        // First call: 4 splits, threshold 6.
        let first_permits = permit_provider
            .get_permits_with_offload(make_splits(4, 5, 0), 6)
            .await;
        assert_eq!(first_permits.len(), 4);
        // Consume all permits from the first batch (they resolve and get dropped).
        for permit_fut in first_permits {
            let _permit = permit_fut.await;
        }
        // Second call: the consumed permits no longer count as pending.
        let second_permits = permit_provider
            .get_permits_with_offload(make_splits(5, 5, 0), 6)
            .await;
        assert_eq!(second_permits.len(), 5);
    }

    #[tokio::test]
    async fn test_warmup_slot() {
        let permit_provider = SearchPermitProvider::new(10, ByteSize::mb(100));
        let mut permit_futs = permit_provider.get_permits(make_splits(16, 5, 0)).await;
        let mut remaining_permit_futs = permit_futs.split_off(10).into_iter();
        assert_eq!(remaining_permit_futs.len(), 6);
        // we should be able to obtain 10 permits right away
        let mut permits: Vec<SearchPermit> = futures::stream::iter(permit_futs.into_iter())
            .buffered(1)
            .collect()
            .await;
        // the next permit is blocked by the warmup slots
        let next_blocked_permit_fut = remaining_permit_futs.next().unwrap();
        try_get(next_blocked_permit_fut).await.err().unwrap();
        // if we drop one of the permits, we can get a new one
        permits.drain(0..1);
        let next_permit_fut = remaining_permit_futs.next().unwrap();
        permits.push(try_get(next_permit_fut).await.unwrap());
        // the next permit is blocked again by the warmup slots
        let next_blocked_permit_fut = remaining_permit_futs.next().unwrap();
        try_get(next_blocked_permit_fut).await.err().unwrap();
        // we can explicitly free the warmup slot on a permit
        permits[0].free_warmup_slot();
        let next_permit_fut = remaining_permit_futs.next().unwrap();
        permits.push(try_get(next_permit_fut).await.unwrap());
        // dropping that same permit does not free up another slot
        permits.drain(0..1);
        let next_blocked_permit_fut = remaining_permit_futs.next().unwrap();
        try_get(next_blocked_permit_fut).await.err().unwrap();
        // but dropping a permit for which the slot wasn't explicitly free does free up a slot
        permits.drain(0..1);
        let next_blocked_permit_fut = remaining_permit_futs.next().unwrap();
        permits.push(try_get(next_blocked_permit_fut).await.unwrap());
    }

    #[tokio::test]
    async fn test_get_load_counts_queued_and_active() {
        let permit_provider = SearchPermitProvider::new(1, ByteSize::mb(100));

        assert_eq!(permit_provider.get_load(), 0);

        let task_metadata = LeafSearchTaskMetadata {
            priority: 0,
            split_job_costs: vec![7, 3, 5],
        };
        let mut permit_futs = permit_provider.get_permits(task_metadata).await;

        assert_eq!(permit_provider.get_load(), 15);

        let first_permit = permit_futs.remove(0).await;
        assert_eq!(permit_provider.get_load(), 15);
        drop(first_permit);
        assert_eq!(permit_provider.get_load(), 8);

        let second_permit = permit_futs.remove(0).await;
        assert_eq!(permit_provider.get_load(), 8);
        drop(second_permit);
        assert_eq!(permit_provider.get_load(), 5);

        let third_permit = permit_futs.remove(0).await;
        assert_eq!(permit_provider.get_load(), 5);
        drop(third_permit);
        assert_eq!(permit_provider.get_load(), 0);
    }

    async fn get_single_permit(permit_provider: &SearchPermitProvider) -> SearchPermit {
        permit_provider
            .get_permits(make_splits(1, 5, 0))
            .await
            .pop()
            .unwrap()
            .await
    }

    #[tokio::test]
    async fn test_reserved_memory_blocks_initial_reservations_until_freed() {
        // 100MB - 95MB = 5MB are shared by the split searches other than the oldest one.
        let permit_provider = SearchPermitProvider::with_split_max_allocation(
            10,
            ByteSize::mb(100),
            ByteSize::mb(95),
        );
        let permit = get_single_permit(&permit_provider).await;
        // Only the growth of the reservation matters here: the memory in use is released right
        // away, but the permit keeps its 95MB reservation.
        {
            let warmup_memory = permit.warmup_memory(ByteSize::mb(10)).await.unwrap();
            let reservation = warmup_memory
                .reserve_memory(ByteSize::mb(95).as_u64())
                .await
                .unwrap();
            drop(reservation);
        }
        // Permits only wait for warmup slots: the next one is granted right away.
        let next_permit = get_single_permit(&permit_provider).await;
        // The oldest split search holds 95MB: the next one cannot reserve its 10MB.
        try_reserve_initial_memory(&next_permit)
            .await
            .err()
            .unwrap();
        // Dropping the permit frees its memory.
        drop(permit);
        try_reserve_initial_memory(&next_permit).await.unwrap();
        drop(next_permit);
        let permit_actor = permit_provider.stop_and_unwrap().await;
        assert_eq!(permit_actor.warmup_memory.allocated(), 0);
    }

    #[tokio::test]
    async fn test_warmup_memory_freed_when_last_reference_dropped() {
        let permit_provider = SearchPermitProvider::with_split_max_allocation(
            10,
            ByteSize::mb(100),
            ByteSize::mb(95),
        );
        let permit = get_single_permit(&permit_provider).await;
        // Held by the split's `CachingDirectory` in practice.
        let warmup_memory = permit.warmup_memory(ByteSize::mb(95)).await.unwrap();
        let next_permit = get_single_permit(&permit_provider).await;
        // The memory is still referenced by the directory, so the next permit cannot reserve its
        // memory.
        drop(permit);
        try_reserve_initial_memory(&next_permit)
            .await
            .err()
            .unwrap();
        // Dropping the last reference frees the memory.
        drop(warmup_memory);
        try_reserve_initial_memory(&next_permit).await.unwrap();
        drop(next_permit);
        let permit_actor = permit_provider.stop_and_unwrap().await;
        assert_eq!(permit_actor.warmup_memory.allocated(), 0);
    }
}
