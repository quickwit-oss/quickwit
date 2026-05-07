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
use std::collections::BinaryHeap;
use std::collections::binary_heap::PeekMut;
use std::sync::Arc;

use anyhow::Context;
use async_trait::async_trait;
use quickwit_actors::{Actor, ActorContext, ActorExitStatus, Handler, Mailbox};
#[cfg(feature = "metrics")]
use quickwit_parquet_engine::merge::policy::ParquetMergeOperation;
use tantivy::TrackedObject;
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use tracing::error;

use super::MergeSplitDownloader;
#[cfg(feature = "metrics")]
use super::parquet_pipeline::{ParquetMergeSplitDownloader, ParquetMergeTask};
use crate::merge_policy::{MergeOperation, MergeTask};

pub struct MergePermit {
    _semaphore_permit: Option<OwnedSemaphorePermit>,
    merge_scheduler_mailbox: Option<Mailbox<MergeSchedulerService>>,
}

impl MergePermit {
    #[cfg(any(test, feature = "testsuite"))]
    pub fn for_test() -> MergePermit {
        MergePermit {
            _semaphore_permit: None,
            merge_scheduler_mailbox: None,
        }
    }
}

impl Drop for MergePermit {
    fn drop(&mut self) {
        let Some(merge_scheduler_mailbox) = self.merge_scheduler_mailbox.take() else {
            return;
        };
        if merge_scheduler_mailbox
            .send_message_with_high_priority(PermitReleased)
            .is_err()
        {
            error!("merge scheduler service is dead");
        }
    }
}

pub async fn schedule_merge(
    merge_scheduler_service: &Mailbox<MergeSchedulerService>,
    merge_operation: TrackedObject<MergeOperation>,
    merge_split_downloader_mailbox: Mailbox<MergeSplitDownloader>,
) -> anyhow::Result<()> {
    let schedule_merge = ScheduleMerge::new(merge_operation, merge_split_downloader_mailbox);
    // TODO add backpressure.
    merge_scheduler_service
        .ask(schedule_merge)
        .await
        .context("failed to acquire permit")?;
    Ok(())
}

#[cfg(feature = "metrics")]
pub async fn schedule_parquet_merge(
    merge_scheduler_service: &Mailbox<MergeSchedulerService>,
    merge_operation: TrackedObject<ParquetMergeOperation>,
    merge_split_downloader_mailbox: Mailbox<ParquetMergeSplitDownloader>,
) -> anyhow::Result<()> {
    let schedule_merge = ScheduleParquetMerge::new(merge_operation, merge_split_downloader_mailbox);
    merge_scheduler_service
        .ask(schedule_merge)
        .await
        .context("failed to schedule parquet merge")?;
    Ok(())
}

struct ScheduledMerge {
    score: u64,
    id: u64, //< just for total ordering.
    merge_operation: TrackedObject<MergeOperation>,
    split_downloader_mailbox: Mailbox<MergeSplitDownloader>,
}

impl ScheduledMerge {
    fn order_key(&self) -> (u64, Reverse<u64>) {
        (self.score, std::cmp::Reverse(self.id))
    }
}

impl Eq for ScheduledMerge {}

impl PartialEq for ScheduledMerge {
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other).is_eq()
    }
}

impl PartialOrd for ScheduledMerge {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for ScheduledMerge {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.order_key().cmp(&other.order_key())
    }
}

#[cfg(feature = "metrics")]
struct ScheduledParquetMerge {
    score: u64,
    id: u64,
    merge_operation: TrackedObject<ParquetMergeOperation>,
    split_downloader_mailbox: Mailbox<ParquetMergeSplitDownloader>,
}

#[cfg(feature = "metrics")]
impl ScheduledParquetMerge {
    fn order_key(&self) -> (u64, Reverse<u64>) {
        (self.score, Reverse(self.id))
    }
}

#[cfg(feature = "metrics")]
impl Eq for ScheduledParquetMerge {}

#[cfg(feature = "metrics")]
impl PartialEq for ScheduledParquetMerge {
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other).is_eq()
    }
}

#[cfg(feature = "metrics")]
impl PartialOrd for ScheduledParquetMerge {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

#[cfg(feature = "metrics")]
impl Ord for ScheduledParquetMerge {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.order_key().cmp(&other.order_key())
    }
}

/// The merge scheduler service is in charge of keeping track of all scheduled merge operations,
/// and schedule them in the best possible order, respecting the `merge_concurrency` limit.
///
/// This actor is not supervised and should stay as simple as possible.
/// In particular,
/// - the `ScheduleMerge` handler should reply in microseconds.
/// - the task should never be dropped before reaching its `split_downloader_mailbox` destination as
///   it would break the consistency of `MergePlanner` with the metastore (ie: several splits will
///   never be merged).
pub struct MergeSchedulerService {
    merge_semaphore: Arc<Semaphore>,
    merge_concurrency: usize,
    pending_merge_queue: BinaryHeap<ScheduledMerge>,
    #[cfg(feature = "metrics")]
    pending_parquet_merge_queue: BinaryHeap<ScheduledParquetMerge>,
    next_merge_id: u64,
    pending_merge_bytes: u64,
}

impl Default for MergeSchedulerService {
    fn default() -> MergeSchedulerService {
        MergeSchedulerService::new(3)
    }
}

impl MergeSchedulerService {
    pub fn new(merge_concurrency: usize) -> MergeSchedulerService {
        let merge_semaphore = Arc::new(Semaphore::new(merge_concurrency));
        MergeSchedulerService {
            merge_semaphore,
            merge_concurrency,
            pending_merge_queue: BinaryHeap::default(),
            #[cfg(feature = "metrics")]
            pending_parquet_merge_queue: BinaryHeap::default(),
            next_merge_id: 0,
            pending_merge_bytes: 0,
        }
    }

    fn schedule_pending_merges(&mut self, ctx: &ActorContext<Self>) {
        // We schedule as many pending merges as we can,
        // until there are no permits available or merges to schedule.
        loop {
            let merge_semaphore = self.merge_semaphore.clone();
            let Some(next_merge) = self.pending_merge_queue.peek_mut() else {
                // No merge to schedule.
                break;
            };
            let Ok(semaphore_permit) = Semaphore::try_acquire_owned(merge_semaphore) else {
                // No permit available right away.
                break;
            };
            let merge_permit = MergePermit {
                _semaphore_permit: Some(semaphore_permit),
                merge_scheduler_mailbox: Some(ctx.mailbox().clone()),
            };
            let ScheduledMerge {
                merge_operation,
                split_downloader_mailbox,
                ..
            } = PeekMut::pop(next_merge);
            let merge_task = MergeTask {
                merge_operation,
                _merge_permit: merge_permit,
            };
            self.pending_merge_bytes -= merge_task.merge_operation.total_num_bytes();
            crate::metrics::INDEXER_METRICS
                .pending_merge_operations
                .set(self.pending_merge_queue.len() as i64);
            crate::metrics::INDEXER_METRICS
                .pending_merge_bytes
                .set(self.pending_merge_bytes as i64);
            match split_downloader_mailbox.try_send_message(merge_task) {
                Ok(_) => {}
                Err(quickwit_actors::TrySendError::Full(_)) => {
                    // The split downloader mailbox has an unbounded queue capacity,
                    error!("split downloader queue is full: please report");
                }
                Err(quickwit_actors::TrySendError::Disconnected) => {
                    // It means the split downloader is dead.
                    // This is fine, the merge pipeline has probably been restarted.
                }
            }
        }
        // Dispatch pending Parquet merges. Shares the same semaphore as
        // Tantivy merges so the node doesn't exceed its merge concurrency
        // limit regardless of how many pipelines of each type are running.
        #[cfg(feature = "metrics")]
        loop {
            let merge_semaphore = self.merge_semaphore.clone();
            let Some(next_merge) = self.pending_parquet_merge_queue.peek_mut() else {
                break;
            };
            let Ok(semaphore_permit) = Semaphore::try_acquire_owned(merge_semaphore) else {
                break;
            };
            let merge_permit = MergePermit {
                _semaphore_permit: Some(semaphore_permit),
                merge_scheduler_mailbox: Some(ctx.mailbox().clone()),
            };
            let ScheduledParquetMerge {
                merge_operation,
                split_downloader_mailbox,
                ..
            } = PeekMut::pop(next_merge);
            // The permit is owned by the task and released via Drop when
            // the executor finishes, triggering PermitReleased back here.
            // Drop-based release ensures the semaphore is freed even on panic.
            let parquet_merge_task = ParquetMergeTask {
                merge_operation,
                merge_permit,
            };
            self.pending_merge_bytes -= parquet_merge_task.merge_operation.total_size_bytes();
            crate::metrics::INDEXER_METRICS
                .pending_merge_operations
                .set(
                    self.pending_merge_queue.len() as i64
                        + self.pending_parquet_merge_queue.len() as i64,
                );
            crate::metrics::INDEXER_METRICS
                .pending_merge_bytes
                .set(self.pending_merge_bytes as i64);
            match split_downloader_mailbox.try_send_message(parquet_merge_task) {
                Ok(_) => {}
                Err(quickwit_actors::TrySendError::Full(_)) => {
                    error!("parquet split downloader queue is full: please report");
                }
                Err(quickwit_actors::TrySendError::Disconnected) => {
                    // The downloader is dead — pipeline probably restarted.
                }
            }
        }

        let num_merges =
            self.merge_concurrency as i64 - self.merge_semaphore.available_permits() as i64;
        crate::metrics::INDEXER_METRICS
            .ongoing_merge_operations
            .set(num_merges);
    }
}

#[async_trait]
impl Actor for MergeSchedulerService {
    type ObservableState = ();

    fn observable_state(&self) {}

    async fn initialize(&mut self, _ctx: &ActorContext<Self>) -> Result<(), ActorExitStatus> {
        Ok(())
    }
}

#[derive(Debug)]
struct ScheduleMerge {
    score: u64,
    merge_operation: TrackedObject<MergeOperation>,
    split_downloader_mailbox: Mailbox<MergeSplitDownloader>,
}

/// Scores a merge operation for priority scheduling.
///
/// Higher score = scheduled sooner. Prefers merges that strongly reduce
/// split count relative to their total byte cost. Used by both Tantivy
/// and Parquet merge scheduling.
fn score_merge(num_splits: usize, total_num_bytes: u64) -> u64 {
    if total_num_bytes == 0 {
        return u64::MAX;
    }
    // We will remove num_splits and add 1 merged split.
    let delta_num_splits = (num_splits - 1) as u64;
    // Integer arithmetic to avoid `f64 are not ordered` silliness.
    (delta_num_splits << 48)
        .checked_div(total_num_bytes)
        .unwrap_or(1u64)
}

fn score_merge_operation(merge_operation: &MergeOperation) -> u64 {
    score_merge(
        merge_operation.splits.len(),
        merge_operation.total_num_bytes(),
    )
}

impl ScheduleMerge {
    pub fn new(
        merge_operation: TrackedObject<MergeOperation>,
        split_downloader_mailbox: Mailbox<MergeSplitDownloader>,
    ) -> ScheduleMerge {
        let score = score_merge_operation(&merge_operation);
        ScheduleMerge {
            score,
            merge_operation,
            split_downloader_mailbox,
        }
    }
}

#[async_trait]
impl Handler<ScheduleMerge> for MergeSchedulerService {
    type Reply = ();

    async fn handle(
        &mut self,
        schedule_merge: ScheduleMerge,
        ctx: &ActorContext<Self>,
    ) -> Result<(), ActorExitStatus> {
        let ScheduleMerge {
            score,
            merge_operation,
            split_downloader_mailbox,
        } = schedule_merge;
        let merge_id = self.next_merge_id;
        self.next_merge_id += 1;
        let scheduled_merge = ScheduledMerge {
            score,
            id: merge_id,
            merge_operation,
            split_downloader_mailbox,
        };
        self.pending_merge_bytes += scheduled_merge.merge_operation.total_num_bytes();
        self.pending_merge_queue.push(scheduled_merge);
        crate::metrics::INDEXER_METRICS
            .pending_merge_operations
            .set(self.pending_merge_queue.len() as i64);
        crate::metrics::INDEXER_METRICS
            .pending_merge_bytes
            .set(self.pending_merge_bytes as i64);
        self.schedule_pending_merges(ctx);
        Ok(())
    }
}

#[derive(Debug)]
struct PermitReleased;

#[async_trait]
impl Handler<PermitReleased> for MergeSchedulerService {
    type Reply = ();

    async fn handle(
        &mut self,
        _: PermitReleased,
        ctx: &ActorContext<Self>,
    ) -> Result<(), ActorExitStatus> {
        self.schedule_pending_merges(ctx);
        Ok(())
    }
}

// --- Parquet merge scheduling (feature-gated) ---

#[cfg(feature = "metrics")]
fn score_parquet_merge_operation(merge_operation: &ParquetMergeOperation) -> u64 {
    score_merge(
        merge_operation.splits.len(),
        merge_operation.total_size_bytes(),
    )
}

#[cfg(feature = "metrics")]
#[derive(Debug)]
struct ScheduleParquetMerge {
    score: u64,
    merge_operation: TrackedObject<ParquetMergeOperation>,
    split_downloader_mailbox: Mailbox<ParquetMergeSplitDownloader>,
}

#[cfg(feature = "metrics")]
impl ScheduleParquetMerge {
    pub fn new(
        merge_operation: TrackedObject<ParquetMergeOperation>,
        split_downloader_mailbox: Mailbox<ParquetMergeSplitDownloader>,
    ) -> Self {
        let score = score_parquet_merge_operation(&merge_operation);
        Self {
            score,
            merge_operation,
            split_downloader_mailbox,
        }
    }
}

#[cfg(feature = "metrics")]
#[async_trait]
impl Handler<ScheduleParquetMerge> for MergeSchedulerService {
    type Reply = ();

    async fn handle(
        &mut self,
        schedule_merge: ScheduleParquetMerge,
        ctx: &ActorContext<Self>,
    ) -> Result<(), ActorExitStatus> {
        let ScheduleParquetMerge {
            score,
            merge_operation,
            split_downloader_mailbox,
        } = schedule_merge;
        let merge_id = self.next_merge_id;
        self.next_merge_id += 1;
        let scheduled = ScheduledParquetMerge {
            score,
            id: merge_id,
            merge_operation,
            split_downloader_mailbox,
        };
        self.pending_merge_bytes += scheduled.merge_operation.total_size_bytes();
        self.pending_parquet_merge_queue.push(scheduled);
        crate::metrics::INDEXER_METRICS
            .pending_merge_operations
            .set(
                self.pending_merge_queue.len() as i64
                    + self.pending_parquet_merge_queue.len() as i64,
            );
        crate::metrics::INDEXER_METRICS
            .pending_merge_bytes
            .set(self.pending_merge_bytes as i64);
        self.schedule_pending_merges(ctx);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use quickwit_actors::Universe;
    use quickwit_metastore::SplitMetadata;
    use tantivy::Inventory;
    use tokio::time::timeout;

    use super::*;
    use crate::merge_policy::{MergeOperation, MergeTask};

    fn build_merge_operation(num_splits: usize, num_bytes_per_split: u64) -> MergeOperation {
        let splits: Vec<SplitMetadata> = std::iter::repeat_with(|| SplitMetadata {
            footer_offsets: num_bytes_per_split..num_bytes_per_split,
            ..Default::default()
        })
        .take(num_splits)
        .collect();
        MergeOperation::new_merge_operation(splits)
    }

    #[test]
    fn test_score_merge_operation() {
        let score_merge_operation_aux = |num_splits, num_bytes_per_split| {
            let merge_operation = build_merge_operation(num_splits, num_bytes_per_split);
            score_merge_operation(&merge_operation)
        };
        assert!(score_merge_operation_aux(10, 10_000_000) < score_merge_operation_aux(10, 999_999));
        assert!(
            score_merge_operation_aux(10, 10_000_000) > score_merge_operation_aux(9, 10_000_000)
        );
        assert_eq!(
            // 9 - 1 = 8 splits removed.
            score_merge_operation_aux(9, 10_000_000),
            // 5 - 1  = 4 splits removed.
            score_merge_operation_aux(5, 10_000_000 * 9 / 10)
        );
    }

    #[tokio::test]
    async fn test_merge_schedule_service_prioritize() {
        let universe = Universe::new();
        let (merge_scheduler_service, _) = universe
            .spawn_builder()
            .spawn(MergeSchedulerService::new(2));
        let inventory = Inventory::new();

        let (merge_split_downloader_mailbox, merge_split_downloader_inbox) =
            universe.create_test_mailbox();
        {
            let large_merge_operation = build_merge_operation(10, 4_000_000);
            let tracked_large_merge_operation = inventory.track(large_merge_operation);
            schedule_merge(
                &merge_scheduler_service,
                tracked_large_merge_operation,
                merge_split_downloader_mailbox.clone(),
            )
            .await
            .unwrap();
        }
        {
            let large_merge_operation2 = build_merge_operation(10, 3_000_000);
            let tracked_large_merge_operation2 = inventory.track(large_merge_operation2);
            schedule_merge(
                &merge_scheduler_service,
                tracked_large_merge_operation2,
                merge_split_downloader_mailbox.clone(),
            )
            .await
            .unwrap();
        }
        {
            let large_merge_operation2 = build_merge_operation(10, 5_000_000);
            let tracked_large_merge_operation2 = inventory.track(large_merge_operation2);
            schedule_merge(
                &merge_scheduler_service,
                tracked_large_merge_operation2,
                merge_split_downloader_mailbox.clone(),
            )
            .await
            .unwrap();
        }
        {
            let large_merge_operation2 = build_merge_operation(10, 2_000_000);
            let tracked_large_merge_operation2 = inventory.track(large_merge_operation2);
            schedule_merge(
                &merge_scheduler_service,
                tracked_large_merge_operation2,
                merge_split_downloader_mailbox.clone(),
            )
            .await
            .unwrap();
        }
        {
            let large_merge_operation2 = build_merge_operation(10, 1_000_000);
            let tracked_large_merge_operation2 = inventory.track(large_merge_operation2);
            schedule_merge(
                &merge_scheduler_service,
                tracked_large_merge_operation2,
                merge_split_downloader_mailbox.clone(),
            )
            .await
            .unwrap();
        }
        {
            let merge_task: MergeTask = merge_split_downloader_inbox
                .recv_typed_message::<MergeTask>()
                .await
                .unwrap();
            assert_eq!(
                merge_task.merge_operation.splits[0].footer_offsets.end,
                4_000_000
            );
            let merge_task2: MergeTask = merge_split_downloader_inbox
                .recv_typed_message::<MergeTask>()
                .await
                .unwrap();
            assert_eq!(
                merge_task2.merge_operation.splits[0].footer_offsets.end,
                3_000_000
            );
            assert!(
                timeout(
                    Duration::from_millis(200),
                    merge_split_downloader_inbox.recv_typed_message::<MergeTask>()
                )
                .await
                .is_err()
            );
        }
        {
            let merge_task: MergeTask = merge_split_downloader_inbox
                .recv_typed_message::<MergeTask>()
                .await
                .unwrap();
            assert_eq!(
                merge_task.merge_operation.splits[0].footer_offsets.end,
                1_000_000
            );
        }
        {
            let merge_task: MergeTask = merge_split_downloader_inbox
                .recv_typed_message::<MergeTask>()
                .await
                .unwrap();
            assert_eq!(
                merge_task.merge_operation.splits[0].footer_offsets.end,
                2_000_000
            );
        }
        {
            let merge_task: MergeTask = merge_split_downloader_inbox
                .recv_typed_message::<MergeTask>()
                .await
                .unwrap();
            assert_eq!(
                merge_task.merge_operation.splits[0].footer_offsets.end,
                5_000_000
            );
        }
        universe.assert_quit().await;
    }
}
