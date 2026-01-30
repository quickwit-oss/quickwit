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

use std::collections::HashMap;
use std::fmt;
use std::ops::{Deref, DerefMut};
use std::path::Path;
use std::sync::{Arc, Weak};
use std::time::{Duration, Instant};

use mrecordlog::error::{DeleteQueueError, TruncateError};
use quickwit_common::pretty::PrettyDisplay;
use quickwit_common::rate_limiter::{RateLimiter, RateLimiterSettings};
use quickwit_doc_mapper::DocMapper;
use quickwit_proto::control_plane::AdviseResetShardsResponse;
use quickwit_proto::ingest::ingester::IngesterStatus;
use quickwit_proto::ingest::{IngestV2Error, IngestV2Result, ShardState};
use quickwit_proto::types::{DocMappingUid, IndexUid, Position, QueueId, SourceId, split_queue_id};
use tokio::sync::{Mutex, MutexGuard, RwLock, RwLockMappedWriteGuard, RwLockWriteGuard, watch};
use tracing::{error, info};

use super::models::IngesterShard;
use super::rate_meter::RateMeter;
use super::replication::{ReplicationStreamTaskHandle, ReplicationTaskHandle};
use crate::ingest_v2::mrecordlog_utils::{force_delete_queue, queue_position_range};
use crate::mrecordlog_async::MultiRecordLogAsync;
use crate::{FollowerId, LeaderId};

/// Stores the state of the ingester and attempts to prevent deadlocks by exposing an API that
/// guarantees that the internal data structures are always locked in the same order.
///
/// `lock_partially` locks `inner` only, while `lock_fully` locks both `inner` and `mrecordlog`. Use
/// the former when you only need to access the in-memory state of the ingester and the latter when
/// you need to access both the in-memory state AND the WAL.
#[derive(Clone)]
pub(super) struct IngesterState {
    // `inner` is a mutex because it's almost always accessed mutably.
    inner: Arc<Mutex<InnerIngesterState>>,
    mrecordlog: Arc<RwLock<Option<MultiRecordLogAsync>>>,
    pub status_rx: watch::Receiver<IngesterStatus>,
}

pub(super) struct InnerIngesterState {
    pub shards: HashMap<QueueId, IngesterShard>,
    pub doc_mappers: HashMap<DocMappingUid, Weak<DocMapper>>,
    // Replication stream opened with followers.
    pub replication_streams: HashMap<FollowerId, ReplicationStreamTaskHandle>,
    // Replication tasks running for each replication stream opened with leaders.
    pub replication_tasks: HashMap<LeaderId, ReplicationTaskHandle>,
    status: IngesterStatus,
    status_tx: watch::Sender<IngesterStatus>,
}

impl InnerIngesterState {
    pub fn status(&self) -> IngesterStatus {
        self.status
    }

    pub fn set_status(&mut self, status: IngesterStatus) {
        self.status = status;
        self.status_tx.send(status).expect("channel should be open");
    }

    /// Returns the shard with the most available permits for this index and source.
    pub fn find_most_capacity_shard_mut(
        &mut self,
        index_uid: &IndexUid,
        source_id: &SourceId,
    ) -> Option<&mut IngesterShard> {
        self.shards
            .values_mut()
            .filter(|shard| {
                shard.is_open() && shard.index_uid == *index_uid && shard.source_id == *source_id
            })
            // We use reduce here because we need a mutable borrow of the shard, since
            // available_permits() calls refill, which is a mutator.
            .reduce(|best, current| {
                if current.rate_limiter.available_permits() > best.rate_limiter.available_permits()
                {
                    current
                } else {
                    best
                }
            })
    }
}

impl IngesterState {
    fn new() -> Self {
        let status = IngesterStatus::Initializing;
        let (status_tx, status_rx) = watch::channel(status);
        let inner = InnerIngesterState {
            shards: Default::default(),
            doc_mappers: Default::default(),
            replication_streams: Default::default(),
            replication_tasks: Default::default(),
            status,
            status_tx,
        };
        let inner = Arc::new(Mutex::new(inner));
        let mrecordlog = Arc::new(RwLock::new(None));

        Self {
            inner,
            mrecordlog,
            status_rx,
        }
    }

    pub fn load(wal_dir_path: &Path, rate_limiter_settings: RateLimiterSettings) -> Self {
        let state = Self::new();
        let state_clone = state.clone();
        let wal_dir_path = wal_dir_path.to_path_buf();

        let init_future = async move {
            state_clone.init(&wal_dir_path, rate_limiter_settings).await;
        };
        tokio::spawn(init_future);

        state
    }

    #[cfg(test)]
    pub async fn for_test() -> (tempfile::TempDir, Self) {
        let temp_dir = tempfile::tempdir().unwrap();
        let mut state = IngesterState::load(temp_dir.path(), RateLimiterSettings::default());

        state
            .status_rx
            .wait_for(|status| *status == IngesterStatus::Ready)
            .await
            .unwrap();

        (temp_dir, state)
    }

    /// Initializes the internal state of the ingester. It loads the local WAL, then lists all its
    /// queues. Empty queues are deleted, while non-empty queues are recovered. However, the
    /// corresponding shards are closed and become read-only.
    pub async fn init(&self, wal_dir_path: &Path, rate_limiter_settings: RateLimiterSettings) {
        let mut inner_guard = self.inner.lock().await;
        let mut mrecordlog_guard = self.mrecordlog.write().await;

        let now = Instant::now();

        info!("opening WAL located at `{}`", wal_dir_path.display());
        let open_result = MultiRecordLogAsync::open_with_prefs(
            wal_dir_path,
            mrecordlog::PersistPolicy::OnDelay {
                interval: Duration::from_secs(5),
                // TODO maybe we want to fsync too?
                action: mrecordlog::PersistAction::Flush,
            },
        )
        .await;

        let mut mrecordlog = match open_result {
            Ok(mrecordlog) => {
                info!(
                    "opened WAL successfully in {}",
                    now.elapsed().pretty_display()
                );
                mrecordlog
            }
            Err(error) => {
                error!("failed to open WAL: {error}");
                inner_guard.set_status(IngesterStatus::Failed);
                return;
            }
        };
        let queue_ids: Vec<QueueId> = mrecordlog
            .list_queues()
            .map(|queue_id| queue_id.to_string())
            .collect();

        if !queue_ids.is_empty() {
            info!("recovering {} shard(s)", queue_ids.len());
        }
        let now = Instant::now();
        let mut num_closed_shards = 0;
        let mut num_deleted_shards = 0;

        for queue_id in queue_ids {
            if let Some(position_range) = queue_position_range(&mrecordlog, &queue_id) {
                let Some((index_uid, source_id, shard_id)) = split_queue_id(&queue_id) else {
                    // `split_queue_id` already logs an error.
                    continue;
                };
                // The queue is not empty: recover it.
                let replication_position_inclusive = Position::offset(*position_range.end());
                let truncation_position_inclusive = if *position_range.start() == 0 {
                    Position::Beginning
                } else {
                    Position::offset(*position_range.start() - 1)
                };
                let rate_limiter = RateLimiter::from_settings(rate_limiter_settings);
                let rate_meter = RateMeter::default();
                // We want to advertise the shard as read-only right away.
                let solo_shard =
                    IngesterShard::new_solo(index_uid.clone(), source_id.clone(), shard_id.clone())
                        .with_state(ShardState::Closed)
                        .with_replication_position_inclusive(replication_position_inclusive)
                        .with_truncation_position_inclusive(truncation_position_inclusive)
                        .with_rate_limiter(rate_limiter)
                        .with_rate_meter(rate_meter)
                        .with_last_write(now)
                        .advertisable() // We want to advertise the shard as read-only right away.
                        .build();
                inner_guard.shards.insert(queue_id.clone(), solo_shard);

                num_closed_shards += 1;
            } else {
                // The queue is empty: delete it.
                if let Err(io_error) = force_delete_queue(&mut mrecordlog, &queue_id).await {
                    error!("failed to delete shard `{queue_id}`: {io_error}");
                    continue;
                }
                num_deleted_shards += 1;
            }
        }
        if num_closed_shards > 0 {
            info!("recovered and closed {num_closed_shards} shard(s)");
        }
        if num_deleted_shards > 0 {
            info!("deleted {num_deleted_shards} empty shard(s)");
        }
        mrecordlog_guard.replace(mrecordlog);
        inner_guard.set_status(IngesterStatus::Ready);
    }

    pub async fn wait_for_ready(&mut self) {
        self.status_rx
            .wait_for(|status| *status == IngesterStatus::Ready)
            .await
            .expect("channel should be open");
    }

    pub async fn lock_partially(&self) -> IngestV2Result<PartiallyLockedIngesterState<'_>> {
        if *self.status_rx.borrow() == IngesterStatus::Initializing {
            return Err(IngestV2Error::Internal(
                "ingester is initializing".to_string(),
            ));
        }
        let inner_guard = self.inner.lock().await;

        if inner_guard.status() == IngesterStatus::Failed {
            return Err(IngestV2Error::Internal(
                "failed to initialize ingester".to_string(),
            ));
        }
        let partial_lock = PartiallyLockedIngesterState { inner: inner_guard };
        Ok(partial_lock)
    }

    pub async fn lock_fully(&self) -> IngestV2Result<FullyLockedIngesterState<'_>> {
        if *self.status_rx.borrow() == IngesterStatus::Initializing {
            return Err(IngestV2Error::Internal(
                "ingester is initializing".to_string(),
            ));
        }
        // We assume that the mrecordlog lock is the most "expensive" one to acquire, so we acquire
        // it first.
        let mrecordlog_opt_guard = self.mrecordlog.write().await;
        let inner_guard = self.inner.lock().await;

        if inner_guard.status() == IngesterStatus::Failed {
            return Err(IngestV2Error::Internal(
                "failed to initialize ingester".to_string(),
            ));
        }
        let mrecordlog_guard = RwLockWriteGuard::map(mrecordlog_opt_guard, |mrecordlog_opt| {
            mrecordlog_opt
                .as_mut()
                .expect("mrecordlog should be initialized")
        });
        let full_lock = FullyLockedIngesterState {
            inner: inner_guard,
            mrecordlog: mrecordlog_guard,
        };
        Ok(full_lock)
    }

    // Leaks the mrecordlog lock for use in fetch tasks. It's safe to do so because fetch tasks
    // never attempt to lock the inner state.
    pub fn mrecordlog(&self) -> Arc<RwLock<Option<MultiRecordLogAsync>>> {
        self.mrecordlog.clone()
    }

    pub fn weak(&self) -> WeakIngesterState {
        WeakIngesterState {
            inner: Arc::downgrade(&self.inner),
            mrecordlog: Arc::downgrade(&self.mrecordlog),
            status_rx: self.status_rx.clone(),
        }
    }
}

pub(super) struct PartiallyLockedIngesterState<'a> {
    pub inner: MutexGuard<'a, InnerIngesterState>,
}

impl fmt::Debug for PartiallyLockedIngesterState<'_> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.debug_struct("PartiallyLockedIngesterState").finish()
    }
}

impl Deref for PartiallyLockedIngesterState<'_> {
    type Target = InnerIngesterState;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl DerefMut for PartiallyLockedIngesterState<'_> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.inner
    }
}

pub(super) struct FullyLockedIngesterState<'a> {
    pub inner: MutexGuard<'a, InnerIngesterState>,
    pub mrecordlog: RwLockMappedWriteGuard<'a, MultiRecordLogAsync>,
}

impl fmt::Debug for FullyLockedIngesterState<'_> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.debug_struct("FullyLockedIngesterState").finish()
    }
}

impl Deref for FullyLockedIngesterState<'_> {
    type Target = InnerIngesterState;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl DerefMut for FullyLockedIngesterState<'_> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.inner
    }
}

impl FullyLockedIngesterState<'_> {
    /// Deletes the shard identified by `queue_id` from the ingester state. It removes the
    /// mrecordlog queue first and then removes the associated in-memory shard and rate trackers.
    pub async fn delete_shard(&mut self, queue_id: &QueueId, initiator: &'static str) {
        match self.mrecordlog.delete_queue(queue_id).await {
            Ok(_) | Err(DeleteQueueError::MissingQueue(_)) => {
                // Log only if the shard was actually removed.
                if let Some(shard) = self.shards.remove(queue_id) {
                    info!("deleted shard `{queue_id}` initiated via `{initiator}`");

                    if let Some(doc_mapper) = shard.doc_mapper_opt {
                        // At this point, we hold the lock so we can safely check the strong count.
                        // The other locations where the doc mapper is cloned also require holding
                        // the lock.
                        if Arc::strong_count(&doc_mapper) == 1 {
                            let doc_mapping_uid = doc_mapper.doc_mapping_uid();

                            if self.doc_mappers.remove(&doc_mapping_uid).is_some() {
                                info!("evicted doc mapper `{doc_mapping_uid}` from cache`");
                            }
                        }
                    }
                }
            }
            Err(DeleteQueueError::IoError(io_error)) => {
                error!("failed to delete shard `{queue_id}`: {io_error}");
            }
        };
    }

    /// Truncates the shard identified by `queue_id` up to `truncate_up_to_position_inclusive` only
    /// if the current truncation position of the shard is smaller.
    pub async fn truncate_shard(
        &mut self,
        queue_id: &QueueId,
        truncate_up_to_position_inclusive: Position,
        initiator: &'static str,
    ) {
        // TODO: Replace with if-let-chains when stabilized.
        let Some(truncate_up_to_offset_inclusive) = truncate_up_to_position_inclusive.as_u64()
        else {
            return;
        };
        let Some(shard) = self.inner.shards.get_mut(queue_id) else {
            return;
        };
        if shard.truncation_position_inclusive >= truncate_up_to_position_inclusive {
            return;
        }
        match self
            .mrecordlog
            .truncate(queue_id, truncate_up_to_offset_inclusive)
            .await
        {
            Ok(_) => {
                info!(
                    "truncated shard `{queue_id}` at {truncate_up_to_position_inclusive} \
                     initiated via `{initiator}`"
                );
                shard.truncation_position_inclusive = truncate_up_to_position_inclusive;
            }
            Err(TruncateError::MissingQueue(_)) => {
                error!("failed to truncate shard `{queue_id}`: WAL queue not found");
                self.shards.remove(queue_id);
                info!("deleted dangling shard `{queue_id}`");
            }
            Err(TruncateError::IoError(io_error)) => {
                error!("failed to truncate shard `{queue_id}`: {io_error}");
            }
        };
    }

    /// Deletes and truncates the shards as directed by the `advise_reset_shards_response` returned
    /// by the control plane.
    pub async fn reset_shards(&mut self, advise_reset_shards_response: &AdviseResetShardsResponse) {
        info!("resetting shards");
        for shard_ids in &advise_reset_shards_response.shards_to_delete {
            for queue_id in shard_ids.queue_ids() {
                self.delete_shard(&queue_id, "control-plane-reset-shards-rpc")
                    .await;
            }
        }
        for shard_id_positions in &advise_reset_shards_response.shards_to_truncate {
            for (queue_id, publish_position) in shard_id_positions.queue_id_positions() {
                self.truncate_shard(
                    &queue_id,
                    publish_position,
                    "control-plane-reset-shards-rpc",
                )
                .await;
            }
        }
    }
}

#[derive(Clone)]
pub(super) struct WeakIngesterState {
    inner: Weak<Mutex<InnerIngesterState>>,
    mrecordlog: Weak<RwLock<Option<MultiRecordLogAsync>>>,
    status_rx: watch::Receiver<IngesterStatus>,
}

impl WeakIngesterState {
    pub fn upgrade(&self) -> Option<IngesterState> {
        let inner = self.inner.upgrade()?;
        let mrecordlog = self.mrecordlog.upgrade()?;
        let status_rx = self.status_rx.clone();
        let state = IngesterState {
            inner,
            mrecordlog,
            status_rx,
        };
        Some(state)
    }
}

#[cfg(test)]
mod tests {
    use bytesize::ByteSize;
    use quickwit_proto::types::ShardId;
    use tokio::time::timeout;

    use super::*;

    #[tokio::test]
    async fn test_ingester_state_does_not_lock_while_initializing() {
        let state = IngesterState::new();
        let inner_guard = state.inner.lock().await;

        assert_eq!(inner_guard.status(), IngesterStatus::Initializing);
        assert_eq!(*state.status_rx.borrow(), IngesterStatus::Initializing);

        let error = state.lock_partially().await.unwrap_err().to_string();
        assert!(error.contains("ingester is initializing"));

        let error = state.lock_fully().await.unwrap_err().to_string();
        assert!(error.contains("ingester is initializing"));
    }

    #[tokio::test]
    async fn test_ingester_state_failed() {
        let state = IngesterState::new();

        state.inner.lock().await.set_status(IngesterStatus::Failed);

        let error = state.lock_partially().await.unwrap_err().to_string();
        assert!(error.to_string().ends_with("failed to initialize ingester"));

        let error = state.lock_fully().await.unwrap_err().to_string();
        assert!(error.contains("failed to initialize ingester"));
    }

    #[tokio::test]
    async fn test_ingester_state_init() {
        let mut state = IngesterState::new();
        let temp_dir = tempfile::tempdir().unwrap();

        state
            .init(temp_dir.path(), RateLimiterSettings::default())
            .await;

        timeout(Duration::from_millis(100), state.wait_for_ready())
            .await
            .unwrap();

        state.lock_partially().await.unwrap();

        let locked_state = state.lock_fully().await.unwrap();
        assert_eq!(locked_state.status(), IngesterStatus::Ready);
        assert_eq!(*locked_state.status_tx.borrow(), IngesterStatus::Ready);
    }

    fn insert_shard_with_used_capacity(
        state: &mut InnerIngesterState,
        index_uid: IndexUid,
        source_id: SourceId,
        shard_id: ShardId,
        shard_state: ShardState,
        used_capacity: ByteSize,
    ) {
        let mut shard = IngesterShard::new_solo(index_uid, source_id, shard_id)
            .with_state(shard_state)
            .build();
        shard.rate_limiter.acquire_bytes(used_capacity);

        let queue_id = shard.queue_id();
        state.shards.insert(queue_id, shard);
    }

    #[tokio::test]
    async fn test_find_most_capacity_shard_returns_shard_with_least_used_capacity() {
        let (_temp_dir, state) = IngesterState::for_test().await;
        let mut state_guard = state.lock_partially().await.unwrap();

        let index_uid = IndexUid::for_test("test-index", 0);
        let source_id = SourceId::from("test-source");

        // Shard 1: 1KB used (most available capacity)
        // Shard 2: 2KB used
        // ...
        // Shard 5: 5KB used (least available capacity)
        for i in 1..=5u64 {
            insert_shard_with_used_capacity(
                &mut state_guard,
                index_uid.clone(),
                source_id.clone(),
                ShardId::from(i),
                ShardState::Open,
                ByteSize::kb(i),
            );
        }

        let shard = state_guard
            .find_most_capacity_shard_mut(&index_uid, &source_id)
            .unwrap();

        assert_eq!(shard.shard_id, ShardId::from(1));
        assert_eq!(shard.shard_state, ShardState::Open);

        let expected_available_permits =
            RateLimiterSettings::default().burst_limit - ByteSize::kb(1).as_u64();
        assert_eq!(
            shard.rate_limiter.available_permits(),
            expected_available_permits
        );
    }

    #[tokio::test]
    async fn test_find_most_capacity_shard_skips_closed_shards() {
        let (_temp_dir, state) = IngesterState::for_test().await;
        let mut locked_state = state.lock_partially().await.unwrap();

        let index_uid = IndexUid::for_test("test-index", 0);
        let source_id = SourceId::from("test-source");

        insert_shard_with_used_capacity(
            &mut locked_state,
            index_uid.clone(),
            source_id.clone(),
            ShardId::from(1),
            ShardState::Open,
            ByteSize::kb(1),
        );
        insert_shard_with_used_capacity(
            &mut locked_state,
            index_uid.clone(),
            source_id.clone(),
            ShardId::from(2),
            ShardState::Open,
            ByteSize::kb(2),
        );

        insert_shard_with_used_capacity(
            &mut locked_state,
            index_uid.clone(),
            source_id.clone(),
            ShardId::from(3),
            ShardState::Closed,
            ByteSize::kb(0),
        );

        let shard = locked_state
            .find_most_capacity_shard_mut(&index_uid, &source_id)
            .unwrap();

        // Should pick shard 1 (most capacity among open shards), not shard 3 (closed)
        assert_eq!(shard.shard_id, ShardId::from(1));
    }

    #[tokio::test]
    async fn test_find_most_capacity_shard_returns_none_for_unknown_index_or_source() {
        let (_temp_dir, state) = IngesterState::for_test().await;
        let mut locked_state = state.lock_partially().await.unwrap();

        let index_uid = IndexUid::for_test("test-index", 0);
        let source_id = SourceId::from("test-source");

        insert_shard_with_used_capacity(
            &mut locked_state,
            index_uid.clone(),
            source_id.clone(),
            ShardId::from(1),
            ShardState::Open,
            ByteSize::kb(0),
        );

        let shard_opt = locked_state
            .find_most_capacity_shard_mut(&IndexUid::for_test("other-index", 0), &source_id);
        assert!(shard_opt.is_none());

        let shard_opt =
            locked_state.find_most_capacity_shard_mut(&index_uid, &SourceId::from("other-source"));
        assert!(shard_opt.is_none());
    }
}
