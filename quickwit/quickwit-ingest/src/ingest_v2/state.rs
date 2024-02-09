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

use std::ops::{Deref, DerefMut};
use std::path::Path;
use std::sync::{Arc, Weak};
use std::time::{Duration, Instant};

use fnv::FnvHashMap;
use mrecordlog::error::{DeleteQueueError, TruncateError};
use mrecordlog::MultiRecordLog;
use quickwit_common::rate_limiter::{RateLimiter, RateLimiterSettings};
use quickwit_proto::ingest::ingester::IngesterStatus;
use quickwit_proto::ingest::{IngestV2Error, IngestV2Result, ShardState};
use quickwit_proto::types::{Position, QueueId};
use tokio::sync::{watch, Mutex, MutexGuard, RwLock, RwLockMappedWriteGuard, RwLockWriteGuard};
use tracing::{error, info};

use super::models::IngesterShard;
use super::rate_meter::RateMeter;
use super::replication::{ReplicationStreamTaskHandle, ReplicationTaskHandle};
use crate::ingest_v2::mrecordlog_utils::{force_delete_queue, queue_position_range};
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
    mrecordlog: Arc<RwLock<Option<MultiRecordLog>>>,
}

pub(super) struct InnerIngesterState {
    pub shards: FnvHashMap<QueueId, IngesterShard>,
    pub rate_trackers: FnvHashMap<QueueId, (RateLimiter, RateMeter)>,
    // Replication stream opened with followers.
    pub replication_streams: FnvHashMap<FollowerId, ReplicationStreamTaskHandle>,
    // Replication tasks running for each replication stream opened with leaders.
    pub replication_tasks: FnvHashMap<LeaderId, ReplicationTaskHandle>,
    pub status: IngesterStatus,
    pub status_tx: watch::Sender<IngesterStatus>,
}

impl IngesterState {
    pub fn load(
        wal_dir_path: &Path,
        rate_limiter_settings: RateLimiterSettings,
    ) -> (Self, watch::Receiver<IngesterStatus>) {
        let status = IngesterStatus::Initializing;
        let (status_tx, status_rx) = watch::channel(status);
        let inner = InnerIngesterState {
            shards: Default::default(),
            rate_trackers: Default::default(),
            replication_streams: Default::default(),
            replication_tasks: Default::default(),
            status,
            status_tx,
        };
        let inner = Arc::new(Mutex::new(inner));
        let mrecordlog = Arc::new(RwLock::new(None));
        let state = Self { inner, mrecordlog };

        let state_clone = state.clone();
        let wal_dir_path = wal_dir_path.to_path_buf();
        let init_future = async move {
            state_clone.init(&wal_dir_path, rate_limiter_settings).await;
        };
        tokio::spawn(init_future);

        (state, status_rx)
    }

    #[cfg(test)]
    pub async fn for_test() -> (tempfile::TempDir, Self, watch::Receiver<IngesterStatus>) {
        let temp_dir = tempfile::tempdir().unwrap();
        let (state, mut status_rx) =
            IngesterState::load(temp_dir.path(), RateLimiterSettings::default());

        status_rx
            .wait_for(|status| *status == IngesterStatus::Ready)
            .await
            .unwrap();

        (temp_dir, state, status_rx)
    }

    /// Initializes the internal state of the ingester. It loads the local WAL, then lists all its
    /// queues. Empty queues are deleted, while non-empty queues are recovered. However, the
    /// corresponding shards are closed and become read-only.
    pub async fn init(&self, wal_dir_path: &Path, rate_limiter_settings: RateLimiterSettings) {
        let mut inner_guard = self.inner.lock().await;
        let now = Instant::now();

        info!(
            "opening write-ahead log located at `{}`",
            wal_dir_path.display()
        );
        let open_result = MultiRecordLog::open_with_prefs(
            wal_dir_path,
            mrecordlog::SyncPolicy::OnDelay(Duration::from_secs(5)),
        )
        .await;

        let mut mrecordlog = match open_result {
            Ok(mrecordlog) => {
                info!(
                    "opened write-ahead log successfully in {} seconds",
                    now.elapsed().as_secs()
                );
                mrecordlog
            }
            Err(error) => {
                error!("failed to open write-ahead log: {error}");
                inner_guard.status = IngesterStatus::Failed;
                let _ = inner_guard.status_tx.send(IngesterStatus::Failed);
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
        let mut num_closed_shards = 0;
        let mut num_deleted_shards = 0;

        for queue_id in queue_ids {
            if let Some(position_range) = queue_position_range(&mrecordlog, &queue_id) {
                // The queue is not empty: recover it.
                let replication_position_inclusive = Position::offset(*position_range.end());
                let truncation_position_inclusive = if *position_range.start() == 0 {
                    Position::Beginning
                } else {
                    Position::offset(*position_range.start() - 1)
                };
                let solo_shard = IngesterShard::new_solo(
                    ShardState::Closed,
                    replication_position_inclusive,
                    truncation_position_inclusive,
                );
                inner_guard.shards.insert(queue_id.clone(), solo_shard);

                let rate_limiter = RateLimiter::from_settings(rate_limiter_settings);
                let rate_meter = RateMeter::default();
                inner_guard
                    .rate_trackers
                    .insert(queue_id, (rate_limiter, rate_meter));

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
        inner_guard.status = IngesterStatus::Ready;
        let _ = inner_guard.status_tx.send(IngesterStatus::Ready);

        self.mrecordlog.write().await.replace(mrecordlog);
    }

    pub async fn lock_partially(&self) -> PartiallyLockedIngesterState<'_> {
        PartiallyLockedIngesterState {
            inner: self.inner.lock().await,
        }
    }

    pub async fn lock_fully(&self) -> IngestV2Result<FullyLockedIngesterState<'_>> {
        // We assume that the mrecordlog lock is the most "expensive" one to acquire, so we acquire
        // it first.
        let mrecordlog = self.mrecordlog.write().await;
        let inner = self.inner.lock().await;

        if inner.status == IngesterStatus::Initializing {
            return Err(IngestV2Error::Internal(
                "ingester is initializing".to_string(),
            ));
        }
        let mrecordlog = RwLockWriteGuard::map(mrecordlog, |mrecordlog| {
            mrecordlog
                .as_mut()
                .expect("mrecordlog should be initialized")
        });
        Ok(FullyLockedIngesterState { inner, mrecordlog })
    }

    // Leaks the mrecordlog lock for use in fetch tasks. It's safe to do so because fetch tasks
    // never attempt to lock the inner state.
    pub fn mrecordlog(&self) -> Arc<RwLock<Option<MultiRecordLog>>> {
        self.mrecordlog.clone()
    }

    pub fn weak(&self) -> WeakIngesterState {
        WeakIngesterState {
            inner: Arc::downgrade(&self.inner),
            mrecordlog: Arc::downgrade(&self.mrecordlog),
        }
    }
}

pub(super) struct PartiallyLockedIngesterState<'a> {
    pub inner: MutexGuard<'a, InnerIngesterState>,
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
    pub mrecordlog: RwLockMappedWriteGuard<'a, MultiRecordLog>,
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
    /// Truncates the shard identified by `queue_id` up to `truncate_up_to_position_inclusive` only
    /// if the current truncation position of the shard is smaller.
    pub async fn truncate_shard(
        &mut self,
        queue_id: &QueueId,
        truncate_up_to_position_inclusive: &Position,
    ) {
        // TODO: Replace with if-let-chains when stabilized.
        let Some(truncate_up_to_offset_inclusive) = truncate_up_to_position_inclusive.as_u64()
        else {
            return;
        };
        let Some(shard) = self.inner.shards.get_mut(queue_id) else {
            return;
        };
        if shard.truncation_position_inclusive >= *truncate_up_to_position_inclusive {
            return;
        }
        match self
            .mrecordlog
            .truncate(queue_id, truncate_up_to_offset_inclusive)
            .await
        {
            Ok(_) => {
                shard.truncation_position_inclusive = truncate_up_to_position_inclusive.clone();
            }
            Err(TruncateError::MissingQueue(_)) => {
                error!("failed to truncate shard `{queue_id}`: WAL queue not found");
                self.shards.remove(queue_id);
                self.rate_trackers.remove(queue_id);
                info!("deleted dangling shard `{queue_id}`");
            }
            Err(TruncateError::IoError(io_error)) => {
                error!("failed to truncate shard `{queue_id}`: {io_error}");
            }
        };
    }

    /// Deletes the shard identified by `queue_id` from the ingester state. It removes the
    /// mrecordlog queue first and then removes the associated in-memory shard and rate trackers.
    pub async fn delete_shard(&mut self, queue_id: &QueueId) {
        // This if-statement is here to avoid needless log.
        if !self.inner.shards.contains_key(queue_id) {
            // No need to do anything. This queue is not on this ingester.
            return;
        }
        match self.mrecordlog.delete_queue(queue_id).await {
            Ok(_) | Err(DeleteQueueError::MissingQueue(_)) => {
                self.shards.remove(queue_id);
                self.rate_trackers.remove(queue_id);
                info!("deleted shard `{queue_id}`");
            }
            Err(DeleteQueueError::IoError(io_error)) => {
                error!("failed to delete shard `{queue_id}`: {io_error}");
            }
        };
    }
}

#[derive(Clone)]
pub(super) struct WeakIngesterState {
    inner: Weak<Mutex<InnerIngesterState>>,
    mrecordlog: Weak<RwLock<Option<MultiRecordLog>>>,
}

impl WeakIngesterState {
    pub fn upgrade(&self) -> Option<IngesterState> {
        let inner = self.inner.upgrade()?;
        let mrecordlog = self.mrecordlog.upgrade()?;
        Some(IngesterState { inner, mrecordlog })
    }
}
