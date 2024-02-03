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
use std::sync::{Arc, Weak};

use fnv::FnvHashMap;
use mrecordlog::error::{DeleteQueueError, TruncateError};
use mrecordlog::MultiRecordLog;
use quickwit_common::rate_limiter::RateLimiter;
use quickwit_proto::ingest::ingester::{IngesterStatus, ObservationMessage};
use quickwit_proto::ingest::IngestV2Result;
use quickwit_proto::types::{Position, QueueId};
use tokio::sync::{watch, Mutex, MutexGuard, RwLock, RwLockWriteGuard};
use tracing::{error, info};

use super::models::IngesterShard;
use super::rate_meter::RateMeter;
use super::replication::{ReplicationStreamTaskHandle, ReplicationTaskHandle};
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
    mrecordlog: Arc<RwLock<MultiRecordLog>>,
}

pub(super) struct InnerIngesterState {
    pub shards: FnvHashMap<QueueId, IngesterShard>,
    pub rate_trackers: FnvHashMap<QueueId, (RateLimiter, RateMeter)>,
    // Replication stream opened with followers.
    pub replication_streams: FnvHashMap<FollowerId, ReplicationStreamTaskHandle>,
    // Replication tasks running for each replication stream opened with leaders.
    pub replication_tasks: FnvHashMap<LeaderId, ReplicationTaskHandle>,
    pub status: IngesterStatus,
    pub observation_tx: watch::Sender<IngestV2Result<ObservationMessage>>,
}

impl IngesterState {
    pub fn new(
        mrecordlog: MultiRecordLog,
        observation_tx: watch::Sender<IngestV2Result<ObservationMessage>>,
    ) -> Self {
        let inner = InnerIngesterState {
            shards: Default::default(),
            rate_trackers: Default::default(),
            replication_streams: Default::default(),
            replication_tasks: Default::default(),
            status: IngesterStatus::Ready,
            observation_tx,
        };
        let inner = Arc::new(Mutex::new(inner));
        let mrecordlog = Arc::new(RwLock::new(mrecordlog));
        Self { inner, mrecordlog }
    }

    pub async fn lock_partially(&self) -> PartiallyLockedIngesterState<'_> {
        PartiallyLockedIngesterState {
            inner: self.inner.lock().await,
        }
    }

    pub async fn lock_fully(&self) -> FullyLockedIngesterState<'_> {
        // We assume that the mrecordlog lock is the most "expensive" one to acquire, so we acquire
        // it first.
        let mrecordlog = self.mrecordlog.write().await;
        let inner = self.inner.lock().await;

        FullyLockedIngesterState { inner, mrecordlog }
    }

    // Leaks the mrecordlog lock for use in fetch tasks. It's safe to do so because fetch tasks
    // never attempt to lock the inner state.
    pub fn mrecordlog(&self) -> Arc<RwLock<MultiRecordLog>> {
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
    pub mrecordlog: RwLockWriteGuard<'a, MultiRecordLog>,
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
        if self.inner.shards.contains_key(queue_id) {
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
    mrecordlog: Weak<RwLock<MultiRecordLog>>,
}

impl WeakIngesterState {
    pub fn upgrade(&self) -> Option<IngesterState> {
        let inner = self.inner.upgrade()?;
        let mrecordlog = self.mrecordlog.upgrade()?;
        Some(IngesterState { inner, mrecordlog })
    }
}
