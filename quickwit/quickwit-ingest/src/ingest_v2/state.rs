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

use std::collections::{BTreeMap, HashMap, HashSet};
use std::fmt;
use std::ops::{Deref, DerefMut};
use std::path::Path;
use std::sync::{Arc, Weak};
use std::time::{Duration, Instant};

use bytesize::ByteSize;
use itertools::Itertools;
use mrecordlog::ResourceUsage;
use mrecordlog::error::{DeleteQueueError, TruncateError};
use quickwit_cluster::Cluster;
use quickwit_common::pretty::PrettyDisplay;
use quickwit_common::rate_limited_warn;
use quickwit_common::shared_consts::INGESTER_STATUS_KEY;
use quickwit_doc_mapper::DocMapper;
use quickwit_metrics::{counter, gauge, histogram, label_values, labels};
use quickwit_proto::control_plane::AdviseResetShardsResponse;
use quickwit_proto::ingest::ingester::{
    IngesterStatus, PersistFailure, PersistFailureReason, PersistSubrequest, PersistSuccess,
    RoutingUpdate, SourceShardUpdate,
};
use quickwit_proto::ingest::{
    DocBatchV2, IngestV2Error, IngestV2Result, ParseFailure, ShardIds, ShardState,
};
use quickwit_proto::types::{
    DocMappingUid, IndexUid, Position, QueueId, ShardId, SourceId, SourceUid, split_queue_id,
};
use tokio::sync::{Mutex, MutexGuard, RwLock, RwLockMappedWriteGuard, RwLockWriteGuard, watch};
use tokio::task::JoinHandle;
use tokio::time::MissedTickBehavior;
use tracing::{error, info, instrument, warn};

use super::local_shards::{ShardInfo, ShardInfos, ShardThroughputReadings};
use super::metrics::report_local_shards_metrics;
use super::models::IngesterShard;
use super::mrecordlog_utils::{
    AppendDocBatchError, append_non_empty_doc_batch, doc_batch_size, read_queue_size,
};
use super::rate_meter::RateMeter;
use super::wal_capacity_tracker::WalCapacityTracker;
use crate::OpenShardCounts;
use crate::metrics::{DOCS_BYTES_TOTAL, DOCS_TOTAL, VALIDITY};
use crate::mrecordlog_async::MultiRecordLogAsync;

const LOCAL_SHARDS_SAMPLE_INTERVAL: Duration = if cfg!(any(test, feature = "testsuite")) {
    Duration::from_millis(50)
} else {
    Duration::from_secs(1)
};

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
    cluster: Cluster,
    pub wal_capacity_tracker: WalCapacityTracker,
    disk_capacity: ByteSize,
    memory_capacity: ByteSize,
    status_tx: watch::Sender<IngesterStatus>,
    local_shards_tx: watch::Sender<Option<Arc<ShardThroughputReadings>>>,
    local_shards_sampled_at: tokio::time::Instant,
}

impl InnerIngesterState {
    pub fn status(&self) -> IngesterStatus {
        *self.status_tx.borrow()
    }

    pub async fn set_status(&mut self, status: IngesterStatus) {
        self.status_tx.send_replace(status);
        self.cluster
            .set_self_key_value(INGESTER_STATUS_KEY, status.as_json_str_name())
            .await;
    }

    /// Checks whether the ingester is fully decommissioned and updates its status accordingly.
    pub async fn check_decommissioning_status(&mut self) {
        if self.status() != IngesterStatus::Decommissioning {
            return;
        }
        // An ingester is decommissioned if:
        // - `self.shards` is empty OR
        // - all shards are non-advertisable and empty
        //
        // see `IngesterShard::is_empty_orphan` for why the latter are never going to be deleted
        // by any other cleanup mechanism, so we must not wait on them here.
        let is_decommissioned = self.shards.values().all(|shard| shard.is_empty_orphan());

        if is_decommissioned {
            self.set_status(IngesterStatus::Decommissioned).await;
        }
    }

    /// Returns the shard with the smallesy queue size for this index and source.
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
            .min_by_key(|shard| shard.queue_size)
    }

    /// Returns per-source open shard counts and closed shard IDs for all advertisable shards.
    pub fn get_shard_snapshot(&self) -> (OpenShardCounts, Vec<ShardIds>) {
        let grouped = self
            .shards
            .values()
            .filter(|shard| shard.is_advertisable)
            .map(|shard| ((shard.index_uid.clone(), shard.source_id.clone()), shard))
            .into_group_map();

        let mut open_counts = Vec::new();
        let mut closed_shards = Vec::new();

        for ((index_uid, source_id), shards) in grouped {
            let mut open_count = 0;
            let mut closed_ids = Vec::new();

            for shard in shards {
                if shard.is_open() {
                    open_count += 1;
                } else if shard.is_closed() {
                    closed_ids.push(shard.shard_id.clone());
                }
            }
            open_counts.push((index_uid.clone(), source_id.clone(), open_count));
            if !closed_ids.is_empty() {
                closed_shards.push(ShardIds {
                    index_uid: Some(index_uid),
                    source_id,
                    shard_ids: closed_ids,
                });
            }
        }
        (open_counts, closed_shards)
    }

    pub fn routing_update(&self, wal_usage: &ResourceUsage) -> RoutingUpdate {
        let capacity_score = self.wal_capacity_tracker.score(
            ByteSize::b(wal_usage.disk_used_bytes as u64),
            ByteSize::b(wal_usage.memory_used_bytes as u64),
        ) as u32;
        let (open_shard_counts, closed_shards) = self.get_shard_snapshot();
        let source_shard_updates = open_shard_counts
            .into_iter()
            .map(|(index_uid, source_id, count)| SourceShardUpdate {
                index_uid: Some(index_uid),
                source_id,
                open_shard_count: count as u32,
            })
            .collect();
        RoutingUpdate {
            capacity_score,
            source_shard_updates,
            closed_shards,
        }
    }

    /// Every LOCAL_SHARDS_SAMPLE_INTERVAL, measure how much work each shard did, per source, to
    /// report to the control plane. The value kept is always the latest, as the only data that
    /// matters is the most recent snapshot.
    pub fn harvest_shard_throughput_readings(&mut self) -> Option<Arc<ShardThroughputReadings>> {
        let now = tokio::time::Instant::now();
        if now.duration_since(self.local_shards_sampled_at) < LOCAL_SHARDS_SAMPLE_INTERVAL {
            return None;
        }
        let mut per_source_shard_infos: BTreeMap<SourceUid, ShardInfos> = BTreeMap::new();

        for shard in self.shards.values_mut() {
            if !shard.is_advertisable {
                continue;
            }
            let ingestion_rates = shard.rate_meter.sample();

            let source_uid = SourceUid {
                index_uid: shard.index_uid.clone(),
                source_id: shard.source_id.clone(),
            };
            let shard_info = ShardInfo {
                shard_id: shard.shard_id.clone(),
                shard_state: shard.shard_state,
                short_term_ingestion_rate: ingestion_rates.short_term,
                long_term_ingestion_rate: ingestion_rates.long_term,
            };
            per_source_shard_infos
                .entry(source_uid)
                .or_default()
                .insert(shard_info);
        }
        let snapshot = Arc::new(ShardThroughputReadings {
            per_source_shard_infos,
        });
        self.local_shards_sampled_at = now;
        self.local_shards_tx.send_replace(Some(snapshot.clone()));
        Some(snapshot)
    }
}

impl IngesterState {
    async fn create(
        cluster: Cluster,
        disk_capacity: ByteSize,
        memory_capacity: ByteSize,
        local_shards_tx: watch::Sender<Option<Arc<ShardThroughputReadings>>>,
    ) -> Self {
        let status = IngesterStatus::Initializing;
        let (status_tx, status_rx) = watch::channel(status);
        let mut inner = InnerIngesterState {
            shards: Default::default(),
            doc_mappers: Default::default(),
            cluster,
            wal_capacity_tracker: WalCapacityTracker::new(disk_capacity, memory_capacity),
            disk_capacity,
            memory_capacity,
            status_tx,
            local_shards_tx,
            local_shards_sampled_at: tokio::time::Instant::now(),
        };
        // We call `set_status` here instead of setting it directly because it also updates the
        // ingester status in chitchat.
        inner.set_status(IngesterStatus::Initializing).await;

        let inner = Arc::new(Mutex::new(inner));
        let mrecordlog = Arc::new(RwLock::new(None));

        Self {
            inner,
            mrecordlog,
            status_rx,
        }
    }

    /// Most readings of shard throughputs will take place through the persist path, when the state
    /// lock is held. This is a backup path for when persist might be idle.
    pub fn spawn_shards_readings_publisher(&self) -> JoinHandle<()> {
        let weak_state = self.weak();
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(LOCAL_SHARDS_SAMPLE_INTERVAL);
            interval.set_missed_tick_behavior(MissedTickBehavior::Skip);
            loop {
                interval.tick().await;
                let Some(state) = weak_state.upgrade() else {
                    return;
                };
                match *state.status_rx.borrow() {
                    IngesterStatus::Initializing => continue,
                    IngesterStatus::Failed => return,
                    _ => {}
                }
                let Ok(inner) = state.inner.try_lock() else {
                    continue;
                };
                let mut state_guard = PartiallyLockedIngesterState {
                    inner,
                    operation: "publish_local_shards",
                    acquired_at: Instant::now(),
                };
                let snapshot = state_guard.harvest_shard_throughput_readings();
                drop(state_guard);
                if let Some(snapshot) = snapshot {
                    report_local_shards_metrics(&snapshot);
                }
            }
        })
    }

    pub async fn load(
        cluster: Cluster,
        wal_dir_path: &Path,
        disk_capacity: ByteSize,
        memory_capacity: ByteSize,
        local_shards_tx: watch::Sender<Option<Arc<ShardThroughputReadings>>>,
    ) -> Self {
        let state = Self::create(cluster, disk_capacity, memory_capacity, local_shards_tx).await;
        let state_clone = state.clone();
        let wal_dir_path = wal_dir_path.to_path_buf();

        let init_future = async move {
            state_clone
                .init(&wal_dir_path, disk_capacity, memory_capacity)
                .await;
        };
        tokio::spawn(init_future);

        state
    }

    #[cfg(test)]
    pub async fn for_test(cluster: Cluster) -> (tempfile::TempDir, Self) {
        Self::for_test_with_disk_capacity(cluster, ByteSize::mb(256)).await
    }

    #[cfg(test)]
    pub async fn for_test_with_disk_capacity(
        cluster: Cluster,
        disk_capacity: ByteSize,
    ) -> (tempfile::TempDir, Self) {
        let temp_dir = tempfile::tempdir().unwrap();
        let mut state = IngesterState::load(
            cluster,
            temp_dir.path(),
            disk_capacity,
            ByteSize::mb(256),
            watch::Sender::new(None),
        )
        .await;

        state.wait_for_ready().await;

        (temp_dir, state)
    }

    /// Initializes the internal state of the ingester. It loads the local WAL, then lists all its
    /// queues. Every queue is recovered as a closed shard, including empty ones.
    pub async fn init(
        &self,
        wal_dir_path: &Path,
        disk_capacity: ByteSize,
        memory_capacity: ByteSize,
    ) {
        // Acquire locks in the same order as `lock_fully` (mrecordlog first, then inner) to
        // prevent ABBA deadlocks with the broadcast capacity task.
        let mut mrecordlog_guard = self.mrecordlog.write().await;
        let mut inner_guard = self.inner.lock().await;

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

        let mrecordlog = match open_result {
            Ok(mrecordlog) => {
                info!(
                    "opened WAL successfully in {}",
                    now.elapsed().pretty_display()
                );
                mrecordlog
            }
            Err(error) => {
                error!("failed to open WAL: {error}");
                inner_guard.set_status(IngesterStatus::Failed).await;
                return;
            }
        };
        let queues_summary = mrecordlog.summary();

        if !queues_summary.queues.is_empty() {
            info!("recovering {} shard(s)", queues_summary.queues.len());
        }
        let now = Instant::now();
        let mut num_closed_shards = 0;

        for (queue_id, queue_summary) in queues_summary.queues {
            let Some((index_uid, source_id, shard_id)) = split_queue_id(&queue_id) else {
                // `split_queue_id` already logs an error.
                continue;
            };
            // We recover every shard found in the WAL as a closed shard, including empty ones.
            //
            // We used to delete empty shards here, but that silently diverged from the control
            // plane, which kept advertising the shard as available even though it no longer
            // existed on the ingester (resulting in "no shards available" errors). Instead, we
            // recover an empty shard as a closed shard. An indexer will drain it, immediately
            // reach EOF (there is nothing to read), and the resulting EOF gossip will delete the
            // shard from the ingester, the control plane, and the metastore.
            let replication_position_inclusive = queue_summary
                .end
                .map(Position::offset)
                .unwrap_or(Position::Beginning); // The queue was created but never written to.
            let truncation_position_inclusive = queue_summary
                .start
                .checked_sub(1)
                .map(Position::offset)
                .unwrap_or(Position::Beginning);
            let queue_size = read_queue_size(&mrecordlog, &queue_id);
            let rate_meter = RateMeter::default();

            let shard =
                IngesterShard::builder(index_uid.clone(), source_id.clone(), shard_id.clone())
                    .with_state(ShardState::Closed)
                    .with_replication_position_inclusive(replication_position_inclusive)
                    .with_truncation_position_inclusive(truncation_position_inclusive)
                    .with_queue_size(queue_size)
                    .with_rate_meter(rate_meter)
                    .with_last_write(now)
                    .advertisable() // We want to advertise the shard as read-only right away.
                    .build();
            inner_guard.shards.insert(queue_id.clone(), shard);

            num_closed_shards += 1;
        }
        if num_closed_shards > 0 {
            info!("recovered and closed {num_closed_shards} shard(s)");
        }
        let wal_usage = mrecordlog.resource_usage();
        mrecordlog_guard.replace(mrecordlog);
        crate::ingest_v2::metrics::report_wal_usage(wal_usage, disk_capacity, memory_capacity);
        inner_guard.set_status(IngesterStatus::Ready).await;
    }

    pub async fn wait_for_ready(&mut self) {
        self.status_rx
            .wait_for(|status| *status == IngesterStatus::Ready)
            .await
            .expect("channel should be open");
    }

    #[instrument(name = "ingester.lock_partially", skip_all, fields(operation))]
    pub async fn lock_partially(
        &self,
        operation: &'static str,
    ) -> IngestV2Result<PartiallyLockedIngesterState<'_>> {
        if *self.status_rx.borrow() == IngesterStatus::Initializing {
            return Err(IngestV2Error::Internal(
                "ingester is initializing".to_string(),
            ));
        }
        let (inner_guard, acquired_at) =
            track_acquire_lock(operation, "partial", self.inner.lock()).await;

        if inner_guard.status() == IngesterStatus::Failed {
            return Err(IngestV2Error::Internal(
                "failed to initialize ingester".to_string(),
            ));
        }
        let partially_locked_state = PartiallyLockedIngesterState {
            inner: inner_guard,
            operation,
            acquired_at,
        };
        Ok(partially_locked_state)
    }

    #[instrument(name = "ingester.lock_fully", skip_all, fields(operation))]
    pub async fn lock_fully(
        &self,
        operation: &'static str,
    ) -> IngestV2Result<FullyLockedIngesterState<'_>> {
        if *self.status_rx.borrow() == IngesterStatus::Initializing {
            return Err(IngestV2Error::Internal(
                "ingester is initializing".to_string(),
            ));
        }
        // We assume that the mrecordlog lock is the most "expensive" one to acquire, so we
        // acquire it first.
        let ((mrecordlog_opt_guard, inner_guard), acquired_at) =
            track_acquire_lock(operation, "full", async {
                let mrecordlog_opt_guard = self.mrecordlog.write().await;
                let inner_guard = self.inner.lock().await;
                (mrecordlog_opt_guard, inner_guard)
            })
            .await;

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
        let fully_locked_state = FullyLockedIngesterState {
            inner: inner_guard,
            mrecordlog: mrecordlog_guard,
            operation,
            acquired_at,
        };
        Ok(fully_locked_state)
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
    operation: &'static str,
    acquired_at: Instant,
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

impl Drop for PartiallyLockedIngesterState<'_> {
    fn drop(&mut self) {
        warn_on_long_lock_hold(self.operation, "partial", self.acquired_at);
    }
}

pub(super) struct FullyLockedIngesterState<'a> {
    pub inner: MutexGuard<'a, InnerIngesterState>,
    pub mrecordlog: RwLockMappedWriteGuard<'a, MultiRecordLogAsync>,
    operation: &'static str,
    acquired_at: Instant,
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

impl Drop for FullyLockedIngesterState<'_> {
    fn drop(&mut self) {
        warn_on_long_lock_hold(self.operation, "full", self.acquired_at);
    }
}

pub(super) fn warn_on_long_lock_hold(
    operation: &'static str,
    lock_type: &'static str,
    acquired_at: Instant,
) {
    let elapsed = acquired_at.elapsed();

    let labels = labels!("operation" => operation, "type" => lock_type);
    histogram!(
        parent: crate::ingest_v2::metrics::WAL_LOCK_HOLD_DURATION_SECS,
        labels: [labels],
    )
    .observe(elapsed.as_secs_f64());

    if elapsed > Duration::from_secs(1) {
        quickwit_common::rate_limited_warn!(
            limit_per_min = 6,
            "held {} lock for {} operation for {}",
            lock_type,
            operation,
            elapsed.pretty_display()
        );
    }
}

/// Wraps a lock-acquisition future with the in-flight gauge, the acquire duration histogram, and
/// a rate-limited warning when acquisition takes longer than 1s. Used by `lock_partially` /
/// `lock_fully` and by other ingest_v2 sites that acquire WAL-related locks (e.g. fetch tasks
/// reading the mrecordlog directly).
pub(super) async fn track_acquire_lock<F, R>(
    operation: &'static str,
    lock_type: &'static str,
    acquire_future: F,
) -> (R, Instant)
where
    F: std::future::Future<Output = R>,
{
    let labels = labels!("operation" => operation, "type" => lock_type);

    gauge!(
        parent: crate::ingest_v2::metrics::WAL_ACQUIRE_LOCK_REQUESTS_IN_FLIGHT,
        labels: [labels],
    )
    .inc();

    let now = Instant::now();
    let guard = acquire_future.await;
    let acquired_at = Instant::now();

    let elapsed = acquired_at.duration_since(now);

    if elapsed > Duration::from_secs(1) {
        quickwit_common::rate_limited_warn!(
            limit_per_min = 6,
            "acquiring {} lock for {} operation took {}",
            lock_type,
            operation,
            elapsed.pretty_display()
        );
    }
    gauge!(
        parent: crate::ingest_v2::metrics::WAL_ACQUIRE_LOCK_REQUESTS_IN_FLIGHT,
        labels: [labels],
    )
    .dec();
    histogram!(
        parent: crate::ingest_v2::metrics::WAL_ACQUIRE_LOCK_REQUEST_DURATION_SECS,
        labels: [labels],
    )
    .observe(elapsed.as_secs_f64());

    (guard, acquired_at)
}

impl FullyLockedIngesterState<'_> {
    pub fn begin_persist(&self, force_commit: bool) -> PersistContext {
        PersistContext {
            force_commit,
            wal_usage: self.mrecordlog.resource_usage(),
            reserved_capacity: ByteSize::default(),
            shards_to_close: HashSet::new(),
            shards_to_delete: HashSet::new(),
        }
    }

    pub async fn stage_persist_subrequest(
        &mut self,
        mut subrequest: PersistSubrequest,
        context: &mut PersistContext,
    ) -> Result<StagedPersistRequest, PersistFailure> {
        let doc_batch = match subrequest.doc_batch.take() {
            Some(doc_batch) if !doc_batch.is_empty() => doc_batch,
            _ => {
                warn!("received empty persist request");
                DocBatchV2::default()
            }
        };
        let failure = |reason: PersistFailureReason| PersistFailure {
            subrequest_id: subrequest.subrequest_id,
            index_uid: subrequest.index_uid.clone(),
            source_id: subrequest.source_id.clone(),
            reason: reason as i32,
        };
        let requested_capacity = doc_batch_size(&doc_batch, context.force_commit);
        let total_requested_capacity = context.reserved_capacity + requested_capacity;
        let disk_used = ByteSize::b(context.wal_usage.disk_used_bytes as u64);
        let memory_used = ByteSize::b(context.wal_usage.memory_used_bytes as u64);
        if disk_used + total_requested_capacity > self.disk_capacity
            || memory_used + total_requested_capacity > self.memory_capacity
        {
            rate_limited_warn!(
                limit_per_min = 10,
                "failed to stage persist request: WAL disk usage {}, disk capacity {}, memory \
                 usage {}, memory capacity {}, requested capacity {}",
                disk_used,
                self.disk_capacity,
                memory_used,
                self.memory_capacity,
                total_requested_capacity
            );
            return Err(failure(PersistFailureReason::WalFull));
        }

        let Some(shard) = self
            .inner
            .find_most_capacity_shard_mut(subrequest.index_uid(), &subrequest.source_id)
        else {
            warn!(
                index_uid=%subrequest.index_uid(),
                source_id=%subrequest.source_id,
                "no open shard found on ingester"
            );
            return Err(failure(PersistFailureReason::NoShardsForSource));
        };
        shard.is_advertisable = true;
        let (valid_doc_batch, parse_failures) = match validate_doc_batch(shard, doc_batch).await {
            Ok(validated) => validated,
            Err(error) => {
                error!(queue_id=%shard.queue_id(), "failed to validate documents: {error}");
                return Err(failure(PersistFailureReason::Internal));
            }
        };
        let batch_size = if parse_failures.is_empty() {
            requested_capacity
        } else {
            doc_batch_size(&valid_doc_batch, context.force_commit)
        };
        context.reserved_capacity += batch_size;
        shard.queue_size += batch_size;

        Ok(StagedPersistRequest {
            subrequest_id: subrequest.subrequest_id,
            index_uid: subrequest.index_uid,
            source_id: subrequest.source_id,
            shard_id: shard.shard_id.clone(),
            queue_id: shard.queue_id(),
            num_docs: valid_doc_batch.num_docs() as u32,
            doc_batch: valid_doc_batch,
            batch_size,
            parse_failures,
            from_position_exclusive: shard.replication_position_inclusive.clone(),
        })
    }

    pub async fn persist_subrequest(
        &mut self,
        staged_request: StagedPersistRequest,
        context: &mut PersistContext,
    ) -> Result<PersistSuccess, PersistFailure> {
        let replication_position_inclusive = if staged_request.num_docs > 0 {
            let append_result = append_non_empty_doc_batch(
                &mut self.mrecordlog,
                &staged_request.queue_id,
                staged_request.doc_batch,
                context.force_commit,
            )
            .await;
            let position = match append_result {
                Ok(position) => position,
                Err(append_error) => {
                    self.shards
                        .get_mut(&staged_request.queue_id)
                        .expect("shard should exist")
                        .queue_size -= staged_request.batch_size;
                    match append_error {
                        AppendDocBatchError::Io(io_error) => {
                            error!(queue_id=%staged_request.queue_id, "failed to persist records: {io_error}");
                            context.shards_to_close.insert(staged_request.queue_id);
                        }
                        AppendDocBatchError::QueueNotFound(_) => {
                            error!(queue_id=%staged_request.queue_id, "failed to persist records: WAL queue not found");
                            context.shards_to_delete.insert(staged_request.queue_id);
                        }
                    }
                    return Err(PersistFailure {
                        subrequest_id: staged_request.subrequest_id,
                        index_uid: staged_request.index_uid,
                        source_id: staged_request.source_id,
                        reason: PersistFailureReason::Internal as i32,
                    });
                }
            };
            self.shards
                .get_mut(&staged_request.queue_id)
                .expect("shard should exist")
                .set_replication_position_inclusive(position.clone(), Instant::now());
            position
        } else {
            staged_request.from_position_exclusive
        };

        Ok(PersistSuccess {
            subrequest_id: staged_request.subrequest_id,
            index_uid: staged_request.index_uid,
            source_id: staged_request.source_id,
            shard_id: Some(staged_request.shard_id),
            replication_position_inclusive: Some(replication_position_inclusive),
            num_persisted_docs: staged_request.num_docs,
            parse_failures: staged_request.parse_failures,
        })
    }

    pub fn finish_persist(&mut self, context: PersistContext) {
        for queue_id in context.shards_to_close {
            self.shards
                .get_mut(&queue_id)
                .expect("shard should exist")
                .close();
            warn!("closed shard `{queue_id}` following IO error");
        }
        for queue_id in context.shards_to_delete {
            self.shards.remove(&queue_id);
            warn!("deleted dangling shard `{queue_id}`");
        }
    }

    /// Reports the current WAL disk/memory usage and usage-ratio metrics against the configured
    /// capacity limits. Called after any operation that grows or shrinks WAL usage so that
    /// dashboards and alerts relying on these metrics stay accurate even when the ingester is
    /// otherwise idle (e.g. after shards are cleaned up via gossip or a control-plane RPC).
    fn report_wal_usage(&self) {
        let wal_usage = self.mrecordlog.resource_usage();
        crate::ingest_v2::metrics::report_wal_usage(
            wal_usage,
            self.disk_capacity,
            self.memory_capacity,
        );
    }

    /// Deletes the shard identified by `queue_id` from the ingester state. It removes the
    /// mrecordlog queue first and then removes the associated in-memory shard and rate trackers.
    #[instrument(name = "ingester.delete_shard", skip_all, fields(queue_id, initiator))]
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
                    self.report_wal_usage();
                }
            }
            Err(DeleteQueueError::IoError(io_error)) => {
                error!("failed to delete shard `{queue_id}`: {io_error}");
            }
        };
    }

    /// Truncates the shard identified by `queue_id` up to `truncate_up_to_position_inclusive` only
    /// if the current truncation position of the shard is smaller.
    #[instrument(
        name = "ingester.truncate_shard",
        skip_all,
        fields(queue_id, truncate_up_to_position_inclusive, initiator)
    )]
    pub async fn truncate_shard(
        &mut self,
        queue_id: &QueueId,
        truncate_up_to_position_inclusive: Position,
        initiator: &'static str,
    ) {
        let Some(shard) = self.inner.shards.get_mut(queue_id) else {
            return;
        };
        if shard.truncation_position_inclusive >= truncate_up_to_position_inclusive {
            return;
        }
        if let Some(truncate_up_to_offset_inclusive) = truncate_up_to_position_inclusive.as_u64() {
            match self
                .mrecordlog
                .truncate(queue_id, truncate_up_to_offset_inclusive)
                .await
            {
                Ok(evicted_size) => {
                    let queue_size = shard
                        .queue_size
                        .as_u64()
                        .saturating_sub(evicted_size.as_u64());
                    shard.queue_size = ByteSize::b(queue_size);
                }
                Err(TruncateError::MissingQueue(_)) => {
                    error!("failed to truncate shard `{queue_id}`: WAL queue not found");
                    self.shards.remove(queue_id);
                    info!("deleted dangling shard `{queue_id}`");
                    return;
                }
                Err(TruncateError::IoError(io_error)) => {
                    error!("failed to truncate shard `{queue_id}`: {io_error}");
                    return;
                }
            }
        }
        info!(
            "truncated shard `{queue_id}` at {truncate_up_to_position_inclusive} initiated via \
             `{initiator}`"
        );
        shard.truncation_position_inclusive = truncate_up_to_position_inclusive;
        self.report_wal_usage();
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

pub(super) struct PersistContext {
    force_commit: bool,
    wal_usage: ResourceUsage,
    reserved_capacity: ByteSize,
    shards_to_close: HashSet<QueueId>,
    shards_to_delete: HashSet<QueueId>,
}

pub(super) struct StagedPersistRequest {
    subrequest_id: u32,
    index_uid: Option<IndexUid>,
    source_id: SourceId,
    shard_id: ShardId,
    queue_id: QueueId,
    doc_batch: DocBatchV2,
    batch_size: ByteSize,
    num_docs: u32,
    parse_failures: Vec<ParseFailure>,
    from_position_exclusive: Position,
}

async fn validate_doc_batch(
    shard: &mut IngesterShard,
    doc_batch: DocBatchV2,
) -> IngestV2Result<(DocBatchV2, Vec<ParseFailure>)> {
    let original_batch_num_bytes = doc_batch.num_bytes() as u64;
    let doc_mapper = shard.doc_mapper_opt.clone().expect("shard should be open");
    let (valid_doc_batch, parse_failures) = if shard.validate_docs {
        super::doc_mapper::validate_doc_batch(doc_batch, doc_mapper).await?
    } else {
        (doc_batch, Vec::new())
    };
    doc_batch_metrics(
        shard,
        original_batch_num_bytes,
        &valid_doc_batch,
        &parse_failures,
    );
    Ok((valid_doc_batch, parse_failures))
}

fn doc_batch_metrics(
    shard: &mut IngesterShard,
    original_batch_num_bytes: u64,
    valid_doc_batch: &DocBatchV2,
    parse_failures: &[ParseFailure],
) {
    let valid_batch_num_bytes = valid_doc_batch.num_bytes() as u64;
    if valid_doc_batch.is_empty() || !parse_failures.is_empty() {
        counter!(
            parent: DOCS_TOTAL,
            labels: [label_values!(VALIDITY => "invalid")],
        )
        .inc_by(parse_failures.len() as u64);
        counter!(
            parent: DOCS_BYTES_TOTAL,
            labels: [label_values!(VALIDITY => "invalid")],
        )
        .inc_by(original_batch_num_bytes - valid_batch_num_bytes);
    }
    if !valid_doc_batch.is_empty() {
        counter!(
            parent: DOCS_TOTAL,
            labels: [label_values!(VALIDITY => "valid")],
        )
        .inc_by(valid_doc_batch.num_docs() as u64);
        counter!(
            parent: DOCS_BYTES_TOTAL,
            labels: [label_values!(VALIDITY => "valid")],
        )
        .inc_by(valid_batch_num_bytes);
        shard.rate_meter.update(valid_batch_num_bytes);
    }
}

#[cfg(test)]
mod tests {
    use bytesize::ByteSize;
    use quickwit_cluster::{ChitchatTransport, create_cluster_for_test};
    use quickwit_config::service::QuickwitService;
    use quickwit_proto::types::{ShardId, SourceId, queue_id};
    use tokio::time::timeout;

    use super::*;

    async fn test_cluster() -> Cluster {
        create_cluster_for_test(
            Vec::new(),
            &[QuickwitService::Indexer.as_str()],
            &ChitchatTransport::default(),
            true,
        )
        .await
        .unwrap()
    }

    #[tokio::test]
    async fn test_publish_local_shards_cadence_and_snapshot() {
        let (sender, mut receiver) = watch::channel(None);
        let state = IngesterState::create(
            test_cluster().await,
            ByteSize::mb(256),
            ByteSize::mb(256),
            sender,
        )
        .await;
        tokio::time::pause();
        let mut inner = state.inner.lock().await;
        let index_uid = IndexUid::for_test("index", 0);
        for (source_id, shard_id, advertisable) in [
            ("source-a", 1, true),
            ("source-a", 2, false),
            ("source-b", 3, true),
        ] {
            let mut shard = IngesterShard::builder(
                index_uid.clone(),
                source_id.to_string(),
                ShardId::from(shard_id),
            )
            .build();
            shard.is_advertisable = advertisable;
            shard.rate_meter.update(100);
            inner.shards.insert(shard.queue_id(), shard);
        }
        assert!(inner.harvest_shard_throughput_readings().is_none());
        assert!(receiver.borrow().is_none());
        tokio::time::advance(LOCAL_SHARDS_SAMPLE_INTERVAL).await;
        let snapshot = inner.harvest_shard_throughput_readings().unwrap();
        assert_eq!(snapshot.per_source_shard_infos.len(), 2);
        for shards in snapshot.per_source_shard_infos.values() {
            assert_eq!(shards.len(), 1);
            let shard = shards.first().unwrap();
            assert_ne!(shard.shard_id, ShardId::from(2));
            assert_eq!(shard.short_term_ingestion_rate, ByteSize::b(2_000));
            assert_eq!(shard.long_term_ingestion_rate, ByteSize::b(2_000));
        }
        assert!(Arc::ptr_eq(
            receiver.borrow_and_update().as_ref().unwrap(),
            &snapshot
        ));
        assert!(inner.harvest_shard_throughput_readings().is_none());
        assert!(!receiver.has_changed().unwrap());

        inner.shards.clear();
        tokio::time::advance(LOCAL_SHARDS_SAMPLE_INTERVAL).await;
        assert!(
            inner
                .harvest_shard_throughput_readings()
                .unwrap()
                .per_source_shard_infos
                .is_empty()
        );
        assert!(
            receiver
                .borrow()
                .as_ref()
                .unwrap()
                .per_source_shard_infos
                .is_empty()
        );
        assert_eq!(snapshot.per_source_shard_infos.len(), 2);
    }

    #[tokio::test]
    async fn test_local_shards_publisher_skips_busy_state_and_stops() {
        let (sender, receiver) = watch::channel(None);
        let state = IngesterState::create(
            test_cluster().await,
            ByteSize::mb(256),
            ByteSize::mb(256),
            sender,
        )
        .await;
        tokio::time::pause();
        let publisher = state.spawn_shards_readings_publisher();
        tokio::task::yield_now().await;
        tokio::time::advance(LOCAL_SHARDS_SAMPLE_INTERVAL).await;
        tokio::task::yield_now().await;
        assert!(receiver.borrow().is_none());

        let mut inner = state.inner.lock().await;
        inner.set_status(IngesterStatus::Ready).await;
        tokio::time::advance(LOCAL_SHARDS_SAMPLE_INTERVAL).await;
        tokio::task::yield_now().await;
        assert!(receiver.borrow().is_none());
        drop(inner);
        let inner = state
            .inner
            .try_lock()
            .expect("publisher must not queue for the lock");
        drop(inner);

        tokio::time::advance(LOCAL_SHARDS_SAMPLE_INTERVAL).await;
        tokio::task::yield_now().await;
        assert!(receiver.borrow().is_some());

        state
            .inner
            .lock()
            .await
            .set_status(IngesterStatus::Failed)
            .await;
        tokio::time::advance(LOCAL_SHARDS_SAMPLE_INTERVAL).await;
        timeout(LOCAL_SHARDS_SAMPLE_INTERVAL, publisher)
            .await
            .unwrap()
            .unwrap();

        let publisher = state.spawn_shards_readings_publisher();
        let weak = state.weak();
        drop(state);
        assert!(weak.upgrade().is_none());
        timeout(LOCAL_SHARDS_SAMPLE_INTERVAL, publisher)
            .await
            .unwrap()
            .unwrap();
    }

    #[tokio::test]
    async fn test_ingester_state_does_not_lock_while_initializing() {
        let cluster = test_cluster().await;
        let state = IngesterState::create(
            cluster,
            ByteSize::mb(256),
            ByteSize::mb(256),
            watch::Sender::new(None),
        )
        .await;
        let inner_guard = state.inner.lock().await;

        assert_eq!(inner_guard.status(), IngesterStatus::Initializing);
        assert_eq!(*state.status_rx.borrow(), IngesterStatus::Initializing);

        let error = state.lock_partially("test").await.unwrap_err().to_string();
        assert!(error.contains("ingester is initializing"));

        let error = state.lock_fully("test").await.unwrap_err().to_string();
        assert!(error.contains("ingester is initializing"));
    }

    #[tokio::test]
    async fn test_ingester_state_failed() {
        let cluster = test_cluster().await;
        let state = IngesterState::create(
            cluster,
            ByteSize::mb(256),
            ByteSize::mb(256),
            watch::Sender::new(None),
        )
        .await;

        state
            .inner
            .lock()
            .await
            .set_status(IngesterStatus::Failed)
            .await;

        let error = state.lock_partially("test").await.unwrap_err().to_string();
        assert!(error.to_string().ends_with("failed to initialize ingester"));

        let error = state.lock_fully("test").await.unwrap_err().to_string();
        assert!(error.contains("failed to initialize ingester"));
    }

    #[tokio::test]
    async fn test_ingester_state_init() {
        let index_uid = IndexUid::for_test("test-index", 0);
        let source_id = SourceId::from("test-source");

        // Queue with live records, partially truncated.
        let queue_id_01 = queue_id(&index_uid, &source_id, &ShardId::from(1));
        // Queue written to and then fully truncated: empty, but it remembers its position.
        let queue_id_02 = queue_id(&index_uid, &source_id, &ShardId::from(2));
        // Queue created but never written to.
        let queue_id_03 = queue_id(&index_uid, &source_id, &ShardId::from(3));

        let temp_dir = tempfile::tempdir().unwrap();

        // Populate a WAL then close it, so `init` reopens it from disk.
        {
            let mut mrecordlog = MultiRecordLogAsync::open(temp_dir.path()).await.unwrap();

            mrecordlog.create_queue(&queue_id_01).await.unwrap();
            mrecordlog
                .append_records(
                    &queue_id_01,
                    None,
                    [
                        &b"test-doc-foo"[..],
                        &b"test-doc-bar"[..],
                        &b"test-doc-qux"[..],
                    ]
                    .into_iter(),
                )
                .await
                .unwrap();
            // Records 0..=2 remain; truncate record 0 so `start` advances to 1.
            mrecordlog.truncate(&queue_id_01, 0).await.unwrap();

            mrecordlog.create_queue(&queue_id_02).await.unwrap();
            mrecordlog
                .append_records(
                    &queue_id_02,
                    None,
                    [&b"test-doc-foo"[..], &b"test-doc-bar"[..]].into_iter(),
                )
                .await
                .unwrap();
            // Truncate everything: the queue is now empty but remembers position 1.
            mrecordlog.truncate(&queue_id_02, 1).await.unwrap();

            mrecordlog.create_queue(&queue_id_03).await.unwrap();
        }
        let cluster = test_cluster().await;
        let mut state = IngesterState::create(
            cluster,
            ByteSize::mb(256),
            ByteSize::mb(256),
            watch::Sender::new(None),
        )
        .await;
        state
            .init(
                temp_dir.path(),
                ByteSize::mb(256),
                ByteSize::mb(256),
                RateLimiterSettings::default(),
            )
            .await;
        timeout(Duration::from_millis(100), state.wait_for_ready())
            .await
            .unwrap();

        let state_guard = state.lock_fully("test").await.unwrap();
        assert_eq!(state_guard.status(), IngesterStatus::Ready);
        assert_eq!(*state_guard.status_tx.borrow(), IngesterStatus::Ready);

        // Non-empty queue: recovers at its last position, truncated up to the first kept record.
        let shard_01 = state_guard.shards.get(&queue_id_01).unwrap();
        assert_eq!(shard_01.shard_state, ShardState::Closed);
        assert_eq!(
            shard_01.replication_position_inclusive,
            Position::offset(2u64)
        );
        assert_eq!(
            shard_01.truncation_position_inclusive,
            Position::offset(0u64)
        );
        assert_eq!(shard_01.queue_size, ByteSize::b(24));

        // Fully truncated queue: recovers at its last position rather than the beginning.
        let shard_02 = state_guard.shards.get(&queue_id_02).unwrap();
        assert_eq!(shard_02.shard_state, ShardState::Closed);
        assert_eq!(
            shard_02.replication_position_inclusive,
            Position::offset(1u64)
        );
        assert_eq!(
            shard_02.truncation_position_inclusive,
            Position::offset(1u64)
        );
        assert_eq!(shard_02.queue_size, ByteSize::b(0));

        // Never-written queue: recovers at the beginning.
        let shard_03 = state_guard.shards.get(&queue_id_03).unwrap();
        assert_eq!(shard_03.shard_state, ShardState::Closed);
        assert_eq!(shard_03.replication_position_inclusive, Position::Beginning);
        assert_eq!(shard_03.truncation_position_inclusive, Position::Beginning);
        assert_eq!(shard_03.queue_size, ByteSize::b(0));
    }

    fn insert_shard_with_used_capacity(
        state: &mut InnerIngesterState,
        index_uid: IndexUid,
        source_id: SourceId,
        shard_id: ShardId,
        shard_state: ShardState,
        used_capacity: ByteSize,
    ) {
        let mut shard = IngesterShard::builder(index_uid, source_id, shard_id)
            .with_state(shard_state)
            .build();
        shard.rate_limiter.acquire_bytes(used_capacity);

        let queue_id = shard.queue_id();
        state.shards.insert(queue_id, shard);
    }

    #[tokio::test]
    async fn test_find_most_capacity_shard_returns_shard_with_least_used_capacity() {
        let cluster = create_cluster_for_test(
            Vec::new(),
            &[QuickwitService::Indexer.as_str()],
            &ChitchatTransport::default(),
            true,
        )
        .await
        .unwrap();
        let (_temp_dir, state) = IngesterState::for_test(cluster).await;
        let mut state_guard = state.lock_partially("test").await.unwrap();

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
        let cluster = create_cluster_for_test(
            Vec::new(),
            &[QuickwitService::Indexer.as_str()],
            &ChitchatTransport::default(),
            true,
        )
        .await
        .unwrap();
        let (_temp_dir, state) = IngesterState::for_test(cluster).await;
        let mut locked_state = state.lock_partially("test").await.unwrap();

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
        let cluster = create_cluster_for_test(
            Vec::new(),
            &[QuickwitService::Indexer.as_str()],
            &ChitchatTransport::default(),
            true,
        )
        .await
        .unwrap();
        let (_temp_dir, state) = IngesterState::for_test(cluster).await;
        let mut locked_state = state.lock_partially("test").await.unwrap();

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

    #[tokio::test]
    async fn test_ingester_state_set_status() {
        let cluster = test_cluster().await;
        let state = IngesterState::create(
            cluster.clone(),
            ByteSize::mb(256),
            ByteSize::mb(256),
            watch::Sender::new(None),
        )
        .await;
        let temp_dir = tempfile::tempdir().unwrap();

        state
            .init(
                temp_dir.path(),
                ByteSize::mb(256),
                ByteSize::mb(256),
                RateLimiterSettings::default(),
            )
            .await;

        let mut state_guard = state.lock_fully("test").await.unwrap();
        state_guard.set_status(IngesterStatus::Failed).await;
        assert_eq!(state_guard.status(), IngesterStatus::Failed);
        assert_eq!(*state.status_rx.borrow(), IngesterStatus::Failed);

        let status_json_str = cluster
            .get_self_key_value(INGESTER_STATUS_KEY)
            .await
            .unwrap();
        let status = IngesterStatus::from_json_str_name(&status_json_str).unwrap();
        assert_eq!(status, IngesterStatus::Failed);
    }

    fn open_shard(index_uid: IndexUid, source_id: SourceId, shard_id: ShardId) -> IngesterShard {
        IngesterShard::builder(index_uid, source_id, shard_id)
            .advertisable()
            .build()
    }

    #[tokio::test]
    async fn test_get_shard_snapshot() {
        let cluster = test_cluster().await;
        let (_temp_dir, state) = IngesterState::for_test(cluster).await;
        let mut state_guard = state.lock_partially("test").await.unwrap();

        let index_uid = IndexUid::for_test("test-index", 0);

        // source-a: 2 open shards + 1 closed shard.
        let shard = open_shard(index_uid.clone(), "source-a".into(), ShardId::from(1));
        state_guard.shards.insert(shard.queue_id(), shard);
        let shard = open_shard(index_uid.clone(), "source-a".into(), ShardId::from(2));
        state_guard.shards.insert(shard.queue_id(), shard);
        let shard = IngesterShard::builder(index_uid.clone(), "source-a".into(), ShardId::from(3))
            .with_state(ShardState::Closed)
            .advertisable()
            .build();
        state_guard.shards.insert(shard.queue_id(), shard);

        // source-b: 2 closed shards, no open shards.
        let shard = IngesterShard::builder(index_uid.clone(), "source-b".into(), ShardId::from(5))
            .with_state(ShardState::Closed)
            .advertisable()
            .build();
        state_guard.shards.insert(shard.queue_id(), shard);
        let shard = IngesterShard::builder(index_uid.clone(), "source-b".into(), ShardId::from(6))
            .with_state(ShardState::Closed)
            .advertisable()
            .build();
        state_guard.shards.insert(shard.queue_id(), shard);

        let (mut open_counts, mut closed_shards) = state_guard.get_shard_snapshot();

        // Open counts: source-a has 2, source-b has 0.
        open_counts.sort_by(|a, b| a.1.cmp(&b.1));
        assert_eq!(open_counts.len(), 2);
        assert_eq!(
            open_counts[0],
            (index_uid.clone(), SourceId::from("source-a"), 2)
        );
        assert_eq!(
            open_counts[1],
            (index_uid.clone(), SourceId::from("source-b"), 0)
        );

        // Closed shards: source-a has shard 3, source-b has shards 5 and 6.
        closed_shards.sort_by(|a, b| a.source_id.cmp(&b.source_id));
        assert_eq!(closed_shards.len(), 2);

        assert_eq!(closed_shards[0].source_id, "source-a");
        assert_eq!(closed_shards[0].shard_ids, vec![ShardId::from(3)]);

        assert_eq!(closed_shards[1].source_id, "source-b");
        let mut source_b_ids = closed_shards[1].shard_ids.clone();
        source_b_ids.sort();
        assert_eq!(source_b_ids, vec![ShardId::from(5), ShardId::from(6)]);
    }

    #[tokio::test]
    async fn test_truncate_shard() {
        let cluster = test_cluster().await;
        let (_temp_dir, state) = IngesterState::for_test(cluster).await;

        let index_uid = IndexUid::for_test("test-index", 0);
        let source_id = SourceId::from("test-source");
        // Shard 1 is empty (never written): its EOF is `Eof(None)`, with no WAL offset.
        let queue_id_01 = queue_id(&index_uid, &source_id, &ShardId::from(1));
        // Shard 2 holds two records: its EOF is `Eof(Some(1))`.
        let queue_id_02 = queue_id(&index_uid, &source_id, &ShardId::from(2));

        let mut state_guard = state.lock_fully("test").await.unwrap();

        state_guard
            .mrecordlog
            .create_queue(&queue_id_01)
            .await
            .unwrap();
        state_guard
            .mrecordlog
            .create_queue(&queue_id_02)
            .await
            .unwrap();
        state_guard
            .mrecordlog
            .append_records(
                &queue_id_02,
                None,
                [&b"test-doc-foo"[..], &b"test-doc-bar"[..]].into_iter(),
            )
            .await
            .unwrap();

        let shard_01 =
            IngesterShard::builder(index_uid.clone(), source_id.clone(), ShardId::from(1))
                .with_state(ShardState::Closed)
                .build();
        state_guard.shards.insert(queue_id_01.clone(), shard_01);
        let shard_02 =
            IngesterShard::builder(index_uid.clone(), source_id.clone(), ShardId::from(2))
                .with_state(ShardState::Closed)
                .with_replication_position_inclusive(Position::offset(1u64))
                .with_queue_size(ByteSize::b(24))
                .build();
        state_guard.shards.insert(queue_id_02.clone(), shard_02);

        state_guard
            .truncate_shard(&queue_id_01, Position::Beginning.as_eof(), "test")
            .await;
        let shard_01 = state_guard.shards.get(&queue_id_01).unwrap();
        assert_eq!(
            shard_01.truncation_position_inclusive,
            Position::Beginning.as_eof()
        );
        assert!(state_guard.mrecordlog.queue_exists(&queue_id_01));
        assert_eq!(
            state_guard
                .shards
                .get(&queue_id_01)
                .unwrap()
                .truncation_position_inclusive,
            Position::Beginning.as_eof()
        );

        state_guard
            .truncate_shard(&queue_id_02, Position::eof(1u64), "test")
            .await;
        let shard_02 = state_guard.shards.get(&queue_id_02).unwrap();
        assert_eq!(shard_02.truncation_position_inclusive, Position::eof(1u64));
        assert_eq!(shard_02.queue_size, ByteSize::b(0));
        state_guard
            .mrecordlog
            .assert_records_eq(&queue_id_02, .., &[]);

        assert_eq!(
            state_guard
                .shards
                .get(&queue_id_02)
                .unwrap()
                .truncation_position_inclusive,
            Position::eof(1u64)
        );
    }
}
