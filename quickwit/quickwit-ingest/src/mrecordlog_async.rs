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
use std::hash::{Hash, Hasher};
use std::io;
use std::ops::RangeBounds;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};

use bytes::Buf;
use mrecordlog::error::*;
use mrecordlog::{MultiRecordLog, PersistAction, PersistPolicy, Record, ResourceUsage};
use tracing::{Span, error, info, info_span, instrument, warn};

use crate::ingest_v2::metrics::{
    WAL_BYTES_WRITTEN_APPEND, WAL_BYTES_WRITTEN_CREATE_QUEUE, WAL_BYTES_WRITTEN_DELETE_QUEUE,
    WAL_BYTES_WRITTEN_TRUNCATE,
};

/// One mrecordlog instance (one directory, one file writer) and its cached resource usage.
struct WalInstance {
    mrecordlog: Mutex<MultiRecordLog>,
    memory_used_bytes: AtomicUsize,
    memory_allocated_bytes: AtomicUsize,
    disk_used_bytes: AtomicUsize,
}

impl WalInstance {
    fn new(mrecordlog: MultiRecordLog) -> Self {
        let instance = WalInstance {
            mrecordlog: Mutex::new(mrecordlog),
            memory_used_bytes: AtomicUsize::new(0),
            memory_allocated_bytes: AtomicUsize::new(0),
            disk_used_bytes: AtomicUsize::new(0),
        };
        instance.update_usage(&instance.lock());
        instance
    }

    fn lock(&self) -> MutexGuard<'_, MultiRecordLog> {
        match self.mrecordlog.lock() {
            Ok(guard) => guard,
            Err(_) => {
                // An operation panicked while writing to the WAL.
                error!("wal is poisoned, aborting process");
                std::process::abort();
            }
        }
    }

    fn update_usage(&self, mrecordlog: &MultiRecordLog) {
        let usage = mrecordlog.resource_usage();
        self.memory_used_bytes
            .store(usage.memory_used_bytes, Ordering::Relaxed);
        self.memory_allocated_bytes
            .store(usage.memory_allocated_bytes, Ordering::Relaxed);
        self.disk_used_bytes
            .store(usage.disk_used_bytes, Ordering::Relaxed);
    }
}

/// The locked instance of a queue. See [`MultiRecordLogAsync::lock_queue_instance`].
pub struct InstanceGuard<'a>(MutexGuard<'a, MultiRecordLog>);

impl InstanceGuard<'_> {
    pub fn range<R>(
        &self,
        queue: &str,
        range: R,
    ) -> Result<impl Iterator<Item = Record<'_>> + '_, MissingQueue>
    where
        R: RangeBounds<u64> + 'static,
    {
        self.0.range(queue, range)
    }
}

/// The ingester's write-ahead log: one or several independent mrecordlog instances.
///
/// Each queue lives in exactly one instance. With several instances, appends to queues of
/// different instances run in parallel ([`Self::append_records_shared`]): a single mrecordlog
/// serializes every append (frame encoding, checksum, file write, in-memory copy), which caps the
/// ingest throughput of a node.
///
/// Operations taking `&mut self` are only called by the owner of the ingester's exclusive WAL lock
/// and never wait on an instance. Operations taking `&self` may run concurrently, under the shared
/// WAL lock, and lock the instance of their queue.
pub struct MultiRecordLogAsync {
    instances: Vec<Arc<WalInstance>>,
    /// Instance of every queue. Queues created by this process are placed by hashing their ID.
    queue_instances: HashMap<String, usize>,
}

/// Directory of the `instance_idx`-th instance: the WAL directory itself for the first one (the
/// layout of a single-instance WAL), and sibling directories for the others.
fn instance_dir_path(directory_path: &Path, instance_idx: usize) -> PathBuf {
    if instance_idx == 0 {
        return directory_path.to_path_buf();
    }
    let mut dir_name = directory_path
        .file_name()
        .map(|name| name.to_os_string())
        .unwrap_or_default();
    dir_name.push(format!("-{instance_idx}"));
    directory_path.with_file_name(dir_name)
}

impl MultiRecordLogAsync {
    pub async fn open(directory_path: &Path) -> Result<Self, ReadRecordError> {
        Self::open_with_prefs(
            directory_path,
            PersistPolicy::Always(PersistAction::Flush),
            1,
        )
        .await
    }

    /// Opens the WAL with `num_instances` instances. Instances left over by a previous run with
    /// more instances are opened too, so their queues are recovered.
    #[instrument(name = "mrecordlog.open_async", skip_all, fields(directory_path = %directory_path.display(), ?persist_policy))]
    pub async fn open_with_prefs(
        directory_path: &Path,
        persist_policy: PersistPolicy,
        num_instances: usize,
    ) -> Result<Self, ReadRecordError> {
        let num_instances = num_instances.max(1);
        let mut instance_dir_paths: Vec<PathBuf> = (0..num_instances)
            .map(|instance_idx| instance_dir_path(directory_path, instance_idx))
            .collect();
        loop {
            let leftover_dir_path = instance_dir_path(directory_path, instance_dir_paths.len());
            if !leftover_dir_path.is_dir() {
                break;
            }
            warn!(
                "opening WAL instance `{}` left over by a previous run",
                leftover_dir_path.display()
            );
            instance_dir_paths.push(leftover_dir_path);
        }
        let mut open_tasks = Vec::with_capacity(instance_dir_paths.len());
        for instance_dir_path in instance_dir_paths {
            let persist_policy = persist_policy.clone();
            open_tasks.push(tokio::task::spawn_blocking(move || {
                std::fs::create_dir_all(&instance_dir_path)?;
                MultiRecordLog::open_with_prefs(&instance_dir_path, persist_policy)
            }));
        }
        let mut instances = Vec::with_capacity(open_tasks.len());
        for open_task in open_tasks {
            let mrecordlog = open_task.await.map_err(|join_err| {
                error!(error=?join_err, "failed to load WAL");
                ReadRecordError::IoError(io::Error::other("loading wal from directory failed"))
            })??;
            instances.push(Arc::new(WalInstance::new(mrecordlog)));
        }
        let mut queue_instances = HashMap::new();
        for (instance_idx, instance) in instances.iter().enumerate() {
            for queue in instance.lock().list_queues() {
                if let Some(other_instance_idx) =
                    queue_instances.insert(queue.to_string(), instance_idx)
                {
                    error!(
                        queue,
                        "queue found in WAL instances {other_instance_idx} and {instance_idx}"
                    );
                }
            }
        }
        if instances.len() > 1 {
            info!("opened WAL with {} instances", instances.len());
        }
        Ok(Self {
            instances,
            queue_instances,
        })
    }

    /// Instance of an existing queue, or of a new queue.
    fn instance_idx(&self, queue: &str) -> usize {
        if let Some(instance_idx) = self.queue_instances.get(queue) {
            return *instance_idx;
        }
        let mut hasher = std::collections::hash_map::DefaultHasher::new();
        queue.hash(&mut hasher);
        (hasher.finish() % self.instances.len() as u64) as usize
    }

    fn instance(&self, queue: &str) -> &Arc<WalInstance> {
        &self.instances[self.instance_idx(queue)]
    }

    /// Runs a WAL operation on a blocking thread, holding the lock of the queue's instance.
    async fn run_operation<F, T>(&self, queue: &str, inner_span: Span, operation: F) -> T
    where
        F: FnOnce(&mut MultiRecordLog) -> T + Send + 'static,
        T: Send + 'static,
    {
        let instance = self.instance(queue).clone();
        let join_res = tokio::task::spawn_blocking(move || {
            let _entered = inner_span.entered();
            let mut mrecordlog = instance.lock();
            let res = operation(&mut mrecordlog);
            instance.update_usage(&mrecordlog);
            res
        })
        .await;

        match join_res {
            Ok(operation_result) => operation_result,
            Err(error) => {
                // This could be caused by a panic
                error!(%error, "failed to run mrecordlog operation");
                panic!("failed to run mrecordlog operation");
            }
        }
    }

    #[instrument(name = "mrecordlog.create_queue_async", skip_all, fields(queue))]
    pub async fn create_queue(&mut self, queue: &str) -> Result<(), CreateQueueError> {
        let span = info_span!("mrecordlog.create_queue", queue);
        let instance_idx = self.instance_idx(queue);
        let queue_owned = queue.to_string();
        self.run_operation(queue, span, move |mrecordlog| {
            mrecordlog
                .create_queue(&queue_owned)
                .inspect(|outcome| {
                    WAL_BYTES_WRITTEN_CREATE_QUEUE.inc_by(outcome.wal_bytes_written);
                })
                .map(|_| ())
        })
        .await?;
        self.queue_instances.insert(queue.to_string(), instance_idx);
        Ok(())
    }

    #[instrument(name = "mrecordlog.delete_queue_async", skip_all, fields(queue))]
    pub async fn delete_queue(&mut self, queue: &str) -> Result<(), DeleteQueueError> {
        let span = info_span!("mrecordlog.delete_queue", queue);
        let queue_owned = queue.to_string();
        let result = self
            .run_operation(queue, span, move |mrecordlog| {
                mrecordlog
                    .delete_queue(&queue_owned)
                    .inspect(|outcome| {
                        WAL_BYTES_WRITTEN_DELETE_QUEUE.inc_by(outcome.wal_bytes_written);
                    })
                    .map(|_| ())
            })
            .await;
        if !matches!(result, Err(DeleteQueueError::IoError(_))) {
            self.queue_instances.remove(queue);
        }
        result
    }

    #[instrument(name = "mrecordlog.append_records_async", skip_all, fields(queue))]
    pub async fn append_records<T: Iterator<Item = impl Buf> + Send + 'static>(
        &mut self,
        queue: &str,
        position_opt: Option<u64>,
        payloads: T,
    ) -> Result<Option<u64>, AppendError> {
        self.append_records_shared(queue, position_opt, payloads)
            .await
    }

    /// Same as [`Self::append_records`], callable concurrently: appends to queues of different
    /// instances run in parallel.
    #[instrument(
        name = "mrecordlog.append_records_shared_async",
        skip_all,
        fields(queue)
    )]
    pub async fn append_records_shared<T: Iterator<Item = impl Buf> + Send + 'static>(
        &self,
        queue: &str,
        position_opt: Option<u64>,
        payloads: T,
    ) -> Result<Option<u64>, AppendError> {
        let span = info_span!("mrecordlog.append_records", queue);
        let queue_owned = queue.to_string();
        self.run_operation(queue, span, move |mrecordlog| {
            mrecordlog
                .append_records(&queue_owned, position_opt, payloads)
                .inspect(|outcome| {
                    WAL_BYTES_WRITTEN_APPEND.inc_by(outcome.wal_bytes_written);
                })
                .map(|outcome| outcome.last_position)
        })
        .await
    }

    #[instrument(name = "mrecordlog.truncate_async", skip_all, fields(queue, position))]
    /// Callable concurrently with appends and truncations: locks only the instance of `queue`.
    pub async fn truncate(&self, queue: &str, position: u64) -> Result<usize, TruncateError> {
        let span = info_span!("mrecordlog.truncate", queue, position);
        let queue_owned = queue.to_string();
        self.run_operation(queue, span, move |mrecordlog| {
            mrecordlog
                .truncate(&queue_owned, ..=position)
                .inspect(|outcome| {
                    WAL_BYTES_WRITTEN_TRUNCATE.inc_by(outcome.wal_bytes_written);
                })
                .map(|outcome| outcome.evicted_records)
        })
        .await
    }

    /// Calls `f` with the records of `queue` in `range`, holding the lock of the queue's
    /// instance: `f` should be short (copying the records, for instance).
    pub fn with_range<R, T>(
        &self,
        queue: &str,
        range: R,
        f: impl FnOnce(&mut dyn Iterator<Item = Record<'_>>) -> T,
    ) -> Result<T, MissingQueue>
    where
        R: RangeBounds<u64> + 'static,
    {
        let mrecordlog = self.instance(queue).lock();
        let mut records = mrecordlog.range(queue, range)?;
        Ok(f(&mut records))
    }

    /// Locks the instance of `queue`, for reading its records with [`InstanceGuard::range`].
    pub fn lock_queue_instance(&self, queue: &str) -> InstanceGuard<'_> {
        InstanceGuard(self.instance(queue).lock())
    }

    pub fn queue_exists(&self, queue: &str) -> bool {
        self.queue_instances.contains_key(queue)
    }

    pub fn list_queues(&self) -> Vec<String> {
        let mut queues: Vec<String> = self.queue_instances.keys().cloned().collect();
        queues.sort_unstable();
        queues
    }

    /// Resource usage of every instance as of their last operation. Does not lock the instances.
    pub fn resource_usage(&self) -> ResourceUsage {
        let mut usage = ResourceUsage {
            memory_used_bytes: 0,
            memory_allocated_bytes: 0,
            disk_used_bytes: 0,
        };
        for instance in &self.instances {
            usage.memory_used_bytes += instance.memory_used_bytes.load(Ordering::Relaxed);
            usage.memory_allocated_bytes += instance.memory_allocated_bytes.load(Ordering::Relaxed);
            usage.disk_used_bytes += instance.disk_used_bytes.load(Ordering::Relaxed);
        }
        usage
    }

    pub fn summary(&self) -> mrecordlog::QueuesSummary {
        let mut queues = BTreeMap::new();
        for instance in &self.instances {
            queues.extend(instance.lock().summary().queues);
        }
        mrecordlog::QueuesSummary { queues }
    }

    #[track_caller]
    #[cfg(test)]
    pub fn assert_records_eq<R>(
        &self,
        queue_id: &str,
        range: R,
        expected_records: &[(u64, [u8; 2], &str)],
    ) where
        R: RangeBounds<u64> + 'static,
    {
        let records = self
            .with_range(queue_id, range, |records| {
                records
                    .map(|Record { position, payload }| {
                        let header: [u8; 2] = payload[..2].try_into().unwrap();
                        let payload = String::from_utf8(payload[2..].to_vec()).unwrap();
                        (position, header, payload)
                    })
                    .collect::<Vec<_>>()
            })
            .unwrap();
        assert_eq!(
            records.len(),
            expected_records.len(),
            "expected {} records, got {}",
            expected_records.len(),
            records.len()
        );
        for ((position, header, payload), (expected_position, expected_header, expected_payload)) in
            records.iter().zip(expected_records.iter())
        {
            assert_eq!(
                position, expected_position,
                "expected record at position `{expected_position}`, got `{position}`",
            );
            assert_eq!(
                header, expected_header,
                "expected record header, `{expected_header:?}`, got `{header:?}`",
            );
            assert_eq!(
                payload, expected_payload,
                "expected record payload, `{expected_payload}`, got `{payload}`",
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_multi_instance_wal_recovers_queues() {
        let tempdir = tempfile::tempdir().unwrap();
        let wal_dir_path = tempdir.path().join("wal");
        let policy = PersistPolicy::Always(PersistAction::Flush);
        let queues: Vec<String> = (0..16).map(|idx| format!("queue-{idx}")).collect();
        {
            let mut wal = MultiRecordLogAsync::open_with_prefs(&wal_dir_path, policy.clone(), 4)
                .await
                .unwrap();
            assert_eq!(wal.instances.len(), 4);
            for (idx, queue) in queues.iter().enumerate() {
                wal.create_queue(queue).await.unwrap();
                let payloads = (0..=idx).map(|record_idx| format!("{queue}-{record_idx}"));
                let payloads: Vec<bytes::Bytes> = payloads.map(bytes::Bytes::from).collect();
                wal.append_records_shared(queue, None, payloads.into_iter())
                    .await
                    .unwrap();
            }
            // Queues are spread over the instances.
            let num_used_instances = (0..4)
                .filter(|instance_idx| wal.queue_instances.values().any(|idx| idx == instance_idx))
                .count();
            assert!(num_used_instances > 1);
            wal.truncate(&queues[3], 1).await.unwrap();
            wal.delete_queue(&queues[5]).await.unwrap();
        }
        // Reopened with fewer instances: the queues of the leftover instances are recovered.
        let wal = MultiRecordLogAsync::open_with_prefs(&wal_dir_path, policy, 2)
            .await
            .unwrap();
        assert_eq!(wal.instances.len(), 4);
        let summary = wal.summary();
        assert_eq!(summary.queues.len(), 15);
        for (idx, queue) in queues.iter().enumerate() {
            if idx == 5 {
                assert!(!wal.queue_exists(queue));
                continue;
            }
            assert!(wal.queue_exists(queue));
            let positions: Vec<u64> = wal
                .with_range(queue, .., |records| {
                    records.map(|record| record.position).collect()
                })
                .unwrap();
            let expected: Vec<u64> = if idx == 3 {
                (2..=3).collect()
            } else {
                (0..=idx as u64).collect()
            };
            assert_eq!(positions, expected, "{queue}");
        }
        assert!(wal.resource_usage().memory_used_bytes > 0);
    }
}
