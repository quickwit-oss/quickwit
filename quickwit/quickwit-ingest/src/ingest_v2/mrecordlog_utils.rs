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

use std::io;
use std::iter::once;

use bytesize::ByteSize;
#[cfg(feature = "failpoints")]
use fail::fail_point;
use mrecordlog::error::AppendError;
use quickwit_proto::ingest::DocBatchV2;
use quickwit_proto::types::{Position, QueueId};
use tracing::instrument;

use super::mrecord::MRECORD_HEADER_LEN;
use crate::MRecord;
use crate::mrecordlog_async::MultiRecordLogAsync;

#[derive(Debug, thiserror::Error)]
pub(super) enum AppendDocBatchError {
    #[error("IO error: {0}")]
    Io(#[from] io::Error),
    #[error("WAL queue `{0}` not found")]
    QueueNotFound(QueueId),
}

/// Appends a non-empty document batch to the WAL queue `queue_id`.
///
/// # Panics
///
/// Panics if `doc_batch` is empty.
#[instrument(
    name = "ingester.append_doc_batch",
    skip_all,
    fields(
        queue_id,
        num_docs = doc_batch.num_docs(),
        num_bytes = doc_batch.doc_buffer.len(),
        force_commit,
    )
)]
pub(super) async fn append_non_empty_doc_batch(
    mrecordlog: &mut MultiRecordLogAsync,
    queue_id: &QueueId,
    doc_batch: DocBatchV2,
    force_commit: bool,
) -> Result<(Position, ByteSize), AppendDocBatchError> {
    let append_result = if force_commit {
        let encoded_mrecords = doc_batch
            .into_docs()
            .map(|(_doc_uid, doc)| MRecord::Doc(doc).encode())
            .chain(once(MRecord::Commit.encode()));

        #[cfg(feature = "failpoints")]
        fail_point!("ingester:append_records", |_| {
            let io_error = io::Error::from(io::ErrorKind::PermissionDenied);
            Err(AppendDocBatchError::Io(io_error))
        });

        mrecordlog
            .append_records(queue_id, None, encoded_mrecords)
            .await
    } else {
        let encoded_mrecords = doc_batch
            .into_docs()
            .map(|(_doc_uid, doc)| MRecord::Doc(doc).encode());

        #[cfg(feature = "failpoints")]
        fail_point!("ingester:append_records", |_| {
            let io_error = io::Error::from(io::ErrorKind::PermissionDenied);
            Err(AppendDocBatchError::Io(io_error))
        });

        mrecordlog
            .append_records(queue_id, None, encoded_mrecords)
            .await
    };
    match append_result {
        Ok(outcome) => {
            let offset = outcome
                .last_position
                .expect("`doc_batch` should not be empty");
            Ok((
                Position::offset(offset),
                ByteSize::b(outcome.queue_size_bytes as u64),
            ))
        }
        Err(AppendError::IoError(io_error)) => Err(AppendDocBatchError::Io(io_error)),
        Err(AppendError::MissingQueue(queue_id)) => {
            Err(AppendDocBatchError::QueueNotFound(queue_id))
        }
        Err(AppendError::Past) => {
            panic!("`append_records` should be called with `position_opt: None`")
        }
    }
}

pub(super) fn doc_batch_size(doc_batch: &DocBatchV2, force_commit: bool) -> ByteSize {
    if doc_batch.is_empty() {
        return ByteSize::b(0);
    }
    let num_records = doc_batch.num_docs() + usize::from(force_commit);
    let num_bytes = doc_batch.doc_buffer.len() + num_records * MRECORD_HEADER_LEN;
    ByteSize::b(num_bytes as u64)
}

/// Error returned when the mrecordlog does not have enough capacity to store some records.
#[derive(Debug, Clone, Copy, thiserror::Error)]
pub(super) enum NotEnoughCapacityError {
    #[error(
        "write-ahead log is full, capacity: {capacity}, usage: {usage}, requested: {requested}"
    )]
    Disk {
        usage: ByteSize,
        capacity: ByteSize,
        requested: ByteSize,
    },
    #[error(
        "write-ahead log memory buffer is full: capacity: {capacity}, usage: {usage}, requested: \
         {requested}"
    )]
    Memory {
        usage: ByteSize,
        capacity: ByteSize,
        requested: ByteSize,
    },
}

/// Checks whether the log has enough capacity to store some records.
pub(super) fn check_enough_capacity(
    mrecordlog: &MultiRecordLogAsync,
    disk_capacity: ByteSize,
    memory_capacity: ByteSize,
    requested_capacity: ByteSize,
) -> Result<(), NotEnoughCapacityError> {
    let wal_usage = mrecordlog.resource_usage();
    let disk_used = ByteSize(wal_usage.disk_used_bytes as u64);

    if disk_used + requested_capacity > disk_capacity {
        return Err(NotEnoughCapacityError::Disk {
            usage: disk_used,
            capacity: disk_capacity,
            requested: requested_capacity,
        });
    }
    let memory_used = ByteSize(wal_usage.memory_used_bytes as u64);

    if memory_used + requested_capacity > memory_capacity {
        return Err(NotEnoughCapacityError::Memory {
            usage: memory_used,
            capacity: memory_capacity,
            requested: requested_capacity,
        });
    }
    Ok(())
}

/// Reports the WAL memory usage, disk usage, and total number of records, or `(0, 0, 0)` if the
/// WAL is not initialized.
pub(super) fn wal_stats(mrecordlog_opt: Option<&MultiRecordLogAsync>) -> (u64, u64, u64) {
    let Some(mrecordlog) = mrecordlog_opt else {
        return (0, 0, 0);
    };
    let wal_resource_usage = mrecordlog.resource_usage();
    let wal_memory_used_bytes = wal_resource_usage.memory_used_bytes as u64;
    let wal_disk_used_bytes = wal_resource_usage.disk_used_bytes as u64;
    let wal_num_records = mrecordlog
        .summary()
        .queues
        .values()
        .map(|queue_summary| {
            queue_summary
                .end
                .map(|end| end.saturating_sub(queue_summary.start) + 1)
                .unwrap_or(0)
        })
        .sum();
    (wal_memory_used_bytes, wal_disk_used_bytes, wal_num_records)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[cfg(not(feature = "failpoints"))]
    #[tokio::test]
    async fn test_append_non_empty_doc_batch() {
        let tempdir = tempfile::tempdir().unwrap();
        let mut mrecordlog = MultiRecordLogAsync::open(tempdir.path()).await.unwrap();

        let queue_id = "test-queue".to_string();
        let doc_batch = DocBatchV2::for_test(["test-doc-foo"]);

        let append_error =
            append_non_empty_doc_batch(&mut mrecordlog, &queue_id, doc_batch.clone(), false)
                .await
                .unwrap_err();

        assert!(matches!(
            append_error,
            AppendDocBatchError::QueueNotFound(..)
        ));

        mrecordlog.create_queue(&queue_id).await.unwrap();

        let (position, queue_size) =
            append_non_empty_doc_batch(&mut mrecordlog, &queue_id, doc_batch.clone(), false)
                .await
                .unwrap();
        assert_eq!(position, Position::offset(0u64));
        assert_eq!(queue_size, ByteSize::b(14));

        let (position, queue_size) =
            append_non_empty_doc_batch(&mut mrecordlog, &queue_id, doc_batch.clone(), true)
                .await
                .unwrap();
        assert_eq!(position, Position::offset(2u64));
        assert_eq!(queue_size, ByteSize::b(30));
    }

    #[test]
    fn test_empty_doc_batch_size() {
        let doc_batch = DocBatchV2::default();
        assert_eq!(doc_batch_size(&doc_batch, false), ByteSize::b(0));
        assert_eq!(doc_batch_size(&doc_batch, true), ByteSize::b(0));
    }

    #[cfg(not(feature = "failpoints"))]
    #[tokio::test]
    async fn test_doc_batch_size_after_repeated_append_and_truncate() {
        let tempdir = tempfile::tempdir().unwrap();
        let mut mrecordlog = MultiRecordLogAsync::open(tempdir.path()).await.unwrap();

        let queue_id = "test-queue".to_string();
        mrecordlog.create_queue(&queue_id).await.unwrap();

        let doc_batch = DocBatchV2::for_test(["test-doc-foo", "test-doc-bar"]);
        let mut retained_bytes = 0;
        let mut next_position = 0u64;
        for iteration in 0..32 {
            let force_commit = iteration % 2 == 0;
            let batch_bytes = if force_commit { 30 } else { 28 };
            assert_eq!(
                doc_batch_size(&doc_batch, force_commit),
                ByteSize::b(batch_bytes)
            );

            let (_, queue_size) = append_non_empty_doc_batch(
                &mut mrecordlog,
                &queue_id,
                doc_batch.clone(),
                force_commit,
            )
            .await
            .unwrap();
            assert_eq!(queue_size.as_u64(), retained_bytes + batch_bytes);

            let outcome = mrecordlog.truncate(&queue_id, next_position).await.unwrap();
            assert_eq!(outcome.evicted_bytes as u64, retained_bytes + 14);
            retained_bytes = if force_commit { 16 } else { 14 };
            assert_eq!(outcome.queue_size_bytes as u64, retained_bytes);
            assert_eq!(
                mrecordlog.summary().queues[&queue_id].num_bytes as u64,
                retained_bytes
            );
            next_position += 2 + u64::from(force_commit);
        }
    }

    // This test should be run manually and independently of other tests with the `failpoints`
    // feature enabled:
    // ```sh
    // cargo test --manifest-path quickwit/Cargo.toml -p quickwit-ingest --features failpoints -- test_append_non_empty_doc_batch_io_error
    // ```
    #[cfg(feature = "failpoints")]
    #[tokio::test]
    async fn test_append_non_empty_doc_batch_io_error() {
        let scenario = fail::FailScenario::setup();
        fail::cfg("ingester:append_records", "return").unwrap();

        let tempdir = tempfile::tempdir().unwrap();
        let mut mrecordlog = MultiRecordLogAsync::open(tempdir.path()).await.unwrap();

        let queue_id = "test-queue".to_string();
        mrecordlog.create_queue(&queue_id).await.unwrap();

        let doc_batch = DocBatchV2::for_test(["test-doc-foo"]);
        let append_error = append_non_empty_doc_batch(&mut mrecordlog, &queue_id, doc_batch, false)
            .await
            .unwrap_err();

        assert!(matches!(append_error, AppendDocBatchError::Io(..)));

        scenario.teardown();
    }

    #[tokio::test]
    async fn test_check_enough_capacity() {
        let tempdir = tempfile::tempdir().unwrap();
        let mrecordlog = MultiRecordLogAsync::open(tempdir.path()).await.unwrap();

        let disk_error =
            check_enough_capacity(&mrecordlog, ByteSize(0), ByteSize(0), ByteSize(12)).unwrap_err();

        assert!(matches!(disk_error, NotEnoughCapacityError::Disk { .. }));

        let memory_error =
            check_enough_capacity(&mrecordlog, ByteSize::mb(256), ByteSize(11), ByteSize(12))
                .unwrap_err();

        assert!(matches!(
            memory_error,
            NotEnoughCapacityError::Memory { .. }
        ));

        check_enough_capacity(&mrecordlog, ByteSize::mb(256), ByteSize(12), ByteSize(12)).unwrap();
    }

    #[tokio::test]
    async fn test_wal_stats() {
        assert_eq!(wal_stats(None), (0, 0, 0));

        let tempdir = tempfile::tempdir().unwrap();
        let mut mrecordlog = MultiRecordLogAsync::open(tempdir.path()).await.unwrap();

        let (wal_memory_used_bytes, _wal_disk_used_bytes, wal_num_records) =
            wal_stats(Some(&mrecordlog));
        assert_eq!(wal_memory_used_bytes, 0);
        assert_eq!(wal_num_records, 0);

        let queue_id = "test-queue".to_string();
        mrecordlog.create_queue(&queue_id).await.unwrap();

        let doc_batch = DocBatchV2::for_test(["test-doc-foo"]);
        append_non_empty_doc_batch(&mut mrecordlog, &queue_id, doc_batch, true)
            .await
            .unwrap();

        let (wal_memory_used_bytes, wal_disk_used_bytes, wal_num_records) =
            wal_stats(Some(&mrecordlog));
        assert!(wal_memory_used_bytes > 0);
        assert!(wal_disk_used_bytes > 0);
        assert!(wal_num_records > 0);
    }
}
