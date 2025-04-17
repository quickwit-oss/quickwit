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
use std::ops::RangeInclusive;

use bytesize::ByteSize;
#[cfg(feature = "failpoints")]
use fail::fail_point;
use mrecordlog::error::{AppendError, DeleteQueueError};
use quickwit_proto::ingest::DocBatchV2;
use quickwit_proto::types::{Position, QueueId};

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
pub(super) async fn append_non_empty_doc_batch(
    mrecordlog: &mut MultiRecordLogAsync,
    queue_id: &QueueId,
    doc_batch: DocBatchV2,
    force_commit: bool,
) -> Result<Position, AppendDocBatchError> {
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
        Ok(Some(offset)) => Ok(Position::offset(offset)),
        Ok(None) => panic!("`doc_batch` should not be empty"),
        Err(AppendError::IoError(io_error)) => Err(AppendDocBatchError::Io(io_error)),
        Err(AppendError::MissingQueue(queue_id)) => {
            Err(AppendDocBatchError::QueueNotFound(queue_id))
        }
        Err(AppendError::Past) => {
            panic!("`append_records` should be called with `position_opt: None`")
        }
    }
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

/// Deletes a queue from the WAL. Returns without error if the queue does not exist.
pub async fn force_delete_queue(
    mrecordlog: &mut MultiRecordLogAsync,
    queue_id: &QueueId,
) -> io::Result<()> {
    match mrecordlog.delete_queue(queue_id).await {
        Ok(_) | Err(DeleteQueueError::MissingQueue(_)) => Ok(()),
        Err(DeleteQueueError::IoError(error)) => Err(error),
    }
}

/// Returns the first and last position of the records currently stored in the queue. Returns `None`
/// if the queue does not exist or is empty.
pub(super) fn queue_position_range(
    mrecordlog: &MultiRecordLogAsync,
    queue_id: &QueueId,
) -> Option<RangeInclusive<u64>> {
    let first_position = mrecordlog
        .range(queue_id, ..)
        .ok()?
        .next()
        .map(|record| record.position)?;

    let last_position = mrecordlog
        .last_record(queue_id)
        .ok()?
        .map(|record| record.position)?;

    Some(first_position..=last_position)
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

        let position =
            append_non_empty_doc_batch(&mut mrecordlog, &queue_id, doc_batch.clone(), false)
                .await
                .unwrap();
        assert_eq!(position, Position::offset(0u64));

        let position =
            append_non_empty_doc_batch(&mut mrecordlog, &queue_id, doc_batch.clone(), true)
                .await
                .unwrap();
        assert_eq!(position, Position::offset(2u64));
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
    async fn test_append_queue_position_range() {
        let tempdir = tempfile::tempdir().unwrap();
        let mut mrecordlog = MultiRecordLogAsync::open(tempdir.path()).await.unwrap();

        assert!(queue_position_range(&mrecordlog, &"queue-not-found".to_string()).is_none());

        mrecordlog.create_queue("test-queue").await.unwrap();
        assert!(queue_position_range(&mrecordlog, &"test-queue".to_string()).is_none());

        mrecordlog
            .append_records("test-queue", None, std::iter::once(&b"test-doc-foo"[..]))
            .await
            .unwrap();
        let position_range = queue_position_range(&mrecordlog, &"test-queue".to_string()).unwrap();
        assert_eq!(position_range, 0..=0);

        mrecordlog
            .append_records("test-queue", None, std::iter::once(&b"test-doc-bar"[..]))
            .await
            .unwrap();
        let position_range = queue_position_range(&mrecordlog, &"test-queue".to_string()).unwrap();
        assert_eq!(position_range, 0..=1);

        mrecordlog.truncate("test-queue", 0).await.unwrap();
        let position_range = queue_position_range(&mrecordlog, &"test-queue".to_string()).unwrap();
        assert_eq!(position_range, 1..=1);
    }
}
