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

use std::io;
use std::iter::once;
use std::ops::RangeInclusive;

use bytesize::ByteSize;
use fail::fail_point;
use mrecordlog::error::{AppendError, DeleteQueueError};
use mrecordlog::MultiRecordLog;
use quickwit_proto::ingest::DocBatchV2;
use quickwit_proto::types::{Position, QueueId};

use crate::MRecord;

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
    mrecordlog: &mut MultiRecordLog,
    queue_id: &QueueId,
    doc_batch: &DocBatchV2,
    force_commit: bool,
) -> Result<Position, AppendDocBatchError> {
    let append_result = if force_commit {
        let encoded_mrecords = doc_batch
            .docs()
            .map(|doc| MRecord::Doc(doc).encode())
            .chain(once(MRecord::Commit.encode()));
        fail_point!("ingester:append_records", |_| {
            let io_error = io::Error::from(io::ErrorKind::PermissionDenied);
            Err(AppendDocBatchError::Io(io_error))
        });
        mrecordlog
            .append_records(queue_id, None, encoded_mrecords)
            .await
    } else {
        let encoded_mrecords = doc_batch.docs().map(|doc| MRecord::Doc(doc).encode());
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

#[derive(Debug, Clone, Copy)]
pub(super) struct MRecordLogUsage {
    pub disk: ByteSize,
    pub memory: ByteSize,
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
    mrecordlog: &MultiRecordLog,
    disk_capacity: ByteSize,
    memory_capacity: ByteSize,
    requested_capacity: ByteSize,
) -> Result<MRecordLogUsage, NotEnoughCapacityError> {
    let disk_usage = ByteSize(mrecordlog.disk_usage() as u64);

    if disk_usage + requested_capacity > disk_capacity {
        return Err(NotEnoughCapacityError::Disk {
            usage: disk_usage,
            capacity: disk_capacity,
            requested: requested_capacity,
        });
    }
    let memory_usage = ByteSize(mrecordlog.memory_usage() as u64);

    if memory_usage + requested_capacity > memory_capacity {
        return Err(NotEnoughCapacityError::Memory {
            usage: memory_usage,
            capacity: memory_capacity,
            requested: requested_capacity,
        });
    }
    let usage = MRecordLogUsage {
        disk: disk_usage,
        memory: memory_usage,
    };
    Ok(usage)
}

/// Deletes a queue from the WAL. Returns without error if the queue does not exist.
pub async fn force_delete_queue(
    mrecordlog: &mut MultiRecordLog,
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
    mrecordlog: &MultiRecordLog,
    queue_id: &QueueId,
) -> Option<RangeInclusive<u64>> {
    let first_position = mrecordlog
        .range(queue_id, ..)
        .ok()?
        .next()
        .map(|(position, _)| position)?;

    let last_position = mrecordlog
        .last_record(queue_id)
        .ok()?
        .map(|(position, _)| position)?;

    Some(first_position..=last_position)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_append_non_empty_doc_batch() {
        let tempdir = tempfile::tempdir().unwrap();
        let mut mrecordlog = MultiRecordLog::open(tempdir.path()).await.unwrap();

        let queue_id = "test-queue".to_string();
        let doc_batch = DocBatchV2::for_test(["test-doc-foo"]);

        let append_error =
            append_non_empty_doc_batch(&mut mrecordlog, &queue_id, &doc_batch, false)
                .await
                .unwrap_err();

        assert!(matches!(
            append_error,
            AppendDocBatchError::QueueNotFound(..)
        ));

        mrecordlog.create_queue(&queue_id).await.unwrap();

        let position = append_non_empty_doc_batch(&mut mrecordlog, &queue_id, &doc_batch, false)
            .await
            .unwrap();
        assert_eq!(position, Position::offset(0u64));

        let position = append_non_empty_doc_batch(&mut mrecordlog, &queue_id, &doc_batch, true)
            .await
            .unwrap();
        assert_eq!(position, Position::offset(2u64));
    }

    // This test should be run manually and independently of other tests with the `fail/failpoints`
    // feature enabled.
    #[tokio::test]
    #[ignore]
    async fn test_append_non_empty_doc_batch_io_error() {
        let scenario = fail::FailScenario::setup();
        fail::cfg("ingester:append_records", "return").unwrap();

        let tempdir = tempfile::tempdir().unwrap();
        let mut mrecordlog = MultiRecordLog::open(tempdir.path()).await.unwrap();

        let queue_id = "test-queue".to_string();
        mrecordlog.create_queue(&queue_id).await.unwrap();

        let doc_batch = DocBatchV2::for_test(["test-doc-foo"]);
        let append_error =
            append_non_empty_doc_batch(&mut mrecordlog, &queue_id, &doc_batch, false)
                .await
                .unwrap_err();

        assert!(matches!(append_error, AppendDocBatchError::Io(..)));

        scenario.teardown();
    }

    #[tokio::test]
    async fn test_check_enough_capacity() {
        let tempdir = tempfile::tempdir().unwrap();
        let mrecordlog = MultiRecordLog::open(tempdir.path()).await.unwrap();

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
        let mut mrecordlog = MultiRecordLog::open(tempdir.path()).await.unwrap();

        assert!(queue_position_range(&mrecordlog, &"queue-not-found".to_string()).is_none());

        mrecordlog.create_queue("test-queue").await.unwrap();
        assert!(queue_position_range(&mrecordlog, &"test-queue".to_string()).is_none());

        mrecordlog
            .append_record("test-queue", None, &b"test-doc-foo"[..])
            .await
            .unwrap();
        let position_range = queue_position_range(&mrecordlog, &"test-queue".to_string()).unwrap();
        assert_eq!(position_range, 0..=0);

        mrecordlog
            .append_record("test-queue", None, &b"test-doc-bar"[..])
            .await
            .unwrap();
        let position_range = queue_position_range(&mrecordlog, &"test-queue".to_string()).unwrap();
        assert_eq!(position_range, 0..=1);

        mrecordlog.truncate("test-queue", 0).await.unwrap();
        let position_range = queue_position_range(&mrecordlog, &"test-queue".to_string()).unwrap();
        assert_eq!(position_range, 1..=1);
    }
}
