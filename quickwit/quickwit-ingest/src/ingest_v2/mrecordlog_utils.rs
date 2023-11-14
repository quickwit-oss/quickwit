// Copyright (C) 2023 Quickwit, Inc.
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

use bytesize::ByteSize;
use mrecordlog::error::{AppendError, MissingQueue};
use mrecordlog::MultiRecordLog;
use quickwit_proto::types::{Position, QueueId};
use tracing::warn;

use super::mrecord::is_eof_mrecord;
use crate::MRecord;

/// Appends an EOF record to the queue if it is empty or the last record is not an EOF
/// record.
pub(super) async fn append_eof_record_if_necessary(
    mrecordlog: &mut MultiRecordLog,
    queue_id: &QueueId,
) {
    let should_append_eof_record = match mrecordlog.last_record(queue_id) {
        Ok(Some((_, last_mrecord))) => !is_eof_mrecord(&last_mrecord),
        Ok(None) => true,
        Err(MissingQueue(_)) => {
            warn!("failed to append EOF record to queue `{queue_id}`: queue does not exist");
            return;
        }
    };
    if should_append_eof_record {
        match mrecordlog
            .append_record(queue_id, None, MRecord::Eof.encode())
            .await
        {
            Ok(_) | Err(AppendError::MissingQueue(_)) => {}
            Err(error) => {
                warn!("failed to append EOF record to queue `{queue_id}`: {error}");
            }
        }
    }
}

/// Error returned when the mrecordlog does not have enough capacity to store some records.
#[derive(Debug, Clone, Copy, thiserror::Error)]
pub(super) enum NotEnoughCapacityError {
    #[error(
        "write-ahead log is full, capacity: usage: {usage}, capacity: {capacity}, requested: \
         {requested}"
    )]
    Disk {
        usage: ByteSize,
        capacity: ByteSize,
        requested: ByteSize,
    },
    #[error(
        "write-ahead log memory buffer is full, usage: {usage}, capacity: {capacity}, requested: \
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
) -> Result<(), NotEnoughCapacityError> {
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
    Ok(())
}

/// Returns the position up to which the queue has been truncated (inclusive) taking into account
/// `Eof` records. Returns `None` if the queue does not exist.
pub(super) fn get_truncation_position(
    mrecordlog: &MultiRecordLog,
    queue_id: &QueueId,
) -> Option<Position> {
    let Ok(mut mrecords) = mrecordlog.range(queue_id, ..) else {
        return None;
    };
    let position_opt = if let Some((position, mrecord)) = mrecords.next() {
        if is_eof_mrecord(&mrecord) {
            return Some(Position::Eof);
        }
        position.checked_sub(1)
    } else {
        // The queue exists and is empty. The last position should give us the truncation position.
        mrecordlog
            .last_position(queue_id)
            .expect("queue should exist")
    };
    Some(position_opt.map(Position::from).unwrap_or_default())
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;

    use super::*;

    #[tokio::test]
    async fn test_append_eof_record_if_necessary() {
        let tempdir = tempfile::tempdir().unwrap();
        let mut mrecordlog = MultiRecordLog::open(tempdir.path()).await.unwrap();

        append_eof_record_if_necessary(&mut mrecordlog, &"queue-not-found".to_string()).await;

        mrecordlog.create_queue("test-queue").await.unwrap();
        append_eof_record_if_necessary(&mut mrecordlog, &"test-queue".to_string()).await;

        let (last_position, last_record) = mrecordlog.last_record("test-queue").unwrap().unwrap();
        assert_eq!(last_position, 0);
        assert!(is_eof_mrecord(&last_record));

        append_eof_record_if_necessary(&mut mrecordlog, &"test-queue".to_string()).await;
        let (last_position, last_record) = mrecordlog.last_record("test-queue").unwrap().unwrap();
        assert_eq!(last_position, 0);
        assert!(is_eof_mrecord(&last_record));

        mrecordlog.truncate("test-queue", 0).await.unwrap();

        append_eof_record_if_necessary(&mut mrecordlog, &"test-queue".to_string()).await;
        let (last_position, last_record) = mrecordlog.last_record("test-queue").unwrap().unwrap();
        assert_eq!(last_position, 1);
        assert!(is_eof_mrecord(&last_record));
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
    async fn test_get_truncation_position() {
        let tempdir = tempfile::tempdir().unwrap();
        let mut mrecordlog = MultiRecordLog::open(tempdir.path()).await.unwrap();
        let queue_id = "test-queue".to_string();

        let truncation_position = get_truncation_position(&mrecordlog, &queue_id);
        assert!(truncation_position.is_none());

        mrecordlog.create_queue(&queue_id).await.unwrap();
        let truncation_position = get_truncation_position(&mrecordlog, &queue_id).unwrap();
        assert_eq!(truncation_position, Position::Beginning);

        mrecordlog
            .append_record(&queue_id, None, Bytes::from_static(b"test-record-foo"))
            .await
            .unwrap();
        mrecordlog
            .append_record(&queue_id, None, Bytes::from_static(b"test-record-bar"))
            .await
            .unwrap();
        let truncation_position = get_truncation_position(&mrecordlog, &queue_id).unwrap();
        assert_eq!(truncation_position, Position::Beginning);

        mrecordlog.truncate(&queue_id, 0).await.unwrap();
        let truncation_position = get_truncation_position(&mrecordlog, &queue_id).unwrap();
        assert_eq!(truncation_position, Position::from(0u64));

        mrecordlog.truncate(&queue_id, 1).await.unwrap();
        let truncation_position = get_truncation_position(&mrecordlog, &queue_id).unwrap();
        assert_eq!(truncation_position, Position::from(1u64));

        mrecordlog
            .append_record(&queue_id, None, MRecord::Eof.encode())
            .await
            .unwrap();
        let truncation_position = get_truncation_position(&mrecordlog, &queue_id).unwrap();
        assert_eq!(truncation_position, Position::Eof);
    }
}
