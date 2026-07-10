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

use bytesize::ByteSize;
#[cfg(feature = "failpoints")]
use fail::fail_point;
use mrecordlog::error::AppendError;
use quickwit_proto::ingest::{DocBatchV2, DocCompression};
use quickwit_proto::types::{Position, QueueId};
use tracing::instrument;

use super::mrecord::encode_compressed_doc_v1;
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
        num_bytes = doc_batch.num_bytes(),
        force_commit,
    )
)]
pub(super) async fn append_non_empty_doc_batch(
    mrecordlog: &mut MultiRecordLogAsync,
    queue_id: &QueueId,
    doc_batch: DocBatchV2,
    force_commit: bool,
) -> Result<Position, AppendDocBatchError> {
    #[cfg(feature = "failpoints")]
    fail_point!("ingester:append_records", |_| {
        let io_error = io::Error::from(io::ErrorKind::PermissionDenied);
        Err(AppendDocBatchError::Io(io_error))
    });

    // The document compression is uniform across the batch (stamped by the router). Each document
    // becomes exactly one WAL record and an optional commit record follows, so the per-document
    // positions assigned downstream are the same whether or not the documents are compressed.
    let append_result = match doc_batch.doc_compression() {
        DocCompression::None => {
            // Zero-copy legacy V0 framing. Kept identical to the pre-compression path so that
            // compression stays a zero-cost feature when it is disabled.
            let commit_mrecord = force_commit.then(|| MRecord::Commit.encode());
            let encoded_mrecords = doc_batch
                .into_docs()
                .map(|(_doc_uid, doc)| MRecord::Doc(doc).encode())
                .chain(commit_mrecord);
            mrecordlog
                .append_records(queue_id, None, encoded_mrecords)
                .await
        }
        compression => {
            // The documents are already compressed by the router; frame each in the extensible V1
            // format carrying the codec marker so the reader knows to decompress. The commit is
            // emitted as V1 too to keep the iterator item type uniform (its decoding is unchanged).
            let commit_mrecord = force_commit.then(|| MRecord::Commit.encode_v1());
            let encoded_mrecords = doc_batch
                .into_docs()
                .map(move |(_doc_uid, doc)| encode_compressed_doc_v1(doc, compression))
                .chain(commit_mrecord);
            mrecordlog
                .append_records(queue_id, None, encoded_mrecords)
                .await
        }
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

    #[cfg(not(feature = "failpoints"))]
    #[tokio::test]
    async fn test_append_compressed_doc_batch() {
        use super::super::mrecord::compress_doc_batch;

        let tempdir = tempfile::tempdir().unwrap();
        let mut mrecordlog = MultiRecordLogAsync::open(tempdir.path()).await.unwrap();

        let queue_id = "test-queue".to_string();
        mrecordlog.create_queue(&queue_id).await.unwrap();

        let doc_batch = compress_doc_batch(DocBatchV2::for_test(["test-doc-foo", "test-doc-bar"]));
        assert_eq!(doc_batch.doc_compression(), DocCompression::Zstd);

        // Compression keeps one WAL record per document, so the per-document positions are the same
        // as in the uncompressed case: two docs at offsets 0 and 1, then the commit at offset 2.
        let position = append_non_empty_doc_batch(&mut mrecordlog, &queue_id, doc_batch, true)
            .await
            .unwrap();
        assert_eq!(position, Position::offset(2u64));

        // Reading the WAL back and decoding transparently decompresses each document.
        let decoded_mrecords: Vec<MRecord> = mrecordlog
            .range(&queue_id, ..)
            .unwrap()
            .filter_map(|record| MRecord::decode(&*record.payload))
            .collect();
        assert_eq!(
            decoded_mrecords,
            vec![
                MRecord::new_doc("test-doc-foo"),
                MRecord::new_doc("test-doc-bar"),
                MRecord::Commit,
            ]
        );
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
}
