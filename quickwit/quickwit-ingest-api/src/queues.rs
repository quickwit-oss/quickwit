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

use std::ops::Bound;
use std::path::Path;

use mrecordlog::error::CreateQueueError;
use mrecordlog::MultiRecordLog;

use crate::{add_doc, DocBatch, FetchResponse, IngestServiceError, ListQueuesResponse};

const FETCH_PAYLOAD_LIMIT: usize = 2_000_000; // 2MB

/// Wraps a [`MultiRecordLog`] with an async [`RwLock`] and provides a concurrent API.
pub struct Queues {
    record_log: MultiRecordLog,
}

impl Queues {
    pub(crate) async fn open(queues_dir_path: &Path) -> crate::Result<Self> {
        tokio::fs::create_dir_all(queues_dir_path).await?;
        let record_log = MultiRecordLog::open(queues_dir_path).await?;
        Ok(Self { record_log })
    }

    pub(crate) async fn create_queue(&mut self, queue_id: &str) -> crate::Result<()> {
        self.record_log
            .create_queue(queue_id)
            .await
            .map_err(|error| match error {
                CreateQueueError::AlreadyExists => IngestServiceError::IndexAlreadyExists {
                    index_id: queue_id.to_string(),
                },
                CreateQueueError::IoError(io_error) => io_error.into(),
            })?;
        Ok(())
    }

    pub(crate) async fn create_queue_if_not_exists(&mut self, queue_id: &str) -> crate::Result<()> {
        match self.record_log.create_queue(queue_id).await {
            Ok(_) => Ok(()),
            Err(CreateQueueError::AlreadyExists) => Ok(()),
            Err(_error) => Err(IngestServiceError::IndexAlreadyExists {
                index_id: queue_id.to_string(),
            }),
        }
    }

    pub(crate) async fn queue_exists(&self, queue_id: &str) -> bool {
        self.record_log.queue_exists(queue_id)
    }

    pub(crate) async fn list_queues(&self) -> crate::Result<ListQueuesResponse> {
        let queues = self
            .record_log
            .list_queues()
            .map(|queue| queue.to_string())
            .collect();
        Ok(ListQueuesResponse { queues })
    }

    pub(crate) async fn drop_queue(&mut self, queue_id: &str) -> crate::Result<()> {
        self.record_log.delete_queue(queue_id).await?;
        Ok(())
    }

    /// Suggests to truncate the queue.
    ///
    /// This function allows the queue to remove all records up to and
    /// including `up_to_offset_included`.
    ///
    /// The role of this truncation is to release memory and disk space.
    ///
    /// There are no guarantees that the record will effectively be removed.
    /// Nothing might happen, or the truncation might be partial.
    ///
    /// In other words, truncating from a position, and fetching records starting
    /// earlier than this position can yield undefined result:
    /// the truncated records may or may not be returned.
    pub(crate) async fn suggest_truncate(
        &mut self,
        queue_id: &str,
        position: Bound<u64>,
    ) -> crate::Result<()> {
        let Bound::Included(position) = position else {
            return Err(IngestServiceError::InvalidPosition("Position is not included".to_string()));
        };
        self.record_log.truncate(queue_id, position).await?;
        Ok(())
    }

    // Append a single record to a target queue.
    #[cfg(test)]
    pub(crate) async fn append(&mut self, queue_id: &str, record: &[u8]) -> crate::Result<()> {
        self.append_batch(queue_id, std::iter::once(record)).await
    }

    // Appends a batch of records to a target queue.
    //
    // This operation is atomic: the batch of records is either entirely added or not.
    pub(crate) async fn append_batch<'a>(
        &mut self,
        queue_id: &str,
        records: impl Iterator<Item = &'a [u8]>,
    ) -> crate::Result<()> {
        self.record_log
            .append_records(queue_id, None, records)
            .await?;
        Ok(())
    }

    // Streams records from in `]after_position, +∞[`.
    //
    // If the `start_after` position is set to None, then fetches from the start of the queue.
    pub(crate) async fn fetch(
        &self,
        queue_id: &str,
        start_after: Option<u64>,
        num_bytes_limit: Option<usize>,
    ) -> crate::Result<FetchResponse> {
        let starting_bound = match start_after {
            Some(position) => Bound::Excluded(position),
            None => Bound::Unbounded,
        };
        let records = self
            .record_log
            .range(queue_id, (starting_bound, Bound::Unbounded))
            .ok_or_else(|| crate::IngestServiceError::IndexNotFound {
                index_id: queue_id.to_string(),
            })?;
        let mut doc_batch = DocBatch {
            index_id: queue_id.to_string(),
            ..Default::default()
        };
        let mut num_bytes = 0;
        let num_bytes_limit = num_bytes_limit.unwrap_or(FETCH_PAYLOAD_LIMIT);
        let mut first_key_opt = None;

        for (pos, record) in records {
            if first_key_opt.is_none() {
                first_key_opt = Some(pos);
            }
            num_bytes += add_doc(&record, &mut doc_batch);

            if num_bytes > num_bytes_limit {
                break;
            }
        }
        Ok(FetchResponse {
            first_position: first_key_opt,
            doc_batch: Some(doc_batch),
        })
    }

    // Streams records from the start of the queue.
    pub(crate) async fn tail(&self, queue_id: &str) -> crate::Result<FetchResponse> {
        self.fetch(queue_id, None, None).await
    }

    /// Returns the resource used by the queue.
    ///
    /// Returns the in-memory size, and the on disk size of the queue.
    pub(crate) async fn resource_usage(&self) -> (usize, usize) {
        (
            self.record_log.in_memory_size(),
            self.record_log.on_disk_size(),
        )
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;
    use std::ops::{Deref, DerefMut};

    use super::Queues;
    use crate::errors::IngestServiceError;
    use crate::iter_doc_payloads;

    const TEST_QUEUE_ID: &str = "my-queue";
    const TEST_QUEUE_ID2: &str = "my-queue2";

    struct LogForTest {
        queues: Option<Queues>,
        tempdir: tempfile::TempDir,
    }

    impl LogForTest {
        async fn new() -> Self {
            let tempdir = tempfile::tempdir().unwrap();
            let mut queues_for_test = LogForTest {
                tempdir,
                queues: None,
            };
            queues_for_test.reload().await;
            queues_for_test
        }
    }

    impl LogForTest {
        async fn reload(&mut self) {
            std::mem::drop(self.queues.take());
            self.queues = Some(Queues::open(self.tempdir.path()).await.unwrap());
        }

        async fn fetch_test(
            &self,
            queue_id: &str,
            start_after: Option<u64>,
            expected_first_pos_opt: Option<u64>,
            expected: &[&[u8]],
        ) {
            let fetch_resp = self.fetch(queue_id, start_after, None).await.unwrap();
            assert_eq!(fetch_resp.first_position, expected_first_pos_opt);
            let doc_batch = fetch_resp.doc_batch.unwrap();
            let records: Vec<&[u8]> = iter_doc_payloads(&doc_batch).collect();
            assert_eq!(&records, expected);
        }
    }

    impl Deref for LogForTest {
        type Target = Queues;

        fn deref(&self) -> &Self::Target {
            self.queues.as_ref().unwrap()
        }
    }

    impl DerefMut for LogForTest {
        fn deref_mut(&mut self) -> &mut Self::Target {
            self.queues.as_mut().unwrap()
        }
    }

    impl Drop for LogForTest {
        fn drop(&mut self) {
            std::mem::drop(self.queues.take().unwrap());
        }
    }

    #[tokio::test]
    async fn test_access_queue_twice() {
        let queues = LogForTest::new().await;
        queues.create_queue(TEST_QUEUE_ID).await.unwrap();
        let queue_err = queues.create_queue(TEST_QUEUE_ID).await.err().unwrap();
        assert!(matches!(
            queue_err,
            IngestServiceError::IndexAlreadyExists { .. }
        ));
    }

    #[tokio::test]
    async fn test_list_queues() {
        let queue_ids = vec!["foo".to_string(), "bar".to_string(), "baz".to_string()];
        let queues = LogForTest::new().await;
        for queue_id in queue_ids.iter() {
            queues.create_queue(queue_id).await.unwrap();
        }
        assert_eq!(
            HashSet::<String>::from_iter(queue_ids),
            HashSet::from_iter(queues.list_queues().await.unwrap().queues)
        );

        queues.drop_queue("foo").await.unwrap();
        assert_eq!(
            HashSet::<String>::from_iter(vec!["bar".to_string(), "baz".to_string()]),
            HashSet::from_iter(queues.list_queues().await.unwrap().queues)
        );
    }

    #[tokio::test]
    async fn test_simple() {
        let mut queues = LogForTest::new().await;

        queues.create_queue(TEST_QUEUE_ID).await.unwrap();
        queues
            .append_batch(
                TEST_QUEUE_ID,
                [b"hello", b"happy"].iter().map(|bytes| bytes.as_slice()),
            )
            .await
            .unwrap();

        queues.reload().await;
        queues
            .fetch_test(
                TEST_QUEUE_ID,
                None,
                Some(0),
                &[&b"hello"[..], &b"happy"[..]],
            )
            .await;

        queues.reload().await;
        queues
            .fetch_test(
                TEST_QUEUE_ID,
                None,
                Some(0),
                &[&b"hello"[..], &b"happy"[..]],
            )
            .await;
    }

    #[tokio::test]
    async fn test_distinct_queues() {
        let queues = LogForTest::new().await;

        queues.create_queue(TEST_QUEUE_ID).await.unwrap();
        queues.create_queue(TEST_QUEUE_ID2).await.unwrap();
        queues.append(TEST_QUEUE_ID, b"hello").await.unwrap();
        queues.append(TEST_QUEUE_ID2, b"hello2").await.unwrap();

        queues
            .fetch_test(TEST_QUEUE_ID, None, Some(0), &[&b"hello"[..]])
            .await;
        queues
            .fetch_test(TEST_QUEUE_ID2, None, Some(0), &[&b"hello2"[..]])
            .await;
    }

    #[tokio::test]
    async fn test_create_reopen() {
        let mut queues = LogForTest::new().await;
        queues.create_queue(TEST_QUEUE_ID).await.unwrap();

        queues.reload().await;
        queues.append(TEST_QUEUE_ID, b"hello").await.unwrap();

        queues.reload().await;
        queues.append(TEST_QUEUE_ID, b"happy").await.unwrap();

        queues
            .fetch_test(
                TEST_QUEUE_ID,
                None,
                Some(0),
                &[&b"hello"[..], &b"happy"[..]],
            )
            .await;
    }

    // Note this test is specific to the current implementation of truncate.
    //
    // The truncate contract is actually not as accurate as what we are testing here.
    #[tokio::test]
    async fn test_truncation() {
        let queues = LogForTest::new().await;
        queues.create_queue(TEST_QUEUE_ID).await.unwrap();
        queues.append(TEST_QUEUE_ID, b"hello").await.unwrap();
        queues.append(TEST_QUEUE_ID, b"happy").await.unwrap();
        queues.suggest_truncate(TEST_QUEUE_ID, 0).await.unwrap();
        queues
            .fetch_test(TEST_QUEUE_ID, None, Some(1), &[&b"happy"[..]])
            .await;
    }

    #[tokio::test]
    async fn test_truncation_and_reload() {
        // This test ensures we don't reset the position counter when we truncate an entire
        // queue.
        let mut queues = LogForTest::new().await;
        queues.create_queue(TEST_QUEUE_ID).await.unwrap();
        queues.append(TEST_QUEUE_ID, b"hello").await.unwrap();
        queues.append(TEST_QUEUE_ID, b"happy").await.unwrap();
        queues.reload().await;
        queues.suggest_truncate(TEST_QUEUE_ID, 1).await.unwrap();
        queues.reload().await;
        queues.append(TEST_QUEUE_ID, b"tax").await.unwrap();
        queues
            .fetch_test(TEST_QUEUE_ID, Some(1), Some(2), &[&b"tax"[..]])
            .await;
    }

    struct Record {
        queue_id: String,
        payload: Vec<u8>,
    }

    #[ignore]
    #[tokio::test]
    async fn test_create_multiple_queue() {
        use std::iter::repeat_with;

        use rand::rngs::StdRng;
        use rand::{Rng, SeedableRng};
        use rand_distr::{Distribution, LogNormal, WeightedIndex};

        const NUM_QUEUES: usize = 100;
        const NUM_RECORDS: usize = 1_000_000;

        LogForTest::new().await;

        // mean 2, standard deviation 3
        let log_normal = LogNormal::new(10.0f32, 3.0f32).unwrap();
        let mut rng = StdRng::seed_from_u64(4u64);
        let queue_weights: Vec<f32> = repeat_with(|| log_normal.sample(&mut rng))
            .take(NUM_QUEUES)
            .collect();

        let dist = WeightedIndex::new(&queue_weights).unwrap();
        let record_queue_ids: Vec<usize> = repeat_with(|| dist.sample(&mut rng))
            .take(NUM_RECORDS)
            .collect();

        let records: Vec<Record> = record_queue_ids
            .into_iter()
            .map(|queue_id| {
                let num_bytes: usize = rng.gen_range(80..800);
                let payload: Vec<u8> = repeat_with(rand::random::<u8>).take(num_bytes).collect();
                Record {
                    queue_id: queue_id.to_string(),
                    payload,
                }
            })
            .collect();

        let tmpdir = tempfile::tempdir_in(".").unwrap();
        let queues = Queues::open(tmpdir.path()).await.unwrap();
        for queue_id in 0..NUM_QUEUES {
            queues.create_queue(&queue_id.to_string()).await.unwrap();
        }
        let start = std::time::Instant::now();
        let mut num_bytes = 0;
        for record in records.iter() {
            queues
                .append(&record.queue_id, &record.payload)
                .await
                .unwrap();
            num_bytes += record.payload.len();
        }
        let elapsed = start.elapsed();
        println!("{elapsed:?}");
        println!("{num_bytes}");
        let throughput = num_bytes as f64 / (elapsed.as_micros() as f64);
        println!("Throughput: {throughput}");
    }
}
