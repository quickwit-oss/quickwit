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

use std::ops::Bound;
use std::path::Path;

use bytes::Buf;
use mrecordlog::error::CreateQueueError;
use mrecordlog::{Record, ResourceUsage};
use quickwit_actors::ActorContext;

use crate::mrecordlog_async::MultiRecordLogAsync;
use crate::{
    DocBatchBuilder, FetchResponse, IngestApiService, IngestServiceError, ListQueuesResponse,
};

const FETCH_PAYLOAD_LIMIT: usize = 2_000_000; // 2MB

// TODO do we need to keep this?
const QUICKWIT_CF_PREFIX: &str = ".queue_";

pub struct Queues {
    record_log: MultiRecordLogAsync,
}

impl Queues {
    pub async fn open(queues_dir_path: &Path) -> crate::Result<Queues> {
        tokio::fs::create_dir_all(queues_dir_path)
            .await
            .map_err(|error| {
                IngestServiceError::IoError(format!(
                    "failed to create WAL directory `{}`: {}",
                    queues_dir_path.display(),
                    error
                ))
            })?;
        let record_log = MultiRecordLogAsync::open(queues_dir_path).await?;
        Ok(Queues { record_log })
    }

    pub fn queue_exists(&self, queue_id: &str) -> bool {
        let real_queue_id = format!("{QUICKWIT_CF_PREFIX}{queue_id}");
        self.record_log.queue_exists(&real_queue_id)
    }

    pub async fn create_queue(
        &mut self,
        queue_id: &str,
        ctx: &ActorContext<IngestApiService>,
    ) -> crate::Result<()> {
        if self.queue_exists(queue_id) {
            return Err(crate::IngestServiceError::IndexAlreadyExists {
                index_id: queue_id.to_string(),
            });
        }
        let real_queue_id = format!("{QUICKWIT_CF_PREFIX}{queue_id}");
        ctx.protect_future(self.record_log.create_queue(&real_queue_id))
            .await
            .map_err(|e| match e {
                CreateQueueError::AlreadyExists => IngestServiceError::IndexAlreadyExists {
                    index_id: queue_id.to_owned(),
                },
                CreateQueueError::IoError(ioe) => ioe.into(),
            })?;
        Ok(())
    }

    pub async fn drop_queue(
        &mut self,
        queue_id: &str,
        ctx: &ActorContext<IngestApiService>,
    ) -> crate::Result<()> {
        let real_queue_id = format!("{QUICKWIT_CF_PREFIX}{queue_id}");
        ctx.protect_future(self.record_log.delete_queue(&real_queue_id))
            .await?;
        Ok(())
    }

    /// Suggest to truncate the queue.
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
    pub async fn suggest_truncate(
        &mut self,
        queue_id: &str,
        up_to_offset_included: u64,
        ctx: &ActorContext<IngestApiService>,
    ) -> crate::Result<()> {
        let real_queue_id = format!("{QUICKWIT_CF_PREFIX}{queue_id}");

        ctx.protect_future(
            self.record_log
                .truncate(&real_queue_id, up_to_offset_included),
        )
        .await?;

        Ok(())
    }

    // Append a single record to a target queue.
    #[cfg(test)]
    async fn append(
        &mut self,
        queue_id: &str,
        record: &[u8],
        ctx: &ActorContext<IngestApiService>,
    ) -> crate::Result<Option<u64>> {
        use bytes::Bytes;

        self.append_batch(queue_id, std::iter::once(Bytes::from(record.to_vec())), ctx)
            .await
    }

    // Append a batch of records to a target queue.
    //
    // This operation is atomic: the batch of records is either entirely added or not.
    pub async fn append_batch<'a>(
        &mut self,
        queue_id: &str,
        records_it: impl Iterator<Item = impl Buf> + Send + 'static,
        ctx: &ActorContext<IngestApiService>,
    ) -> crate::Result<Option<u64>> {
        let real_queue_id = format!("{QUICKWIT_CF_PREFIX}{queue_id}");

        // TODO None means we don't have itempotent inserts
        let max_position = ctx
            .protect_future(
                self.record_log
                    .append_records(&real_queue_id, None, records_it),
            )
            .await?;

        Ok(max_position)
    }

    // Streams messages from in `]after_position, +∞[`.
    //
    // If after_position is set to None, then fetch from the start of the Stream.
    pub fn fetch(
        &self,
        queue_id: &str,
        start_after: Option<u64>,
        num_bytes_limit: Option<usize>,
    ) -> crate::Result<FetchResponse> {
        let real_queue_id = format!("{QUICKWIT_CF_PREFIX}{queue_id}");

        let starting_bound = match start_after {
            Some(pos) => Bound::Excluded(pos),
            None => Bound::Unbounded,
        };
        let records = self
            .record_log
            .range(&real_queue_id, (starting_bound, Bound::Unbounded))
            .map_err(|_| crate::IngestServiceError::IndexNotFound {
                // we want to return the queue_id, not the real_queue_id, so we can't just
                // implement From<MissingQueue>
                index_id: queue_id.to_string(),
            })?;

        let size_limit = num_bytes_limit.unwrap_or(FETCH_PAYLOAD_LIMIT);
        let mut doc_batch = DocBatchBuilder::new(queue_id.to_string());
        let mut num_bytes = 0;
        let mut first_key_opt = None;

        for Record { position, payload } in records {
            if first_key_opt.is_none() {
                first_key_opt = Some(position);
            }
            num_bytes += doc_batch.command_from_buf(payload.as_ref());
            if num_bytes > size_limit {
                break;
            }
        }

        Ok(FetchResponse {
            first_position: first_key_opt,
            doc_batch: Some(doc_batch.build()),
        })
    }

    // Streams messages from the start of the Stream.
    pub fn tail(&self, queue_id: &str) -> crate::Result<FetchResponse> {
        self.fetch(queue_id, None, None)
    }

    pub fn list_queues(&self) -> crate::Result<ListQueuesResponse> {
        Ok(ListQueuesResponse {
            queues: self
                .record_log
                .list_queues()
                .flat_map(|real_queue_id| real_queue_id.strip_prefix(QUICKWIT_CF_PREFIX))
                .map(|queue| queue.to_string())
                .collect(),
        })
    }

    pub(crate) fn resource_usage(&self) -> ResourceUsage {
        self.record_log.resource_usage()
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;
    use std::ops::{Deref, DerefMut};

    use bytes::Bytes;
    use quickwit_actors::{ActorContext, Universe};
    use tokio::sync::watch;

    use super::Queues;
    use crate::error::IngestServiceError;
    use crate::IngestApiService;

    const TEST_QUEUE_ID: &str = "my-queue";
    const TEST_QUEUE_ID2: &str = "my-queue2";

    struct QueuesForTest {
        queues: Option<Queues>,
        temp_dir: tempfile::TempDir,
    }

    impl QueuesForTest {
        async fn new() -> (Self, ActorContext<IngestApiService>) {
            let temp_dir = tempfile::tempdir().unwrap();
            let mut queues_for_test = QueuesForTest {
                temp_dir,
                queues: None,
            };
            queues_for_test.reload().await;

            let universe = Universe::with_accelerated_time();
            let (source_mailbox, _source_inbox) = universe.create_test_mailbox();
            let (observable_state_tx, _observable_state_rx) = watch::channel(());
            let ctx = ActorContext::for_test(&universe, source_mailbox, observable_state_tx);
            (queues_for_test, ctx)
        }
    }

    impl QueuesForTest {
        async fn reload(&mut self) {
            std::mem::drop(self.queues.take());
            self.queues = Some(Queues::open(self.temp_dir.path()).await.unwrap());
        }

        #[track_caller]
        fn fetch_test(
            &mut self,
            queue_id: &str,
            start_after: Option<u64>,
            expected_first_pos_opt: Option<u64>,
            expected: &[&[u8]],
        ) {
            let fetch_resp = self.fetch(queue_id, start_after, None).unwrap();
            assert_eq!(fetch_resp.first_position, expected_first_pos_opt);
            let doc_batch = fetch_resp.doc_batch.unwrap();
            let records: Vec<Bytes> = doc_batch.clone().into_iter_raw().collect();
            assert_eq!(&records, expected);
        }
    }

    impl Deref for QueuesForTest {
        type Target = Queues;

        fn deref(&self) -> &Self::Target {
            self.queues.as_ref().unwrap()
        }
    }

    impl DerefMut for QueuesForTest {
        fn deref_mut(&mut self) -> &mut Self::Target {
            self.queues.as_mut().unwrap()
        }
    }

    impl Drop for QueuesForTest {
        fn drop(&mut self) {
            std::mem::drop(self.queues.take().unwrap());
        }
    }

    #[tokio::test]
    async fn test_access_queue_twice() {
        let (mut queues, ctx) = QueuesForTest::new().await;
        queues.create_queue(TEST_QUEUE_ID, &ctx).await.unwrap();
        let queue_err = queues
            .create_queue(TEST_QUEUE_ID, &ctx)
            .await
            .err()
            .unwrap();
        assert!(matches!(
            queue_err,
            IngestServiceError::IndexAlreadyExists { .. }
        ));
    }

    #[tokio::test]
    async fn test_list_queues() {
        let queue_ids = vec!["foo".to_string(), "bar".to_string(), "baz".to_string()];
        let (mut queues, ctx) = QueuesForTest::new().await;
        for queue_id in queue_ids.iter() {
            queues.create_queue(queue_id, &ctx).await.unwrap();
        }
        assert_eq!(
            HashSet::<String>::from_iter(queue_ids),
            HashSet::from_iter(queues.list_queues().unwrap().queues)
        );

        queues.drop_queue("foo", &ctx).await.unwrap();
        assert_eq!(
            HashSet::<String>::from_iter(vec!["bar".to_string(), "baz".to_string()]),
            HashSet::from_iter(queues.list_queues().unwrap().queues)
        );
    }

    #[tokio::test]
    async fn test_simple() {
        let (mut queues, ctx) = QueuesForTest::new().await;

        queues.create_queue(TEST_QUEUE_ID, &ctx).await.unwrap();
        queues
            .append_batch(
                TEST_QUEUE_ID,
                [b"hello", b"happy"].iter().map(|bytes| bytes.as_slice()),
                &ctx,
            )
            .await
            .unwrap();

        queues.reload().await;
        queues.fetch_test(
            TEST_QUEUE_ID,
            None,
            Some(0),
            &[&b"hello"[..], &b"happy"[..]],
        );

        queues.reload().await;
        queues.fetch_test(
            TEST_QUEUE_ID,
            None,
            Some(0),
            &[&b"hello"[..], &b"happy"[..]],
        );
    }

    #[tokio::test]
    async fn test_distinct_queues() {
        let (mut queues, ctx) = QueuesForTest::new().await;

        queues.create_queue(TEST_QUEUE_ID, &ctx).await.unwrap();
        queues.create_queue(TEST_QUEUE_ID2, &ctx).await.unwrap();
        queues.append(TEST_QUEUE_ID, b"hello", &ctx).await.unwrap();
        queues
            .append(TEST_QUEUE_ID2, b"hello2", &ctx)
            .await
            .unwrap();

        queues.fetch_test(TEST_QUEUE_ID, None, Some(0), &[&b"hello"[..]]);
        queues.fetch_test(TEST_QUEUE_ID2, None, Some(0), &[&b"hello2"[..]]);
    }

    #[tokio::test]
    async fn test_create_reopen() {
        let (mut queues, ctx) = QueuesForTest::new().await;
        queues.create_queue(TEST_QUEUE_ID, &ctx).await.unwrap();

        queues.reload().await;
        queues.append(TEST_QUEUE_ID, b"hello", &ctx).await.unwrap();

        queues.reload().await;
        queues.append(TEST_QUEUE_ID, b"happy", &ctx).await.unwrap();

        queues.fetch_test(
            TEST_QUEUE_ID,
            None,
            Some(0),
            &[&b"hello"[..], &b"happy"[..]],
        );
    }

    // Note this test is specific to the current implementation of truncate.
    //
    // The truncate contract is actually not as accurate as what we are testing here.
    #[tokio::test]
    async fn test_truncation() {
        let (mut queues, ctx) = QueuesForTest::new().await;
        queues.create_queue(TEST_QUEUE_ID, &ctx).await.unwrap();
        queues.append(TEST_QUEUE_ID, b"hello", &ctx).await.unwrap();
        queues.append(TEST_QUEUE_ID, b"happy", &ctx).await.unwrap();
        queues
            .suggest_truncate(TEST_QUEUE_ID, 0, &ctx)
            .await
            .unwrap();
        queues.fetch_test(TEST_QUEUE_ID, None, Some(1), &[&b"happy"[..]]);
    }

    #[tokio::test]
    async fn test_truncation_and_reload() {
        // This test makes sure that we don't reset the position counter when we truncate an entire
        // queue.
        let (mut queues, ctx) = QueuesForTest::new().await;
        queues.create_queue(TEST_QUEUE_ID, &ctx).await.unwrap();
        queues.append(TEST_QUEUE_ID, b"hello", &ctx).await.unwrap();
        queues.append(TEST_QUEUE_ID, b"happy", &ctx).await.unwrap();
        queues.reload().await;
        queues
            .suggest_truncate(TEST_QUEUE_ID, 1, &ctx)
            .await
            .unwrap();
        queues.reload().await;
        queues.append(TEST_QUEUE_ID, b"tax", &ctx).await.unwrap();
        queues.fetch_test(TEST_QUEUE_ID, Some(1), Some(2), &[&b"tax"[..]]);
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

        let (_, ctx) = QueuesForTest::new().await;

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
        let mut queues = Queues::open(tmpdir.path()).await.unwrap();
        for queue_id in 0..NUM_QUEUES {
            queues
                .create_queue(&queue_id.to_string(), &ctx)
                .await
                .unwrap();
        }
        let start = std::time::Instant::now();
        let mut num_bytes = 0;
        for record in records.iter() {
            queues
                .append(&record.queue_id, &record.payload, &ctx)
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
