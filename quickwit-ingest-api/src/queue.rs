// Copyright (C) 2021 Quickwit, Inc.
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

use std::collections::HashMap;
use std::path::Path;

use quickwit_proto::ingest_api::{DocBatch, FetchResponse, ListQueuesResponse};
use rocksdb::{Direction, IteratorMode, WriteBatch, WriteOptions, DB};
use tracing::warn;

use crate::{add_doc, Position};

const FETCH_PAYLOAD_LIMIT: usize = 2_000_000; // 2MB

pub struct Queues {
    db: DB,
    last_position_per_queue: HashMap<String, Option<Position>>,
}

fn default_rocks_db_options() -> rocksdb::Options {
    // TODO tweak
    let mut options = rocksdb::Options::default();
    options.create_if_missing(true);
    options
}

fn default_rocks_db_write_options() -> rocksdb::WriteOptions {
    let mut write_options = WriteOptions::default();
    write_options.set_sync(false);
    write_options.disable_wal(false);
    write_options
}

fn next_position(db: &DB, queue_id: &str) -> crate::Result<Option<Position>> {
    let cf = db
        .cf_handle(queue_id)
        .ok_or_else(|| crate::IngestApiError::Corruption {
            msg: format!("RocksDB error: Missing column `{queue_id}`"),
        })?;
    // That's iterating backward
    let mut full_it = db.full_iterator_cf(&cf, IteratorMode::End);
    if let Some((key, _)) = full_it.next() {
        let position = Position::try_from(&*key)?;
        Ok(Some(position))
    } else {
        Ok(None)
    }
}

impl Queues {
    pub fn open(db_dir_path: &Path) -> crate::Result<Queues> {
        let options = default_rocks_db_options();
        let queue_ids = if db_dir_path.join("CURRENT").exists() {
            DB::list_cf(&options, db_dir_path)?
        } else {
            Vec::new()
        };
        let db = DB::open_cf(&options, db_dir_path, &queue_ids)?;
        let mut next_position_per_queue = HashMap::default();
        for queue_id in queue_ids {
            let next_position = next_position(&db, &queue_id)?;
            next_position_per_queue.insert(queue_id, next_position);
        }
        Ok(Queues {
            db,
            last_position_per_queue: next_position_per_queue,
        })
    }

    pub fn queue_exists(&self, queue_id: &str) -> bool {
        self.db.cf_handle(queue_id).is_some()
    }

    pub fn create_queue(&mut self, queue_id: &str) -> crate::Result<()> {
        if self.queue_exists(queue_id) {
            return Err(crate::IngestApiError::IndexAlreadyExists {
                index_id: queue_id.to_string(),
            });
        }
        let cf_opts = default_rocks_db_options();
        self.db.create_cf(queue_id, &cf_opts)?;
        self.last_position_per_queue
            .insert(queue_id.to_string(), None);
        Ok(())
    }

    pub fn drop_queue(&mut self, queue_id: &str) -> crate::Result<()> {
        self.db.drop_cf(queue_id)?;
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
    pub fn suggest_truncate(
        &mut self,
        queue_id: &str,
        up_to_offset_included: Position,
    ) -> crate::Result<()> {
        let cf_ref = self.db.cf_handle(queue_id).unwrap(); // FIXME
                                                           // We want to keep the last record.
        let last_position_opt = *self.last_position_per_queue.get(queue_id).ok_or_else(|| {
            crate::IngestApiError::IndexDoesNotExist {
                index_id: queue_id.to_string(),
            }
        })?;

        let last_position = if let Some(last_position) = last_position_opt {
            last_position
        } else {
            warn!("Attempted to truncate an empty queue.");
            return Ok(());
        };

        // We make sure that we keep one record, in order to ensure we do not reset
        // the position counter.
        let truncation_end_offset = if last_position > up_to_offset_included.inc() {
            up_to_offset_included.inc()
        } else {
            last_position
        };

        self.db
            .delete_range_cf(&cf_ref, Position::default(), truncation_end_offset)?;
        Ok(())
    }

    // Append a single record to a target queue.
    #[cfg(test)]
    fn append(&mut self, queue_id: &str, record: &[u8]) -> crate::Result<()> {
        self.append_batch(queue_id, std::iter::once(record))
    }

    // Append a batch of records to a target queue.
    //
    // This operation is atomic: the batch of records is either entirely added or not.
    pub fn append_batch<'a>(
        &mut self,
        queue_id: &str,
        records_it: impl Iterator<Item = &'a [u8]>,
    ) -> crate::Result<()> {
        let column_does_not_exist = || crate::IngestApiError::IndexDoesNotExist {
            index_id: queue_id.to_string(),
        };
        let last_position_opt = self
            .last_position_per_queue
            .get_mut(queue_id)
            .ok_or_else(column_does_not_exist)?;

        let mut next_position = last_position_opt
            .as_ref()
            .map(Position::inc)
            .unwrap_or_default();

        let cf_ref = self
            .db
            .cf_handle(queue_id)
            .ok_or_else(column_does_not_exist)?;

        let mut batch = WriteBatch::default();
        for record in records_it {
            batch.put_cf(&cf_ref, next_position.as_ref(), record);
            *last_position_opt = Some(next_position);
            next_position = next_position.inc();
        }

        let write_options = default_rocks_db_write_options();
        self.db.write_opt(batch, &write_options)?;

        Ok(())
    }

    // Streams messages from in `]after_position, +∞[`.
    //
    // If after_position is set to None, then fetch from the start of the Stream.
    pub fn fetch(
        &self,
        queue_id: &str,
        start_after: Option<Position>,
        num_bytes_limit: Option<usize>,
    ) -> crate::Result<FetchResponse> {
        let cf = self.db.cf_handle(queue_id).ok_or_else(|| {
            crate::IngestApiError::IndexDoesNotExist {
                index_id: queue_id.to_string(),
            }
        })?;

        let start_position = start_after
            .map(|position| position.inc())
            .unwrap_or_default();
        let full_it = self.db.full_iterator_cf(
            &cf,
            IteratorMode::From(start_position.as_ref(), Direction::Forward),
        );
        let mut doc_batch = DocBatch::default();
        let mut num_bytes = 0;
        let mut first_key_opt: Option<u64> = None;
        let size_limit = num_bytes_limit.unwrap_or(FETCH_PAYLOAD_LIMIT);
        for (key, payload) in full_it {
            let position = Position::try_from(&*key)?;
            if first_key_opt.is_none() {
                first_key_opt = Some(position.into());
            }
            num_bytes += add_doc(&*payload, &mut doc_batch);
            if num_bytes > size_limit {
                break;
            }
        }
        Ok(FetchResponse {
            first_position: first_key_opt,
            doc_batch: Some(doc_batch),
        })
    }

    // Streams messages from the start of the Stream.
    pub fn tail(&self, queue_id: &str) -> crate::Result<FetchResponse> {
        let cf = self.db.cf_handle(queue_id).ok_or_else(|| {
            crate::IngestApiError::IndexDoesNotExist {
                index_id: queue_id.to_string(),
            }
        })?;
        let full_it = self.db.full_iterator_cf(&cf, IteratorMode::End);
        let mut doc_batch = DocBatch::default();
        let mut num_bytes = 0;
        let mut first_key_opt: Option<u64> = None;
        for (key, payload) in full_it {
            let position = Position::try_from(&*key)?;
            if first_key_opt.is_none() {
                first_key_opt = Some(position.into());
            }
            num_bytes += add_doc(&*payload, &mut doc_batch);
            if num_bytes > FETCH_PAYLOAD_LIMIT {
                break;
            }
        }
        Ok(FetchResponse {
            first_position: first_key_opt,
            doc_batch: Some(doc_batch),
        })
    }

    pub fn list_queues(&self) -> crate::Result<ListQueuesResponse> {
        Ok(ListQueuesResponse {
            queues: self.last_position_per_queue.keys().cloned().collect(),
        })
    }
}

#[cfg(test)]
mod tests {
    use std::ops::{Deref, DerefMut};

    use super::Queues;
    use crate::errors::IngestApiError;
    use crate::iter_doc_payloads;
    use crate::queue::Position;

    const TEST_QUEUE_ID: &str = "my-queue";
    const TEST_QUEUE_ID2: &str = "my-queue2";

    struct QueuesForTest {
        queues: Option<Queues>,
        tempdir: tempfile::TempDir,
    }

    impl Default for QueuesForTest {
        fn default() -> Self {
            let tempdir = tempfile::tempdir().unwrap();
            let mut queues_for_test = QueuesForTest {
                tempdir,
                queues: None,
            };
            queues_for_test.reload();
            queues_for_test
        }
    }

    impl QueuesForTest {
        fn reload(&mut self) {
            std::mem::drop(self.queues.take());
            self.queues = Some(Queues::open(self.tempdir.path()).unwrap());
        }

        #[track_caller]
        fn fetch_test(
            &mut self,
            queue_id: &str,
            start_after: Option<super::Position>,
            expected_first_pos_opt: Option<u64>,
            expected: &[&[u8]],
        ) {
            let fetch_resp = self.fetch(queue_id, start_after, None).unwrap();
            assert_eq!(fetch_resp.first_position, expected_first_pos_opt);
            let doc_batch = fetch_resp.doc_batch.unwrap();
            let records: Vec<&[u8]> = iter_doc_payloads(&doc_batch).collect();
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

    #[test]
    fn test_access_queue_twice() {
        let mut queues = QueuesForTest::default();
        queues.create_queue(TEST_QUEUE_ID).unwrap();
        let queue_err = queues.create_queue(TEST_QUEUE_ID).err().unwrap();
        assert!(matches!(
            queue_err,
            IngestApiError::IndexAlreadyExists { .. }
        ));
    }

    #[test]
    fn test_simple() {
        let mut queues = QueuesForTest::default();

        queues.create_queue(TEST_QUEUE_ID).unwrap();
        queues
            .append_batch(
                TEST_QUEUE_ID,
                [b"hello", b"happy"].iter().map(|bytes| bytes.as_slice()),
            )
            .unwrap();

        queues.reload();
        queues.fetch_test(
            TEST_QUEUE_ID,
            None,
            Some(0),
            &[&b"hello"[..], &b"happy"[..]],
        );

        queues.reload();
        queues.fetch_test(
            TEST_QUEUE_ID,
            None,
            Some(0),
            &[&b"hello"[..], &b"happy"[..]],
        );
    }

    #[test]
    fn test_distinct_queues() {
        let mut queues = QueuesForTest::default();

        queues.create_queue(TEST_QUEUE_ID).unwrap();
        queues.create_queue(TEST_QUEUE_ID2).unwrap();
        queues.append(TEST_QUEUE_ID, b"hello").unwrap();
        queues.append(TEST_QUEUE_ID2, b"hello2").unwrap();

        queues.fetch_test(TEST_QUEUE_ID, None, Some(0), &[&b"hello"[..]]);
        queues.fetch_test(TEST_QUEUE_ID2, None, Some(0), &[&b"hello2"[..]]);
    }

    #[test]
    fn test_create_reopen() {
        let mut queues = QueuesForTest::default();
        queues.create_queue(TEST_QUEUE_ID).unwrap();

        queues.reload();
        queues.append(TEST_QUEUE_ID, b"hello").unwrap();

        queues.reload();
        queues.append(TEST_QUEUE_ID, b"happy").unwrap();

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
    #[test]
    fn test_truncation() {
        let mut queues = QueuesForTest::default();
        queues.create_queue(TEST_QUEUE_ID).unwrap();
        queues.append(TEST_QUEUE_ID, b"hello").unwrap();
        queues.append(TEST_QUEUE_ID, b"happy").unwrap();
        queues
            .suggest_truncate(TEST_QUEUE_ID, Position::from(0))
            .unwrap();
        queues.fetch_test(TEST_QUEUE_ID, None, Some(1), &[&b"happy"[..]]);
    }

    #[test]
    fn test_truncation_and_reload() {
        // This test makes sure that we don't reset the position counter when we truncate an entire
        // queue.
        let mut queues = QueuesForTest::default();
        queues.create_queue(TEST_QUEUE_ID).unwrap();
        queues.append(TEST_QUEUE_ID, b"hello").unwrap();
        queues.append(TEST_QUEUE_ID, b"happy").unwrap();
        queues.reload();
        queues
            .suggest_truncate(TEST_QUEUE_ID, Position::from(100))
            .unwrap();
        queues.reload();
        queues.append(TEST_QUEUE_ID, b"tax").unwrap();
        queues.fetch_test(
            TEST_QUEUE_ID,
            Some(Position::from(1)),
            Some(2),
            &[&b"tax"[..]],
        );
    }

    struct Record {
        queue_id: String,
        payload: Vec<u8>,
    }
    #[ignore]
    #[test]
    fn test_create_multiple_queue() {
        use std::iter::repeat_with;

        use rand::rngs::StdRng;
        use rand::{Rng, SeedableRng};
        use rand_distr::{Distribution, LogNormal, WeightedIndex};

        const NUM_QUEUES: usize = 100;
        const NUM_RECORDS: usize = 1_000_000;

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
        let mut queues = Queues::open(tmpdir.path()).unwrap();
        for queue_id in 0..NUM_QUEUES {
            println!("create queue {queue_id}");
            queues.create_queue(&queue_id.to_string()).unwrap();
        }
        let start = std::time::Instant::now();
        let mut num_bytes = 0;
        for record in records.iter() {
            queues.append(&record.queue_id, &record.payload).unwrap();
            num_bytes += record.payload.len();
        }
        let elapsed = start.elapsed();
        println!("{elapsed:?}");
        println!("{num_bytes}");
        let throughput = num_bytes as f64 / (elapsed.as_micros() as f64);
        println!("throughput: {throughput}MB/s");
    }
}
