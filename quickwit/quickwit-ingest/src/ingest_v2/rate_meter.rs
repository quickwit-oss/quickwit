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

use std::collections::HashMap;
use std::collections::hash_map::Entry;
use std::sync::{Mutex, MutexGuard};
use std::time::Duration;

use bytesize::ByteSize;
use quickwit_common::ring_buffer::RingBuffer;
use quickwit_common::tower::{ConstantRate, Rate};
use quickwit_proto::ingest::ShardState;
use quickwit_proto::types::{QueueId, ShardId, SourceUid};
use tokio::time::Instant;

use super::shard_readings::{ShardReadingsBySource, ShardThroughputReading};

const SHORT_TERM_WINDOW_LEN: usize = 5;

const LONG_TERM_WINDOW_LEN: usize = 60;

/// The SharedRateMeter is shared by all shards on the ingester. It's used to report shard
/// throughput readings to the control plane, which scales shards up and down accordingly.
///
/// It is a top-level struct on the Ingester. It's kept out of the normal locking path to not
/// contend with other RPCs for lock priority.
///
/// The IngesterShard struct is wired such that writes/deletions also update the rate meter per
/// shard so that explicit calls are not required.
#[derive(Debug, Default)]
pub struct SharedRateMeter {
    entries: Mutex<HashMap<QueueId, ShardRateEntry>>,
}

#[derive(Debug)]
struct ShardRateEntry {
    source_uid: SourceUid,
    shard_id: ShardId,
    shard_state: ShardState,
    is_advertisable: bool,
    rate_meter: RateMeter,
}

impl ShardRateEntry {
    fn sample(&mut self) -> ShardThroughputReading {
        let rates = self.rate_meter.sample();
        ShardThroughputReading {
            shard_id: self.shard_id.clone(),
            shard_state: self.shard_state,
            short_term_ingestion_rate: rates.short_term,
            long_term_ingestion_rate: rates.long_term,
        }
    }
}

impl SharedRateMeter {
    fn lock(&self) -> MutexGuard<'_, HashMap<QueueId, ShardRateEntry>> {
        self.entries
            .lock()
            .expect("shared rate meter lock poisoned")
    }

    fn get_mut<'a>(
        entries: &'a mut HashMap<QueueId, ShardRateEntry>,
        queue_id: &QueueId,
    ) -> &'a mut ShardRateEntry {
        entries
            .get_mut(queue_id)
            .expect("shard rate entry should exist")
    }

    pub(super) fn insert(
        &self,
        queue_id: QueueId,
        source_uid: SourceUid,
        shard_id: ShardId,
        shard_state: ShardState,
        is_advertisable: bool,
    ) {
        let mut entries = self.lock();
        let Entry::Vacant(entry) = entries.entry(queue_id) else {
            panic!("shard rate entry should not already exist");
        };
        entry.insert(ShardRateEntry {
            source_uid,
            shard_id,
            shard_state,
            is_advertisable,
            rate_meter: RateMeter::default(),
        });
    }
    /// Harvest, like the underlying rate meter, resets the current meter to 0 and returns the delta
    /// since the last reading.
    pub fn harvest(&self) -> ShardReadingsBySource {
        let mut entries_guard = self.lock();
        let shard_readings: Vec<_> = entries_guard
            .values_mut()
            .filter(|entry| entry.is_advertisable)
            .map(|entry| {
                let reading = entry.sample();
                (entry.source_uid.clone(), reading)
            })
            .collect();
        drop(entries_guard);
        let mut readings = ShardReadingsBySource::default();
        for (source_uid, shard_reading) in shard_readings {
            readings
                .readings_by_source
                .entry(source_uid)
                .or_default()
                .push(shard_reading);
        }
        readings
    }

    pub(super) fn record_persisted_bytes(&self, queue_id: &QueueId, num_bytes: u64) {
        let mut entries = self.lock();
        let entry = Self::get_mut(&mut entries, queue_id);
        entry.rate_meter.update(num_bytes);
    }

    pub(super) fn close(&self, queue_id: &QueueId) {
        let mut entries = self.lock();
        let entry = Self::get_mut(&mut entries, queue_id);
        entry.shard_state = ShardState::Closed;
    }

    pub(super) fn make_advertisable(&self, queue_id: &QueueId) {
        let mut entries = self.lock();
        let entry = Self::get_mut(&mut entries, queue_id);
        entry.is_advertisable = true;
    }

    pub(super) fn remove(&self, queue_id: &QueueId) {
        self.lock()
            .remove(queue_id)
            .expect("shard rate entry should exist");
    }
}

struct IngestionRates {
    short_term: ByteSize,
    long_term: ByteSize,
}

/// A naive rate meter that tracks how much work was performed during a period of time defined by
/// two successive calls to `harvest`.
#[derive(Debug)]
struct RateMeter {
    total_work: u64,
    harvested_at: Instant,
    short_term_rates: RingBuffer<ByteSize, SHORT_TERM_WINDOW_LEN>,
    long_term_rates: RingBuffer<ByteSize, LONG_TERM_WINDOW_LEN>,
}

impl Default for RateMeter {
    fn default() -> Self {
        Self {
            total_work: 0,
            harvested_at: Instant::now(),
            short_term_rates: RingBuffer::default(),
            long_term_rates: RingBuffer::default(),
        }
    }
}

impl RateMeter {
    /// Increments the amount of work performed since the last call to `harvest`.
    fn update(&mut self, work: u64) {
        self.total_work += work;
    }

    /// Returns the average work rate since the last call to this method and resets the internal
    /// state.
    fn harvest(&mut self) -> ConstantRate {
        let now = Instant::now();
        let elapsed = now.duration_since(self.harvested_at);
        let rate = ConstantRate::new(self.total_work, elapsed);
        self.total_work = 0;
        self.harvested_at = now;
        rate
    }

    fn sample(&mut self) -> IngestionRates {
        let rate = self.harvest();
        let rate_per_sec = rate.rescale(Duration::from_secs(1)).work_bytes();
        self.short_term_rates.push_back(rate_per_sec);
        self.long_term_rates.push_back(rate_per_sec);
        IngestionRates {
            short_term: average_rate(&self.short_term_rates),
            long_term: average_rate(&self.long_term_rates),
        }
    }
}

fn average_rate<const N: usize>(rates: &RingBuffer<ByteSize, N>) -> ByteSize {
    if rates.is_empty() {
        return ByteSize::default();
    }
    ByteSize::b(rates.sum().as_u64() / rates.len() as u64)
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use quickwit_common::tower::Rate;
    use quickwit_proto::types::{IndexUid, queue_id};

    use super::*;

    #[tokio::test(start_paused = true)]
    async fn test_shared_rate_meter() {
        let meter = SharedRateMeter::default();
        let source_uid = SourceUid {
            index_uid: IndexUid::for_test("test-index", 0),
            source_id: "test-source".to_string(),
        };
        let shard_id_01 = ShardId::from(1);
        let shard_id_02 = ShardId::from(2);
        let queue_id_01 = queue_id(&source_uid.index_uid, &source_uid.source_id, &shard_id_01);
        let queue_id_02 = queue_id(&source_uid.index_uid, &source_uid.source_id, &shard_id_02);

        meter.insert(
            queue_id_01.clone(),
            source_uid.clone(),
            shard_id_01.clone(),
            ShardState::Open,
            true,
        );
        meter.insert(
            queue_id_02.clone(),
            source_uid.clone(),
            shard_id_02.clone(),
            ShardState::Open,
            false,
        );
        meter.record_persisted_bytes(&queue_id_01, 100);
        meter.record_persisted_bytes(&queue_id_02, 200);
        tokio::time::advance(Duration::from_secs(1)).await;

        let readings = meter.harvest();
        let source_readings = &readings.readings_by_source[&source_uid];
        assert_eq!(source_readings.len(), 1);
        assert_eq!(source_readings[0].shard_id, shard_id_01);
        assert_eq!(
            source_readings[0].short_term_ingestion_rate,
            ByteSize::b(100)
        );

        meter.make_advertisable(&queue_id_02);
        meter.close(&queue_id_01);
        tokio::time::advance(Duration::from_secs(1)).await;

        let mut readings = meter.harvest();
        let source_readings = readings.readings_by_source.get_mut(&source_uid).unwrap();
        source_readings.sort_by(|left, right| left.shard_id.cmp(&right.shard_id));
        assert_eq!(source_readings.len(), 2);
        assert_eq!(source_readings[0].shard_state, ShardState::Closed);
        assert_eq!(source_readings[1].shard_state, ShardState::Open);
        // Shard 2's 200 bytes span the 2s since it was inserted.
        assert_eq!(
            source_readings[1].short_term_ingestion_rate,
            ByteSize::b(100)
        );

        meter.remove(&queue_id_01);
        let readings = meter.harvest();
        let source_readings = &readings.readings_by_source[&source_uid];
        assert_eq!(source_readings.len(), 1);
        assert_eq!(source_readings[0].shard_id, shard_id_02);
    }

    #[tokio::test]
    async fn test_rate_meter() {
        tokio::time::pause();
        let mut rate_meter = RateMeter::default();

        let rate = rate_meter.harvest();
        assert_eq!(rate.work(), 0);
        assert!(rate.period().is_zero());

        tokio::time::advance(Duration::from_millis(100)).await;

        let rate = rate_meter.harvest();
        assert_eq!(rate.work(), 0);
        assert_eq!(rate.period(), Duration::from_millis(100));

        rate_meter.update(1);
        tokio::time::advance(Duration::from_millis(100)).await;

        let rate = rate_meter.harvest();
        assert_eq!(rate.work(), 1);
        assert_eq!(rate.period(), Duration::from_millis(100));
    }

    #[tokio::test(start_paused = true)]
    async fn test_sample_normalizes_elapsed_time() {
        let mut meter = RateMeter::default();
        meter.update(100);
        tokio::time::advance(Duration::from_millis(250)).await;
        let rates = meter.sample();
        assert_eq!(rates.short_term, ByteSize::b(400));
        assert_eq!(rates.long_term, ByteSize::b(400));

        meter.update(800);
        tokio::time::advance(Duration::from_secs(2)).await;
        let rates = meter.sample();
        assert_eq!(rates.short_term, ByteSize::b(400));
        assert_eq!(rates.long_term, ByteSize::b(400));
    }

    #[tokio::test(start_paused = true)]
    async fn test_sample_short_term_window_expires_before_long_term() {
        let mut meter = RateMeter::default();
        meter.update(600);
        tokio::time::advance(Duration::from_secs(1)).await;
        meter.sample();
        // Five idle samples evict the 600 B/s sample from the short-term window only.
        for _ in 0..4 {
            tokio::time::advance(Duration::from_secs(1)).await;
            meter.sample();
        }
        tokio::time::advance(Duration::from_secs(1)).await;
        let rates = meter.sample();
        assert_eq!(rates.short_term, ByteSize::b(0));
        assert_eq!(rates.long_term, ByteSize::b(600 / 6));
    }
}
