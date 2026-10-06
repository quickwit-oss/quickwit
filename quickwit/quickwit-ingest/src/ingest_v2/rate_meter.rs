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
use std::sync::{Mutex, MutexGuard};
use std::time::Duration;

use bytesize::ByteSize;
use quickwit_common::ring_buffer::RingBuffer;
use quickwit_common::tower::{ConstantRate, Rate};
use quickwit_proto::ingest::ShardState;
use quickwit_proto::types::{ShardId, SourceUid};
use tokio::time::Instant;

use super::broadcast::ShardInfo;
use super::local_shards_utils::ShardThroughputReadings;
use crate::ingest_v2::Entry;

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
    entries: Mutex<HashMap<ShardId, ShardRateEntry>>,
}

#[derive(Debug)]
struct ShardRateEntry {
    source_uid: SourceUid,
    shard_state: ShardState,
    is_advertisable: bool,
    rate_meter: RateMeter,
}

impl ShardRateEntry {
    fn sample(&mut self, shard_id: &ShardId) -> ShardInfo {
        let rates = self.rate_meter.sample();
        ShardInfo {
            shard_id: shard_id.clone(),
            shard_state: self.shard_state,
            short_term_ingestion_rate: rates.short_term,
            long_term_ingestion_rate: rates.long_term,
        }
    }
}

impl SharedRateMeter {
    fn lock(&self) -> MutexGuard<'_, HashMap<ShardId, ShardRateEntry>> {
        self.entries.lock().expect("shard rate meter lock poisoned")
    }

    fn get_mut<'a>(
        entries: &'a mut HashMap<ShardId, ShardRateEntry>,
        shard_id: &ShardId,
    ) -> &'a mut ShardRateEntry {
        entries
            .get_mut(shard_id)
            .expect("shard rate entry should exist")
    }

    pub(super) fn insert(
        &self,
        source_uid: SourceUid,
        shard_id: ShardId,
        shard_state: ShardState,
        is_advertisable: bool,
    ) {
        let mut entries = self.lock();
        let Entry::Vacant(entry) = entries.entry(shard_id) else {
            panic!("shard rate entry should not already exist");
        };
        entry.insert(ShardRateEntry {
            source_uid,
            shard_state,
            is_advertisable,
            rate_meter: RateMeter::default(),
        });
    }
    /// Harvest, like the underlying rate meter, resets the current meter to 0 and returns the delta
    /// since the last reading.
    pub fn harvest(&self) -> ShardThroughputReadings {
        let mut entries_guard = self.lock();
        let shard_infos: Vec<_> = entries_guard
            .iter_mut()
            .filter(|(_, entry)| entry.is_advertisable)
            .map(|(shard_id, entry)| (entry.source_uid.clone(), entry.sample(shard_id)))
            .collect();
        drop(entries_guard);
        let mut readings = ShardThroughputReadings::default();
        for (source_uid, shard_info) in shard_infos {
            readings
                .per_source_shard_infos
                .entry(source_uid)
                .or_default()
                .insert(shard_info);
        }
        readings
    }

    pub(super) fn record_persisted_bytes(&self, shard_id: &ShardId, num_bytes: u64) {
        let mut entries = self.lock();
        let entry = Self::get_mut(&mut entries, shard_id);
        entry.rate_meter.update(num_bytes);
    }

    pub(super) fn close(&self, shard_id: &ShardId) {
        let mut entries = self.lock();
        let entry = Self::get_mut(&mut entries, shard_id);
        entry.shard_state = ShardState::Closed;
    }

    pub(super) fn make_advertisable(&self, shard_id: &ShardId) {
        let mut entries = self.lock();
        let entry = Self::get_mut(&mut entries, shard_id);
        entry.is_advertisable = true;
    }

    pub(super) fn remove(&self, shard_id: &ShardId) {
        self.lock()
            .remove(shard_id)
            .expect("shard rate entry should exist");
    }
}

const SHORT_TERM_WINDOW_LEN: usize = 5;

const LONG_TERM_WINDOW_LEN: usize = 60;

pub(super) struct IngestionRates {
    pub short_term: ByteSize,
    pub long_term: ByteSize,
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
    let sum = rates.iter().map(ByteSize::as_u64).sum::<u64>();
    ByteSize::b(sum / rates.len() as u64)
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use quickwit_common::tower::Rate;

    use super::*;

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

        tokio::time::advance(Duration::from_secs(1)).await;
        let rates = meter.sample();
        assert_eq!(rates.short_term, ByteSize::b(266));
        assert_eq!(rates.long_term, ByteSize::b(266));
    }

    #[tokio::test(start_paused = true)]
    async fn test_sample_expires_short_and_long_term_history() {
        let mut meter = RateMeter::default();
        meter.update(600);
        tokio::time::advance(Duration::from_secs(1)).await;
        let rates = meter.sample();
        assert_eq!(rates.short_term, ByteSize::b(600));
        assert_eq!(rates.long_term, ByteSize::b(600));

        for sample in 2..=61 {
            tokio::time::advance(Duration::from_secs(1)).await;
            let rates = meter.sample();
            let expected_short = if sample <= 5 { 600 / sample } else { 0 };
            let expected_long = if sample <= 60 { 600 / sample } else { 0 };
            assert_eq!(rates.short_term, ByteSize::b(expected_short));
            assert_eq!(rates.long_term, ByteSize::b(expected_long));
        }
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
}
