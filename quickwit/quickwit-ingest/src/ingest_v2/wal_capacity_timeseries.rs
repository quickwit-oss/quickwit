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

use bytesize::ByteSize;
use quickwit_common::ring_buffer::RingBuffer;

/// The lookback window length is meant to capture readings far enough back in time to give
/// a rough rate of change estimate. At size 6, with broadcast interval of 5 seconds, this would be
/// 30 seconds of readings.
const WAL_CAPACITY_LOOKBACK_WINDOW_LEN: usize = 6;

/// The ring buffer stores one extra element so that `delta()` can compare the newest reading
/// with the one that is exactly `WAL_CAPACITY_LOOKBACK_WINDOW_LEN` steps ago. Otherwise, that
/// reading would be discarded when the next reading is inserted.
const WAL_CAPACITY_READINGS_LEN: usize = WAL_CAPACITY_LOOKBACK_WINDOW_LEN + 1;

pub struct WalDiskCapacityTimeSeries {
    disk_capacity: ByteSize,
    readings: RingBuffer<f64, WAL_CAPACITY_READINGS_LEN>,
}

impl WalDiskCapacityTimeSeries {
    pub fn new(disk_capacity: ByteSize) -> Self {
        #[cfg(not(test))]
        assert!(disk_capacity.as_u64() > 0);
        Self {
            disk_capacity,
            readings: RingBuffer::default(),
        }
    }

    pub(super) fn record(&mut self, disk_used: ByteSize) {
        let remaining = 1.0 - (disk_used.as_u64() as f64 / self.disk_capacity.as_u64() as f64);
        self.readings.push_back(remaining.clamp(0.0, 1.0));
    }

    pub(super) fn current(&self) -> Option<f64> {
        self.readings.last()
    }

    /// How much remaining capacity changed between the oldest and newest readings.
    /// Positive = improving, negative = draining.
    pub(super) fn delta(&self) -> Option<f64> {
        let current = self.readings.last()?;
        let oldest = self.readings.front()?;
        Some(current - oldest)
    }

    pub fn score(&self, disk_used: ByteSize) -> usize {
        let remaining = 1.0 - (disk_used.as_u64() as f64 / self.disk_capacity.as_u64() as f64);
        let delta = self.delta().unwrap_or(0.0);
        compute_capacity_score(remaining, delta)
    }
}

/// Computes a capacity score from 0 to 10 using a PD controller.
///
/// The score has two components:
///
/// - **P (proportional):** How much WAL capacity remains right now. An ingester with 100% free
///   capacity gets `PROPORTIONAL_WEIGHT` points; 50% gets half; and so on. If remaining capacity
///   drops to `MIN_PERMISSIBLE_CAPACITY` or below, the score is immediately 0.
///
/// - **D (derivative):** Up to `DERIVATIVE_WEIGHT` bonus points based on how fast remaining
///   capacity is changing over the lookback window. A higher drain rate is worse, so we invert it:
///   `drain / MAX_DRAIN_RATE` normalizes the drain to a 0–1 penalty, and subtracting from 1
///   converts it into a 0–1 bonus. Multiplied by `DERIVATIVE_WEIGHT`, a stable node gets the full
///   bonus and a node draining at `MAX_DRAIN_RATE` or faster gets nothing.
///
/// Putting it together: a completely idle ingester scores 10 (8 + 2).
/// One that is full but stable scores ~2. One that is draining rapidly scores less.
/// A score of 0 means the ingester is at or below minimum permissible capacity.
///
/// Below this remaining capacity fraction, the score is immediately 0.
const MIN_PERMISSIBLE_CAPACITY: f64 = 0.05;
/// Weight of the proportional term (max points from P).
const PROPORTIONAL_WEIGHT: f64 = 8.0;
/// Weight of the derivative term (max points from D).
const DERIVATIVE_WEIGHT: f64 = 2.0;
/// The drain rate (as a fraction of total capacity over the lookback window) at which the
/// derivative penalty is fully applied. Drain rates beyond this are clamped.
const MAX_DRAIN_RATE: f64 = 0.10;

pub(super) fn compute_capacity_score(remaining_capacity: f64, capacity_delta: f64) -> usize {
    if remaining_capacity <= MIN_PERMISSIBLE_CAPACITY {
        return 0;
    }
    let p = PROPORTIONAL_WEIGHT * remaining_capacity;
    let drain = (-capacity_delta).clamp(0.0, MAX_DRAIN_RATE);
    let d = DERIVATIVE_WEIGHT * (1.0 - drain / MAX_DRAIN_RATE);
    (p + d).clamp(0.0, 10.0) as usize
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ts() -> WalDiskCapacityTimeSeries {
        WalDiskCapacityTimeSeries::new(ByteSize::b(100))
    }

    /// Helper: record a reading with `used` bytes against the series' fixed capacity.
    fn record(series: &mut WalDiskCapacityTimeSeries, used: u64) {
        series.record(ByteSize::b(used));
    }

    #[test]
    fn test_wal_disk_capacity_current_after_record() {
        let mut series = WalDiskCapacityTimeSeries::new(ByteSize::b(256));
        // 192 of 256 used => 25% remaining
        series.record(ByteSize::b(192));
        assert_eq!(series.current(), Some(0.25));

        // 16 of 256 used => 93.75% remaining
        series.record(ByteSize::b(16));
        assert_eq!(series.current(), Some(0.9375));
    }

    #[test]
    fn test_wal_disk_capacity_record_saturates_at_zero() {
        let mut series = ts();
        // 200 used out of 100 capacity => clamped to 0.0
        record(&mut series, 200);
        assert_eq!(series.current(), Some(0.0));
    }

    #[test]
    fn test_wal_disk_capacity_delta_growing() {
        let mut series = ts();
        // oldest: 60 of 100 used => 40% remaining
        record(&mut series, 60);
        // current: 20 of 100 used => 80% remaining
        record(&mut series, 20);
        // delta = 0.80 - 0.40 = 0.40
        assert_eq!(series.delta(), Some(0.40));
    }

    #[test]
    fn test_wal_disk_capacity_delta_shrinking() {
        let mut series = ts();
        // oldest: 20 of 100 used => 80% remaining
        record(&mut series, 20);
        // current: 60 of 100 used => 40% remaining
        record(&mut series, 60);
        // delta = 0.40 - 0.80 = -0.40
        assert_eq!(series.delta(), Some(-0.40));
    }

    #[test]
    fn test_capacity_score_draining_vs_stable() {
        // Node A: capacity draining — usage increases 10, 20, ..., 70 over 7 ticks.
        let mut node_a = ts();
        for used in (10..=70).step_by(10) {
            record(&mut node_a, used);
        }
        let a_remaining = node_a.current().unwrap();
        let a_delta = node_a.delta().unwrap();
        let a_score = compute_capacity_score(a_remaining, a_delta);

        // Node B: steady at 50% usage over 7 ticks.
        let mut node_b = ts();
        for _ in 0..7 {
            record(&mut node_b, 50);
        }
        let b_remaining = node_b.current().unwrap();
        let b_delta = node_b.delta().unwrap();
        let b_score = compute_capacity_score(b_remaining, b_delta);

        // p=2.4, d=0 (max drain) => 2
        assert_eq!(a_score, 2);
        // p=4, d=2 (stable) => 6
        assert_eq!(b_score, 6);
        assert!(b_score > a_score);
    }

    #[test]
    fn test_wal_disk_capacity_delta_spans_lookback_window() {
        let mut series = ts();

        // Fill to exactly the lookback window length (6 readings), all same value.
        for _ in 0..WAL_CAPACITY_LOOKBACK_WINDOW_LEN {
            record(&mut series, 50);
        }
        assert_eq!(series.delta(), Some(0.0));

        // 7th reading fills the ring buffer. Delta spans 6 intervals.
        record(&mut series, 0);
        assert_eq!(series.delta(), Some(0.50));

        // 8th reading evicts the oldest 50-remaining. Delta still spans 6 intervals.
        record(&mut series, 0);
        assert_eq!(series.delta(), Some(0.50));
    }
}
