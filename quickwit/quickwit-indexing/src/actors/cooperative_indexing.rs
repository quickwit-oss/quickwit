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

use std::hash::{DefaultHasher, Hasher};
use std::sync::Arc;
use std::time::Duration;

use quickwit_proto::indexing::{CpuCapacity, PipelineMetrics, PIPELINE_FULL_CAPACITY};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use tokio::time::Instant;

const NUDGE_TOLERANCE: Duration = Duration::from_secs(2);

pub struct CooperativeIndexing {
    target_phase: Duration,
    commit_timeout: Duration,
    shared_semaphore: Arc<Semaphore>,
    t0: Instant,
}

impl CooperativeIndexing {
    pub fn new(
        id: impl std::hash::Hash,
        commit_timeout: Duration,
        shared_semaphore: Arc<Semaphore>,
    ) -> CooperativeIndexing {
        assert!(commit_timeout.as_millis() > 0);
        let mut hasher = DefaultHasher::new();
        id.hash(&mut hasher);
        let target_phase_ms: u64 = hasher.finish() % commit_timeout.as_millis() as u64;
        Self::new_with_phase(
            Duration::from_millis(target_phase_ms),
            commit_timeout,
            shared_semaphore,
        )
    }

    fn new_with_phase(
        target_phase: Duration,
        commit_timeout: Duration,
        shared_semaphore: Arc<Semaphore>,
    ) -> CooperativeIndexing {
        CooperativeIndexing {
            target_phase,
            commit_timeout,
            shared_semaphore,
            t0: Instant::now(),
        }
    }

    pub async fn cycle_guard(&self) -> CooperativeIndexingCycleGuard {
        let create_instant = Instant::now();
        let permit = Semaphore::acquire_owned(self.shared_semaphore.clone())
            .await
            .unwrap();
        let start_instant = Instant::now();
        CooperativeIndexingCycleGuard {
            create_instant,
            start_instant,
            commit_timeout: self.commit_timeout,
            target_phase: self.target_phase,
            t0: self.t0,
            _permit: permit,
        }
    }
}

pub struct CooperativeIndexingCycleGuard {
    // As measured before the acquisition of the semaphore.
    create_instant: Instant,
    // As measured after the acquisition of the semaphore.
    start_instant: Instant,
    commit_timeout: Duration,
    target_phase: Duration,
    t0: Instant,
    _permit: OwnedSemaphorePermit,
}

impl CooperativeIndexingCycleGuard {
    fn compute_pipeline_metrics(
        &self,
        end: Instant,
        uncompressed_num_bytes: u64,
    ) -> PipelineMetrics {
        let elapsed = end - self.start_instant;
        let throughput_mb_per_sec: u64 =
            uncompressed_num_bytes / (1u64 + elapsed.as_micros() as u64);
        let commit_timeout = self.commit_timeout;
        let pipeline_throughput_fraction =
            (elapsed.as_micros() as f32 / commit_timeout.as_micros() as f32).min(1.0f32);
        let cpu_millis: CpuCapacity = PIPELINE_FULL_CAPACITY * pipeline_throughput_fraction;
        PipelineMetrics {
            cpu_load: cpu_millis,
            throughput_mb_per_sec: throughput_mb_per_sec as u16,
        }
    }

    fn compute_sleep_duration(&self, end: Instant) -> Duration {
        let commit_timeout_ms = self.commit_timeout.as_millis() as u64;
        let phase_ms: u64 = ((end - self.t0).as_millis() as u64) % commit_timeout_ms;
        let delta_phase: i64 = phase_ms as i64 - self.target_phase.as_millis() as i64;
        // delta phase is within (-commit_timeout_ms, commit_timeout_ms)
        // We fold it back to [-commit_timeout_ms/2, commit_timeout_ms/2)
        let half_commit_timeout_ms = commit_timeout_ms as i64 / 2;
        let delta_phase = if delta_phase >= half_commit_timeout_ms {
            delta_phase - commit_timeout_ms as i64
        } else if delta_phase < -half_commit_timeout_ms {
            delta_phase + commit_timeout_ms as i64
        } else {
            delta_phase
        };
        let nudge_tolerance_ms = NUDGE_TOLERANCE.as_millis() as i64;
        let nudge_ms: i64 = delta_phase.clamp(-nudge_tolerance_ms, nudge_tolerance_ms);
        let elapsed = end - self.create_instant;
        let sleep_duration_ms =
            self.commit_timeout.as_millis() as i64 - nudge_ms - elapsed.as_millis() as i64;
        if sleep_duration_ms > 0 {
            Duration::from_millis(sleep_duration_ms as u64)
        } else {
            Duration::ZERO
        }
    }

    /// This drops the indexing permit, allowing another indexer to start indexing.
    /// This function also returns the amount of time to sleep until the next cycle.
    pub fn end_of_indexing_cycle(self, uncompressed_num_bytes: u64) -> (Duration, PipelineMetrics) {
        let end = Instant::now();
        let sleep_duration = self.compute_sleep_duration(end);
        let metrics = self.compute_pipeline_metrics(end, uncompressed_num_bytes);
        (sleep_duration, metrics)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[track_caller]
    fn assert_approx_equal_sleep_time(left: Duration, right: Duration) {
        let delta = (left.as_millis() as i128 - right.as_millis() as i128).unsigned_abs();
        if delta >= NUDGE_TOLERANCE.mul_f32(1.1).as_millis() {
            panic!("{left:?} and {right:?} are not approximatively equal.");
        }
    }

    #[track_caller]
    fn assert_approx_equal(left: u32, right: u32) {
        assert!(
            left.abs_diff(right) * 10 <= (left + right),
            "inequal values {left} != {right}"
        );
    }

    #[track_caller]
    fn assert_approx_metrics(left_metrics: &PipelineMetrics, right_metrics: &PipelineMetrics) {
        assert_approx_equal(
            left_metrics.throughput_mb_per_sec as u32,
            right_metrics.throughput_mb_per_sec as u32,
        );
        assert_approx_equal(
            left_metrics.cpu_load.cpu_millis(),
            right_metrics.cpu_load.cpu_millis(),
        );
    }

    #[tokio::test]
    async fn test_cooperative_indexing_simple() {
        tokio::time::pause();
        let semaphore = Arc::new(Semaphore::new(1));
        let cooperative_indexing =
            CooperativeIndexing::new("id", Duration::from_secs(30), semaphore.clone());
        let guard = cooperative_indexing.cycle_guard().await;
        tokio::time::advance(Duration::from_secs(10)).await;
        let (sleep_time, metrics) = guard.end_of_indexing_cycle(100_000_000);
        assert_approx_equal_sleep_time(sleep_time, Duration::from_secs(20));
        let expected_metrics = PipelineMetrics {
            cpu_load: CpuCapacity::from_cpu_millis(PIPELINE_FULL_CAPACITY.cpu_millis() * 10 / 30),
            throughput_mb_per_sec: 10u16,
        };
        assert_approx_metrics(&metrics, &expected_metrics)
    }

    fn drop_after<T: Send + 'static>(guard: T, duration: Duration) {
        tokio::task::spawn(async move {
            tokio::time::sleep(duration).await;
            drop(guard);
        });
    }

    #[tokio::test]
    async fn test_cooperative_indexing_maximum_throughput() {
        tokio::time::pause();
        let semaphore = Arc::new(Semaphore::new(1));
        let cooperative_indexing =
            CooperativeIndexing::new("id", Duration::from_secs(30), semaphore.clone());
        let semaphore_guard = Semaphore::acquire_owned(semaphore).await;
        drop_after(semaphore_guard, Duration::from_secs(30));
        let cycle_guard = cooperative_indexing.cycle_guard().await;
        tokio::time::advance(Duration::from_secs(15)).await;
        let (sleep_time, metrics) = cycle_guard.end_of_indexing_cycle(30_000_000);
        let expected_metrics = PipelineMetrics {
            cpu_load: CpuCapacity::from_cpu_millis(PIPELINE_FULL_CAPACITY.cpu_millis() * 15 / 30),
            throughput_mb_per_sec: 1u16,
        };
        assert_approx_metrics(&metrics, &expected_metrics);
        assert!(sleep_time.is_zero());
    }

    #[tokio::test]
    async fn test_cooperative_indexing_simple_contention() {
        tokio::time::pause();
        let semaphore = Arc::new(Semaphore::new(1));
        let cooperative_indexing =
            CooperativeIndexing::new("id", Duration::from_secs(30), semaphore.clone());
        let semaphore_guard = Semaphore::acquire_owned(semaphore).await;
        drop_after(semaphore_guard, Duration::from_secs(10));
        let cycle_guard = cooperative_indexing.cycle_guard().await;
        tokio::time::advance(Duration::from_secs(10)).await;
        let (sleep_time, metrics) = cycle_guard.end_of_indexing_cycle(100_000_000);
        assert_approx_equal_sleep_time(sleep_time, Duration::from_secs(10));
        let expected_metrics = PipelineMetrics {
            cpu_load: CpuCapacity::from_cpu_millis(PIPELINE_FULL_CAPACITY.cpu_millis() * 10 / 30),
            throughput_mb_per_sec: 10u16,
        };
        assert_approx_metrics(&metrics, &expected_metrics);
    }

    #[tokio::test]
    async fn test_cooperative_indexing_nudge_to_phase() {
        tokio::time::pause();
        let num_threads = 10;
        let num_pipelines = 100;
        let num_steps = 15;
        let semaphore = Arc::new(Semaphore::new(num_threads));
        let commit_timeout = Duration::from_secs(30);
        let t0 = Instant::now();
        let mut handles = Vec::new();
        for i in 0..num_pipelines {
            let target_phase =
                Duration::from_millis(commit_timeout.as_millis() as u64 * i / num_pipelines);
            let cooperative_indexing = CooperativeIndexing::new_with_phase(
                target_phase,
                commit_timeout,
                semaphore.clone(),
            );
            let join_handle = tokio::task::spawn(async move {
                let mut last_phase = 0;
                for _ in 0..num_steps {
                    let cycle_guard = cooperative_indexing.cycle_guard().await;
                    let work_time = Duration::from_millis(10);
                    tokio::time::sleep(work_time).await;
                    last_phase =
                        t0.elapsed().as_millis() as u64 % commit_timeout.as_millis() as u64;
                    let (sleep_time, _) = cycle_guard.end_of_indexing_cycle(1_000_000);
                    tokio::time::sleep(sleep_time).await;
                }
                last_phase
            });
            handles.push(join_handle);
        }
        for (i, phase_handle) in handles.into_iter().enumerate() {
            let phase = phase_handle.await.unwrap() as u32;
            let expected_phase_ms: u32 =
                commit_timeout.as_millis() as u32 * i as u32 / num_pipelines as u32;
            assert!(phase.abs_diff(expected_phase_ms) < 3);
        }
    }
}
