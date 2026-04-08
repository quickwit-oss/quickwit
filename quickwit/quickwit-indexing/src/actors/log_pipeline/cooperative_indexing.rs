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

use std::hash::{DefaultHasher, Hash, Hasher};
use std::sync::Arc;
use std::time::Duration;

use once_cell::sync::Lazy;
use quickwit_proto::indexing::{CpuCapacity, PIPELINE_FULL_CAPACITY, PipelineMetrics};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use tokio::time::Instant;

/// We allow ourselves to adjust the sleep time by at most `NUDGE_TOLERANCE`
/// in order to steer a pipeline to its phase.
const NUDGE_TOLERANCE: Duration = Duration::from_secs(5);

// Origin of time. It is used to compute the phase of the pipeline.
static ORIGIN_OF_TIME: Lazy<Instant> = Lazy::new(Instant::now);

/// Cooperative indexing is a mechanism to deal with a large amount of pipelines.
///
/// Instead of having all pipelines index concurrently, cooperative indexing:
/// - have them take turn, making sure that at most only N pipelines are indexing at the same time.
///   This has the benefit is reducing RAM using (by having a limited number of `IndexWriter` at the
///   same time), reducing context switching.
/// - keeps the different pipelines work uniformously spread in time. If the system is not at
///   capacity, we prefer to have the indexing pipeline as desynchronized as possible to make sure
///   they don't all use the same resources (disk/cpu/network) at the same time.
///
/// It works by:
/// - a semaphore is used to restrict the number of pipelines indexing at the same time.
/// - in the indexer when `on_drain` is called, the indexer will cut a split and "go to sleep" for a
///   given amount of time.
///
/// The key logic is in the computation of that sleep time.
///
/// We want to set it in order to steer the pipeline toward an ideal cycle with a period
/// of `commit_timeout`,
///
/// A period in this ideal cycle should, for some k,
/// - start at `t0 + k * commit_timeout + target_phase`
/// - end at `t0 + (k+1)*commit_timeout + target_phase`.
///
/// `target_phase` is computed using a hash over the pipeline id, and meant to follow
/// a uniform distribution over the interval [0, commit_timeout).
///
/// Each period of this cycle is divided into three phases.
/// - waking [t_wake..t_work_start) acquisition of the period guard (this is instantaneous)
///   acquisition of the semaphore
/// - working [t_work_start..t_work_end)
/// - sleeping [t=t_work_end..t_sleep_end)
///
/// The idea is to first pick the sleep time to to create a cycle of period
/// `commit_timeout`.
///   sleep_time := max(0, commit_timeout - (t_workend - t_wake))
///
/// If the work phase is too long, the regular commit timeout mechanism
/// kicks in an the pipeline will create a split without waiting for the
/// mailbox to be drained.
///
/// We then allow ourselves to tweak the sleep time one way or another by at
/// most two seconds to eventually nudge the system toward the desired phase.
pub(crate) struct CooperativeIndexingCycle {
    target_phase: Duration,
    commit_timeout: Duration,
    indexing_permits: Arc<Semaphore>,
}

impl CooperativeIndexingCycle {
    /// Creates a new cooperative indexing cycle object.
    /// `phase_id` is hashed to compute the target phase.
    pub fn new(
        phase_id: &(impl Hash + ?Sized),
        commit_timeout: Duration,
        indexing_permits: Arc<Semaphore>,
    ) -> CooperativeIndexingCycle {
        assert!(commit_timeout.as_millis() > 0);
        let mut hasher = DefaultHasher::new();
        phase_id.hash(&mut hasher);
        let target_phase_millis: u64 = hasher.finish() % commit_timeout.as_millis() as u64;
        Self::new_with_phase(
            Duration::from_millis(target_phase_millis),
            commit_timeout,
            indexing_permits,
        )
    }

    fn new_with_phase(
        target_phase: Duration,
        commit_timeout: Duration,
        indexing_permits: Arc<Semaphore>,
    ) -> CooperativeIndexingCycle {
        // Force the initial of the origin of time.
        let _t0 = *ORIGIN_OF_TIME;
        CooperativeIndexingCycle {
            target_phase,
            commit_timeout,
            indexing_permits,
        }
    }

    pub fn initial_sleep_duration(&self) -> Duration {
        let t0 = *ORIGIN_OF_TIME;
        let commit_timeout_millis = self.commit_timeout.as_millis() as u64;
        let current_phase_millis: u64 = t0.elapsed().as_millis() as u64 % commit_timeout_millis;
        let target_phase_millis: u64 = self.target_phase.as_millis() as u64 % commit_timeout_millis;
        let initial_sleep_millis: u64 = (commit_timeout_millis + target_phase_millis
            - current_phase_millis)
            % commit_timeout_millis;
        if initial_sleep_millis + 2 * NUDGE_TOLERANCE.as_millis() as u64 > commit_timeout_millis {
            // We are reasonably close to the target phase. No need to sleep. The nudge
            // will be enough.
            return Duration::default();
        }
        Duration::from_millis(initial_sleep_millis)
    }

    pub async fn cooperative_indexing_period(&self) -> CooperativeIndexingPeriod {
        let t_wake = Instant::now();
        let permit = Semaphore::acquire_owned(self.indexing_permits.clone())
            .await
            .unwrap();
        let t_work_start = Instant::now();
        CooperativeIndexingPeriod {
            t_wake,
            t_work_start,
            commit_timeout: self.commit_timeout,
            target_phase: self.target_phase,
            _permit: permit,
        }
    }
}

pub(crate) struct CooperativeIndexingPeriod {
    // measured right before the acquisition of the indexing semaphore
    t_wake: Instant,
    // measured after the acquisition of the semaphore.
    t_work_start: Instant,
    commit_timeout: Duration,
    target_phase: Duration,
    _permit: OwnedSemaphorePermit,
}

impl CooperativeIndexingPeriod {
    fn compute_pipeline_metrics(
        &self,
        end: Instant,
        uncompressed_num_bytes: u64,
    ) -> PipelineMetrics {
        let elapsed = end - self.t_work_start;
        let throughput_mb_per_sec: u64 =
            uncompressed_num_bytes / (1u64 + elapsed.as_micros() as u64);
        let commit_timeout = self.commit_timeout;
        let pipeline_throughput_fraction =
            (elapsed.as_micros() as f32 / commit_timeout.as_micros() as f32).min(1.0f32);
        let cpu_load: CpuCapacity = PIPELINE_FULL_CAPACITY * pipeline_throughput_fraction;
        PipelineMetrics {
            cpu_load,
            throughput_mb_per_sec: throughput_mb_per_sec as u16,
        }
    }

    fn compute_sleep_duration(&self, t_work_end: Instant) -> Duration {
        let commit_timeout_millis = self.commit_timeout.as_millis() as u64;
        let phase_millis: u64 =
            ((t_work_end - *ORIGIN_OF_TIME).as_millis() as u64) % commit_timeout_millis;
        let delta_phase: i64 = phase_millis as i64 - self.target_phase.as_millis() as i64;
        // delta phase is within (-commit_timeout_millis, commit_timeout_millis)
        // We fold it back to [-commit_timeout_millis/2, commit_timeout_millis/2)
        let half_commit_timeout_millis = commit_timeout_millis as i64 / 2;
        let delta_phase = if delta_phase >= half_commit_timeout_millis {
            delta_phase - commit_timeout_millis as i64
        } else if delta_phase < -half_commit_timeout_millis {
            delta_phase + commit_timeout_millis as i64
        } else {
            delta_phase
        };
        let nudge_tolerance_millis = NUDGE_TOLERANCE.as_millis() as i64;
        let nudge_millis: i64 = delta_phase.clamp(-nudge_tolerance_millis, nudge_tolerance_millis);
        let sleep_duration_millis = self.commit_timeout.as_millis() as i64
            - (t_work_end - self.t_wake).as_millis() as i64
            - nudge_millis;
        if sleep_duration_millis > 0 {
            Duration::from_millis(sleep_duration_millis as u64)
        } else {
            Duration::ZERO
        }
    }

    /// This drops the indexing permit, allowing another indexer to start indexing.
    /// This function also returns the amount of time to sleep until the next period.
    pub fn end_of_work(self, uncompressed_num_bytes: u64) -> (Duration, PipelineMetrics) {
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
            panic!("{left:?} and {right:?} are not approximately equal.");
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
    async fn test_initial_sleep_time() {
        tokio::time::pause();
        let t0 = *ORIGIN_OF_TIME;
        for target_phase_secs in [0, 1, 2, 5, 10, 15, 20, 25, 29, 30, 1_000] {
            for start_time_secs in [0, 1, 2, 5, 10, 15, 20, 25, 29, 30] {
                let target_phase = Duration::from_secs(target_phase_secs);
                let semaphore = Arc::new(Semaphore::new(1));
                tokio::time::sleep(Duration::from_secs(start_time_secs)).await;
                let cooperative_indexing = CooperativeIndexingCycle::new_with_phase(
                    target_phase,
                    Duration::from_secs(30),
                    semaphore.clone(),
                );
                let initial_sleep_duration: Duration =
                    cooperative_indexing.initial_sleep_duration();
                tokio::time::sleep(initial_sleep_duration).await;
                let target_phase_millis = cooperative_indexing.target_phase.as_millis() as i64;
                let commit_timeout_ms = cooperative_indexing.commit_timeout.as_millis() as i64;
                let phase_millis =
                    (t0.elapsed().as_millis() as i64 - target_phase_millis) % commit_timeout_ms;
                assert!(phase_millis >= -100, "{phase_millis}");
                assert!(phase_millis <= (NUDGE_TOLERANCE.as_millis() as i64) * 2 + 100);
            }
        }
    }

    #[tokio::test]
    async fn test_cooperative_indexing_simple() {
        tokio::time::pause();
        let semaphore = Arc::new(Semaphore::new(1));
        let cooperative_indexing =
            CooperativeIndexingCycle::new("id", Duration::from_secs(30), semaphore.clone());
        let guard = cooperative_indexing.cooperative_indexing_period().await;
        tokio::time::advance(Duration::from_secs(10)).await;
        let (sleep_time, metrics) = guard.end_of_work(100_000_000);
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
            CooperativeIndexingCycle::new("id", Duration::from_secs(30), semaphore.clone());
        let semaphore_guard = Semaphore::acquire_owned(semaphore).await;
        drop_after(semaphore_guard, Duration::from_secs(30));
        let cycle_guard = cooperative_indexing.cooperative_indexing_period().await;
        tokio::time::advance(Duration::from_secs(15)).await;
        let (sleep_time, metrics) = cycle_guard.end_of_work(30_000_000);
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
            CooperativeIndexingCycle::new("id", Duration::from_secs(30), semaphore.clone());
        let semaphore_guard = Semaphore::acquire_owned(semaphore).await;
        drop_after(semaphore_guard, Duration::from_secs(10));
        let cycle_guard = cooperative_indexing.cooperative_indexing_period().await;
        tokio::time::advance(Duration::from_secs(10)).await;
        let (sleep_time, metrics) = cycle_guard.end_of_work(100_000_000);
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
            let cooperative_indexing = CooperativeIndexingCycle::new_with_phase(
                target_phase,
                commit_timeout,
                semaphore.clone(),
            );
            let join_handle = tokio::task::spawn(async move {
                let mut last_phase = 0;
                for _ in 0..num_steps {
                    let cycle_guard = cooperative_indexing.cooperative_indexing_period().await;
                    let work_time = Duration::from_millis(10);
                    tokio::time::sleep(work_time).await;
                    last_phase =
                        t0.elapsed().as_millis() as u64 % commit_timeout.as_millis() as u64;
                    let (sleep_time, _) = cycle_guard.end_of_work(1_000_000);
                    tokio::time::sleep(sleep_time).await;
                }
                last_phase
            });
            handles.push(join_handle);
        }
        for (i, phase_handle) in handles.into_iter().enumerate() {
            let phase = phase_handle.await.unwrap() as u32;
            let expected_phase_millis: u32 =
                commit_timeout.as_millis() as u32 * i as u32 / num_pipelines as u32;
            assert!(phase.abs_diff(expected_phase_millis) < 3);
        }
    }
}
