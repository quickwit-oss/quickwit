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

use std::fmt;
use std::sync::Arc;

use futures::{Future, TryFutureExt};
use once_cell::sync::Lazy;
use prometheus::IntGauge;
use tokio::sync::oneshot;
use tracing::error;

use crate::metrics::{GaugeGuard, IntGaugeVec, OwnedGaugeGuard, new_gauge_vec};

/// An executor backed by a thread pool to run CPU-intensive tasks.
///
/// tokio::spawn_blocking should only used for IO-bound tasks, as it has not limit on its
/// thread count.
#[derive(Clone)]
pub struct ThreadPool {
    thread_pool: Arc<rayon::ThreadPool>,
    ongoing_tasks: IntGauge,
    pending_tasks: IntGauge,
}

impl ThreadPool {
    pub fn new(name: &'static str, num_threads_opt: Option<usize>) -> ThreadPool {
        let mut rayon_pool_builder = rayon::ThreadPoolBuilder::new()
            .thread_name(move |thread_id| format!("quickwit-{name}-{thread_id}"))
            .panic_handler(move |_my_panic| {
                error!("task running in the quickwit {name} thread pool panicked");
            });
        if let Some(num_threads) = num_threads_opt {
            rayon_pool_builder = rayon_pool_builder.num_threads(num_threads);
        }
        let thread_pool = rayon_pool_builder
            .build()
            .expect("failed to spawn thread pool");
        let ongoing_tasks = THREAD_POOL_METRICS.ongoing_tasks.with_label_values([name]);
        let pending_tasks = THREAD_POOL_METRICS.pending_tasks.with_label_values([name]);
        ThreadPool {
            thread_pool: Arc::new(thread_pool),
            ongoing_tasks,
            pending_tasks,
        }
    }

    pub fn get_underlying_rayon_thread_pool(&self) -> Arc<rayon::ThreadPool> {
        self.thread_pool.clone()
    }

    /// Function similar to `tokio::spawn_blocking`.
    ///
    /// Here are two important differences however:
    ///
    /// 1) The task runs on a rayon thread pool managed by Quickwit. This pool is specifically used
    ///    only to run CPU-intensive work and is configured to contain `num_cpus` cores.
    ///
    /// 2) Before the task is effectively scheduled, we check that the spawner is still interested
    ///    in its result.
    ///
    /// It is therefore required to `await` the result of this
    /// function to get any work done.
    ///
    /// This is nice because it makes work that has been scheduled
    /// but is not running yet "cancellable".
    pub fn run_cpu_intensive<F, R>(
        &self,
        cpu_intensive_fn: F,
    ) -> impl Future<Output = Result<R, Panicked>>
    where
        F: FnOnce() -> R + Send + 'static,
        R: Send + 'static,
    {
        let span = tracing::Span::current();
        let ongoing_tasks = self.ongoing_tasks.clone();
        let mut pending_tasks_guard: OwnedGaugeGuard =
            OwnedGaugeGuard::from_gauge(self.pending_tasks.clone());
        pending_tasks_guard.add(1i64);
        let (tx, rx) = oneshot::channel();
        self.thread_pool.spawn(move || {
            drop(pending_tasks_guard);
            if tx.is_closed() {
                return;
            }
            let _guard = span.enter();
            let mut ongoing_task_guard = GaugeGuard::from_gauge(&ongoing_tasks);
            ongoing_task_guard.add(1i64);
            let result = cpu_intensive_fn();
            let _ = tx.send(result);
        });
        rx.map_err(|_| Panicked)
    }
}

/// Run a small (<200ms) CPU-intensive task on a dedicated thread pool with a few threads.
///
/// When running blocking io (or side-effects in general), prefer using `tokio::spawn_blocking`
/// instead. When running long tasks or a set of tasks that you expect to take more than 33% of
/// your vCPUs, use a dedicated thread/runtime or executor instead.
///
/// Disclaimer: The function will no be executed if the Future is dropped.
#[must_use = "run_cpu_intensive will not run if the future it returns is dropped"]
pub fn run_cpu_intensive<F, R>(cpu_intensive_fn: F) -> impl Future<Output = Result<R, Panicked>>
where
    F: FnOnce() -> R + Send + 'static,
    R: Send + 'static,
{
    static SMALL_TASK_EXECUTOR: std::sync::OnceLock<ThreadPool> = std::sync::OnceLock::new();
    SMALL_TASK_EXECUTOR
        .get_or_init(|| {
            let num_threads: usize = (crate::num_cpus() / 3).max(2);
            ThreadPool::new("small_tasks", Some(num_threads))
        })
        .run_cpu_intensive(cpu_intensive_fn)
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Panicked;

impl fmt::Display for Panicked {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "scheduled task panicked")
    }
}

impl std::error::Error for Panicked {}

struct ThreadPoolMetrics {
    ongoing_tasks: IntGaugeVec<1>,
    pending_tasks: IntGaugeVec<1>,
}

impl Default for ThreadPoolMetrics {
    fn default() -> Self {
        ThreadPoolMetrics {
            ongoing_tasks: new_gauge_vec(
                "ongoing_tasks",
                "number of tasks being currently processed by threads in the thread pool",
                "thread_pool",
                &[],
                ["pool"],
            ),
            pending_tasks: new_gauge_vec(
                "pending_tasks",
                "number of tasks waiting in the queue before being processed by the thread pool",
                "thread_pool",
                &[],
                ["pool"],
            ),
        }
    }
}

static THREAD_POOL_METRICS: Lazy<ThreadPoolMetrics> = Lazy::new(ThreadPoolMetrics::default);

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::time::Duration;

    use super::*;

    #[tokio::test]
    async fn test_run_cpu_intensive() {
        assert_eq!(run_cpu_intensive(|| 1).await, Ok(1));
    }

    #[tokio::test]
    async fn test_run_cpu_intensive_panicks() {
        assert!(run_cpu_intensive(|| panic!("")).await.is_err());
    }

    #[tokio::test]
    async fn test_run_cpu_intensive_panicks_do_not_shrink_thread_pool() {
        for _ in 0..100 {
            assert!(run_cpu_intensive(|| panic!("")).await.is_err());
        }
    }

    #[tokio::test]
    async fn test_run_cpu_intensive_abort() {
        let counter: Arc<AtomicU64> = Default::default();
        let mut futures = Vec::new();
        for _ in 0..1_000 {
            let counter_clone = counter.clone();
            let fut = run_cpu_intensive(move || {
                std::thread::sleep(Duration::from_millis(5));
                counter_clone.fetch_add(1, Ordering::SeqCst)
            });
            // The first few num_cores tasks should run, but the other should get cancelled.
            futures.push(tokio::time::timeout(Duration::from_millis(1), fut));
        }
        futures::future::join_all(futures).await;
        assert!(counter.load(Ordering::SeqCst) < 100);
    }
}
