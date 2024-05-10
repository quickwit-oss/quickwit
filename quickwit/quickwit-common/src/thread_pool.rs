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

use std::fmt;
use std::sync::Arc;

use futures::{Future, TryFutureExt};
use once_cell::sync::Lazy;
use prometheus::IntGauge;
use tokio::sync::oneshot;
use tracing::error;

use crate::metrics::{new_gauge_vec, GaugeGuard, IntGaugeVec};

/// An executor backed by a thread pool to run CPU intensive tasks.
///
/// tokio::spawn_blocking should only used for IO-bound tasks, as it has not limit on its
/// thread count.
#[derive(Clone)]
pub struct ThreadPool {
    thread_pool: Arc<rayon::ThreadPool>,
    active_threads_gauge: IntGauge,
}

impl ThreadPool {
    pub fn new(name: &'static str, num_threads_opt: Option<usize>) -> ThreadPool {
        let mut rayon_pool_builder = rayon::ThreadPoolBuilder::new()
            .thread_name(move |thread_id| format!("quickwit-{name}-{thread_id}"))
            .panic_handler(|_my_panic| {
                error!("task running in the quickwit search pool panicked");
            });
        if let Some(num_threads) = num_threads_opt {
            rayon_pool_builder = rayon_pool_builder.num_threads(num_threads);
        }
        let thread_pool = rayon_pool_builder
            .build()
            .expect("Failed to spawn the spawning pool");
        let active_threads_gauge = ACTIVE_THREAD_COUNT.with_label_values([name]);
        ThreadPool {
            thread_pool: Arc::new(thread_pool),
            active_threads_gauge,
        }
    }

    pub fn get_underlying_rayon_thread_pool(&self) -> Arc<rayon::ThreadPool> {
        self.thread_pool.clone()
    }

    /// Function similar to `tokio::spawn_blocking`.
    ///
    /// Here are two important differences however:
    ///
    /// 1) The task is running on a rayon thread pool managed by quickwit.
    /// This pool is specifically used only to run CPU intensive work
    /// and is configured to contain `num_cpus` cores.
    ///
    /// 2) Before the task is effectively scheduled, we check that
    /// the spawner is still interested by its result.
    ///
    /// It is therefore required to `await` the result of this
    /// function to get anywork done.
    ///
    /// This is nice, because it makes work that has been scheduled
    /// but is not running yet "cancellable".
    pub fn run_cpu_intensive<F, R>(
        &self,
        cpu_heavy_task: F,
    ) -> impl Future<Output = Result<R, Panicked>>
    where
        F: FnOnce() -> R + Send + 'static,
        R: Send + 'static,
    {
        let span = tracing::Span::current();
        let gauge = self.active_threads_gauge.clone();
        let (tx, rx) = oneshot::channel();
        self.thread_pool.spawn(move || {
            if tx.is_closed() {
                return;
            }
            let _guard = span.enter();
            let mut active_thread_guard = GaugeGuard::from_gauge(&gauge);
            active_thread_guard.add(1i64);
            let result = cpu_heavy_task();
            let _ = tx.send(result);
        });
        rx.map_err(|_| Panicked)
    }
}

/// Run a small (<200ms) CPU intensive task on a dedicated thread pool with a few threads.
///
/// When running blocking io (or side-effects in general), prefer use `tokio::spawn_blocking`
/// instead. When running long tasks or a set of tasks that you expect should take more than 33% of
/// your vCPUs, use a dedicated thread/runtime or executor instead.
///
/// Disclaimer: The function will no be executed if the Future is dropped.
#[must_use = "run_cpu_intensive will not run if the future it returns is dropped"]
pub fn run_cpu_intensive<F, R>(cpu_heavy_task: F) -> impl Future<Output = Result<R, Panicked>>
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
        .run_cpu_intensive(cpu_heavy_task)
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Panicked;

impl fmt::Display for Panicked {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "Scheduled job panicked")
    }
}

impl std::error::Error for Panicked {}

pub static ACTIVE_THREAD_COUNT: Lazy<IntGaugeVec<1>> = Lazy::new(|| {
    new_gauge_vec(
        "active_thread_count",
        "Number of active threads in a given thread pool.",
        "thread_pool",
        &[],
        ["pool"],
    )
});

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::sync::Arc;
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
