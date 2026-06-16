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

use std::collections::VecDeque;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use futures::{Future, TryFutureExt};
use quickwit_metrics::{Gauge, GaugeGuard, LazyGauge, gauge, labels, lazy_gauge};
use tokio::sync::oneshot;
use tracing::error;

use super::Panicked;

static THREAD_POOL_ONGOING_TASKS: LazyGauge = lazy_gauge!(
    name: "ongoing_tasks",
    description: "number of tasks being currently processed by threads in the thread pool",
    subsystem: "thread_pool",
);

static THREAD_POOL_PENDING_TASKS: LazyGauge = lazy_gauge!(
    name: "pending_tasks",
    description: "number of tasks waiting in the queue before being processed by the thread pool",
    subsystem: "thread_pool",
);

/// An executor backed by a thread pool to run CPU-intensive tasks.
///
/// tokio::spawn_blocking should only used for IO-bound tasks, as it has not limit on its
/// thread count.
#[derive(Clone)]
pub struct ThreadPoolWithPriority {
    inner: Arc<ThreadPoolInner>,
}

struct ThreadPoolInner {
    thread_pool: Arc<rayon::ThreadPool>,
    max_running_tasks: usize,
    num_running_tasks: AtomicUsize,
    state: Mutex<State>,
    ongoing_tasks: Gauge,
    pending_tasks: Gauge,
}

struct State {
    high_priority_tasks: VecDeque<Box<dyn RunnableTask>>,
    normal_priority_tasks: VecDeque<Box<dyn RunnableTask>>,
}

impl State {
    fn pop_next_task(&mut self) -> Option<Box<dyn RunnableTask>> {
        self.high_priority_tasks
            .pop_front()
            .or_else(|| self.normal_priority_tasks.pop_front())
    }
}

struct QueuedTask<F, R> {
    cpu_intensive_fn: F,
    tx: oneshot::Sender<R>,
    span: tracing::Span,
    pending_tasks_guard: GaugeGuard,
}

trait RunnableTask: Send {
    fn is_cancelled(&self) -> bool;

    fn run(self: Box<Self>, ongoing_tasks: Gauge);
}

impl<F, R> RunnableTask for QueuedTask<F, R>
where
    F: FnOnce() -> R + Send + 'static,
    R: Send + 'static,
{
    fn is_cancelled(&self) -> bool {
        self.tx.is_closed()
    }

    fn run(self: Box<Self>, ongoing_tasks: Gauge) {
        let QueuedTask {
            cpu_intensive_fn,
            tx,
            span,
            pending_tasks_guard,
        } = *self;
        drop(pending_tasks_guard);
        if tx.is_closed() {
            return;
        }
        let _guard = span.enter();
        let _ongoing_task_guard = GaugeGuard::new(&ongoing_tasks, 1.0);
        let result = cpu_intensive_fn();
        let _ = tx.send(result);
    }
}

/// A claimed execution slot in the thread pool.
///
/// Dropping this permit decrements the running-task count and re-triggers scheduling so the
/// next queued task can fill the freed slot.
struct Permit(Arc<ThreadPoolInner>);

impl Permit {
    fn new(thread_pool_inner: Arc<ThreadPoolInner>) -> Self {
        thread_pool_inner
            .num_running_tasks
            .fetch_add(1, Ordering::AcqRel);
        Self(thread_pool_inner)
    }
}

impl Drop for Permit {
    fn drop(&mut self) {
        let prev = self.0.num_running_tasks.fetch_sub(1, Ordering::AcqRel);
        debug_assert!(prev > 0, "dropped more permits than were acquired");
        ThreadPoolInner::schedule(&self.0);
    }
}

/// The priority of a task submitted to a [`ThreadPoolWithPriority`].
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Priority {
    /// The default priority.
    Normal,
    /// A high-priority task is scheduled before normal-priority tasks that are still pending.
    High,
}

impl ThreadPoolWithPriority {
    pub fn new(name: &'static str, num_threads_opt: Option<usize>) -> ThreadPoolWithPriority {
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
        let max_running_tasks = thread_pool.current_num_threads();
        let labels = labels!("pool" => name);
        let ongoing_tasks = gauge!(parent: THREAD_POOL_ONGOING_TASKS, labels: [labels]);
        let pending_tasks = gauge!(parent: THREAD_POOL_PENDING_TASKS, labels: [labels]);
        ThreadPoolWithPriority {
            inner: Arc::new(ThreadPoolInner {
                thread_pool: Arc::new(thread_pool),
                max_running_tasks,
                num_running_tasks: AtomicUsize::new(0),
                state: Mutex::new(State {
                    high_priority_tasks: VecDeque::new(),
                    normal_priority_tasks: VecDeque::new(),
                }),
                ongoing_tasks,
                pending_tasks,
            }),
        }
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
        self.run_cpu_intensive_with_priority(Priority::Normal, cpu_intensive_fn)
    }

    /// Function similar to [`Self::run_cpu_intensive`], but with explicit priority.
    pub fn run_cpu_intensive_with_priority<F, R>(
        &self,
        priority: Priority,
        cpu_intensive_fn: F,
    ) -> impl Future<Output = Result<R, Panicked>>
    where
        F: FnOnce() -> R + Send + 'static,
        R: Send + 'static,
    {
        let span = tracing::Span::current();
        let pending_tasks_guard = GaugeGuard::new(&self.inner.pending_tasks, 1.0);
        let (tx, rx) = oneshot::channel();
        let task = QueuedTask {
            cpu_intensive_fn,
            tx,
            span,
            pending_tasks_guard,
        };
        self.inner.enqueue(priority, Box::new(task));
        ThreadPoolInner::schedule(&self.inner);
        rx.map_err(|_| Panicked)
    }
}

impl ThreadPoolInner {
    fn enqueue(&self, priority: Priority, task: Box<dyn RunnableTask>) {
        let mut state = self.state.lock().unwrap();
        match priority {
            Priority::Normal => state.normal_priority_tasks.push_back(task),
            Priority::High => state.high_priority_tasks.push_back(task),
        }
    }

    fn schedule(inner: &Arc<Self>) {
        // Fast path: skip lock acquisition entirely when already at capacity.
        if inner.num_running_tasks.load(Ordering::Acquire) >= inner.max_running_tasks {
            return;
        }
        let mut state = inner.state.lock().unwrap();
        while inner.num_running_tasks.load(Ordering::Acquire) < inner.max_running_tasks {
            let Some(task) = state.pop_next_task() else {
                return;
            };
            if task.is_cancelled() {
                continue;
            }
            let permit = Permit::new(inner.clone());
            let ongoing_tasks = inner.ongoing_tasks.clone();
            inner.thread_pool.spawn(move || {
                task.run(ongoing_tasks);
                // We explicitly drop here to force the move of the permit into the closure.
                drop(permit);
            });
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::time::Duration;

    use tokio::sync::oneshot;

    use super::*;

    #[tokio::test]
    async fn test_run_cpu_intensive() {
        let thread_pool = ThreadPoolWithPriority::new("priority_basic_test", Some(1));
        assert_eq!(thread_pool.run_cpu_intensive(|| 1).await, Ok(1));
    }

    #[tokio::test]
    async fn test_run_cpu_intensive_panicks() {
        let thread_pool = ThreadPoolWithPriority::new("priority_panic_basic_test", Some(1));
        assert!(thread_pool.run_cpu_intensive(|| panic!("")).await.is_err());
    }

    #[tokio::test]
    async fn test_run_cpu_intensive_panicks_do_not_shrink_thread_pool() {
        let thread_pool = ThreadPoolWithPriority::new("priority_repeated_panic_test", Some(1));
        for _ in 0..100 {
            assert!(thread_pool.run_cpu_intensive(|| panic!("")).await.is_err());
        }
    }

    #[tokio::test]
    async fn test_run_cpu_intensive_abort() {
        let thread_pool = ThreadPoolWithPriority::new("priority_abort_test", Some(1));
        let counter: Arc<AtomicU64> = Default::default();
        let mut futures = Vec::new();
        for _ in 0..1_000 {
            let counter_clone = counter.clone();
            let fut = thread_pool.run_cpu_intensive(move || {
                std::thread::sleep(Duration::from_millis(5));
                counter_clone.fetch_add(1, Ordering::SeqCst)
            });
            futures.push(tokio::time::timeout(Duration::from_millis(1), fut));
        }
        futures::future::join_all(futures).await;
        assert!(counter.load(Ordering::SeqCst) < 100);
    }

    #[tokio::test]
    async fn test_run_cpu_intensive_high_priority_runs_before_pending_normal_priority_tasks() {
        let thread_pool = ThreadPoolWithPriority::new("priority_order_test", Some(1));
        let execution_order: Arc<std::sync::Mutex<Vec<u64>>> = Default::default();
        let (started_tx, started_rx) = oneshot::channel();
        let (release_tx, release_rx) = std::sync::mpsc::channel();

        let first_task = thread_pool.run_cpu_intensive(move || {
            let _ = started_tx.send(());
            release_rx.recv().unwrap();
        });
        started_rx.await.unwrap();

        let execution_order_clone = execution_order.clone();
        let normal_task_1 =
            thread_pool.run_cpu_intensive_with_priority(Priority::Normal, move || {
                execution_order_clone.lock().unwrap().push(1);
            });

        let execution_order_clone = execution_order.clone();
        let normal_task_2 =
            thread_pool.run_cpu_intensive_with_priority(Priority::Normal, move || {
                execution_order_clone.lock().unwrap().push(2);
            });

        let execution_order_clone = execution_order.clone();
        let high_priority_task =
            thread_pool.run_cpu_intensive_with_priority(Priority::High, move || {
                execution_order_clone.lock().unwrap().push(0);
            });

        release_tx.send(()).unwrap();
        first_task.await.unwrap();
        high_priority_task.await.unwrap();
        normal_task_1.await.unwrap();
        normal_task_2.await.unwrap();

        assert_eq!(*execution_order.lock().unwrap(), vec![0, 1, 2]);
    }

    #[tokio::test]
    async fn test_run_cpu_intensive_with_priority_skips_cancelled_pending_task() {
        let thread_pool = ThreadPoolWithPriority::new("priority_cancellation_test", Some(1));
        let counter: Arc<AtomicU64> = Default::default();
        let (started_tx, started_rx) = oneshot::channel();
        let (release_tx, release_rx) = std::sync::mpsc::channel();

        let first_task = thread_pool.run_cpu_intensive(move || {
            let _ = started_tx.send(());
            release_rx.recv().unwrap();
        });
        started_rx.await.unwrap();

        let counter_clone = counter.clone();
        let cancelled_task =
            thread_pool.run_cpu_intensive_with_priority(Priority::High, move || {
                counter_clone.fetch_add(1, Ordering::SeqCst);
            });
        drop(cancelled_task);

        let counter_clone = counter.clone();
        let normal_task = thread_pool.run_cpu_intensive(move || {
            counter_clone.fetch_add(10, Ordering::SeqCst);
        });

        release_tx.send(()).unwrap();
        first_task.await.unwrap();
        normal_task.await.unwrap();

        assert_eq!(counter.load(Ordering::SeqCst), 10);
    }

    #[tokio::test]
    async fn test_run_cpu_intensive_panic_releases_scheduler_slot() {
        let thread_pool = ThreadPoolWithPriority::new("priority_panic_test", Some(1));
        assert!(
            thread_pool
                .run_cpu_intensive_with_priority(Priority::High, || panic!("expected panic"))
                .await
                .is_err()
        );

        let result =
            tokio::time::timeout(Duration::from_secs(1), thread_pool.run_cpu_intensive(|| 1))
                .await
                .unwrap();
        assert_eq!(result, Ok(1));
    }
}
