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
use std::sync::{Arc, LazyLock, Mutex};
use std::time::Duration;

use quickwit_metrics::{
    Counter, Gauge, LazyCounter, LazyGauge, counter, gauge, labels, lazy_counter, lazy_gauge,
};
use tokio::runtime::{Handle, Id, RuntimeMetrics};
use tokio_metrics::TaskMonitor;

static METRICS: LazyLock<Arc<Mutex<MetricsRegistry>>> = LazyLock::new(|| {
    let registry = Arc::new(Mutex::new(MetricsRegistry::default()));
    let sampler_registry = registry.clone();
    std::thread::Builder::new()
        .name("runtime-metrics".to_string())
        .spawn(move || {
            loop {
                sampler_registry
                    .lock()
                    .expect("runtime metrics lock poisoned")
                    .record();
                std::thread::sleep(Duration::from_secs(1));
            }
        })
        .expect("failed to spawn runtime metrics sampler");
    registry
});

static TOKIO_SCHEDULED_TASKS: LazyGauge = lazy_gauge!(
    name: "tokio_scheduled_tasks",
    description: "Runnable tasks in local worker queues, excluding the global queue and LIFO slots.",
    subsystem: "runtime",
);

static TOKIO_WORKER_BUSY_DURATION_MILLISECONDS_TOTAL: LazyCounter = lazy_counter!(
    name: "tokio_worker_busy_duration_milliseconds_total",
    description: "Cumulative busy time summed over all runtime workers.",
    subsystem: "runtime",
);

static TOKIO_WORKER_THREADS: LazyGauge = lazy_gauge!(
    name: "tokio_worker_threads",
    description: "Number of runtime worker threads.",
    subsystem: "runtime",
);

static TOKIO_TASK_POLLS_TOTAL: LazyCounter = lazy_counter!(
    name: "tokio_task_polls_total",
    description: "Number of times tasks of this type were polled.",
    subsystem: "runtime",
);

static TOKIO_TASK_POLL_DURATION_MICROSECONDS_TOTAL: LazyCounter = lazy_counter!(
    name: "tokio_task_poll_duration_microseconds_total",
    description: "Wall-clock time spent polling instrumented futures, excluding separately spawned tasks.",
    subsystem: "runtime",
);

static TOKIO_TASK_SLOW_POLLS_TOTAL: LazyCounter = lazy_counter!(
    name: "tokio_task_slow_polls_total",
    description: "Number of polls lasting at least 50 microseconds.",
    subsystem: "runtime",
);

static TOKIO_TASK_SCHEDULES_TOTAL: LazyCounter = lazy_counter!(
    name: "tokio_task_schedules_total",
    description: "Number of observed wake-to-poll intervals.",
    subsystem: "runtime",
);

static TOKIO_TASK_SCHEDULED_DURATION_MICROSECONDS_TOTAL: LazyCounter = lazy_counter!(
    name: "tokio_task_scheduled_duration_microseconds_total",
    description: "Cumulative time tasks of this type spent waiting for a worker thread after being woken up.",
    subsystem: "runtime",
);

static TOKIO_TASK_IDLE_DURATION_MICROSECONDS_TOTAL: LazyCounter = lazy_counter!(
    name: "tokio_task_idle_duration_microseconds_total",
    description: "Cumulative time tasks of this type spent idle, waiting to be woken up by an external event.",
    subsystem: "runtime",
);

pub fn register_runtime(handle: &Handle, label: &'static str) {
    METRICS
        .lock()
        .expect("runtime metrics lock poisoned")
        .register_runtime(handle, label);
}

pub fn task_monitor(task_name: &'static str, handle: &Handle) -> TaskMonitor {
    METRICS
        .lock()
        .expect("runtime metrics lock poisoned")
        .task_monitor(task_name, handle)
}

#[derive(Default)]
struct MetricsRegistry {
    runtimes: HashMap<Id, RuntimeMetricsRecorder>,
    tasks: HashMap<(&'static str, &'static str), TaskMetricsRecorder>,
}

impl MetricsRegistry {
    fn register_runtime(&mut self, handle: &Handle, label: &'static str) {
        if let Some(recorder) = self.runtimes.get(&handle.id()) {
            assert_eq!(
                recorder.label, label,
                "runtime already registered with another label"
            );
            return;
        }
        assert!(
            self.runtimes
                .values()
                .all(|recorder| recorder.label != label),
            "runtime label already registered"
        );
        self.runtimes
            .insert(handle.id(), RuntimeMetricsRecorder::new(handle, label));
    }

    fn task_monitor(&mut self, task_name: &'static str, handle: &Handle) -> TaskMonitor {
        let label = match self.runtimes.get(&handle.id()) {
            Some(recorder) => recorder.label,
            None => {
                crate::rate_limited_warn!(
                    limit_per_min = 1,
                    runtime_id = %handle.id(),
                    "task runtime is not registered for metrics"
                );
                "unregistered"
            }
        };
        self.tasks
            .entry((task_name, label))
            .or_insert_with(|| TaskMetricsRecorder::new(task_name, label))
            .monitor
            .clone()
    }

    fn record(&self) {
        for recorder in self.runtimes.values() {
            recorder.record();
        }
        for recorder in self.tasks.values() {
            recorder.record();
        }
    }
}

struct RuntimeMetricsRecorder {
    label: &'static str,
    metrics: RuntimeMetrics,
    scheduled_tasks: Gauge,
    busy_duration_millis: Counter,
    worker_threads: Gauge,
}

impl RuntimeMetricsRecorder {
    fn new(handle: &Handle, label: &'static str) -> Self {
        let labels = labels!("runtime_type" => label);
        Self {
            label,
            metrics: handle.metrics(),
            scheduled_tasks: gauge!(parent: TOKIO_SCHEDULED_TASKS, labels: [labels]),
            busy_duration_millis: counter!(parent: TOKIO_WORKER_BUSY_DURATION_MILLISECONDS_TOTAL, labels: [labels]),
            worker_threads: gauge!(parent: TOKIO_WORKER_THREADS, labels: [labels]),
        }
    }

    fn record(&self) {
        let num_workers = self.metrics.num_workers();
        let mut busy_duration = Duration::ZERO;
        let mut local_queue_depth = 0;
        for worker in 0..num_workers {
            busy_duration += self.metrics.worker_total_busy_duration(worker);
            local_queue_depth += self.metrics.worker_local_queue_depth(worker);
        }
        self.scheduled_tasks.set(local_queue_depth as f64);
        self.busy_duration_millis
            .absolute(busy_duration.as_millis() as u64);
        self.worker_threads.set(num_workers as f64);
    }
}

struct TaskMetricsRecorder {
    monitor: TaskMonitor,
    polls: Counter,
    poll_duration_micros: Counter,
    slow_polls: Counter,
    schedules: Counter,
    scheduled_duration_micros: Counter,
    idle_duration_micros: Counter,
}

impl TaskMetricsRecorder {
    fn new(task_name: &'static str, runtime_label: &'static str) -> Self {
        let labels = labels!("task" => task_name, "runtime_type" => runtime_label);
        Self {
            monitor: TaskMonitor::with_slow_poll_threshold(Duration::from_micros(50)),
            polls: counter!(parent: TOKIO_TASK_POLLS_TOTAL, labels: [labels]),
            poll_duration_micros: counter!(parent: TOKIO_TASK_POLL_DURATION_MICROSECONDS_TOTAL, labels: [labels]),
            slow_polls: counter!(parent: TOKIO_TASK_SLOW_POLLS_TOTAL, labels: [labels]),
            schedules: counter!(parent: TOKIO_TASK_SCHEDULES_TOTAL, labels: [labels]),
            scheduled_duration_micros: counter!(parent: TOKIO_TASK_SCHEDULED_DURATION_MICROSECONDS_TOTAL, labels: [labels]),
            idle_duration_micros: counter!(parent: TOKIO_TASK_IDLE_DURATION_MICROSECONDS_TOTAL, labels: [labels]),
        }
    }

    fn record(&self) {
        let metrics = self.monitor.cumulative();
        self.polls.absolute(metrics.total_poll_count);
        self.poll_duration_micros
            .absolute(metrics.total_poll_duration.as_micros() as u64);
        self.slow_polls.absolute(metrics.total_slow_poll_count);
        self.schedules.absolute(metrics.total_scheduled_count);
        self.scheduled_duration_micros
            .absolute(metrics.total_scheduled_duration.as_micros() as u64);
        self.idle_duration_micros
            .absolute(metrics.total_idle_duration.as_micros() as u64);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_task_monitor_preserves_cumulative_metrics() {
        let mut registry = MetricsRegistry::default();
        let handle = Handle::current();
        let task_name = "test_task_monitor_preserves_cumulative_metrics";
        registry.register_runtime(&handle, "cumulative");
        registry
            .task_monitor(task_name, &handle)
            .instrument(async {})
            .await;
        registry.record();
        assert_eq!(registry.tasks[&(task_name, "cumulative")].polls.get(), 1);

        let monitor = registry.task_monitor(task_name, &handle);
        monitor.instrument(tokio::task::yield_now()).await;
        let metrics = monitor.cumulative();
        assert!(metrics.total_poll_count > 1);
        assert!(metrics.total_scheduled_count > 0);
        assert_eq!(monitor.slow_poll_threshold(), Duration::from_micros(50));
        registry.record();
        registry.record();
        let recorder = &registry.tasks[&(task_name, "cumulative")];
        assert_eq!(recorder.polls.get(), metrics.total_poll_count);
        assert_eq!(
            recorder.poll_duration_micros.get(),
            metrics.total_poll_duration.as_micros() as u64
        );
        assert_eq!(recorder.slow_polls.get(), metrics.total_slow_poll_count);
        assert_eq!(recorder.schedules.get(), metrics.total_scheduled_count);
        assert_eq!(
            recorder.scheduled_duration_micros.get(),
            metrics.total_scheduled_duration.as_micros() as u64
        );
        assert_eq!(
            recorder.idle_duration_micros.get(),
            metrics.total_idle_duration.as_micros() as u64
        );
    }

    #[test]
    fn test_task_monitors_are_isolated_by_task_and_runtime() {
        let first_runtime = tokio::runtime::Builder::new_current_thread()
            .build()
            .unwrap();
        let second_runtime = tokio::runtime::Builder::new_current_thread()
            .build()
            .unwrap();
        let mut registry = MetricsRegistry::default();
        registry.register_runtime(first_runtime.handle(), "isolation_first");
        registry.register_runtime(second_runtime.handle(), "isolation_second");
        let first = registry.task_monitor("isolation_task", first_runtime.handle());
        let other_task = registry.task_monitor("isolation_other_task", first_runtime.handle());
        let other_runtime = registry.task_monitor("isolation_task", second_runtime.handle());

        first_runtime.block_on(first.instrument(async {}));
        assert_eq!(first.cumulative().total_poll_count, 1);
        assert_eq!(other_task.cumulative().total_poll_count, 0);
        assert_eq!(other_runtime.cumulative().total_poll_count, 0);
    }

    #[test]
    fn test_runtime_registration_is_idempotent() {
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .build()
            .unwrap();
        let mut registry = MetricsRegistry::default();
        registry.register_runtime(runtime.handle(), "idempotent");
        let monitor = registry.task_monitor("idempotent_task", runtime.handle());
        runtime.block_on(monitor.instrument(async {}));
        registry.register_runtime(runtime.handle(), "idempotent");
        registry.record();

        assert_eq!(registry.runtimes.len(), 1);
        assert_eq!(
            registry.runtimes[&runtime.handle().id()]
                .worker_threads
                .get(),
            2.0
        );
        assert_eq!(
            registry
                .task_monitor("idempotent_task", runtime.handle())
                .cumulative()
                .total_poll_count,
            1
        );
    }

    #[test]
    #[should_panic(expected = "runtime already registered with another label")]
    fn test_runtime_cannot_be_relabeled() {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .build()
            .unwrap();
        let mut registry = MetricsRegistry::default();
        registry.register_runtime(runtime.handle(), "original_label");
        registry.register_runtime(runtime.handle(), "changed_label");
    }

    #[test]
    #[should_panic(expected = "runtime label already registered")]
    fn test_runtime_labels_are_unique() {
        let first_runtime = tokio::runtime::Builder::new_current_thread()
            .build()
            .unwrap();
        let second_runtime = tokio::runtime::Builder::new_current_thread()
            .build()
            .unwrap();
        let mut registry = MetricsRegistry::default();
        registry.register_runtime(first_runtime.handle(), "duplicate_label");
        registry.register_runtime(second_runtime.handle(), "duplicate_label");
    }

    #[tokio::test]
    async fn test_unregistered_runtime_is_identified() {
        let mut registry = MetricsRegistry::default();
        registry
            .task_monitor("unregistered_task", &Handle::current())
            .instrument(async {})
            .await;
        registry.record();
        assert_eq!(
            registry.tasks[&("unregistered_task", "unregistered")]
                .polls
                .get(),
            1
        );
        assert!(registry.runtimes.is_empty());
    }
}
