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
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use once_cell::sync::OnceCell;
use prometheus::{Gauge, IntCounter, IntGauge};
use tokio::runtime::Runtime;
use tokio_metrics::{RuntimeMetrics, RuntimeMonitor};

use crate::metrics::{new_counter, new_float_gauge, new_gauge};

static RUNTIMES: OnceCell<HashMap<RuntimeType, tokio::runtime::Runtime>> = OnceCell::new();

/// Describes which runtime an actor should run on.
#[derive(Clone, Copy, Debug, Hash, Eq, PartialEq)]
pub enum RuntimeType {
    /// The blocking runtime runs blocking actors.
    /// This runtime is only used as a nice thread pool with
    /// the interface as tokio stasks.
    ///
    /// This runtime should not be used to run tokio
    /// io operations.
    ///
    /// Tasks are allowed to block for an arbitrary amount of time.
    Blocking,

    /// The non-blocking runtime is closer to what one would expect from
    /// a regular tokio runtime.
    ///
    /// Task are expect to yield within 500 micros.
    NonBlocking,
}

#[derive(Debug, Clone, Copy)]
pub struct RuntimesConfig {
    /// Number of worker threads allocated to the non-blocking runtime.
    pub num_threads_non_blocking: usize,
    /// Number of worker threads allocated to the blocking runtime.
    pub num_threads_blocking: usize,
}

impl RuntimesConfig {
    #[cfg(any(test, feature = "testsuite"))]
    pub fn light_for_tests() -> RuntimesConfig {
        RuntimesConfig {
            num_threads_blocking: 1,
            num_threads_non_blocking: 1,
        }
    }

    pub fn with_num_cpus(num_cpus: usize) -> Self {
        // Non blocking task are supposed to be io intensive, and not require many threads.
        // On the other hand the blocking actors are cpu intensive. We allocate
        // almost all of the threads to them.
        match num_cpus {
            0..=3 => {
                // We do not have enough vCPUs to allocate a full thread to
                // non-blocking.
                RuntimesConfig {
                    num_threads_non_blocking: 1,
                    num_threads_blocking: num_cpus,
                }
            }
            4..=6 => RuntimesConfig {
                num_threads_non_blocking: 1,
                num_threads_blocking: num_cpus - 1,
            },
            7.. => RuntimesConfig {
                num_threads_non_blocking: 2,
                num_threads_blocking: num_cpus - 2,
            },
        }
    }
}

impl Default for RuntimesConfig {
    fn default() -> Self {
        let num_cpus = crate::num_cpus();
        Self::with_num_cpus(num_cpus)
    }
}

fn start_runtimes(config: RuntimesConfig) -> HashMap<RuntimeType, Runtime> {
    let mut runtimes = HashMap::with_capacity(2);

    let disable_lifo_slot = crate::get_bool_from_env("QW_DISABLE_TOKIO_LIFO_SLOT", true);

    let mut blocking_runtime_builder = tokio::runtime::Builder::new_multi_thread();
    if disable_lifo_slot {
        blocking_runtime_builder.disable_lifo_slot();
    }
    let blocking_runtime = blocking_runtime_builder
        .worker_threads(config.num_threads_blocking)
        .thread_name_fn(|| {
            static ATOMIC_ID: AtomicUsize = AtomicUsize::new(0);
            let id = ATOMIC_ID.fetch_add(1, Ordering::AcqRel);
            format!("blocking-{id}")
        })
        .enable_all()
        .build()
        .unwrap();

    scrape_tokio_runtime_metrics(blocking_runtime.handle(), "blocking");
    runtimes.insert(RuntimeType::Blocking, blocking_runtime);

    let non_blocking_runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(config.num_threads_non_blocking)
        .thread_name_fn(|| {
            static ATOMIC_ID: AtomicUsize = AtomicUsize::new(0);
            let id = ATOMIC_ID.fetch_add(1, Ordering::AcqRel);
            format!("non-blocking-{id}")
        })
        .enable_all()
        .build()
        .unwrap();

    scrape_tokio_runtime_metrics(non_blocking_runtime.handle(), "non_blocking");
    runtimes.insert(RuntimeType::NonBlocking, non_blocking_runtime);

    runtimes
}

pub fn initialize_runtimes(runtimes_config: RuntimesConfig) -> anyhow::Result<()> {
    RUNTIMES.get_or_init(|| start_runtimes(runtimes_config));
    Ok(())
}

impl RuntimeType {
    pub fn get_runtime_handle(self) -> tokio::runtime::Handle {
        RUNTIMES
            .get_or_init(|| {
                #[cfg(any(test, feature = "testsuite"))]
                {
                    tracing::warn!("starting Tokio actor runtimes for tests");
                    start_runtimes(RuntimesConfig::light_for_tests())
                }
                #[cfg(not(any(test, feature = "testsuite")))]
                {
                    panic!("Tokio runtimes not initialized. Please, report this issue on GitHub: https://github.com/quickwit-oss/quickwit/issues.");
                }
            })
            .get(&self)
            .unwrap()
            .handle()
            .clone()
    }
}

/// Spawns a background task
pub fn scrape_tokio_runtime_metrics(handle: &tokio::runtime::Handle, label: &'static str) {
    let runtime_monitor = RuntimeMonitor::new(handle);
    handle.spawn(async move {
        let mut interval = tokio::time::interval(Duration::from_secs(1));
        let mut prometheus_runtime_metrics = PrometheusRuntimeMetrics::new(label);

        for tokio_runtime_metrics in runtime_monitor.intervals() {
            interval.tick().await;
            prometheus_runtime_metrics.update(&tokio_runtime_metrics);
        }
    });
}

struct PrometheusRuntimeMetrics {
    scheduled_tasks: IntGauge,
    worker_busy_duration_milliseconds_total: IntCounter,
    worker_busy_ratio: Gauge,
    worker_threads: IntGauge,
}

impl PrometheusRuntimeMetrics {
    pub fn new(label: &'static str) -> Self {
        Self {
            scheduled_tasks: new_gauge(
                "tokio_scheduled_tasks",
                "The total number of tasks currently scheduled in workers' local queues.",
                "runtime",
                &[("runtime_type", label)],
            ),
            worker_busy_duration_milliseconds_total: new_counter(
                "tokio_worker_busy_duration_milliseconds_total",
                " The total amount of time worker threads were busy.",
                "runtime",
                &[("runtime_type", label)],
            ),
            worker_busy_ratio: new_float_gauge(
                "tokio_worker_busy_ratio",
                "The ratio of time worker threads were busy since the last time runtime metrics \
                 were collected.",
                "runtime",
                &[("runtime_type", label)],
            ),
            worker_threads: new_gauge(
                "tokio_worker_threads",
                "The number of worker threads used by the runtime.",
                "runtime",
                &[("runtime_type", label)],
            ),
        }
    }

    pub fn update(&mut self, runtime_metrics: &RuntimeMetrics) {
        self.scheduled_tasks
            .set(runtime_metrics.total_local_queue_depth as i64);
        self.worker_busy_duration_milliseconds_total
            .inc_by(runtime_metrics.total_busy_duration.as_millis() as u64);
        self.worker_busy_ratio.set(runtime_metrics.busy_ratio());
        self.worker_threads
            .set(runtime_metrics.workers_count as i64);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_runtimes_config_default() {
        let runtime_default = RuntimesConfig::default();
        assert!(runtime_default.num_threads_non_blocking <= runtime_default.num_threads_blocking);
        assert!(runtime_default.num_threads_non_blocking <= 2);
    }

    #[test]
    fn test_runtimes_with_given_num_cpus_10() {
        let runtime = RuntimesConfig::with_num_cpus(10);
        assert_eq!(runtime.num_threads_blocking, 8);
        assert_eq!(runtime.num_threads_non_blocking, 2);
    }

    #[test]
    fn test_runtimes_with_given_num_cpus_3() {
        let runtime = RuntimesConfig::with_num_cpus(3);
        assert_eq!(runtime.num_threads_blocking, 3);
        assert_eq!(runtime.num_threads_non_blocking, 1);
    }
}
