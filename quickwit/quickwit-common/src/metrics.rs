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

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, LazyLock, OnceLock};
#[cfg(not(test))]
use std::time::Duration;

use metrics_exporter_prometheus::PrometheusHandle;
pub use prometheus::{exponential_buckets, linear_buckets};
use quickwit_metrics::{Counter, Gauge, Labels, gauge};

const SYSTEM: &str = "quickwit";

static PROMETHEUS_HANDLE: OnceLock<PrometheusHandle> = OnceLock::new();

#[derive(Clone)]
pub struct MaybeRegisteredCounter {
    inner: MaybeRegisteredCounterInner,
}

#[derive(Clone)]
enum MaybeRegisteredCounterInner {
    Local(Arc<AtomicU64>),
    Registered(Counter),
}

impl Default for MaybeRegisteredCounter {
    fn default() -> Self {
        Self::local()
    }
}

impl MaybeRegisteredCounter {
    pub fn local() -> Self {
        Self {
            inner: MaybeRegisteredCounterInner::Local(Arc::new(AtomicU64::new(0))),
        }
    }

    pub fn registered(counter: Counter) -> Self {
        Self {
            inner: MaybeRegisteredCounterInner::Registered(counter),
        }
    }

    pub fn increment(&self, value: u64) {
        match &self.inner {
            MaybeRegisteredCounterInner::Local(counter) => {
                counter.fetch_add(value, Ordering::Relaxed);
            }
            MaybeRegisteredCounterInner::Registered(counter) => counter.increment(value),
        }
    }

    pub fn get(&self) -> u64 {
        match &self.inner {
            MaybeRegisteredCounterInner::Local(counter) => counter.load(Ordering::Relaxed),
            MaybeRegisteredCounterInner::Registered(counter) => counter.get(),
        }
    }
}

pub fn set_prometheus_handle(handle: PrometheusHandle) -> Result<(), String> {
    #[cfg(not(test))]
    let upkeep_handle = handle.clone();
    PROMETHEUS_HANDLE
        .set(handle)
        .map_err(|_| "Prometheus metrics renderer is already installed".to_string())?;
    #[cfg(not(test))]
    spawn_prometheus_upkeep(upkeep_handle)?;
    Ok(())
}

pub fn metrics_text_payload() -> Result<String, String> {
    let handle = PROMETHEUS_HANDLE
        .get()
        .ok_or_else(|| "Prometheus metrics rendering is not installed yet".to_string())?;
    Ok(handle.render())
}

#[cfg(not(test))]
fn spawn_prometheus_upkeep(handle: PrometheusHandle) -> Result<(), String> {
    // Quickwit serves the existing `/metrics` route itself, so we build only the
    // Prometheus recorder instead of using the exporter's HTTP listener. That lower-level
    // API does not spawn the upkeep task that periodically drains histogram buffers.
    std::thread::Builder::new()
        .name("metrics-exporter-prometheus-upkeep".to_string())
        .spawn(move || {
            loop {
                std::thread::sleep(Duration::from_secs(5));
                handle.run_upkeep();
            }
        })
        .map(|_| ())
        .map_err(|error| format!("failed to spawn Prometheus metrics upkeep thread: {error}"))
}

pub fn register_info(name: &'static str, help: &'static str, kvs: BTreeMap<&'static str, String>) {
    let key_name = metric_key_name("", name);
    let labels = kvs
        .into_iter()
        .map(|(label, value)| metrics::Label::new(label, value))
        .collect::<Vec<_>>();
    let key = metrics::Key::from_parts(key_name.clone(), labels);
    let metadata = metrics::Metadata::new("", metrics::Level::INFO, Some(module_path!()));
    metrics::with_recorder(|recorder| {
        recorder.describe_counter(metrics::KeyName::from(key_name), None, help.into());
        recorder.register_counter(&key, &metadata).increment(1);
    });
}

pub fn index_label(index_id: &str) -> &str {
    static PER_INDEX_METRICS_ENABLED: LazyLock<bool> =
        LazyLock::new(|| !crate::get_bool_from_env("QW_DISABLE_PER_INDEX_METRICS", false));

    if *PER_INDEX_METRICS_ENABLED {
        index_id
    } else {
        "__any__"
    }
}

pub static MEMORY_ACTIVE_BYTES: LazyLock<Gauge> = LazyLock::new(|| {
    gauge!(
        name: "active_bytes",
        description: "Total number of bytes in active pages allocated by the application, as reported by jemalloc `stats.active`.",
        subsystem: "memory",
    )
});

pub static MEMORY_ALLOCATED_BYTES: LazyLock<Gauge> = LazyLock::new(|| {
    gauge!(
        name: "allocated_bytes",
        description: "Total number of bytes allocated by the application, as reported by jemalloc `stats.allocated`.",
        subsystem: "memory",
    )
});

pub static MEMORY_RESIDENT_BYTES: LazyLock<Gauge> = LazyLock::new(|| {
    gauge!(
        name: "resident_bytes",
        description: " Total number of bytes in physically resident data pages mapped by the allocator, as reported by jemalloc `stats.resident`.",
        subsystem: "memory",
    )
});

static IN_FLIGHT_DATA_BYTES: LazyLock<Gauge> = LazyLock::new(|| {
    gauge!(
        name: "in_flight_data_bytes",
        description: "Amount of data in-flight in various buffers in bytes.",
        subsystem: "memory",
    )
});

const COMPONENT_LABELS: Labels<1> = Labels::new(["component"]);

pub static IN_FLIGHT_REST_SERVER: LazyLock<Gauge> =
    LazyLock::new(|| in_flight_data_gauge("rest_server"));

pub static IN_FLIGHT_INGEST_ROUTER: LazyLock<Gauge> =
    LazyLock::new(|| in_flight_data_gauge("ingest_router"));

pub static IN_FLIGHT_INGESTER_PERSIST: LazyLock<Gauge> =
    LazyLock::new(|| in_flight_data_gauge("ingester_persist"));

pub static IN_FLIGHT_INGESTER_REPLICATE: LazyLock<Gauge> =
    LazyLock::new(|| in_flight_data_gauge("ingester_replicate"));

pub static IN_FLIGHT_WAL: LazyLock<Gauge> = LazyLock::new(|| in_flight_data_gauge("wal"));

pub static IN_FLIGHT_FETCH_STREAM: LazyLock<Gauge> =
    LazyLock::new(|| in_flight_data_gauge("fetch_stream"));

pub static IN_FLIGHT_MULTI_FETCH_STREAM: LazyLock<Gauge> =
    LazyLock::new(|| in_flight_data_gauge("multi_fetch_stream"));

pub static IN_FLIGHT_DOC_PROCESSOR_MAILBOX: LazyLock<Gauge> =
    LazyLock::new(|| in_flight_data_gauge("doc_processor_mailbox"));

pub static IN_FLIGHT_INDEXER_MAILBOX: LazyLock<Gauge> =
    LazyLock::new(|| in_flight_data_gauge("indexer_mailbox"));

pub static IN_FLIGHT_INDEX_WRITER: LazyLock<Gauge> =
    LazyLock::new(|| in_flight_data_gauge("index_writer"));

pub static IN_FLIGHT_FILE_SOURCE: LazyLock<Gauge> =
    LazyLock::new(|| in_flight_data_gauge("file_source"));

pub static IN_FLIGHT_INGEST_SOURCE: LazyLock<Gauge> =
    LazyLock::new(|| in_flight_data_gauge("ingest_source"));

pub static IN_FLIGHT_KAFKA_SOURCE: LazyLock<Gauge> =
    LazyLock::new(|| in_flight_data_gauge("kafka_source"));

pub static IN_FLIGHT_KINESIS_SOURCE: LazyLock<Gauge> =
    LazyLock::new(|| in_flight_data_gauge("kinesis_source"));

pub static IN_FLIGHT_PUBSUB_SOURCE: LazyLock<Gauge> =
    LazyLock::new(|| in_flight_data_gauge("pubsub_source"));

pub static IN_FLIGHT_PULSAR_SOURCE: LazyLock<Gauge> =
    LazyLock::new(|| in_flight_data_gauge("pulsar_source"));

pub static IN_FLIGHT_OTHER_SOURCE: LazyLock<Gauge> =
    LazyLock::new(|| in_flight_data_gauge("pulsar_source"));

fn in_flight_data_gauge(component: &'static str) -> Gauge {
    let labels = COMPONENT_LABELS.with_values([component]);
    gauge!(parent: &*IN_FLIGHT_DATA_BYTES, labels: &labels)
}

fn metric_key_name(subsystem: &str, name: &str) -> String {
    if subsystem.is_empty() {
        format!("{SYSTEM}_{name}")
    } else {
        format!("{SYSTEM}_{subsystem}_{name}")
    }
}

#[cfg(test)]
mod tests {
    use metrics::with_local_recorder;
    use metrics_exporter_prometheus::PrometheusBuilder;
    use metrics_util::debugging::{DebugValue, DebuggingRecorder};
    use quickwit_metrics::counter;

    use super::*;

    #[test]
    fn maybe_registered_counter_counts_locally() {
        let counter = MaybeRegisteredCounter::local();
        let counter_clone = counter.clone();

        counter.increment(3);
        counter_clone.increment(4);

        assert_eq!(counter.get(), 7);
        assert_eq!(counter_clone.get(), 7);
    }

    #[test]
    fn maybe_registered_counter_wraps_registered_counter() {
        let registered_counter = counter!(
            name: "maybe_registered_counter_test",
            description: "Maybe registered counter test.",
            subsystem: "",
            observable: true,
        );
        let counter = MaybeRegisteredCounter::registered(registered_counter.clone());

        counter.increment(5);

        assert_eq!(counter.get(), 5);
        assert_eq!(registered_counter.get(), 5);
    }

    #[test]
    fn metrics_text_payload_renders_prometheus_handle() {
        let recorder = PrometheusBuilder::new().build_recorder();
        set_prometheus_handle(recorder.handle()).expect("Prometheus handle should be set once");

        with_local_recorder(&recorder, || {
            register_info(
                "prometheus_payload_info",
                "prometheus payload info",
                BTreeMap::new(),
            );
        });

        let payload = metrics_text_payload().expect("Prometheus payload should render");
        assert!(payload.contains("# HELP quickwit_prometheus_payload_info"));
        assert!(payload.contains("quickwit_prometheus_payload_info 1"));
    }

    #[test]
    fn register_info_records_labeled_counter() {
        let recorder = DebuggingRecorder::new();
        let snapshotter = recorder.snapshotter();
        with_local_recorder(&recorder, || {
            let labels = BTreeMap::from([("version", "test".to_string())]);
            register_info("build_info_test", "build info test", labels);
        });

        let snapshot = snapshotter.snapshot().into_vec();
        let (_, _, description, value) = snapshot
            .into_iter()
            .find(|(composite_key, _, _, _)| {
                let (_, key) = composite_key.clone().into_parts();
                key.name() == "quickwit_build_info_test"
                    && key
                        .labels()
                        .any(|label| label.key() == "version" && label.value() == "test")
            })
            .expect("build info metric should be recorded");
        assert_eq!(description.as_deref(), Some("build info test"));
        assert_eq!(value, DebugValue::Counter(1));
    }

    #[test]
    fn bucket_helpers_are_reexported() {
        assert_eq!(linear_buckets(0.0, 1.0, 3).unwrap(), vec![0.0, 1.0, 2.0]);
        assert_eq!(
            exponential_buckets(1.0, 2.0, 3).unwrap(),
            vec![1.0, 2.0, 4.0]
        );
    }
}
