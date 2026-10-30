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

use std::borrow::Cow;
use std::fmt;

use mixtrics::metrics::{
    BoxedCounter, BoxedCounterVec, BoxedGauge, BoxedGaugeVec, BoxedHistogram, BoxedHistogramVec,
    Buckets, CounterOps, CounterVecOps, GaugeOps, GaugeVecOps, HistogramOps, HistogramVecOps,
    RegistryOps,
};
use quickwit_metrics::{LazyHistogram, lazy_histogram};

fn feuer_histogram_buckets(name: &str) -> Vec<f64> {
    let (start, count) = match name {
        "feuer_lookup_duration_seconds" => (0.000_001, 30),
        "feuer_disk_lookup_duration_seconds"
        | "feuer_disk_io_duration_seconds"
        | "feuer_disk_write_queue_duration_seconds"
        | "feuer_disk_read_queue_wait_duration_seconds" => (0.000_001, 25),
        "feuer_disk_read_size_bytes" => (1024.0, 21),
        _ => panic!("unregistered Feuer histogram: {name}"),
    };
    Buckets::exponential(start, 2.0, count)
}

// The exporter reads bucket definitions before Feuer registers metrics at runtime.
static _LOOKUP_DURATION: LazyHistogram = lazy_histogram!(
    name: "feuer_lookup_duration_seconds",
    description: "Completed successful Feuer lookup duration, including callback work",
    system: "",
    subsystem: "",
    buckets: feuer_histogram_buckets("feuer_lookup_duration_seconds"),
);
static _DISK_READ_SIZE: LazyHistogram = lazy_histogram!(
    name: "feuer_disk_read_size_bytes",
    description: "Requested bytes per successful Feuer data-file read, excluding alignment padding",
    system: "",
    subsystem: "",
    buckets: feuer_histogram_buckets("feuer_disk_read_size_bytes"),
);
static _DISK_LOOKUP_DURATION: LazyHistogram = lazy_histogram!(
    name: "feuer_disk_lookup_duration_seconds",
    description: "Completed Feuer disk hit duration, including integrity checking and copying",
    system: "",
    subsystem: "",
    buckets: feuer_histogram_buckets("feuer_disk_lookup_duration_seconds"),
);
static _DISK_IO_DURATION: LazyHistogram = lazy_histogram!(
    name: "feuer_disk_io_duration_seconds",
    description: "Successful Feuer disk I/O duration",
    system: "",
    subsystem: "",
    buckets: feuer_histogram_buckets("feuer_disk_io_duration_seconds"),
);
static _DISK_WRITE_QUEUE_DURATION: LazyHistogram = lazy_histogram!(
    name: "feuer_disk_write_queue_duration_seconds",
    description: "Feuer disk-write queue admission to dequeue duration, including stale entries",
    system: "",
    subsystem: "",
    buckets: feuer_histogram_buckets("feuer_disk_write_queue_duration_seconds"),
);

static _DISK_READ_QUEUE_WAIT_DURATION: LazyHistogram = lazy_histogram!(
    name: "feuer_disk_read_queue_wait_duration_seconds",
    description: "Feuer read-piece channel-capacity wait ending in admission, cancellation or closure",
    system: "",
    subsystem: "",
    buckets: feuer_histogram_buckets("feuer_disk_read_queue_wait_duration_seconds"),
);

/// Forward Feuer's metrics to the process recorder, preserving native labels and adding `cache`.
#[derive(Debug)]
pub(crate) struct QuickwitMetricsRegistry {
    pub(crate) cache_name: &'static str,
}

impl RegistryOps for QuickwitMetricsRegistry {
    fn register_counter_vec(
        &self,
        name: Cow<'static, str>,
        desc: Cow<'static, str>,
        label_names: &'static [&'static str],
    ) -> BoxedCounterVec {
        ::metrics::describe_counter!(name.clone(), desc);
        Box::new(MetricsCounterVec {
            name,
            label_names,
            cache_name: self.cache_name,
        })
    }

    fn register_gauge_vec(
        &self,
        name: Cow<'static, str>,
        desc: Cow<'static, str>,
        label_names: &'static [&'static str],
    ) -> BoxedGaugeVec {
        ::metrics::describe_gauge!(name.clone(), desc);
        Box::new(MetricsGaugeVec {
            name,
            label_names,
            cache_name: self.cache_name,
        })
    }

    fn register_histogram_vec(
        &self,
        name: Cow<'static, str>,
        desc: Cow<'static, str>,
        label_names: &'static [&'static str],
    ) -> BoxedHistogramVec {
        ::metrics::describe_histogram!(name.clone(), desc);
        Box::new(MetricsHistogramVec {
            name,
            label_names,
            cache_name: self.cache_name,
        })
    }

    fn register_histogram_vec_with_buckets(
        &self,
        name: Cow<'static, str>,
        desc: Cow<'static, str>,
        label_names: &'static [&'static str],
        buckets: Vec<f64>,
    ) -> BoxedHistogramVec {
        assert_eq!(
            feuer_histogram_buckets(&name),
            buckets,
            "Feuer histogram bucket definitions have changed"
        );
        self.register_histogram_vec(name, desc, label_names)
    }
}

#[derive(Debug)]
struct MetricsCounterVec {
    name: Cow<'static, str>,
    label_names: &'static [&'static str],
    cache_name: &'static str,
}

impl CounterVecOps for MetricsCounterVec {
    fn counter(&self, labels: &[Cow<'static, str>]) -> BoxedCounter {
        Box::new(MetricsCounter(::metrics::counter!(
            self.name.clone(),
            labeled(self.cache_name, self.label_names, labels)
        )))
    }
}

#[derive(Debug)]
struct MetricsGaugeVec {
    name: Cow<'static, str>,
    label_names: &'static [&'static str],
    cache_name: &'static str,
}

impl GaugeVecOps for MetricsGaugeVec {
    fn gauge(&self, labels: &[Cow<'static, str>]) -> BoxedGauge {
        Box::new(MetricsGauge(::metrics::gauge!(
            self.name.clone(),
            labeled(self.cache_name, self.label_names, labels)
        )))
    }
}

#[derive(Debug)]
struct MetricsHistogramVec {
    name: Cow<'static, str>,
    label_names: &'static [&'static str],
    cache_name: &'static str,
}

impl HistogramVecOps for MetricsHistogramVec {
    fn histogram(&self, labels: &[Cow<'static, str>]) -> BoxedHistogram {
        Box::new(MetricsHistogram(::metrics::histogram!(
            self.name.clone(),
            labeled(self.cache_name, self.label_names, labels)
        )))
    }
}

struct MetricsCounter(::metrics::Counter);
struct MetricsGauge(::metrics::Gauge);
struct MetricsHistogram(::metrics::Histogram);

impl fmt::Debug for MetricsCounter {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("MetricsCounter").finish()
    }
}

impl fmt::Debug for MetricsGauge {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("MetricsGauge").finish()
    }
}

impl fmt::Debug for MetricsHistogram {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("MetricsHistogram").finish()
    }
}

impl CounterOps for MetricsCounter {
    fn increase(&self, val: u64) {
        self.0.increment(val);
    }
}

impl GaugeOps for MetricsGauge {
    fn increase(&self, val: u64) {
        self.0.increment(val as f64);
    }

    fn decrease(&self, val: u64) {
        self.0.decrement(val as f64);
    }

    fn absolute(&self, val: u64) {
        self.0.set(val as f64);
    }
}

impl HistogramOps for MetricsHistogram {
    fn record(&self, val: f64) {
        self.0.record(val);
    }
}

fn labeled(
    cache_name: &'static str,
    label_names: &'static [&'static str],
    labels: &[Cow<'static, str>],
) -> Vec<::metrics::Label> {
    assert_eq!(label_names.len(), labels.len());
    label_names
        .iter()
        .zip(labels)
        .map(|(name, value)| ::metrics::Label::new(*name, value.clone().into_owned()))
        .chain(std::iter::once(::metrics::Label::new("cache", cache_name)))
        .collect()
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use metrics_util::debugging::{DebugValue, DebuggingRecorder};

    use super::*;

    #[test]
    fn test_histogram_buckets_are_registered_with_exporter() {
        let configured: HashMap<_, _> = quickwit_metrics::histogram_buckets().collect();
        for name in [
            "feuer_lookup_duration_seconds",
            "feuer_disk_read_size_bytes",
            "feuer_disk_lookup_duration_seconds",
            "feuer_disk_io_duration_seconds",
            "feuer_disk_write_queue_duration_seconds",
            "feuer_disk_read_queue_wait_duration_seconds",
        ] {
            assert_eq!(configured[name], feuer_histogram_buckets(name));
        }
    }

    #[test]
    fn test_registry_preserves_values_and_labels_across_caches() {
        let recorder = DebuggingRecorder::new();
        let snapshotter = recorder.snapshotter();
        let caches = [("split_range", 1), ("partial_request", 2), ("predicate", 3)];
        ::metrics::with_local_recorder(&recorder, || {
            for (cache_name, value) in caches {
                let registry = QuickwitMetricsRegistry { cache_name };
                registry
                    .register_counter_vec(
                        "feuer_lookup_total".into(),
                        "Lookups".into(),
                        &["outcome"],
                    )
                    .counter(&["memory_hit".into()])
                    .increase(value);
                let gauge = registry
                    .register_gauge_vec("feuer_memory_used_bytes".into(), "Memory".into(), &[])
                    .gauge(&[]);
                gauge.absolute(value);
                gauge.increase(3);
                gauge.decrease(2);
                registry
                    .register_histogram_vec_with_buckets(
                        "feuer_lookup_duration_seconds".into(),
                        "Lookup duration".into(),
                        &["outcome"],
                        feuer_histogram_buckets("feuer_lookup_duration_seconds"),
                    )
                    .histogram(&["disk_hit".into()])
                    .record(value as f64);
            }
        });
        let values: HashMap<_, _> = snapshotter
            .snapshot()
            .into_vec()
            .into_iter()
            .map(|(key, _, _, value)| (key.key().clone(), value))
            .collect();
        assert_eq!(values.len(), caches.len() * 3);
        for (cache_name, value) in caches {
            for (name, outcome, expected) in [
                (
                    "feuer_lookup_total",
                    Some("memory_hit"),
                    DebugValue::Counter(value),
                ),
                (
                    "feuer_memory_used_bytes",
                    None,
                    DebugValue::Gauge(((value + 1) as f64).into()),
                ),
                (
                    "feuer_lookup_duration_seconds",
                    Some("disk_hit"),
                    DebugValue::Histogram(vec![(value as f64).into()]),
                ),
            ] {
                let mut labels = Vec::new();
                if let Some(outcome) = outcome {
                    labels.push(::metrics::Label::new("outcome", outcome));
                }
                labels.push(::metrics::Label::new("cache", cache_name));
                assert_eq!(values[&::metrics::Key::from_parts(name, labels)], expected);
            }
        }
    }

    #[cfg(target_os = "linux")]
    #[tokio::test]
    async fn test_native_metric_registration_matches_exporter() {
        let configured: HashMap<_, _> = quickwit_metrics::histogram_buckets()
            .filter(|(name, _)| name.starts_with("feuer_"))
            .map(|(name, buckets)| (name.to_owned(), buckets))
            .collect();
        let recorder = DebuggingRecorder::new();
        let snapshotter = recorder.snapshotter();
        let _guard = ::metrics::set_default_local_recorder(&recorder);
        let temp_dir = tempfile::tempdir().unwrap();
        let _cache = super::super::FeuerSplitRangeCache::open(&super::super::config_for_test(
            temp_dir.path(),
        ))
        .await
        .unwrap();
        let mut registered = HashMap::new();
        let mut pool_statuses = std::collections::HashSet::new();
        for (key, _, _, value) in snapshotter.snapshot().into_vec() {
            let key = key.key();
            let name = key.name();
            if !name.starts_with("feuer_") {
                continue;
            }
            assert!(
                key.labels()
                    .any(|label| label.key() == "cache" && label.value() == "split_range")
            );
            if matches!(value, DebugValue::Histogram(_)) {
                registered.insert(name.to_owned(), feuer_histogram_buckets(name));
            }
            if name == "feuer_io_buffer_pool_bytes" {
                assert!(matches!(value, DebugValue::Gauge(_)));
                assert!(
                    key.labels()
                        .any(|label| label.key() == "bucket" && !label.value().is_empty())
                );
                let status = key.labels().find(|label| label.key() == "status").unwrap();
                pool_statuses.insert(status.value().to_owned());
            }
        }
        assert!(!registered.is_empty());
        assert_eq!(registered, configured);
        assert_eq!(pool_statuses, ["idle".to_owned(), "used".to_owned()].into());
    }
}
