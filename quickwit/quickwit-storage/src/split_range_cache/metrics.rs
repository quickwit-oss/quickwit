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
#[path = "metrics_tests.rs"]
mod tests;
