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

use mixtrics::metrics::{
    BoxedCounter, BoxedCounterVec, BoxedGauge, BoxedGaugeVec, BoxedHistogram, BoxedHistogramVec,
    CounterOps, CounterVecOps, GaugeOps, GaugeVecOps, HistogramOps, HistogramVecOps, RegistryOps,
};
use quickwit_metrics::MetricKind;

#[path = "metric_declarations.rs"]
mod declarations;
#[cfg(test)]
#[path = "metrics_tests.rs"]
mod tests;

// Create metrics from Quickwit parents, inheriting global labels. Each declaration appears
// once in the inventory, even when Feuer registers it from several tiers.
#[derive(Debug)]
enum CreateMetric {
    Counter(fn(&'static str, &[Cow<'static, str>]) -> BoxedCounter),
    Gauge(fn(&'static str, &[Cow<'static, str>]) -> BoxedGauge),
    Histogram(fn(&'static str, &[Cow<'static, str>]) -> BoxedHistogram),
}

#[derive(Debug)]
struct Declaration {
    name: &'static str,
    description: &'static str,
    kind: MetricKind,
    label_names: &'static [&'static str],
    buckets: Option<fn() -> Vec<f64>>,
    create_metric: CreateMetric,
}

impl Declaration {
    fn lookup(name: &str) -> &'static Self {
        declarations::DECLARATIONS
            .iter()
            .find(|declaration| declaration.name == name)
            .unwrap_or_else(|| panic!("unregistered Feuer metric: {name}"))
    }

    fn validate(&self, kind: MetricKind, description: &str, label_names: &[&str]) {
        assert_eq!(self.kind, kind, "Feuer metric type changed: {}", self.name);
        assert_eq!(
            self.label_names, label_names,
            "Feuer label schema changed: {}",
            self.name
        );
        assert_eq!(
            self.description, description,
            "Feuer description changed: {}",
            self.name
        );
    }
}

/// Forward Feuer metrics through Quickwit's metric declarations.
///
/// Install the recorder and global labels before the first registration. Parents register
/// series with global labels. Children receive observations with Feuer's labels and the
/// caller-provided `cache` label.
#[derive(Debug)]
pub(crate) struct QuickwitMetricsRegistry {
    pub(crate) cache_name: &'static str,
}

impl QuickwitMetricsRegistry {
    fn register(
        &self,
        name: &str,
        description: &str,
        label_names: &[&str],
        kind: MetricKind,
    ) -> MetricVec {
        let declaration = Declaration::lookup(name);
        declaration.validate(kind, description, label_names);
        MetricVec {
            declaration,
            cache_name: self.cache_name,
        }
    }
}

impl RegistryOps for QuickwitMetricsRegistry {
    fn register_counter_vec(
        &self,
        name: Cow<'static, str>,
        desc: Cow<'static, str>,
        label_names: &'static [&'static str],
    ) -> BoxedCounterVec {
        Box::new(self.register(&name, &desc, label_names, MetricKind::Counter))
    }

    fn register_gauge_vec(
        &self,
        name: Cow<'static, str>,
        desc: Cow<'static, str>,
        label_names: &'static [&'static str],
    ) -> BoxedGaugeVec {
        Box::new(self.register(&name, &desc, label_names, MetricKind::Gauge))
    }

    fn register_histogram_vec(
        &self,
        name: Cow<'static, str>,
        _desc: Cow<'static, str>,
        _label_names: &'static [&'static str],
    ) -> BoxedHistogramVec {
        panic!("Feuer histogram requires explicit buckets: {name}");
    }

    fn register_histogram_vec_with_buckets(
        &self,
        name: Cow<'static, str>,
        desc: Cow<'static, str>,
        label_names: &'static [&'static str],
        buckets: Vec<f64>,
    ) -> BoxedHistogramVec {
        let metric = self.register(&name, &desc, label_names, MetricKind::Histogram);
        let declared_buckets = metric
            .declaration
            .buckets
            .expect("histogram must declare buckets");
        assert_eq!(
            declared_buckets(),
            buckets,
            "Feuer histogram buckets changed: {name}"
        );
        Box::new(metric)
    }
}

#[derive(Debug)]
struct MetricVec {
    declaration: &'static Declaration,
    cache_name: &'static str,
}

impl CounterVecOps for MetricVec {
    fn counter(&self, label_values: &[Cow<'static, str>]) -> BoxedCounter {
        let CreateMetric::Counter(create) = self.declaration.create_metric else {
            panic!("expected counter: {}", self.declaration.name);
        };
        create(self.cache_name, label_values)
    }
}

impl GaugeVecOps for MetricVec {
    fn gauge(&self, label_values: &[Cow<'static, str>]) -> BoxedGauge {
        let CreateMetric::Gauge(create) = self.declaration.create_metric else {
            panic!("expected gauge: {}", self.declaration.name);
        };
        create(self.cache_name, label_values)
    }
}

impl HistogramVecOps for MetricVec {
    fn histogram(&self, label_values: &[Cow<'static, str>]) -> BoxedHistogram {
        let CreateMetric::Histogram(create) = self.declaration.create_metric else {
            panic!("expected histogram: {}", self.declaration.name);
        };
        create(self.cache_name, label_values)
    }
}

#[derive(Debug)]
struct MetricsCounter(quickwit_metrics::Counter);
#[derive(Debug)]
struct MetricsGauge(quickwit_metrics::Gauge);
#[derive(Debug)]
struct MetricsHistogram(quickwit_metrics::Histogram);

impl CounterOps for MetricsCounter {
    fn increase(&self, val: u64) {
        self.0.inc_by(val);
    }
}

impl GaugeOps for MetricsGauge {
    fn increase(&self, val: u64) {
        self.0.inc_by(val as f64);
    }

    fn decrease(&self, val: u64) {
        self.0.dec_by(val as f64);
    }

    fn absolute(&self, val: u64) {
        self.0.set(val as f64);
    }
}

impl HistogramOps for MetricsHistogram {
    fn record(&self, val: f64) {
        self.0.observe(val);
    }
}
