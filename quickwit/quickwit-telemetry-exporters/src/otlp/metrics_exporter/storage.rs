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
use std::sync::Arc;
use std::sync::atomic::Ordering;

use metrics::atomics::AtomicU64;
use metrics::{CounterFn, GaugeFn, HistogramFn, Key, Unit};
use metrics_util::MetricKind;
use metrics_util::registry::Storage;
use opentelemetry::KeyValue;
use opentelemetry::metrics::{
    Counter as OtelCounterInstrument, Histogram as OtelHistogramInstrument, ObservableGauge,
};
use portable_atomic::AtomicF64;

use super::state::OtlpMetricMetadata;

pub(super) struct OtlpMetricStorage {
    meter: opentelemetry::metrics::Meter,
    metadata: OtlpMetricMetadata,
}

impl OtlpMetricStorage {
    pub(super) fn new(meter: opentelemetry::metrics::Meter, metadata: OtlpMetricMetadata) -> Self {
        Self { meter, metadata }
    }

    fn attributes(key: &Key) -> Vec<KeyValue> {
        key.labels()
            .map(|label| {
                let (key, value) = label.clone().into_parts();
                let key: Cow<'static, str> = key.into();
                let value: Cow<'static, str> = value.into();
                KeyValue::new(key, value)
            })
            .collect()
    }
}

impl Storage<Key> for OtlpMetricStorage {
    type Counter = Arc<OtlpCounter>;
    type Gauge = Arc<OtlpGauge>;
    type Histogram = Arc<OtlpHistogram>;

    fn counter(&self, key: &Key) -> Self::Counter {
        let key_name = key.name_shared();
        let mut builder = self.meter.u64_counter(key_name.clone().into_inner());
        if let Some(description) = self
            .metadata
            .get_description(&key_name, MetricKind::Counter)
        {
            builder = builder.with_description(description.description);
            if let Some(unit) = description.unit {
                builder = builder.with_unit(unit_as_ucum_label(unit));
            }
        }
        Arc::new(OtlpCounter::new(builder.build(), Self::attributes(key)))
    }

    fn gauge(&self, key: &Key) -> Self::Gauge {
        let key_name = key.name_shared();
        let mut builder = self
            .meter
            .f64_observable_gauge(key_name.clone().into_inner());
        if let Some(description) = self.metadata.get_description(&key_name, MetricKind::Gauge) {
            builder = builder.with_description(description.description);
            if let Some(unit) = description.unit {
                builder = builder.with_unit(unit_as_ucum_label(unit));
            }
        }

        let attributes = Self::attributes(key);
        let value = Arc::new(AtomicF64::new(0.0));
        let observed_value = value.clone();
        let otel_gauge = builder
            .with_callback(move |observer| {
                observer.observe(observed_value.load(Ordering::Acquire), &attributes);
            })
            .build();
        Arc::new(OtlpGauge {
            value,
            _otel_gauge: otel_gauge,
        })
    }

    fn histogram(&self, key: &Key) -> Self::Histogram {
        let key_name = key.name_shared();
        let mut builder = self.meter.f64_histogram(key_name.clone().into_inner());
        if let Some(description) = self
            .metadata
            .get_description(&key_name, MetricKind::Histogram)
        {
            builder = builder.with_description(description.description);
            if let Some(unit) = description.unit {
                builder = builder.with_unit(unit_as_ucum_label(unit));
            }
        }
        if let Some(bounds) = self.metadata.get_histogram_bounds(&key_name) {
            builder = builder.with_boundaries(bounds);
        }
        Arc::new(OtlpHistogram {
            histogram: builder.build(),
            attributes: Self::attributes(key),
        })
    }
}

pub(super) struct OtlpCounter {
    counter: OtelCounterInstrument<u64>,
    value: AtomicU64,
    attributes: Vec<KeyValue>,
}

impl OtlpCounter {
    fn new(counter: OtelCounterInstrument<u64>, attributes: Vec<KeyValue>) -> Self {
        Self {
            counter,
            value: AtomicU64::new(0),
            attributes,
        }
    }
}

impl CounterFn for OtlpCounter {
    fn increment(&self, value: u64) {
        self.value.fetch_add(value, Ordering::Release);
        self.counter.add(value, &self.attributes);
    }

    fn absolute(&self, value: u64) {
        let previous = self.value.fetch_max(value, Ordering::AcqRel);
        if value > previous {
            self.counter.add(value - previous, &self.attributes);
        }
    }
}

pub(super) struct OtlpGauge {
    value: Arc<AtomicF64>,
    _otel_gauge: ObservableGauge<f64>,
}

impl GaugeFn for OtlpGauge {
    fn increment(&self, value: f64) {
        self.value.fetch_add(value, Ordering::AcqRel);
    }

    fn decrement(&self, value: f64) {
        self.value.fetch_sub(value, Ordering::AcqRel);
    }

    fn set(&self, value: f64) {
        self.value.store(value, Ordering::Release);
    }
}

pub(super) struct OtlpHistogram {
    histogram: OtelHistogramInstrument<f64>,
    attributes: Vec<KeyValue>,
}

impl HistogramFn for OtlpHistogram {
    fn record(&self, value: f64) {
        self.histogram.record(value, &self.attributes);
    }
}

fn unit_as_ucum_label(unit: Unit) -> &'static str {
    match unit {
        Unit::Count => "1",
        Unit::Percent => "%",
        Unit::Seconds => "s",
        Unit::Milliseconds => "ms",
        Unit::Microseconds => "us",
        Unit::Nanoseconds => "ns",
        Unit::Tebibytes => "TiBy",
        Unit::Gibibytes => "GiBy",
        Unit::Mebibytes => "MiBy",
        Unit::Kibibytes => "KiBy",
        Unit::Bytes => "By",
        Unit::TerabitsPerSecond => "Tbit/s",
        Unit::GigabitsPerSecond => "Gbit/s",
        Unit::MegabitsPerSecond => "Mbit/s",
        Unit::KilobitsPerSecond => "kbit/s",
        Unit::BitsPerSecond => "bit/s",
        Unit::CountPerSecond => "1/s",
    }
}
