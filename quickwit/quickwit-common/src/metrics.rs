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

use std::collections::{BTreeMap, HashMap};
use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};
use std::sync::{Arc, LazyLock, Mutex, OnceLock};
use std::time::Instant;

use opentelemetry::metrics::Meter;
use opentelemetry::KeyValue;
pub use prometheus::{exponential_buckets, linear_buckets};
use prometheus::{HistogramOpts, Opts, TextEncoder};

static OTEL_METER: OnceLock<Meter> = OnceLock::new();
const METRICS_NAMESPACE: &str = "quickwit";
type DeferredOtelBinding = Arc<dyn DeferredOtelInstrument + Send + Sync>;
static OTEL_PENDING_INSTRUMENTS: LazyLock<Mutex<Vec<DeferredOtelBinding>>> =
    LazyLock::new(|| Mutex::new(Vec::new()));

pub fn install_otel_meter(meter: Meter) {
    OTEL_METER
        .set(meter)
        .expect("OTel meter should only be installed once");
    let meter = otel_meter().expect("OTel meter should be installed");
    let pending_instruments = {
        let mut pending_instruments = OTEL_PENDING_INSTRUMENTS
            .lock()
            .expect("OTel pending instruments mutex should not be poisoned");
        std::mem::take(&mut *pending_instruments)
    };
    for instrument in pending_instruments {
        instrument.bind(meter);
    }
}

fn otel_meter() -> Option<&'static Meter> {
    OTEL_METER.get()
}

fn otel_metric_name(name: &str, subsystem: &str) -> String {
    if subsystem.is_empty() {
        format!("{METRICS_NAMESPACE}_{name}")
    } else {
        format!("{METRICS_NAMESPACE}_{subsystem}_{name}")
    }
}

fn build_const_attributes(const_labels: &[(&str, &str)]) -> Vec<KeyValue> {
    const_labels
        .iter()
        .map(|(k, v)| KeyValue::new(k.to_string(), v.to_string()))
        .collect()
}

fn owned_const_labels(const_labels: &[(&str, &str)]) -> HashMap<String, String> {
    const_labels
        .iter()
        .map(|(k, v)| (k.to_string(), v.to_string()))
        .collect()
}

fn build_label_attributes<const N: usize>(
    base_attributes: &[KeyValue],
    label_names: &[String],
    label_values: [&str; N],
) -> (Vec<String>, Vec<KeyValue>) {
    let key = label_values
        .iter()
        .map(|label_value| label_value.to_string())
        .collect();
    let mut attributes = base_attributes.to_vec();
    for (label_name, label_value) in label_names.iter().zip(label_values.iter()) {
        attributes.push(KeyValue::new(label_name.clone(), label_value.to_string()));
    }
    (key, attributes)
}

// ---------------------------------------------------------------------------
// Deferred OTel instrument binding
//
// OTel instruments are bound at creation time when the meter is already
// available. Metrics created before `install_otel_meter` are registered in a
// central pending list and bound in one shot when the meter becomes available.
// This keeps deferred initialization out of the hot path.
// ---------------------------------------------------------------------------

trait DeferredOtelInstrument {
    fn bind(&self, meter: &Meter);
}

struct OtelState<T> {
    build_instrument: Box<dyn Fn(&Meter) -> T + Send + Sync>,
    instrument: OnceLock<T>,
}

impl<T> OtelState<T> {
    fn new(build_instrument: impl Fn(&Meter) -> T + Send + Sync + 'static) -> Self {
        OtelState {
            build_instrument: Box::new(build_instrument),
            instrument: OnceLock::new(),
        }
    }

    fn bind_if_needed(&self, meter: &Meter) -> Option<&T> {
        Some(
            self.instrument
                .get_or_init(|| (self.build_instrument)(meter)),
        )
    }

    fn get(&self) -> Option<&T> {
        self.instrument.get()
    }
}

impl<T> DeferredOtelInstrument for OtelState<T>
where
    T: Send + Sync + 'static,
{
    fn bind(&self, meter: &Meter) {
        let _ = self.bind_if_needed(meter);
    }
}

#[derive(Clone)]
struct OtelMetric<T> {
    state: Option<Arc<OtelState<T>>>,
    attributes: Vec<KeyValue>,
}

impl<T> OtelMetric<T> {
    fn new(state: Option<Arc<OtelState<T>>>, attributes: Vec<KeyValue>) -> Self {
        Self { state, attributes }
    }

    fn with_attributes<const N: usize>(&self, names: &[String], values: [&str; N]) -> Self {
        if self.state.is_none() {
            return Self::default();
        }
        let mut attributes = self.attributes.clone();
        for (name, value) in names.iter().zip(values.iter()) {
            attributes.push(KeyValue::new(name.clone(), value.to_string()));
        }
        Self {
            state: self.state.clone(),
            attributes,
        }
    }

    /// Invokes a recording operation (e.g. `counter.add`) on the
    /// bound OTel instrument, passing in this metric's attributes.
    /// No-ops when OTel is disabled or the instrument has not been
    /// bound yet through `install_otel_meter`.
    fn with_instrument(&self, f: impl FnOnce(&T, &[KeyValue])) {
        let Some(state) = self.state.as_ref() else {
            return;
        };
        if let Some(instrument) = state.get() {
            f(instrument, &self.attributes);
        }
    }
}

impl<T> Default for OtelMetric<T> {
    fn default() -> Self {
        Self {
            state: None,
            attributes: Vec::new(),
        }
    }
}

impl OtelMetric<opentelemetry::metrics::Counter<u64>> {
    fn add(&self, value: u64) {
        self.with_instrument(|counter, attributes| counter.add(value, attributes));
    }
}

impl OtelMetric<opentelemetry::metrics::Histogram<f64>> {
    fn record(&self, value: f64) {
        self.with_instrument(|histogram, attributes| histogram.record(value, attributes));
    }
}

struct ObservableI64GaugeEntry {
    value: AtomicI64,
    attributes: Vec<KeyValue>,
}

impl ObservableI64GaugeEntry {
    fn new(attributes: Vec<KeyValue>) -> Self {
        Self {
            value: AtomicI64::new(0),
            attributes,
        }
    }
}

struct ObservableI64GaugeState {
    name: String,
    description: String,
    entries: Arc<Mutex<HashMap<Vec<String>, Arc<ObservableI64GaugeEntry>>>>,
    bound: OnceLock<()>,
}

impl ObservableI64GaugeState {
    fn new(name: String, description: String) -> Self {
        Self {
            name,
            description,
            entries: Arc::new(Mutex::new(HashMap::new())),
            bound: OnceLock::new(),
        }
    }

    fn get_or_create_entry(
        &self,
        key: Vec<String>,
        attributes: Vec<KeyValue>,
    ) -> Arc<ObservableI64GaugeEntry> {
        let mut entries = self
            .entries
            .lock()
            .expect("OTel i64 gauge entries mutex should not be poisoned");
        entries
            .entry(key)
            .or_insert_with(|| Arc::new(ObservableI64GaugeEntry::new(attributes)))
            .clone()
    }
}

impl DeferredOtelInstrument for ObservableI64GaugeState {
    fn bind(&self, meter: &Meter) {
        self.bound.get_or_init(|| {
            let entries = Arc::clone(&self.entries);
            let _ = meter
                .i64_observable_gauge(self.name.clone())
                .with_description(self.description.clone())
                .with_callback(move |observer| {
                    let entries: Vec<_> = entries
                        .lock()
                        .expect("OTel i64 gauge entries mutex should not be poisoned")
                        .values()
                        .cloned()
                        .collect();
                    for entry in entries {
                        observer.observe(entry.value.load(Ordering::Relaxed), &entry.attributes);
                    }
                })
                .build();
        });
    }
}

struct ObservableF64GaugeEntry {
    value_bits: AtomicU64,
    attributes: Vec<KeyValue>,
}

impl ObservableF64GaugeEntry {
    fn new(attributes: Vec<KeyValue>) -> Self {
        Self {
            value_bits: AtomicU64::new(0.0f64.to_bits()),
            attributes,
        }
    }
}

struct ObservableF64GaugeState {
    name: String,
    description: String,
    entries: Arc<Mutex<HashMap<Vec<String>, Arc<ObservableF64GaugeEntry>>>>,
    bound: OnceLock<()>,
}

impl ObservableF64GaugeState {
    fn new(name: String, description: String) -> Self {
        Self {
            name,
            description,
            entries: Arc::new(Mutex::new(HashMap::new())),
            bound: OnceLock::new(),
        }
    }

    fn get_or_create_entry(
        &self,
        key: Vec<String>,
        attributes: Vec<KeyValue>,
    ) -> Arc<ObservableF64GaugeEntry> {
        let mut entries = self
            .entries
            .lock()
            .expect("OTel f64 gauge entries mutex should not be poisoned");
        entries
            .entry(key)
            .or_insert_with(|| Arc::new(ObservableF64GaugeEntry::new(attributes)))
            .clone()
    }
}

impl DeferredOtelInstrument for ObservableF64GaugeState {
    fn bind(&self, meter: &Meter) {
        self.bound.get_or_init(|| {
            let entries = Arc::clone(&self.entries);
            let _ = meter
                .f64_observable_gauge(self.name.clone())
                .with_description(self.description.clone())
                .with_callback(move |observer| {
                    let entries: Vec<_> = entries
                        .lock()
                        .expect("OTel f64 gauge entries mutex should not be poisoned")
                        .values()
                        .cloned()
                        .collect();
                    for entry in entries {
                        observer.observe(
                            f64::from_bits(entry.value_bits.load(Ordering::Relaxed)),
                            &entry.attributes,
                        );
                    }
                })
                .build();
        });
    }
}

// ---------------------------------------------------------------------------
// Wrapper types
// ---------------------------------------------------------------------------

#[derive(Clone)]
pub struct IntCounter {
    prometheus: prometheus::IntCounter,
    otel: OtelMetric<opentelemetry::metrics::Counter<u64>>,
}

impl std::fmt::Debug for IntCounter {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        f.debug_struct("IntCounter")
            .field("value", &self.prometheus.get())
            .finish()
    }
}

impl IntCounter {
    /// Creates an IntCounter **without** registering it with Prometheus or
    /// OTel. Use the [`new_counter`] factory for globally-registered counters.
    pub fn new(
        name: &str,
        help: &str,
        subsystem: &str,
        const_labels: &[(&str, &str)],
    ) -> IntCounter {
        let counter_opts = Opts::new(name, help)
            .namespace(METRICS_NAMESPACE)
            .subsystem(subsystem)
            .const_labels(owned_const_labels(const_labels));
        let prom =
            prometheus::IntCounter::with_opts(counter_opts).expect("failed to create counter");
        IntCounter {
            prometheus: prom,
            otel: OtelMetric::default(),
        }
    }

    pub fn inc(&self) {
        self.prometheus.inc();
        self.otel.add(1);
    }

    pub fn inc_by(&self, v: u64) {
        self.prometheus.inc_by(v);
        self.otel.add(v);
    }

    pub fn get(&self) -> u64 {
        self.prometheus.get()
    }
}

#[derive(Clone)]
pub struct IntCounterVec<const N: usize> {
    prometheus: prometheus::IntCounterVec,
    otel: OtelMetric<opentelemetry::metrics::Counter<u64>>,
    label_names: Vec<String>,
}

impl<const N: usize> IntCounterVec<N> {
    /// Creates an IntCounterVec **without** registering it with Prometheus or
    /// OTel. Use the [`new_counter_vec`] factory for globally-registered
    /// counters.
    pub fn new(
        name: &str,
        help: &str,
        subsystem: &str,
        const_labels: &[(&str, &str)],
        label_names: [&str; N],
    ) -> IntCounterVec<N> {
        let counter_opts = Opts::new(name, help)
            .namespace(METRICS_NAMESPACE)
            .subsystem(subsystem)
            .const_labels(owned_const_labels(const_labels));
        let prom = prometheus::IntCounterVec::new(counter_opts, &label_names)
            .expect("failed to create counter vec");

        IntCounterVec {
            prometheus: prom,
            otel: OtelMetric::new(None, build_const_attributes(const_labels)),
            label_names: label_names.iter().map(|s| s.to_string()).collect(),
        }
    }

    pub fn with_label_values(&self, label_values: [&str; N]) -> IntCounter {
        IntCounter {
            prometheus: self.prometheus.with_label_values(&label_values),
            otel: self.otel.with_attributes(&self.label_names, label_values),
        }
    }
}

/// For relative operations (`inc`, `dec`, `add`, `sub`), the OTel value is derived by reading the
/// Prometheus gauge after mutation, since OTel gauges do not support relative updates. This is not
/// atomic: under concurrent updates, the OTel side may briefly record a stale value. This is
/// acceptable for now because gauges are inherently point-in-time approximations, and the next
/// update self-corrects.
///
/// TODO: for strict correctness, manage a single `AtomicI64` as the source of truth and feed its
/// value into both Prometheus and OTel.
#[derive(Clone)]
pub struct IntGauge {
    prometheus: prometheus::IntGauge,
    otel: Arc<ObservableI64GaugeEntry>,
}

impl IntGauge {
    pub fn set(&self, v: i64) {
        self.prometheus.set(v);
        self.otel.value.store(v, Ordering::Relaxed);
    }

    pub fn inc(&self) {
        self.prometheus.inc();
        self.otel.value.fetch_add(1, Ordering::Relaxed);
    }

    pub fn dec(&self) {
        self.prometheus.dec();
        self.otel.value.fetch_sub(1, Ordering::Relaxed);
    }

    pub fn add(&self, delta: i64) {
        self.prometheus.add(delta);
        self.otel.value.fetch_add(delta, Ordering::Relaxed);
    }

    pub fn sub(&self, delta: i64) {
        self.prometheus.sub(delta);
        self.otel.value.fetch_sub(delta, Ordering::Relaxed);
    }

    pub fn get(&self) -> i64 {
        self.prometheus.get()
    }
}

#[derive(Clone)]
pub struct IntGaugeVec<const N: usize> {
    prometheus: prometheus::IntGaugeVec,
    otel_state: Arc<ObservableI64GaugeState>,
    const_attributes: Vec<KeyValue>,
    label_names: Vec<String>,
}

impl<const N: usize> IntGaugeVec<N> {
    pub fn with_label_values(&self, label_values: [&str; N]) -> IntGauge {
        let (key, attributes) =
            build_label_attributes(&self.const_attributes, &self.label_names, label_values);
        IntGauge {
            prometheus: self.prometheus.with_label_values(&label_values),
            otel: self.otel_state.get_or_create_entry(key, attributes),
        }
    }
}

#[derive(Clone)]
pub struct Gauge {
    prometheus: prometheus::Gauge,
    otel: Arc<ObservableF64GaugeEntry>,
}

impl Gauge {
    pub fn set(&self, v: f64) {
        self.prometheus.set(v);
        self.otel.value_bits.store(v.to_bits(), Ordering::Relaxed);
    }

    pub fn get(&self) -> f64 {
        self.prometheus.get()
    }
}

#[derive(Clone)]
pub struct Histogram {
    prometheus: prometheus::Histogram,
    otel: OtelMetric<opentelemetry::metrics::Histogram<f64>>,
}

impl Histogram {
    pub fn observe(&self, v: f64) {
        self.prometheus.observe(v);
        self.otel.record(v);
    }

    pub fn start_timer(&self) -> HistogramTimer {
        HistogramTimer {
            histogram: Some(self.clone()),
            start: Instant::now(),
        }
    }
}

pub struct HistogramTimer {
    histogram: Option<Histogram>,
    start: Instant,
}

impl HistogramTimer {
    pub fn observe_duration(mut self) {
        if let Some(histogram) = self.histogram.take() {
            histogram.observe(self.start.elapsed().as_secs_f64());
        }
    }
}

impl Drop for HistogramTimer {
    fn drop(&mut self) {
        if let Some(histogram) = self.histogram.take() {
            histogram.observe(self.start.elapsed().as_secs_f64());
        }
    }
}

#[derive(Clone)]
pub struct HistogramVec<const N: usize> {
    prometheus: prometheus::HistogramVec,
    otel: OtelMetric<opentelemetry::metrics::Histogram<f64>>,
    label_names: Vec<String>,
}

impl<const N: usize> HistogramVec<N> {
    pub fn with_label_values(&self, label_values: [&str; N]) -> Histogram {
        Histogram {
            prometheus: self.prometheus.with_label_values(&label_values),
            otel: self.otel.with_attributes(&self.label_names, label_values),
        }
    }
}

// ---------------------------------------------------------------------------
// Factory functions
// ---------------------------------------------------------------------------

pub fn register_info(name: &'static str, help: &'static str, kvs: BTreeMap<&'static str, String>) {
    let mut counter_opts = Opts::new(name, help).namespace(METRICS_NAMESPACE);
    for (k, v) in kvs {
        counter_opts = counter_opts.const_label(k, v);
    }
    let counter =
        prometheus::IntCounter::with_opts(counter_opts).expect("failed to create counter");
    counter.inc();
    prometheus::register(Box::new(counter)).expect("failed to register counter");
}

fn new_otel_state<T>(
    name: &str,
    subsystem: &str,
    help: &str,
    build_instrument: impl Fn(&Meter, &str, &str) -> T + Send + Sync + 'static,
) -> Arc<OtelState<T>>
where
    T: Send + Sync + 'static,
{
    let name = otel_metric_name(name, subsystem);
    let description = help.to_string();
    let state = Arc::new(OtelState::new(move |meter| {
        build_instrument(meter, &name, &description)
    }));
    bind_or_defer_otel_instrument(state.clone());
    state
}

fn bind_or_defer_otel_instrument(instrument: DeferredOtelBinding) {
    if let Some(meter) = otel_meter() {
        instrument.bind(meter);
    } else {
        let mut pending_instruments = OTEL_PENDING_INSTRUMENTS
            .lock()
            .expect("OTel pending instruments mutex should not be poisoned");
        pending_instruments.push(instrument);
    }
}

fn new_counter_otel_state(
    name: &str,
    subsystem: &str,
    help: &str,
) -> Arc<OtelState<opentelemetry::metrics::Counter<u64>>> {
    new_otel_state(name, subsystem, help, |meter, name, description| {
        meter
            .u64_counter(name.to_string())
            .with_description(description.to_string())
            .build()
    })
}

fn new_int_gauge_otel_state(
    name: &str,
    subsystem: &str,
    help: &str,
) -> Arc<ObservableI64GaugeState> {
    let state = Arc::new(ObservableI64GaugeState::new(
        otel_metric_name(name, subsystem),
        help.to_string(),
    ));
    bind_or_defer_otel_instrument(state.clone());
    state
}

fn new_float_gauge_otel_state(
    name: &str,
    subsystem: &str,
    help: &str,
) -> Arc<ObservableF64GaugeState> {
    let state = Arc::new(ObservableF64GaugeState::new(
        otel_metric_name(name, subsystem),
        help.to_string(),
    ));
    bind_or_defer_otel_instrument(state.clone());
    state
}

fn new_histogram_otel_state(
    name: &str,
    subsystem: &str,
    help: &str,
    boundaries: Vec<f64>,
) -> Arc<OtelState<opentelemetry::metrics::Histogram<f64>>> {
    new_otel_state(name, subsystem, help, move |meter, name, description| {
        meter
            .f64_histogram(name.to_string())
            .with_description(description.to_string())
            .with_boundaries(boundaries.clone())
            .build()
    })
}

pub fn new_counter(
    name: &str,
    help: &str,
    subsystem: &str,
    const_labels: &[(&str, &str)],
) -> IntCounter {
    let counter_opts = Opts::new(name, help)
        .namespace(METRICS_NAMESPACE)
        .subsystem(subsystem)
        .const_labels(owned_const_labels(const_labels));
    let prom = prometheus::IntCounter::with_opts(counter_opts).expect("failed to create counter");
    prometheus::register(Box::new(prom.clone())).expect("failed to register counter");

    IntCounter {
        prometheus: prom,
        otel: OtelMetric::new(
            Some(new_counter_otel_state(name, subsystem, help)),
            build_const_attributes(const_labels),
        ),
    }
}

pub fn new_counter_vec<const N: usize>(
    name: &str,
    help: &str,
    subsystem: &str,
    const_labels: &[(&str, &str)],
    label_names: [&str; N],
) -> IntCounterVec<N> {
    let counter_opts = Opts::new(name, help)
        .namespace(METRICS_NAMESPACE)
        .subsystem(subsystem)
        .const_labels(owned_const_labels(const_labels));
    let prom = prometheus::IntCounterVec::new(counter_opts, &label_names)
        .expect("failed to create counter vec");
    prometheus::register(Box::new(prom.clone())).expect("failed to register counter vec");

    IntCounterVec {
        prometheus: prom,
        otel: OtelMetric::new(
            Some(new_counter_otel_state(name, subsystem, help)),
            build_const_attributes(const_labels),
        ),
        label_names: label_names.iter().map(|s| s.to_string()).collect(),
    }
}

pub fn new_gauge(
    name: &str,
    help: &str,
    subsystem: &str,
    const_labels: &[(&str, &str)],
) -> IntGauge {
    let const_attributes = build_const_attributes(const_labels);
    let gauge_opts = Opts::new(name, help)
        .namespace(METRICS_NAMESPACE)
        .subsystem(subsystem)
        .const_labels(owned_const_labels(const_labels));
    let prom = prometheus::IntGauge::with_opts(gauge_opts).expect("failed to create gauge");
    prometheus::register(Box::new(prom.clone())).expect("failed to register gauge");
    let otel_state = new_int_gauge_otel_state(name, subsystem, help);

    IntGauge {
        prometheus: prom,
        otel: otel_state.get_or_create_entry(Vec::new(), const_attributes),
    }
}

pub fn new_gauge_vec<const N: usize>(
    name: &str,
    help: &str,
    subsystem: &str,
    const_labels: &[(&str, &str)],
    label_names: [&str; N],
) -> IntGaugeVec<N> {
    let const_attributes = build_const_attributes(const_labels);
    let gauge_opts = Opts::new(name, help)
        .namespace(METRICS_NAMESPACE)
        .subsystem(subsystem)
        .const_labels(owned_const_labels(const_labels));
    let prom =
        prometheus::IntGaugeVec::new(gauge_opts, &label_names).expect("failed to create gauge vec");
    prometheus::register(Box::new(prom.clone())).expect("failed to register gauge vec");

    IntGaugeVec {
        prometheus: prom,
        otel_state: new_int_gauge_otel_state(name, subsystem, help),
        const_attributes,
        label_names: label_names.iter().map(|s| s.to_string()).collect(),
    }
}

pub fn new_float_gauge(
    name: &str,
    help: &str,
    subsystem: &str,
    const_labels: &[(&str, &str)],
) -> Gauge {
    let const_attributes = build_const_attributes(const_labels);
    let gauge_opts = Opts::new(name, help)
        .namespace(METRICS_NAMESPACE)
        .subsystem(subsystem)
        .const_labels(owned_const_labels(const_labels));
    let prom = prometheus::Gauge::with_opts(gauge_opts).expect("failed to create float gauge");
    prometheus::register(Box::new(prom.clone())).expect("failed to register float gauge");
    let otel_state = new_float_gauge_otel_state(name, subsystem, help);

    Gauge {
        prometheus: prom,
        otel: otel_state.get_or_create_entry(Vec::new(), const_attributes),
    }
}

pub fn new_histogram(name: &str, help: &str, subsystem: &str, buckets: Vec<f64>) -> Histogram {
    let histogram_opts = HistogramOpts::new(name, help)
        .namespace(METRICS_NAMESPACE)
        .subsystem(subsystem)
        .buckets(buckets.clone());
    let prom =
        prometheus::Histogram::with_opts(histogram_opts).expect("failed to create histogram");
    prometheus::register(Box::new(prom.clone())).expect("failed to register histogram");

    Histogram {
        prometheus: prom,
        otel: OtelMetric::new(
            Some(new_histogram_otel_state(
                name,
                subsystem,
                help,
                buckets.clone(),
            )),
            Vec::new(),
        ),
    }
}

pub fn new_histogram_vec<const N: usize>(
    name: &str,
    help: &str,
    subsystem: &str,
    const_labels: &[(&str, &str)],
    label_names: [&str; N],
    buckets: Vec<f64>,
) -> HistogramVec<N> {
    let histogram_opts = HistogramOpts::new(name, help)
        .namespace(METRICS_NAMESPACE)
        .subsystem(subsystem)
        .const_labels(owned_const_labels(const_labels))
        .buckets(buckets.clone());
    let prom = prometheus::HistogramVec::new(histogram_opts, &label_names)
        .expect("failed to create histogram vec");
    prometheus::register(Box::new(prom.clone())).expect("failed to register histogram vec");

    HistogramVec {
        prometheus: prom,
        otel: OtelMetric::new(
            Some(new_histogram_otel_state(
                name,
                subsystem,
                help,
                buckets.clone(),
            )),
            build_const_attributes(const_labels),
        ),
        label_names: label_names.iter().map(|s| s.to_string()).collect(),
    }
}

// ---------------------------------------------------------------------------
// Guard types
// ---------------------------------------------------------------------------

pub struct GaugeGuard<'a> {
    gauge: &'a IntGauge,
    delta: i64,
}

impl std::fmt::Debug for GaugeGuard<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        self.delta.fmt(f)
    }
}

impl<'a> GaugeGuard<'a> {
    pub fn from_gauge(gauge: &'a IntGauge) -> Self {
        Self { gauge, delta: 0i64 }
    }

    pub fn get(&self) -> i64 {
        self.delta
    }

    pub fn add(&mut self, delta: i64) {
        self.gauge.add(delta);
        self.delta += delta;
    }

    pub fn sub(&mut self, delta: i64) {
        self.gauge.sub(delta);
        self.delta -= delta;
    }
}

impl Drop for GaugeGuard<'_> {
    fn drop(&mut self) {
        self.gauge.sub(self.delta)
    }
}

pub struct OwnedGaugeGuard {
    gauge: IntGauge,
    delta: i64,
}

impl std::fmt::Debug for OwnedGaugeGuard {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        self.delta.fmt(f)
    }
}

impl OwnedGaugeGuard {
    pub fn from_gauge(gauge: IntGauge) -> Self {
        Self { gauge, delta: 0i64 }
    }

    pub fn get(&self) -> i64 {
        self.delta
    }

    pub fn add(&mut self, delta: i64) {
        self.gauge.add(delta);
        self.delta += delta;
    }

    pub fn sub(&mut self, delta: i64) {
        self.gauge.sub(delta);
        self.delta -= delta;
    }
}

impl Drop for OwnedGaugeGuard {
    fn drop(&mut self) {
        self.gauge.sub(self.delta)
    }
}

// ---------------------------------------------------------------------------
// Prometheus scrape endpoint
// ---------------------------------------------------------------------------

pub fn metrics_text_payload() -> Result<String, String> {
    let metric_families = prometheus::gather();
    let mut buffer = String::with_capacity(1024);
    let encoder = TextEncoder::new();
    match encoder.encode_utf8(&metric_families, &mut buffer) {
        Ok(()) => Ok(buffer),
        Err(e) => Err(e.to_string()),
    }
}

// ---------------------------------------------------------------------------
// Application-level metric structs
// ---------------------------------------------------------------------------

#[derive(Clone)]
pub struct MemoryMetrics {
    pub active_bytes: IntGauge,
    pub allocated_bytes: IntGauge,
    pub resident_bytes: IntGauge,
    pub in_flight: InFlightDataGauges,
}

impl Default for MemoryMetrics {
    fn default() -> Self {
        Self {
            active_bytes: new_gauge(
                "active_bytes",
                "Total number of bytes in active pages allocated by the application, as reported \
                 by jemalloc `stats.active`.",
                "memory",
                &[],
            ),
            allocated_bytes: new_gauge(
                "allocated_bytes",
                "Total number of bytes allocated by the application, as reported by jemalloc \
                 `stats.allocated`.",
                "memory",
                &[],
            ),
            resident_bytes: new_gauge(
                "resident_bytes",
                " Total number of bytes in physically resident data pages mapped by the \
                 allocator, as reported by jemalloc `stats.resident`.",
                "memory",
                &[],
            ),
            in_flight: InFlightDataGauges::default(),
        }
    }
}

#[derive(Clone)]
pub struct InFlightDataGauges {
    pub rest_server: IntGauge,
    pub ingest_router: IntGauge,
    pub ingester_persist: IntGauge,
    pub ingester_replicate: IntGauge,
    pub wal: IntGauge,
    pub fetch_stream: IntGauge,
    pub multi_fetch_stream: IntGauge,
    pub doc_processor_mailbox: IntGauge,
    pub indexer_mailbox: IntGauge,
    pub index_writer: IntGauge,
    in_flight_gauge_vec: IntGaugeVec<1>,
}

impl Default for InFlightDataGauges {
    fn default() -> Self {
        let in_flight_gauge_vec = new_gauge_vec(
            "in_flight_data_bytes",
            "Amount of data in-flight in various buffers in bytes.",
            "memory",
            &[],
            ["component"],
        );
        Self {
            rest_server: in_flight_gauge_vec.with_label_values(["rest_server"]),
            ingest_router: in_flight_gauge_vec.with_label_values(["ingest_router"]),
            ingester_persist: in_flight_gauge_vec.with_label_values(["ingester_persist"]),
            ingester_replicate: in_flight_gauge_vec.with_label_values(["ingester_replicate"]),
            wal: in_flight_gauge_vec.with_label_values(["wal"]),
            fetch_stream: in_flight_gauge_vec.with_label_values(["fetch_stream"]),
            multi_fetch_stream: in_flight_gauge_vec.with_label_values(["multi_fetch_stream"]),
            doc_processor_mailbox: in_flight_gauge_vec.with_label_values(["doc_processor_mailbox"]),
            indexer_mailbox: in_flight_gauge_vec.with_label_values(["indexer_mailbox"]),
            index_writer: in_flight_gauge_vec.with_label_values(["index_writer"]),
            in_flight_gauge_vec: in_flight_gauge_vec.clone(),
        }
    }
}

impl InFlightDataGauges {
    #[inline]
    pub fn file(&self) -> &IntGauge {
        static GAUGE: OnceLock<IntGauge> = OnceLock::new();
        GAUGE.get_or_init(|| self.in_flight_gauge_vec.with_label_values(["file_source"]))
    }

    #[inline]
    pub fn ingest(&self) -> &IntGauge {
        static GAUGE: OnceLock<IntGauge> = OnceLock::new();
        GAUGE.get_or_init(|| {
            self.in_flight_gauge_vec
                .with_label_values(["ingest_source"])
        })
    }

    #[inline]
    pub fn kafka(&self) -> &IntGauge {
        static GAUGE: OnceLock<IntGauge> = OnceLock::new();
        GAUGE.get_or_init(|| self.in_flight_gauge_vec.with_label_values(["kafka_source"]))
    }

    #[inline]
    pub fn kinesis(&self) -> &IntGauge {
        static GAUGE: OnceLock<IntGauge> = OnceLock::new();
        GAUGE.get_or_init(|| {
            self.in_flight_gauge_vec
                .with_label_values(["kinesis_source"])
        })
    }

    #[inline]
    pub fn pubsub(&self) -> &IntGauge {
        static GAUGE: OnceLock<IntGauge> = OnceLock::new();
        GAUGE.get_or_init(|| {
            self.in_flight_gauge_vec
                .with_label_values(["pubsub_source"])
        })
    }

    #[inline]
    pub fn pulsar(&self) -> &IntGauge {
        static GAUGE: OnceLock<IntGauge> = OnceLock::new();
        GAUGE.get_or_init(|| {
            self.in_flight_gauge_vec
                .with_label_values(["pulsar_source"])
        })
    }

    #[inline]
    pub fn other(&self) -> &IntGauge {
        static GAUGE: OnceLock<IntGauge> = OnceLock::new();
        GAUGE.get_or_init(|| {
            self.in_flight_gauge_vec
                .with_label_values(["pulsar_source"])
        })
    }
}

/// This function returns `index_id` as is if per-index metrics are enabled, or projects it to
/// `"__any__"` otherwise.
pub fn index_label(index_id: &str) -> &str {
    static PER_INDEX_METRICS_ENABLED: LazyLock<bool> =
        LazyLock::new(|| !crate::get_bool_from_env("QW_DISABLE_PER_INDEX_METRICS", false));

    if *PER_INDEX_METRICS_ENABLED {
        index_id
    } else {
        "__any__"
    }
}

pub static MEMORY_METRICS: LazyLock<MemoryMetrics> = LazyLock::new(MemoryMetrics::default);

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::OnceLock;

    use opentelemetry::metrics::MeterProvider;
    use opentelemetry_sdk::metrics::data::{
        AggregatedMetrics, HistogramDataPoint, MetricData, ResourceMetrics,
    };
    use opentelemetry_sdk::metrics::{InMemoryMetricExporter, PeriodicReader, SdkMeterProvider};
    use serial_test::serial;

    static TEST_OTEL_EXPORTER: OnceLock<InMemoryMetricExporter> = OnceLock::new();
    static TEST_OTEL_PROVIDER: OnceLock<SdkMeterProvider> = OnceLock::new();

    fn ensure_test_otel_provider() -> (&'static InMemoryMetricExporter, &'static SdkMeterProvider) {
        let exporter = TEST_OTEL_EXPORTER.get_or_init(InMemoryMetricExporter::default);
        let provider = TEST_OTEL_PROVIDER.get_or_init(|| {
            let reader = PeriodicReader::builder(exporter.clone()).build();
            let provider = SdkMeterProvider::builder().with_reader(reader).build();
            install_otel_meter(provider.meter("quickwit-tests"));
            provider
        });
        (exporter, provider)
    }

    fn find_metric_data<'a>(
        metrics: &'a [ResourceMetrics],
        metric_name: &str,
    ) -> Option<&'a AggregatedMetrics> {
        metrics
            .iter()
            .flat_map(|resource_metrics| resource_metrics.scope_metrics())
            .flat_map(|scope_metrics| scope_metrics.metrics())
            .find(|metric| metric.name() == metric_name)
            .map(|metric| metric.data())
    }

    fn flush_and_read_metric<T>(
        exporter: &InMemoryMetricExporter,
        provider: &SdkMeterProvider,
        metric_name: &str,
        read: impl FnOnce(&AggregatedMetrics) -> T,
    ) -> T {
        provider.force_flush().unwrap();
        let exported_metrics = exporter.get_finished_metrics().unwrap();
        let data = find_metric_data(&exported_metrics, metric_name)
            .unwrap_or_else(|| panic!("metric '{metric_name}' should be exported"));
        read(data)
    }

    fn flush_and_get_counter_value(
        exporter: &InMemoryMetricExporter,
        provider: &SdkMeterProvider,
        metric_name: &str,
    ) -> u64 {
        flush_and_read_metric(exporter, provider, metric_name, |data| {
            let AggregatedMetrics::U64(MetricData::Sum(sum_data)) = data else {
                panic!("expected u64 sum metric");
            };
            sum_data
                .data_points()
                .next()
                .expect("should have one data point")
                .value()
        })
    }

    fn flush_and_get_gauge_value(
        exporter: &InMemoryMetricExporter,
        provider: &SdkMeterProvider,
        metric_name: &str,
    ) -> i64 {
        flush_and_read_metric(exporter, provider, metric_name, |data| {
            let AggregatedMetrics::I64(MetricData::Gauge(gauge_data)) = data else {
                panic!("expected i64 gauge metric");
            };
            gauge_data
                .data_points()
                .last()
                .expect("should have at least one data point")
                .value()
        })
    }

    fn flush_and_get_histogram_data_point(
        exporter: &InMemoryMetricExporter,
        provider: &SdkMeterProvider,
        metric_name: &str,
    ) -> HistogramDataPoint<f64> {
        flush_and_read_metric(exporter, provider, metric_name, |data| {
            let AggregatedMetrics::F64(MetricData::Histogram(histogram_data)) = data else {
                panic!("expected f64 histogram metric");
            };
            histogram_data
                .data_points()
                .next()
                .expect("should have one data point")
                .clone()
        })
    }

    fn flush_and_get_float_gauge_value(
        exporter: &InMemoryMetricExporter,
        provider: &SdkMeterProvider,
        metric_name: &str,
    ) -> f64 {
        flush_and_read_metric(exporter, provider, metric_name, |data| {
            let AggregatedMetrics::F64(MetricData::Gauge(gauge_data)) = data else {
                panic!("expected f64 gauge metric");
            };
            gauge_data
                .data_points()
                .last()
                .expect("should have at least one data point")
                .value()
        })
    }

    #[test]
    #[serial]
    fn test_counter_inc() {
        let (exporter, provider) = ensure_test_otel_provider();
        let counter = new_counter("test_ctr_inc", "test", "test", &[]);
        assert_eq!(counter.get(), 0);
        counter.inc();
        assert_eq!(counter.get(), 1);

        let otel_value =
            flush_and_get_counter_value(exporter, provider, "quickwit_test_test_ctr_inc");
        assert_eq!(otel_value, 1);
    }

    #[test]
    #[serial]
    fn test_counter_inc_by() {
        let (exporter, provider) = ensure_test_otel_provider();
        let counter = new_counter("test_ctr_inc_by", "test", "test", &[]);
        assert_eq!(counter.get(), 0);
        counter.inc_by(5);
        assert_eq!(counter.get(), 5);

        let otel_value =
            flush_and_get_counter_value(exporter, provider, "quickwit_test_test_ctr_inc_by");
        assert_eq!(otel_value, 5);
    }

    #[test]
    #[serial]
    fn test_gauge_set() {
        let (exporter, provider) = ensure_test_otel_provider();
        let gauge = new_gauge("test_gauge_set", "test", "test", &[]);
        assert_eq!(gauge.get(), 0);
        gauge.set(10);
        assert_eq!(gauge.get(), 10);

        let otel_value =
            flush_and_get_gauge_value(exporter, provider, "quickwit_test_test_gauge_set");
        assert_eq!(otel_value, 10);
    }

    #[test]
    #[serial]
    fn test_gauge_inc() {
        let (exporter, provider) = ensure_test_otel_provider();
        let gauge = new_gauge("test_gauge_inc", "test", "test", &[]);
        assert_eq!(gauge.get(), 0);
        gauge.inc();
        assert_eq!(gauge.get(), 1);

        let otel_value =
            flush_and_get_gauge_value(exporter, provider, "quickwit_test_test_gauge_inc");
        assert_eq!(otel_value, 1);
    }

    #[test]
    #[serial]
    fn test_gauge_dec() {
        let (exporter, provider) = ensure_test_otel_provider();
        let gauge = new_gauge("test_gauge_dec", "test", "test", &[]);
        assert_eq!(gauge.get(), 0);
        gauge.dec();
        assert_eq!(gauge.get(), -1);

        let otel_value =
            flush_and_get_gauge_value(exporter, provider, "quickwit_test_test_gauge_dec");
        assert_eq!(otel_value, -1);
    }

    #[test]
    #[serial]
    fn test_gauge_add() {
        let (exporter, provider) = ensure_test_otel_provider();
        let gauge = new_gauge("test_gauge_add", "test", "test", &[]);
        assert_eq!(gauge.get(), 0);
        gauge.add(15);
        assert_eq!(gauge.get(), 15);

        let otel_value =
            flush_and_get_gauge_value(exporter, provider, "quickwit_test_test_gauge_add");
        assert_eq!(otel_value, 15);
    }

    #[test]
    #[serial]
    fn test_gauge_sub() {
        let (exporter, provider) = ensure_test_otel_provider();
        let gauge = new_gauge("test_gauge_sub", "test", "test", &[]);
        assert_eq!(gauge.get(), 0);
        gauge.sub(3);
        assert_eq!(gauge.get(), -3);

        let otel_value =
            flush_and_get_gauge_value(exporter, provider, "quickwit_test_test_gauge_sub");
        assert_eq!(otel_value, -3);
    }

    #[test]
    #[serial]
    fn test_float_gauge_set() {
        let (exporter, provider) = ensure_test_otel_provider();
        let gauge = new_float_gauge("test_float_gauge", "test", "test", &[]);
        assert_eq!(gauge.get(), 0.0);
        gauge.set(3.14);
        assert_eq!(gauge.get(), 3.14);

        let otel_value =
            flush_and_get_float_gauge_value(exporter, provider, "quickwit_test_test_float_gauge");
        assert_eq!(otel_value, 3.14);
    }

    #[test]
    #[serial]
    fn test_histogram_observe() {
        let (exporter, provider) = ensure_test_otel_provider();
        let histogram = new_histogram("test_hist_obs", "test", "test", vec![1.0, 5.0, 10.0]);
        histogram.observe(2.5);
        histogram.observe(7.0);

        let dp =
            flush_and_get_histogram_data_point(exporter, provider, "quickwit_test_test_hist_obs");
        assert_eq!(dp.count(), 2);
        assert_eq!(dp.max().unwrap(), 7.0);
        assert_eq!(dp.min().unwrap(), 2.5);
        assert_eq!(dp.bounds().collect::<Vec<_>>(), vec![1.0, 5.0, 10.0]);
    }

    #[test]
    #[serial]
    fn test_histogram_vec_observe() {
        let (exporter, provider) = ensure_test_otel_provider();
        let histogram_vec = new_histogram_vec(
            "test_hist_vec_obs",
            "test",
            "test",
            &[],
            ["method"],
            vec![0.5, 1.5, 3.0],
        );
        histogram_vec.with_label_values(["GET"]).observe(1.0);

        flush_and_read_metric(
            exporter,
            provider,
            "quickwit_test_test_hist_vec_obs",
            |data| {
                let AggregatedMetrics::F64(MetricData::Histogram(histogram_data)) = data else {
                    panic!("expected f64 histogram metric");
                };
                let data_point = histogram_data
                    .data_points()
                    .find(|point| {
                        point
                            .attributes()
                            .any(|kv| kv.key.as_str() == "method" && kv.value.as_str() == "GET")
                    })
                    .expect("should contain the labelled data point");
                assert_eq!(data_point.count(), 1);
                assert_eq!(data_point.min().unwrap(), 1.0);
            },
        );
    }

    #[test]
    #[serial]
    fn test_histogram_timer_drop_observes() {
        let (exporter, provider) = ensure_test_otel_provider();
        let histogram = new_histogram("test_hist_timer_drop", "test", "test", vec![1.0, 5.0, 10.0]);
        {
            let _timer = histogram.start_timer();
        }

        let dp = flush_and_get_histogram_data_point(
            exporter,
            provider,
            "quickwit_test_test_hist_timer_drop",
        );
        assert_eq!(dp.count(), 1);
    }

    #[test]
    #[serial]
    fn test_histogram_timer_observe_duration() {
        let (exporter, provider) = ensure_test_otel_provider();
        let histogram = new_histogram("test_hist_timer_obs", "test", "test", vec![1.0, 5.0, 10.0]);
        let timer = histogram.start_timer();
        timer.observe_duration();

        let dp = flush_and_get_histogram_data_point(
            exporter,
            provider,
            "quickwit_test_test_hist_timer_obs",
        );
        assert_eq!(dp.count(), 1);
    }

    #[test]
    #[serial]
    fn test_counter_vec_with_label_values() {
        let (exporter, provider) = ensure_test_otel_provider();
        let vec = new_counter_vec("test_cvec", "test", "test", &[], ["method"]);
        let post_counter = vec.with_label_values(["POST"]);
        post_counter.inc_by(3);
        assert_eq!(post_counter.get(), 3);

        flush_and_read_metric(exporter, provider, "quickwit_test_test_cvec", |data| {
            let AggregatedMetrics::U64(MetricData::Sum(sum_data)) = data else {
                panic!("expected u64 sum metric");
            };
            let post_value = sum_data
                .data_points()
                .find(|dp| {
                    dp.attributes()
                        .any(|kv| kv.key.as_str() == "method" && kv.value.as_str() == "POST")
                })
                .expect("should contain POST data point")
                .value();
            assert_eq!(post_value, 3);
        });
    }

    #[test]
    #[serial]
    fn test_gauge_vec_with_label_values() {
        let (exporter, provider) = ensure_test_otel_provider();
        let vec = new_gauge_vec("test_gvec", "test", "test", &[], ["pool"]);
        let indexing = vec.with_label_values(["indexing"]);
        indexing.set(10);
        assert_eq!(indexing.get(), 10);

        flush_and_read_metric(exporter, provider, "quickwit_test_test_gvec", |data| {
            let AggregatedMetrics::I64(MetricData::Gauge(gauge_data)) = data else {
                panic!("expected i64 gauge metric");
            };
            let indexing_value = gauge_data
                .data_points()
                .find(|dp| {
                    dp.attributes()
                        .any(|kv| kv.key.as_str() == "pool" && kv.value.as_str() == "indexing")
                })
                .expect("should contain pool=indexing data point")
                .value();
            assert_eq!(indexing_value, 10);
        });
    }

    #[test]
    fn test_gauge_guard_add_sub_drop() {
        let gauge = new_gauge("test_guard", "test", "test", &[]);
        {
            let mut guard = GaugeGuard::from_gauge(&gauge);
            guard.add(5);
            assert_eq!(gauge.get(), 5);
            guard.sub(2);
            assert_eq!(gauge.get(), 3);
        }
        // After drop, the delta (3) is subtracted.
        assert_eq!(gauge.get(), 0);
    }

    #[test]
    fn test_owned_gauge_guard_add_sub_drop() {
        let gauge = new_gauge("test_owned_guard", "test", "test", &[]);
        {
            let mut guard = OwnedGaugeGuard::from_gauge(gauge.clone());
            guard.add(5);
            assert_eq!(gauge.get(), 5);
            guard.sub(2);
            assert_eq!(gauge.get(), 3);
        }
        assert_eq!(gauge.get(), 0);
    }

    #[test]
    fn test_metrics_text_payload_contains_registered_metrics() {
        let counter = new_counter("test_payload_ctr", "test", "test", &[]);
        counter.inc_by(42);
        let payload = metrics_text_payload().unwrap();
        assert!(payload.contains("quickwit_test_test_payload_ctr"));
        assert!(payload.contains("42"));
    }
}
