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

use std::sync::{LazyLock, OnceLock};

use criterion::{Criterion, criterion_group, criterion_main};
use metrics::{Counter, Gauge, Histogram, Label, counter, gauge, histogram};

// ---------------------------------------------------------------------------
// Recorders
// ---------------------------------------------------------------------------

struct NoopRecorder;

impl metrics::Recorder for NoopRecorder {
    fn describe_counter(
        &self,
        _key: metrics::KeyName,
        _unit: Option<metrics::Unit>,
        _desc: metrics::SharedString,
    ) {
    }
    fn describe_gauge(
        &self,
        _key: metrics::KeyName,
        _unit: Option<metrics::Unit>,
        _desc: metrics::SharedString,
    ) {
    }
    fn describe_histogram(
        &self,
        _key: metrics::KeyName,
        _unit: Option<metrics::Unit>,
        _desc: metrics::SharedString,
    ) {
    }
    fn register_counter(&self, _key: &metrics::Key, _metadata: &metrics::Metadata<'_>) -> Counter {
        Counter::noop()
    }
    fn register_gauge(&self, _key: &metrics::Key, _metadata: &metrics::Metadata<'_>) -> Gauge {
        Gauge::noop()
    }
    fn register_histogram(
        &self,
        _key: &metrics::Key,
        _metadata: &metrics::Metadata<'_>,
    ) -> Histogram {
        Histogram::noop()
    }
}

// ---------------------------------------------------------------------------
// Recorder setup — RECORDER env-var is mandatory.
//
//   RECORDER=noop cargo bench --bench baseline            # noop recorder
//   RECORDER=prometheus cargo bench --bench baseline      # prometheus
// ---------------------------------------------------------------------------

static INSTALL_RECORDER: OnceLock<()> = OnceLock::new();

fn install_recorder() {
    INSTALL_RECORDER.get_or_init(|| {
        let recorder = std::env::var("RECORDER")
            .expect("RECORDER env var is required (set to \"noop\" or \"prometheus\")");

        match recorder.to_ascii_lowercase().as_str() {
            "noop" => {
                eprintln!("[bench] Using noop recorder");
                metrics::set_global_recorder(NoopRecorder)
                    .expect("failed to install noop recorder");
            }
            "prometheus" => {
                eprintln!("[bench] Using prometheus recorder");
                let _handle = metrics_exporter_prometheus::PrometheusBuilder::new()
                    .install_recorder()
                    .expect("failed to install prometheus recorder");
            }
            other => {
                panic!("unknown RECORDER value \"{other}\", expected \"noop\" or \"prometheus\"")
            }
        }
    });
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn make_labels(n: usize) -> Vec<Label> {
    (0..n)
        .map(|i| {
            Label::from_static_parts(
                match i {
                    0 => "service",
                    1 => "method",
                    2 => "endpoint",
                    3 => "status",
                    4 => "region",
                    _ => "extra",
                },
                match i {
                    0 => "api",
                    1 => "GET",
                    2 => "/health",
                    3 => "200",
                    4 => "us-east-1",
                    _ => "val",
                },
            )
        })
        .collect()
}

// ---------------------------------------------------------------------------
// ON-THE-FLY: create handle via macro + record, every iteration.
// ---------------------------------------------------------------------------

fn on_the_fly_counter(c: &mut Criterion) {
    install_recorder();

    let mut group = c.benchmark_group("raw/on_the_fly/counter");
    for n_labels in [0, 1, 3, 5] {
        let labels = make_labels(n_labels);
        group.bench_function(format!("labels/{n_labels}"), |b| {
            b.iter(|| {
                let c = if labels.is_empty() {
                    counter!("otf_counter")
                } else {
                    counter!("otf_counter", labels.iter())
                };
                c.increment(1);
            });
        });
    }
    group.finish();
}

fn on_the_fly_gauge(c: &mut Criterion) {
    install_recorder();

    let mut group = c.benchmark_group("raw/on_the_fly/gauge");
    for n_labels in [0, 1, 3, 5] {
        let labels = make_labels(n_labels);
        group.bench_function(format!("labels/{n_labels}"), |b| {
            b.iter(|| {
                let g = if labels.is_empty() {
                    gauge!("otf_gauge")
                } else {
                    gauge!("otf_gauge", labels.iter())
                };
                g.set(42.0);
            });
        });
    }
    group.finish();
}

fn on_the_fly_histogram(c: &mut Criterion) {
    install_recorder();

    let mut group = c.benchmark_group("raw/on_the_fly/histogram");
    for n_labels in [0, 1, 3, 5] {
        let labels = make_labels(n_labels);
        group.bench_function(format!("labels/{n_labels}"), |b| {
            b.iter(|| {
                let h = if labels.is_empty() {
                    histogram!("otf_histogram")
                } else {
                    histogram!("otf_histogram", labels.iter())
                };
                h.record(0.123);
            });
        });
    }
    group.finish();
}

// ---------------------------------------------------------------------------
// STATIC: real `static` handles via LazyLock, as used in production code.
// ---------------------------------------------------------------------------

static STATIC_COUNTER_0: LazyLock<Counter> = LazyLock::new(|| counter!("static_counter"));
static STATIC_COUNTER_1: LazyLock<Counter> =
    LazyLock::new(|| counter!("static_counter", "service" => "api"));
static STATIC_COUNTER_3: LazyLock<Counter> = LazyLock::new(
    || counter!("static_counter", "service" => "api", "method" => "GET", "endpoint" => "/health"),
);
static STATIC_COUNTER_5: LazyLock<Counter> = LazyLock::new(|| {
    counter!(
        "static_counter",
        "service" => "api", "method" => "GET", "endpoint" => "/health",
        "status" => "200", "region" => "us-east-1"
    )
});

static STATIC_GAUGE_0: LazyLock<Gauge> = LazyLock::new(|| gauge!("static_gauge"));
static STATIC_GAUGE_1: LazyLock<Gauge> =
    LazyLock::new(|| gauge!("static_gauge", "service" => "api"));
static STATIC_GAUGE_3: LazyLock<Gauge> = LazyLock::new(
    || gauge!("static_gauge", "service" => "api", "method" => "GET", "endpoint" => "/health"),
);
static STATIC_GAUGE_5: LazyLock<Gauge> = LazyLock::new(|| {
    gauge!(
        "static_gauge",
        "service" => "api", "method" => "GET", "endpoint" => "/health",
        "status" => "200", "region" => "us-east-1"
    )
});

static STATIC_HISTOGRAM_0: LazyLock<Histogram> = LazyLock::new(|| histogram!("static_histogram"));
static STATIC_HISTOGRAM_1: LazyLock<Histogram> =
    LazyLock::new(|| histogram!("static_histogram", "service" => "api"));
static STATIC_HISTOGRAM_3: LazyLock<Histogram> = LazyLock::new(|| {
    histogram!(
        "static_histogram",
        "service" => "api", "method" => "GET", "endpoint" => "/health"
    )
});
static STATIC_HISTOGRAM_5: LazyLock<Histogram> = LazyLock::new(|| {
    histogram!(
        "static_histogram",
        "service" => "api", "method" => "GET", "endpoint" => "/health",
        "status" => "200", "region" => "us-east-1"
    )
});

fn static_counter(c: &mut Criterion) {
    install_recorder();

    let counters: &[(usize, &LazyLock<Counter>)] = &[
        (0, &STATIC_COUNTER_0),
        (1, &STATIC_COUNTER_1),
        (3, &STATIC_COUNTER_3),
        (5, &STATIC_COUNTER_5),
    ];

    let mut group = c.benchmark_group("raw/static/counter");
    for &(n, handle) in counters {
        let _ = &**handle;
        group.bench_function(format!("labels/{n}"), |b| {
            b.iter(|| handle.increment(1));
        });
    }
    group.finish();
}

fn static_gauge(c: &mut Criterion) {
    install_recorder();

    let gauges: &[(usize, &LazyLock<Gauge>)] = &[
        (0, &STATIC_GAUGE_0),
        (1, &STATIC_GAUGE_1),
        (3, &STATIC_GAUGE_3),
        (5, &STATIC_GAUGE_5),
    ];

    let mut group = c.benchmark_group("raw/static/gauge");
    for &(n, handle) in gauges {
        let _ = &**handle;
        group.bench_function(format!("labels/{n}"), |b| {
            b.iter(|| handle.set(42.0));
        });
    }
    group.finish();
}

fn static_histogram(c: &mut Criterion) {
    install_recorder();

    let histograms: &[(usize, &LazyLock<Histogram>)] = &[
        (0, &STATIC_HISTOGRAM_0),
        (1, &STATIC_HISTOGRAM_1),
        (3, &STATIC_HISTOGRAM_3),
        (5, &STATIC_HISTOGRAM_5),
    ];

    let mut group = c.benchmark_group("raw/static/histogram");
    for &(n, handle) in histograms {
        let _ = &**handle;
        group.bench_function(format!("labels/{n}"), |b| {
            b.iter(|| handle.record(0.123));
        });
    }
    group.finish();
}

// ---------------------------------------------------------------------------

criterion_group!(
    on_the_fly_benches,
    on_the_fly_counter,
    on_the_fly_gauge,
    on_the_fly_histogram,
);

criterion_group!(
    static_benches,
    static_counter,
    static_gauge,
    static_histogram,
);

criterion_main!(on_the_fly_benches, static_benches);
