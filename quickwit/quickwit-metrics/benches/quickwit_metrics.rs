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
use quickwit_metrics::*;

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
    fn register_counter(
        &self,
        _key: &metrics::Key,
        _metadata: &metrics::Metadata<'_>,
    ) -> metrics::Counter {
        metrics::Counter::noop()
    }
    fn register_gauge(
        &self,
        _key: &metrics::Key,
        _metadata: &metrics::Metadata<'_>,
    ) -> metrics::Gauge {
        metrics::Gauge::noop()
    }
    fn register_histogram(
        &self,
        _key: &metrics::Key,
        _metadata: &metrics::Metadata<'_>,
    ) -> metrics::Histogram {
        metrics::Histogram::noop()
    }
}

// ---------------------------------------------------------------------------
// Recorder setup
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
// ON-THE-FLY
// ---------------------------------------------------------------------------

fn on_the_fly_counter(c: &mut Criterion) {
    install_recorder();

    let mut group = c.benchmark_group("macros/on_the_fly/counter");

    group.bench_function("labels/0", |b| {
        b.iter(|| {
            counter!(
                name: "otf_counter",
                description: "bench counter",
                subsystem: "bench"
            )
            .increment(1);
        });
    });

    group.bench_function("labels/1", |b| {
        b.iter(|| {
            counter!(
                name: "otf_counter",
                description: "bench counter",
                subsystem: "bench",
                "service" => "api"
            )
            .increment(1);
        });
    });

    group.bench_function("labels/3", |b| {
        b.iter(|| {
            counter!(
                name: "otf_counter",
                description: "bench counter",
                subsystem: "bench",
                "service" => "api",
                "method" => "GET",
                "endpoint" => "/health"
            )
            .increment(1);
        });
    });

    group.bench_function("labels/5", |b| {
        b.iter(|| {
            counter!(
                name: "otf_counter",
                description: "bench counter",
                subsystem: "bench",
                "service" => "api",
                "method" => "GET",
                "endpoint" => "/health",
                "status" => "200",
                "region" => "us-east-1"
            )
            .increment(1);
        });
    });

    group.finish();
}

fn on_the_fly_gauge(c: &mut Criterion) {
    install_recorder();

    let mut group = c.benchmark_group("macros/on_the_fly/gauge");

    group.bench_function("labels/0", |b| {
        b.iter(|| {
            gauge!(
                name: "otf_gauge",
                description: "bench gauge",
                subsystem: "bench"
            )
            .set(42.0);
        });
    });

    group.bench_function("labels/1", |b| {
        b.iter(|| {
            gauge!(
                name: "otf_gauge",
                description: "bench gauge",
                subsystem: "bench",
                "service" => "api"
            )
            .set(42.0);
        });
    });

    group.bench_function("labels/3", |b| {
        b.iter(|| {
            gauge!(
                name: "otf_gauge",
                description: "bench gauge",
                subsystem: "bench",
                "service" => "api",
                "method" => "GET",
                "endpoint" => "/health"
            )
            .set(42.0);
        });
    });

    group.bench_function("labels/5", |b| {
        b.iter(|| {
            gauge!(
                name: "otf_gauge",
                description: "bench gauge",
                subsystem: "bench",
                "service" => "api",
                "method" => "GET",
                "endpoint" => "/health",
                "status" => "200",
                "region" => "us-east-1"
            )
            .set(42.0);
        });
    });

    group.finish();
}

fn on_the_fly_histogram(c: &mut Criterion) {
    install_recorder();

    let mut group = c.benchmark_group("macros/on_the_fly/histogram");

    group.bench_function("labels/0", |b| {
        b.iter(|| {
            histogram!(
                name: "otf_histogram",
                description: "bench histogram",
                subsystem: "bench",
                buckets: vec![0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0]
            )
            .record(0.123);
        });
    });

    group.bench_function("labels/1", |b| {
        b.iter(|| {
            histogram!(
                name: "otf_histogram",
                description: "bench histogram",
                subsystem: "bench",
                buckets: vec![0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0],
                "service" => "api"
            )
            .record(0.123);
        });
    });

    group.bench_function("labels/3", |b| {
        b.iter(|| {
            histogram!(
                name: "otf_histogram",
                description: "bench histogram",
                subsystem: "bench",
                buckets: vec![0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0],
                "service" => "api",
                "method" => "GET",
                "endpoint" => "/health"
            )
            .record(0.123);
        });
    });

    group.bench_function("labels/5", |b| {
        b.iter(|| {
            histogram!(
                name: "otf_histogram",
                description: "bench histogram",
                subsystem: "bench",
                buckets: vec![0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0],
                "service" => "api",
                "method" => "GET",
                "endpoint" => "/health",
                "status" => "200",
                "region" => "us-east-1"
            )
            .record(0.123);
        });
    });

    group.finish();
}

// ---------------------------------------------------------------------------
// STATIC
// ---------------------------------------------------------------------------

static STATIC_COUNTER_0: LazyLock<Counter> = LazyLock::new(|| {
    counter!(
        name: "static_counter",
        description: "bench counter",
        subsystem: "bench"
    )
});
static STATIC_COUNTER_1: LazyLock<Counter> = LazyLock::new(|| {
    counter!(
        name: "static_counter",
        description: "bench counter",
        subsystem: "bench",
        "service" => "api"
    )
});
static STATIC_COUNTER_3: LazyLock<Counter> = LazyLock::new(|| {
    counter!(
        name: "static_counter",
        description: "bench counter",
        subsystem: "bench",
        "service" => "api",
        "method" => "GET",
        "endpoint" => "/health"
    )
});
static STATIC_COUNTER_5: LazyLock<Counter> = LazyLock::new(|| {
    counter!(
        name: "static_counter",
        description: "bench counter",
        subsystem: "bench",
        "service" => "api",
        "method" => "GET",
        "endpoint" => "/health",
        "status" => "200",
        "region" => "us-east-1"
    )
});

static STATIC_GAUGE_0: LazyLock<Gauge> = LazyLock::new(|| {
    gauge!(
        name: "static_gauge",
        description: "bench gauge",
        subsystem: "bench"
    )
});
static STATIC_GAUGE_1: LazyLock<Gauge> = LazyLock::new(|| {
    gauge!(
        name: "static_gauge",
        description: "bench gauge",
        subsystem: "bench",
        "service" => "api"
    )
});
static STATIC_GAUGE_3: LazyLock<Gauge> = LazyLock::new(|| {
    gauge!(
        name: "static_gauge",
        description: "bench gauge",
        subsystem: "bench",
        "service" => "api",
        "method" => "GET",
        "endpoint" => "/health"
    )
});
static STATIC_GAUGE_5: LazyLock<Gauge> = LazyLock::new(|| {
    gauge!(
        name: "static_gauge",
        description: "bench gauge",
        subsystem: "bench",
        "service" => "api",
        "method" => "GET",
        "endpoint" => "/health",
        "status" => "200",
        "region" => "us-east-1"
    )
});

static STATIC_HISTOGRAM_0: LazyLock<Histogram> = LazyLock::new(|| {
    histogram!(
        name: "static_histogram",
        description: "bench histogram",
        subsystem: "bench",
        buckets: vec![0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0]
    )
});
static STATIC_HISTOGRAM_1: LazyLock<Histogram> = LazyLock::new(|| {
    histogram!(
        name: "static_histogram",
        description: "bench histogram",
        subsystem: "bench",
        buckets: vec![0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0],
        "service" => "api"
    )
});
static STATIC_HISTOGRAM_3: LazyLock<Histogram> = LazyLock::new(|| {
    histogram!(
        name: "static_histogram",
        description: "bench histogram",
        subsystem: "bench",
        buckets: vec![0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0],
        "service" => "api",
        "method" => "GET",
        "endpoint" => "/health"
    )
});
static STATIC_HISTOGRAM_5: LazyLock<Histogram> = LazyLock::new(|| {
    histogram!(
        name: "static_histogram",
        description: "bench histogram",
        subsystem: "bench",
        buckets: vec![0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0],
        "service" => "api",
        "method" => "GET",
        "endpoint" => "/health",
        "status" => "200",
        "region" => "us-east-1"
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

    let mut group = c.benchmark_group("macros/static/counter");
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

    let mut group = c.benchmark_group("macros/static/gauge");
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

    let mut group = c.benchmark_group("macros/static/histogram");
    for &(n, handle) in histograms {
        let _ = &**handle;
        group.bench_function(format!("labels/{n}"), |b| {
            b.iter(|| handle.record(0.123));
        });
    }
    group.finish();
}

// ---------------------------------------------------------------------------
// PARENT EXTENSION
// ---------------------------------------------------------------------------

static PARENT_COUNTER: LazyLock<Counter> = LazyLock::new(|| {
    counter!(
        name: "parent_counter",
        description: "bench parent counter",
        subsystem: "bench",
        "service" => "api"
    )
});

static PARENT_GAUGE: LazyLock<Gauge> = LazyLock::new(|| {
    gauge!(
        name: "parent_gauge",
        description: "bench parent gauge",
        subsystem: "bench",
        "service" => "api"
    )
});

static PARENT_HISTOGRAM: LazyLock<Histogram> = LazyLock::new(|| {
    histogram!(
        name: "parent_histogram",
        description: "bench parent histogram",
        subsystem: "bench",
        buckets: vec![0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0],
        "service" => "api"
    )
});

fn parent_counter(c: &mut Criterion) {
    install_recorder();
    let _ = &*PARENT_COUNTER;

    let mut group = c.benchmark_group("macros/parent/counter");

    group.bench_function("extend/1", |b| {
        b.iter(|| {
            counter!(parent: PARENT_COUNTER, "method" => "GET").increment(1);
        });
    });

    group.bench_function("extend/3", |b| {
        b.iter(|| {
            counter!(
                parent: PARENT_COUNTER,
                "method" => "GET",
                "endpoint" => "/health",
                "status" => "200"
            )
            .increment(1);
        });
    });

    group.finish();
}

fn parent_gauge(c: &mut Criterion) {
    install_recorder();
    let _ = &*PARENT_GAUGE;

    let mut group = c.benchmark_group("macros/parent/gauge");

    group.bench_function("extend/1", |b| {
        b.iter(|| {
            gauge!(parent: PARENT_GAUGE, "method" => "GET").set(42.0);
        });
    });

    group.bench_function("extend/3", |b| {
        b.iter(|| {
            gauge!(
                parent: PARENT_GAUGE,
                "method" => "GET",
                "endpoint" => "/health",
                "status" => "200"
            )
            .set(42.0);
        });
    });

    group.finish();
}

fn parent_histogram(c: &mut Criterion) {
    install_recorder();
    let _ = &*PARENT_HISTOGRAM;

    let mut group = c.benchmark_group("macros/parent/histogram");

    group.bench_function("extend/1", |b| {
        b.iter(|| {
            histogram!(parent: PARENT_HISTOGRAM, "method" => "GET").record(0.123);
        });
    });

    group.bench_function("extend/3", |b| {
        b.iter(|| {
            histogram!(
                parent: PARENT_HISTOGRAM,
                "method" => "GET",
                "endpoint" => "/health",
                "status" => "200"
            )
            .record(0.123);
        });
    });

    group.finish();
}

// ---------------------------------------------------------------------------
// DYNAMIC LABELS
// ---------------------------------------------------------------------------

static DYN_PARENT_COUNTER: LazyLock<Counter> = LazyLock::new(|| {
    counter!(
        name: "dyn_parent_counter",
        description: "bench dynamic parent counter",
        subsystem: "bench",
        "service" => "api"
    )
});

static DYN_PARENT_GAUGE: LazyLock<Gauge> = LazyLock::new(|| {
    gauge!(
        name: "dyn_parent_gauge",
        description: "bench dynamic parent gauge",
        subsystem: "bench",
        "service" => "api"
    )
});

fn dynamic_counter(c: &mut Criterion) {
    install_recorder();
    let _ = &*DYN_PARENT_COUNTER;

    let mut group = c.benchmark_group("macros/dynamic/counter");

    let method = "GET";
    group.bench_function("same_value", |b| {
        b.iter(|| {
            counter!(parent: DYN_PARENT_COUNTER, "method" => method).increment(1);
        });
    });

    let methods: Vec<&'static str> = (0..64)
        .map(|i| &*Box::leak(format!("M{i}").into_boxed_str()))
        .collect();
    let mut idx = 0usize;
    group.bench_function("varying_64", |b| {
        b.iter(|| {
            let method = methods[idx % methods.len()];
            idx += 1;
            counter!(parent: DYN_PARENT_COUNTER, "method" => method).increment(1);
        });
    });

    group.finish();
}

fn dynamic_gauge(c: &mut Criterion) {
    install_recorder();
    let _ = &*DYN_PARENT_GAUGE;

    let mut group = c.benchmark_group("macros/dynamic/gauge");

    let method = "GET";
    group.bench_function("same_value", |b| {
        b.iter(|| {
            gauge!(parent: DYN_PARENT_GAUGE, "method" => method).set(42.0);
        });
    });

    let methods: Vec<&'static str> = (0..64)
        .map(|i| &*Box::leak(format!("M{i}").into_boxed_str()))
        .collect();
    let mut idx = 0usize;
    group.bench_function("varying_64", |b| {
        b.iter(|| {
            let method = methods[idx % methods.len()];
            idx += 1;
            gauge!(parent: DYN_PARENT_GAUGE, "method" => method).set(42.0);
        });
    });

    group.finish();
}

// ---------------------------------------------------------------------------
// OBSERVABLE
// ---------------------------------------------------------------------------

static OBS_COUNTER: LazyLock<Counter> = LazyLock::new(|| {
    counter!(
        name: "obs_counter",
        description: "bench observable counter",
        subsystem: "bench",
    )
});

static OBS_GAUGE: LazyLock<Gauge> = LazyLock::new(|| {
    gauge!(
        name: "obs_gauge",
        description: "bench observable gauge",
        subsystem: "bench",
    )
});

fn observable_counter(c: &mut Criterion) {
    install_recorder();
    let _ = &*STATIC_COUNTER_0;
    let _ = &*OBS_COUNTER;

    let mut group = c.benchmark_group("macros/observable/counter");

    group.bench_function("increment", |b| {
        b.iter(|| OBS_COUNTER.increment(1));
    });

    group.bench_function("increment/baseline", |b| {
        b.iter(|| STATIC_COUNTER_0.increment(1));
    });

    group.bench_function("get", |b| {
        b.iter(|| OBS_COUNTER.get());
    });

    group.finish();
}

fn observable_gauge(c: &mut Criterion) {
    install_recorder();
    let _ = &*STATIC_GAUGE_0;
    let _ = &*OBS_GAUGE;

    let mut group = c.benchmark_group("macros/observable/gauge");

    group.bench_function("set", |b| {
        b.iter(|| OBS_GAUGE.set(42.0));
    });

    group.bench_function("set/baseline", |b| {
        b.iter(|| STATIC_GAUGE_0.set(42.0));
    });

    group.bench_function("increment", |b| {
        b.iter(|| OBS_GAUGE.increment(1.0));
    });

    group.bench_function("increment/baseline", |b| {
        b.iter(|| STATIC_GAUGE_0.increment(1.0));
    });

    group.bench_function("get", |b| {
        b.iter(|| OBS_GAUGE.get());
    });

    group.finish();
}

// ---------------------------------------------------------------------------
// LABELS
// ---------------------------------------------------------------------------

const LABELS_1: LabelNames<1> = label_names!("method");
const LABELS_3: LabelNames<3> = label_names!("method", "endpoint", "status");

fn labels_counter(c: &mut Criterion) {
    install_recorder();
    let _ = &*PARENT_COUNTER;

    let mut group = c.benchmark_group("macros/labels/counter");

    group.bench_function("static/1", |b| {
        b.iter(|| {
            counter!(
                parent: PARENT_COUNTER,
                labels: label_values!(names: LABELS_1, "GET")
            )
            .increment(1);
        });
    });

    group.bench_function("static/3", |b| {
        b.iter(|| {
            counter!(
                parent: PARENT_COUNTER,
                labels: label_values!(names: LABELS_3, "GET", "/health", "200")
            )
            .increment(1);
        });
    });

    group.bench_function("runtime/1", |b| {
        b.iter(|| {
            counter!(
                parent: PARENT_COUNTER,
                labels: label_values!(names: LABELS_1, "GET".to_string())
            )
            .increment(1);
        });
    });

    let methods: Vec<&'static str> = (0..64)
        .map(|i| &*Box::leak(format!("M{i}").into_boxed_str()))
        .collect();
    let mut idx = 0usize;
    group.bench_function("varying_64/1", |b| {
        b.iter(|| {
            let m = methods[idx % methods.len()];
            idx += 1;
            counter!(
                parent: PARENT_COUNTER,
                labels: label_values!(names: LABELS_1, m)
            )
            .increment(1);
        });
    });

    group.finish();
}

fn labels_gauge(c: &mut Criterion) {
    install_recorder();
    let _ = &*PARENT_GAUGE;

    let mut group = c.benchmark_group("macros/labels/gauge");

    group.bench_function("static/1", |b| {
        b.iter(|| {
            gauge!(
                parent: PARENT_GAUGE,
                labels: label_values!(names: LABELS_1, "GET")
            )
            .set(42.0);
        });
    });

    group.bench_function("static/3", |b| {
        b.iter(|| {
            gauge!(
                parent: PARENT_GAUGE,
                labels: label_values!(names: LABELS_3, "GET", "/health", "200")
            )
            .set(42.0);
        });
    });

    group.finish();
}

fn labels_histogram(c: &mut Criterion) {
    install_recorder();
    let _ = &*PARENT_HISTOGRAM;

    let mut group = c.benchmark_group("macros/labels/histogram");

    group.bench_function("static/1", |b| {
        b.iter(|| {
            histogram!(
                parent: PARENT_HISTOGRAM,
                labels: label_values!(names: LABELS_1, "GET")
            )
            .record(0.123);
        });
    });

    group.bench_function("static/3", |b| {
        b.iter(|| {
            histogram!(
                parent: PARENT_HISTOGRAM,
                labels: label_values!(names: LABELS_3, "GET", "/health", "200")
            )
            .record(0.123);
        });
    });

    group.finish();
}

// ---------------------------------------------------------------------------
// COMPOSITE LABELS
// ---------------------------------------------------------------------------

const COMP_METHOD: LabelNames<1> = label_names!("method");
const COMP_ENDPOINT: LabelNames<1> = label_names!("endpoint");
const COMP_STATUS: LabelNames<1> = label_names!("status");
const COMP_ALL_3: LabelNames<3> = label_names!("method", "endpoint", "status");

fn composite_counter(c: &mut Criterion) {
    install_recorder();
    let _ = &*PARENT_COUNTER;

    let mut group = c.benchmark_group("macros/composite/counter");

    group.bench_function("single_3", |b| {
        b.iter(|| {
            counter!(
                parent: PARENT_COUNTER,
                labels: label_values!(names: COMP_ALL_3, "GET", "/health", "200"),
            )
            .increment(1);
        });
    });

    group.bench_function("compose_1x3", |b| {
        b.iter(|| {
            counter!(
                parent: PARENT_COUNTER,
                labels: label_values!(names: COMP_METHOD, "GET"),
                        label_values!(names: COMP_ENDPOINT, "/health"),
                        label_values!(names: COMP_STATUS, "200"),
            )
            .increment(1);
        });
    });

    group.bench_function("compose_1x2", |b| {
        b.iter(|| {
            counter!(
                parent: PARENT_COUNTER,
                labels: label_values!(names: COMP_METHOD, "GET"),
                        label_values!(names: COMP_ENDPOINT, "/health"),
            )
            .increment(1);
        });
    });

    group.finish();
}

fn composite_gauge(c: &mut Criterion) {
    install_recorder();
    let _ = &*PARENT_GAUGE;

    let mut group = c.benchmark_group("macros/composite/gauge");

    group.bench_function("single_3", |b| {
        b.iter(|| {
            gauge!(
                parent: PARENT_GAUGE,
                labels: label_values!(names: COMP_ALL_3, "GET", "/health", "200"),
            )
            .set(42.0);
        });
    });

    group.bench_function("compose_1x3", |b| {
        b.iter(|| {
            gauge!(
                parent: PARENT_GAUGE,
                labels: label_values!(names: COMP_METHOD, "GET"),
                        label_values!(names: COMP_ENDPOINT, "/health"),
                        label_values!(names: COMP_STATUS, "200"),
            )
            .set(42.0);
        });
    });

    group.bench_function("compose_1x2", |b| {
        b.iter(|| {
            gauge!(
                parent: PARENT_GAUGE,
                labels: label_values!(names: COMP_METHOD, "GET"),
                        label_values!(names: COMP_ENDPOINT, "/health"),
            )
            .set(42.0);
        });
    });

    group.finish();
}

fn composite_histogram(c: &mut Criterion) {
    install_recorder();
    let _ = &*PARENT_HISTOGRAM;

    let mut group = c.benchmark_group("macros/composite/histogram");

    group.bench_function("single_3", |b| {
        b.iter(|| {
            histogram!(
                parent: PARENT_HISTOGRAM,
                labels: label_values!(names: COMP_ALL_3, "GET", "/health", "200"),
            )
            .record(0.123);
        });
    });

    group.bench_function("compose_1x3", |b| {
        b.iter(|| {
            histogram!(
                parent: PARENT_HISTOGRAM,
                labels: label_values!(names: COMP_METHOD, "GET"),
                        label_values!(names: COMP_ENDPOINT, "/health"),
                        label_values!(names: COMP_STATUS, "200"),
            )
            .record(0.123);
        });
    });

    group.bench_function("compose_1x2", |b| {
        b.iter(|| {
            histogram!(
                parent: PARENT_HISTOGRAM,
                labels: label_values!(names: COMP_METHOD, "GET"),
                        label_values!(names: COMP_ENDPOINT, "/health"),
            )
            .record(0.123);
        });
    });

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

criterion_group!(
    parent_benches,
    parent_counter,
    parent_gauge,
    parent_histogram,
);

criterion_group!(dynamic_benches, dynamic_counter, dynamic_gauge,);

criterion_group!(observable_benches, observable_counter, observable_gauge,);

criterion_group!(
    labels_benches,
    labels_counter,
    labels_gauge,
    labels_histogram,
);

criterion_group!(
    composite_benches,
    composite_counter,
    composite_gauge,
    composite_histogram,
);

criterion_main!(
    on_the_fly_benches,
    static_benches,
    parent_benches,
    dynamic_benches,
    observable_benches,
    labels_benches,
    composite_benches,
);
