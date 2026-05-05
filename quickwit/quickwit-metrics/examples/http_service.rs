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

//! Example demonstrating all quickwit-metrics features.
//!
//! Run with: `cargo run --example http_service -p quickwit-metrics`

use std::sync::LazyLock;

use metrics_exporter_prometheus::{Matcher, PrometheusBuilder};
use quickwit_metrics::*;

// ─── Helper ───

fn exponential_buckets(start: f64, factor: f64, count: usize) -> Vec<f64> {
    (0..count).map(|i| start * factor.powi(i as i32)).collect()
}

// ─── Metric definitions ───

static HTTP_REQUEST_DURATION: LazyLock<Histogram> = LazyLock::new(|| {
    histogram!(
        name: "request_duration_seconds",
        description: "Time spent processing HTTP requests",
        subsystem: "http",
        buckets: exponential_buckets(0.005, 2.0, 12)
    )
});

static HTTP_RESPONSE_SIZE: LazyLock<Histogram> = LazyLock::new(|| {
    histogram!(
        name: "response_size_bytes",
        description: "Size of HTTP responses in bytes",
        subsystem: "http",
        buckets: vec![100.0, 1_000.0, 10_000.0, 100_000.0, 1_000_000.0]
    )
});

static HTTP_REQUESTS_TOTAL: LazyLock<Counter> = LazyLock::new(|| {
    counter!(
        name: "requests_total",
        description: "Total number of HTTP requests",
        subsystem: "http"
    )
});

static HTTP_REQUESTS_BY_METHOD: LazyLock<Counter> = LazyLock::new(|| {
    counter!(
        name: "requests_by_method_total",
        description: "HTTP requests broken down by method",
        subsystem: "http",
        "method" => "GET",
    )
});

static HTTP_ACTIVE_CONNECTIONS: LazyLock<Gauge> = LazyLock::new(|| {
    gauge!(
        name: "active_connections",
        description: "Number of currently active HTTP connections",
        subsystem: "http"
    )
});

static HTTP_ACTIVE_CONNECTIONS_BY_REGION: LazyLock<Gauge> = LazyLock::new(|| {
    gauge!(
        name: "active_connections",
        description: "Number of currently active HTTP connections",
        subsystem: "http",
        "region" => "us-east-1",
    )
});

// ─── Labels<N> examples ───

const ROUTE_LABELS: Labels<2> = Labels::new(["method", "path"]);

fn record_request(method: &'static str, path: &'static str, duration: f64, size: f64) {
    let route = label_values!(ROUTE_LABELS, [method, path]);
    histogram!(parent: HTTP_REQUEST_DURATION, labels: route).record(duration);
    histogram!(parent: HTTP_RESPONSE_SIZE, labels: route).record(size);
    counter!(parent: HTTP_REQUESTS_TOTAL, labels: route).increment(1);
}

fn record_dynamic_request(method: String, path: String, duration: f64) {
    let route = label_values!(ROUTE_LABELS, [method, path]);
    histogram!(parent: HTTP_REQUEST_DURATION, labels: route).record(duration);
}

fn track_connection(region: &'static str) -> GaugeGuard {
    let g = gauge!(parent: HTTP_ACTIVE_CONNECTIONS, "region" => region);
    let guard = GaugeGuard::from_gauge(&g);
    guard.increment(1.0);
    guard
}

// ─── Prometheus setup ───

fn install_prometheus_recorder() {
    let mut builder = PrometheusBuilder::new().with_http_listener(([127, 0, 0, 1], 9000));

    for (name, buckets) in quickwit_metrics::histogram_buckets() {
        println!("  histogram {name}: buckets={buckets:?}");
        builder = builder
            .set_buckets_for_metric(Matcher::Full(name.to_owned()), &buckets)
            .expect("valid buckets");
    }

    builder
        .install()
        .expect("failed to install Prometheus recorder");

    quickwit_metrics::describe_metrics();
}

// ─── Simulated request handling ───

fn handle_request(method: &'static str, path: &'static str, region: &'static str) {
    let duration_ms: f64 = (path.len() as f64) * 0.013;

    histogram!(
        parent: HTTP_REQUEST_DURATION,
        "method" => method,
        "path" => path,
    )
    .record(duration_ms);

    let response_size = (path.len() * 100) as f64;
    HTTP_RESPONSE_SIZE.record(response_size);

    HTTP_REQUESTS_TOTAL.increment(1);

    let status: &'static str = if path.starts_with("/err") {
        "500"
    } else {
        "200"
    };
    counter!(
        parent: HTTP_REQUESTS_BY_METHOD,
        "path" => path,
        "status" => status,
    )
    .increment(1);

    let conn_gauge = gauge!(
        parent: HTTP_ACTIVE_CONNECTIONS_BY_REGION,
        "method" => method,
    );
    {
        let _guard = GaugeGuard::from_gauge(&conn_gauge);
        _guard.increment(1.0);
    }

    println!("  [{region}] {method} {path} -> {status} ({duration_ms:.3}s)");
}

fn main() {
    println!("=== Installing Prometheus recorder ===");
    install_prometheus_recorder();
    println!();

    println!("=== Simulating HTTP traffic ===");
    let requests = [
        ("GET", "/api/users", "us-east-1"),
        ("POST", "/api/users", "us-east-1"),
        ("GET", "/api/items", "eu-west-1"),
        ("GET", "/err/timeout", "eu-west-1"),
        ("DELETE", "/api/users/42", "ap-south-1"),
    ];

    for &(method, path, region) in &requests {
        handle_request(method, path, region);
    }
    println!();

    println!("=== Labels<N> usage ===");
    for &(method, path, _) in &requests {
        let duration_ms: f64 = (path.len() as f64) * 0.013;
        let size: f64 = (path.len() * 100) as f64;
        record_request(method, path, duration_ms, size);
        println!("  record_request({method}, {path}, {duration_ms:.3}s, {size} bytes)");
    }
    record_dynamic_request("PUT".to_string(), "/api/upload".to_string(), 0.150);
    println!("  record_dynamic_request(PUT, /api/upload, 0.150s)");
    for &region in &["us-east-1", "eu-west-1", "ap-south-1"] {
        let _guard = track_connection(region);
        println!("  track_connection({region}) — guard active");
    }
    println!();

    println!("=== Gauge manipulation ===");
    HTTP_ACTIVE_CONNECTIONS.set(10.0);
    println!("  set active connections = 10");
    HTTP_ACTIVE_CONNECTIONS.increment(3.0);
    println!("  incremented by 3 -> 13");
    HTTP_ACTIVE_CONNECTIONS.decrement(5.0);
    println!("  decremented by 5 -> 8");
    println!();

    println!("=== Counter absolute ===");
    HTTP_REQUESTS_TOTAL.absolute(1_000_000);
    println!("  set quickwit_http_requests_total absolute = 1,000,000");
    println!();

    println!("Prometheus scrape endpoint: http://127.0.0.1:9000/metrics");
    println!("Press Ctrl+C to stop.");
    std::thread::park();
}
