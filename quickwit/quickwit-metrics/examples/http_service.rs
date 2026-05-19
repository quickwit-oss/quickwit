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

use metrics_exporter_prometheus::{Matcher, PrometheusBuilder};
use quickwit_metrics::*;

// ─── Helper ───

fn exponential_buckets(start: f64, factor: f64, count: usize) -> Vec<f64> {
    (0..count).map(|i| start * factor.powi(i as i32)).collect()
}

// ─── Metric definitions ───

static HTTP_REQUEST_DURATION: LazyHistogram = lazy_histogram!(
        name: "request_duration_seconds",
        description: "Time spent processing HTTP requests",
        subsystem: "http",
        buckets: exponential_buckets(0.005, 2.0, 12)
);

static HTTP_RESPONSE_SIZE: LazyHistogram = lazy_histogram!(
        name: "response_size_bytes",
        description: "Size of HTTP responses in bytes",
        subsystem: "http",
        buckets: vec![100.0, 1_000.0, 10_000.0, 100_000.0, 1_000_000.0]
);

static HTTP_REQUESTS_TOTAL: LazyCounter = lazy_counter!(
        name: "requests_total",
        description: "Total number of HTTP requests",
        subsystem: "http"
);

static HTTP_REQUESTS_BY_METHOD: LazyCounter = lazy_counter!(
        name: "requests_by_method_total",
        description: "HTTP requests broken down by method",
        subsystem: "http",
        "method" => "GET",
);

static HTTP_ACTIVE_CONNECTIONS: LazyGauge = lazy_gauge!(
        name: "active_connections",
        description: "Number of currently active HTTP connections",
        subsystem: "http"
);

static HTTP_ACTIVE_CONNECTIONS_BY_REGION: LazyGauge = lazy_gauge!(
        name: "active_connections",
        description: "Number of currently active HTTP connections",
        subsystem: "http",
        "region" => "us-east-1",
);

// ─── Custom system prefix ───
//
// Override the default system prefix ("quickwit") with a custom value.
// This produces metric names like "myapp_db_queries_total" instead of
// "quickwit_db_queries_total".

static DB_QUERIES_TOTAL: LazyCounter = lazy_counter!(
        name: "queries_total",
        description: "Total number of database queries",
        system: "myapp",
        subsystem: "db",
);

static DB_QUERY_DURATION: LazyHistogram = lazy_histogram!(
        name: "query_duration_seconds",
        description: "Time spent executing database queries",
        system: "myapp",
        subsystem: "db",
        buckets: vec![0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1.0],
);

static DB_CONNECTIONS: LazyGauge = lazy_gauge!(
        name: "connections",
        description: "Number of active database connections",
        system: "myapp",
        subsystem: "db",
);

// ─── Custom separator ───
//
// Override the default "_" separator with ".".
// This produces metric names like "myapp.http.requests_total".

static HTTP_REQUESTS_DOTTED: LazyCounter = lazy_counter!(
        name: "requests_total",
        description: "Total HTTP requests with dotted metric name",
        system: "myapp",
        subsystem: "http",
        separator: ".",
);

// ─── LabelNames<N> examples ───

const ROUTE_LABEL_NAMES: LabelNames<2> = label_names!("method", "path");

fn record_request(method: &'static str, path: &'static str, duration: f64, size: f64) {
    let route = label_values!(ROUTE_LABEL_NAMES => method, path);
    histogram!(parent: HTTP_REQUEST_DURATION, labels: [route]).observe(duration);
    histogram!(parent: HTTP_RESPONSE_SIZE, labels: [route]).observe(size);
    counter!(parent: HTTP_REQUESTS_TOTAL, labels: [route]).inc();
}

fn record_dynamic_request(method: String, path: String, duration: f64) {
    let route = label_values!(ROUTE_LABEL_NAMES => method, path);
    histogram!(parent: HTTP_REQUEST_DURATION, labels: [route]).observe(duration);
}

fn track_connection(region: &'static str) -> GaugeGuard {
    let g = gauge!(parent: HTTP_ACTIVE_CONNECTIONS, "region" => region);
    GaugeGuard::new(&g, 1.0)
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

    histogram!(parent: HTTP_REQUEST_DURATION, "method" => method, "path" => path)
        .observe(duration_ms);

    let response_size = (path.len() * 100) as f64;
    HTTP_RESPONSE_SIZE.observe(response_size);

    HTTP_REQUESTS_TOTAL.inc();

    let status: &'static str = if path.starts_with("/err") {
        "500"
    } else {
        "200"
    };
    counter!(parent: HTTP_REQUESTS_BY_METHOD, "path" => path, "status" => status).inc();

    let conn_gauge = gauge!(parent: HTTP_ACTIVE_CONNECTIONS_BY_REGION, "method" => method);
    {
        let _guard = GaugeGuard::new(&conn_gauge, 1.0);
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

    println!("=== LabelNames<N> usage ===");
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
    HTTP_ACTIVE_CONNECTIONS.inc_by(3.0);
    println!("  incremented by 3 -> 13");
    HTTP_ACTIVE_CONNECTIONS.dec_by(5.0);
    println!("  decremented by 5 -> 8");
    println!();

    println!("=== Counter absolute ===");
    HTTP_REQUESTS_TOTAL.absolute(1_000_000);
    println!("  set quickwit_http_requests_total absolute = 1,000,000");
    println!();

    println!("=== Custom system prefix ===");
    DB_QUERIES_TOTAL.inc();
    println!("  myapp_db_queries_total = {}", DB_QUERIES_TOTAL.get());
    DB_QUERY_DURATION.observe(0.042);
    println!("  myapp_db_query_duration_seconds observed 0.042");
    DB_CONNECTIONS.set(5.0);
    println!("  myapp_db_connections = {}", DB_CONNECTIONS.get());
    println!();

    println!("=== Custom separator ===");
    HTTP_REQUESTS_DOTTED.inc_by(3);
    println!(
        "  myapp.http.requests_total = {}",
        HTTP_REQUESTS_DOTTED.get()
    );
    println!();

    println!("Prometheus scrape endpoint: http://127.0.0.1:9000/metrics");
    println!("Press Ctrl+C to stop.");
    std::thread::park();
}
